use dotenv::dotenv;
use futures::future::join_all;
use serde::Serialize;
use tracing_subscriber::util::SubscriberInitExt;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tracing::{debug, info};
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::EnvFilter;

const MODBUS_EXCETION_MODES: &[&str; 10] = &[
    "Unknown Exception",
    "ILLEGAL FUNCTION",
    "ILLEGAL DATA ADDRESS",
    "ILLEGAL DATA VALUE",
    "SLAVE DEVICE FAILURE",
    "ACKNOWLEDGE",
    "SLAVE DEVICE BUSY",
    "MEMORY PARITY ERROR",
    "GATEWAY PATH UNAVAILABLE",
    "GATEWAY TARGET DEVICE FAILED TO RESPOND",
];

#[derive(Serialize)]
struct Host {
    addr: SocketAddr,
    devices: Vec<Device>,
}

#[derive(Serialize, Clone)]
struct Device {
    exception_code: Option<u8>,
    identification: String,
    slave_id: Option<String>,
}

async fn connect_to_server(addr: &SocketAddr) -> Result<TcpStream, Box<dyn Error>> {
    let stream = TcpStream::connect(addr).await?;
    Ok(stream)
}

async fn send_modbus_request(
    stream: &mut TcpStream,
    sid: u8,
    function: u8,
    data: &[u8],
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut request = vec![0u8; 8 + data.len()];
    request[0..5].copy_from_slice(&[0, 0, 0, 0, 0]);
    request[5] = (2 + data.len()) as u8;
    request[6] = sid;
    request[7] = function;
    request[8..].copy_from_slice(data);

    stream.write_all(&request).await?;
    let mut response = vec![0u8; 256];
    let n = stream.read(&mut response).await?;
    response.truncate(n);
    Ok(response)
}

fn extract_slave_id<'a>(response: &[u8]) -> Option<String> {
    if response.len() < 9 {
        return None;
    }
    let byte_count = response[8] as usize;
    if response.len() < 9 + byte_count {
        return None;
    }
    let data = &response[9..9 + byte_count];
    Some(String::from_utf8_lossy(data).to_string())
}

fn discover_device_recursive<'a>(
    stream: &'a mut TcpStream,
    sid: u8,
    start_id: u8,
    objects: &'a mut Vec<(u8, String)>,
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + 'a>> {
    Box::pin(async move {
        let data = vec![0x0E, 0x01, start_id];
        let response = send_modbus_request(stream, sid, 0x2B, &data).await?;
        if response.len() >= 15 && response[7] == 0x2B {
            let more_follows = response[11];
            let next_object_id = response[12];
            let number_of_objects = response[13] as usize;

            let mut offset = 14;
            for _ in 0..number_of_objects {
                if offset + 2 > response.len() {
                    break;
                }
                let object_id = response[offset];
                let object_len = response[offset + 1] as usize;
                if offset + 2 + object_len > response.len() {
                    break;
                }
                let object_value =
                    String::from_utf8_lossy(&response[offset + 2..offset + 2 + object_len])
                        .to_string();
                objects.push((object_id, object_value));
                offset += 2 + object_len;
            }

            if more_follows == 0xFF && next_object_id != 0x00 {
                return discover_device_recursive(stream, sid, next_object_id, objects).await;
            }
        }
        Ok(())
    })
}

async fn discover_device(
    stream: &mut TcpStream,
    sid: u8,
) -> Result<Vec<(u8, String)>, Box<dyn Error>> {
    let mut objects = Vec::new();
    discover_device_recursive(stream, sid, 0x00, &mut objects).await?;
    Ok(objects)
}

async fn get_devices(addr: SocketAddr) -> Result<Host, Box<dyn Error>> {
    let mut devices: HashMap<String, Device> = HashMap::new();
    let mut parent: Option<Device> = None;
    info!("connecting to {addr}");
    let mut stream = connect_to_server(&addr).await?;
    for sid in 1..=246 {
        debug!("addr {addr} sid {sid}");
        let data = vec![];
        if let Ok(response) = timeout(
            Duration::from_millis(500),
            send_modbus_request(&mut stream, sid, 0x11, &data),
        )
        .await
        {
            if let Ok(response) = response {
                if response.len() >= 8 {
                    let slave_id_option = extract_slave_id(&response);
                    let objects: Vec<(u8, String)> = timeout(
                        Duration::from_millis(500),
                        discover_device(&mut stream, sid),
                    )
                    .await
                    .unwrap_or(Ok(Vec::new()))
                    .unwrap_or(Vec::new());
                    let mut exception_code = None;
                    let identification = objects
                        .iter()
                        .map(|device| device.1.clone())
                        .collect::<Vec<String>>()
                        .join(" ");
                    let return_code = response[8];
                    if return_code == 0x11 + 128 {
                        exception_code = Some(response[9]);
                        debug!(
                            "addr {addr} slave_id {slave_id_option:?} exception_code {}",
                            MODBUS_EXCETION_MODES[response[9] as usize]
                        );
                    }
                    match slave_id_option {
                        Some(slave_id) => {
                            if objects.is_empty() && exception_code.is_none() {
                                debug!("{sid} is empty",);
                            } else {
                                devices.insert(
                                    slave_id.clone(),
                                    Device {
                                        exception_code,
                                        identification,
                                        slave_id: Some(slave_id),
                                    },
                                );
                            }
                        }
                        None => {
                            if parent.is_none() {
                                if objects.is_empty() && exception_code.is_none() {
                                    debug!("{sid} is empty",);
                                } else {
                                    parent = Some(Device {
                                        exception_code,
                                        identification,
                                        slave_id: slave_id_option,
                                    })
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some(parent) = parent {
        if devices.len() == 0 {
            devices.insert("".to_string(), parent);
        }
    }
    match devices.len() {
        0 => info!("{addr} - no slaves detected"),
        1 => info!("{addr} - detected {} slave", devices.len()),
        _ => info!("{addr} - detected {} slaves", devices.len()),
    }

    Ok(Host {
        addr,
        devices: devices.values().cloned().collect(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(debug_assertions)]{
        dotenv().ok();
    }
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    SubscriberBuilder::default()
        .with_env_filter(env_filter)
        .init();
    info!("opening ips.txt");
    let file: File = match File::open("ips.txt").await {
        Ok(file) => file,
        Err(_) => {
            let mut file = File::create("ips.txt").await?;
            file.write_all(b"101.200.184.93:502\n120.77.166.219")
                .await?;
            println!("Update the ips.txt file.");
            let _ = file.flush();
            File::open("ips.txt").await?
        }
    };
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut addresses: Vec<SocketAddr> = Vec::new();
    while let Some(line) = lines.next_line().await.expect("Failed to read file") {
        let parts: Vec<&str> = line.split(":").collect();
        let ip: &str = parts.get(0).unwrap_or(&"127.0.0.1");
        let port_str: &str = parts.get(1).unwrap_or(&"502");
        let port: u16 = port_str.parse().unwrap();
        let address: SocketAddr = format!("{ip}:{port}").parse().unwrap();
        addresses.push(address);
    }
    let mut tasks = Vec::new();
    addresses
        .iter()
        .for_each(|addr| tasks.push(get_devices(*addr)));
    let results = join_all(tasks).await;
    let successful_results: Vec<Host> = results
        .into_iter()
        .filter_map(Result::ok)
        .filter(|element| !element.devices.is_empty())
        .collect();
    let json = serde_json::to_string(&successful_results).unwrap();
    info!("saving result.txt");
    let mut file = OpenOptions::new()
        .create(true)
        .append(false)
        .write(true)
        .truncate(true)
        .open("result.txt")
        .await
        .expect("Failed to open file");
    file.write(json.as_bytes())
        .await
        .expect("Couldn't save line");
    Ok(())
}
