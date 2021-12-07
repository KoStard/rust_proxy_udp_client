use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::net::{SocketAddr, UdpSocket};
use std::ops::Add;
use std::time::Duration;
use clap::{App, Arg};

const CONNECT_MESSAGE: &'static str = "Connect";
const ACCEPT_RESPONSE: &'static str = "Accept";
const REQUEST_PREFIX: &'static str = "GET:";
const BYE_MESSAGE: &'static str = "BYE";
const BYE_RESPONSE: &'static str = "BYE";
const REPEAT_REQUEST_PREFIX: &'static str = "REPEAT_BATCH:";
const BUFFER_SIZE: usize = 500;

fn main() {
    let app = App::new("UDP Client for the proxy server")
        .author("Ruben Kostandyan @KoStard")
        .about("Connects to the proxy server, sends the given \
                URL to it and receives the response back, \
                printing to the standard output")
        .arg(Arg::with_name("proxy-server")
            .short("p")
            .long("proxy-server")
            .help("The proxy server address")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("url")
            .long("url")
            .help("The target URL you are trying to read from with the proxy")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("target-file")
            .long("target-file")
            .short("f")
            .help("The target file to write the proxy server response into")
            .takes_value(true)
            .required(true))
        .get_matches();
    let proxy_server_address_raw = app.value_of("proxy-server").expect("Proxy server not provided");
    let url = app.value_of("url").expect("Destination URL not specified");
    let target_file_path = app.value_of("target-file").expect("Target file not specified");

    let proxy_server_address: SocketAddr = proxy_server_address_raw
        .parse()
        .expect("Couldn't parse the proxy address");
    let local_ip = [0,0,0,0,];
    let possible_addresses = [
        SocketAddr::from((local_ip, 5000)),
        SocketAddr::from((local_ip, 5001)),
        SocketAddr::from((local_ip, 5002)),
        SocketAddr::from((local_ip, 5003))
    ];
    let socket = UdpSocket::bind(&possible_addresses[..])
        .expect("Failed to bind to the UDP socket");
    // TODO increase in prod
    socket.set_read_timeout(Some(Duration::new(5 * 60, 0))).expect("Couldn't set the socket timeout");

    let mut connect_attempts = 0;
    loop {
        send_message(CONNECT_MESSAGE.to_owned(), &socket, &proxy_server_address);
        if let Ok(raw_response) = wait_for_response_with_attempts(&socket, &proxy_server_address) {
            let response = response_to_string(raw_response);
            assert_eq!(response, ACCEPT_RESPONSE);
            break;
        }
        println!("Sending connect again...");
        connect_attempts += 1;
        if connect_attempts > 5 {
            panic!("Couldn't connect to the proxy");
        }
    }

    let mut file = File::create(target_file_path).expect("Couldn't create the file");
    let mut request_send_attempts = 0;
    loop {
        send_message(generate_request_from_url(url), &socket, &proxy_server_address);
        if let Some(main_response) = poll_messages(&socket, &proxy_server_address) {
            file.write_all(main_response.as_slice()).expect("Couldn't write to the file");
            break;
        }
        println!("Sending the URL again...");
        request_send_attempts += 1;
        if request_send_attempts > 3 {
            panic!("Couldn't receive any response from the proxy");
        }
    }


    let mut bye_attempts = 0;
    loop {
        send_message(BYE_MESSAGE.to_owned(), &socket, &proxy_server_address);
        if let Ok(raw_response) = wait_for_response_with_attempts(&socket, &proxy_server_address) {
            assert_eq!(response_to_string(raw_response), BYE_RESPONSE);
            break;
        }
        bye_attempts += 1;
        if bye_attempts > 3 {
            panic!("Couldn't receive any bye from the proxy :(");
        }
    }
}

fn send_message(message: String, socket: &UdpSocket, destination: &SocketAddr) {
    println!("Sending {}, {}", message, destination);
    // Maybe we can retry in case of failures
    socket.send_to(message.as_bytes(), destination)
        .expect("Failed sending a message to the proxy");
}

fn generate_request_from_url(url: &str) -> String {
    String::from(REQUEST_PREFIX)
        .add(url)
}

fn wait_for_response_with_attempts<'a>(socket: &'a UdpSocket, proxy_addr: &'a SocketAddr) -> Result<Vec<u8>, String> {
    let mut current = 0;
    let max = 100;
    loop {
        if let Some(resp) = wait_for_response(socket, proxy_addr)? {
            return Ok(resp);
        } else {
            current += 1;
            if current > max {
                panic!("Couldn't receive message from the proxy, was getting many messages from someone else.");
            }
        }
    }
}

fn wait_for_response<'a>(socket: &'a UdpSocket, proxy_addr: &'a SocketAddr) -> Result<Option<Vec<u8>>, String> {
    let mut buffer = [0; BUFFER_SIZE];
    let (msg_length, message_source) = socket.recv_from(&mut buffer)
        .map_err(|e| format!("Failed receiving message from the socket: {}", e))?;
    println!("The message length was {}", msg_length);
    if !check_if_same_source(message_source, *proxy_addr) {
        println!("Received message not from the proxy {} {}", message_source, proxy_addr);
        Ok(None)
    } else {
        Ok(Some(buffer[..msg_length].to_vec()))
    }
}

fn check_if_same_source(a: SocketAddr, b: SocketAddr) -> bool {
    // TODO rethink how to solve the 0.0.0.0 vs 127.0.0.1 issue
    a == b || (a.port() == b.port() && (a.ip().is_loopback() || a.ip().is_unspecified()) && (b.ip().is_loopback()) || b.ip().is_unspecified())
}

fn response_to_string(content: Vec<u8>) -> String {
    String::from_utf8_lossy(content.as_slice()).to_string()
}

/// None means that couldn't
fn poll_messages(socket: &UdpSocket, destination: &SocketAddr) -> Option<Vec<u8>> {
    let mut received_batches: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut expected_overall_count = 0;
    let mut failures = 0;
    let failures_max = 50;
    let failures_max_for_first_batch = 10;

    loop {
        let current_block_res = wait_for_response_with_attempts(socket, destination);
        if let Err(_) = current_block_res {
            failures += 1;
            if expected_overall_count == 0 {
                if failures > failures_max_for_first_batch {
                    return None;
                }
                continue;
            }
            if failures > failures_max {
                panic!("Couldn't receive the message after {} retries", failures_max);
            }

            for i in 0..expected_overall_count {
                if !received_batches.contains_key(&i) {
                    println!("Requesting repeat for {}", i);
                    let repeat_message = String::from(REPEAT_REQUEST_PREFIX)
                        .add(&i.to_string());
                    send_message(repeat_message, socket, destination);
                }
            }
            continue;
        }
        let current_block = current_block_res.unwrap();
        let (current_index, overall_count, body) = process_the_custom_proxy_protocol(current_block.as_slice());
        if expected_overall_count != 0 && overall_count != expected_overall_count {
            panic!("Got different overall batches count in different batches...");
        }
        expected_overall_count = overall_count;
        if current_index > overall_count {
            panic!("Got chunk with bigger current index than the overall count of the batches, something went wrong...");
        }
        received_batches.insert(current_index, body.to_vec());

        println!("{} {}", received_batches.len(), overall_count);
        if received_batches.len() == overall_count.try_into().unwrap() {
            break;
        }
    }

    let mut overall_message = Vec::new();

    for i in 0..expected_overall_count {
        overall_message.extend(received_batches.get(&i).unwrap());
    }
    Some(overall_message)
}

/// The protocol is like this:
/// - 4 byte for the current index of batch
/// - 4 bytes for the overall count of batches
/// - the rest is the content of the current batch
fn process_the_custom_proxy_protocol(content: &[u8]) -> (u32, u32, &[u8]) {
    (u32::from_be_bytes([content[0], content[1], content[2], content[3]]),
     u32::from_be_bytes([content[4], content[5], content[6], content[7]]),
     &content[8..])
}
