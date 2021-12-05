use std::collections::HashMap;
use std::io::Write;
use std::net::{SocketAddr, UdpSocket};
use std::ops::Add;
use clap::{App, Arg};

const CONNECT_MESSAGE: &'static str = "Connect";
const ACCEPT_RESPONSE: &'static str = "Accept";
const REQUEST_PREFIX: &'static str = "GET:";
const BYE_MESSAGE: &'static str = "BYE";
const BYE_RESPONSE: &'static str = "BYE";
const BUFFER_SIZE: usize = 10_000;

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
        .get_matches();
    let proxy_server_address_raw = app.value_of("proxy-server").expect("Proxy server not provided");
    let url = app.value_of("url").expect("Destination URL not specified");

    let proxy_server_address: SocketAddr = proxy_server_address_raw
        .parse()
        .expect("Couldn't parse the proxy address");
    let local_ip = [127, 0, 0, 1];
    let possible_addresses = [
        SocketAddr::from((local_ip, 5000)),
        SocketAddr::from((local_ip, 5001)),
        SocketAddr::from((local_ip, 5002)),
        SocketAddr::from((local_ip, 5003))
    ];
    let socket = UdpSocket::bind(&possible_addresses[..])
        .expect("Failed to bind to the UDP socket");

    send_message(CONNECT_MESSAGE.to_owned(), &socket, &proxy_server_address);
    let response = response_to_string(wait_for_response_with_attempts(&socket, &proxy_server_address));
    assert_eq!(response, ACCEPT_RESPONSE);

    send_message(generate_request_from_url(url), &socket, &proxy_server_address);

    let main_response = poll_messages(&socket, &proxy_server_address);
    std::io::stdout()
        .write(main_response.as_slice());

    send_message(BYE_MESSAGE.to_owned(), &socket, &proxy_server_address);
    assert_eq!(response_to_string(wait_for_response_with_attempts(&socket, &proxy_server_address)), BYE_RESPONSE);
}

fn send_message(message: String, socket: &UdpSocket, destination: &SocketAddr) {
    // Maybe we can retry in case of failures
    socket.send_to(message.as_bytes(), destination)
        .expect("Failed sending a message to the proxy");
}

fn generate_request_from_url(url: &str) -> String {
    String::from(REQUEST_PREFIX)
        .add(url)
}

fn wait_for_response_with_attempts<'a>(socket: &'a UdpSocket, proxy_addr: &'a SocketAddr) -> Vec<u8> {
    let mut current = 0;
    let max = 100;
    loop {
        if let Some(resp) = wait_for_response(socket, proxy_addr) {
            return resp;
        } else {
            current += 1;
            if current > max {
                panic!("Couldn't receive message from the proxy, was getting many messages from someone else.");
            }
        }
    }
}

fn wait_for_response<'a>(socket: &'a UdpSocket, proxy_addr: &'a SocketAddr) -> Option<Vec<u8>> {
    let mut buffer = [0; BUFFER_SIZE];
    // TODO if the server crashes, the client will block
    let (msg_length, message_source) = socket.recv_from(&mut buffer)
        .expect("Failed receiving from the proxy");
    if !check_if_same_source(message_source, *proxy_addr) {
        println!("Received message not from the proxy {} {}", message_source, proxy_addr);
        None
    } else {
        Some(buffer[..msg_length].to_vec())
    }
}

fn check_if_same_source(a: SocketAddr, b: SocketAddr) -> bool {
    // TODO rethink how to solve the 0.0.0.0 vs 127.0.0.1 issue
    a == b || (a.port() == b.port() && (a.ip().is_loopback() || a.ip().is_unspecified()) && (b.ip().is_loopback()) || b.ip().is_unspecified())
}

fn response_to_string(content: Vec<u8>) -> String {
    String::from_utf8_lossy(content.as_slice()).to_string()
}

fn poll_messages(socket: &UdpSocket, destination: &SocketAddr) -> Vec<u8> {
    let mut received_batches: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut expected_overall_count = 0;

    loop {
        let current_block = wait_for_response_with_attempts(socket, destination);
        let (current_index, overall_count, body) = process_the_custom_proxy_protocol(current_block.as_slice());
        if expected_overall_count != 0 && overall_count != expected_overall_count {
            panic!("Got different overall batches count in different batches...");
        }
        expected_overall_count = overall_count;
        if current_index > overall_count {
            panic!("Got chunk with bigger current index than the overall count of the batches, something went wrong...");
        }
        received_batches.insert(current_index, body.to_vec());

        if received_batches.len() == overall_count.try_into().unwrap() {
            break;
        }
    }

    let mut overall_message = Vec::new();

    for i in 0..expected_overall_count {
        overall_message.extend(received_batches.get(&i).unwrap());
    }
    overall_message
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
