use futures::{SinkExt, StreamExt};
use tokio::net::UdpSocket;
use utl::udp::EndpointUdp;

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let sock = UdpSocket::bind("127.0.0.1:25000").await?;
    let mut ep = EndpointUdp::new(sock, 1500);

    loop {
        let dg = match ep.next().await {
            Some(v) => v,
            None => continue,
        };

        if dg.is_err() {
            panic!("Failed to read datagram: {:?}", dg.unwrap_err());
        }

        let data = dg.unwrap();
        println!("GOT: {:?}", data.data);

        ep.feed(data).await?;
        ep.flush().await?;
    }
}
