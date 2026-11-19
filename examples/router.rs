use futures::{SinkExt, StreamExt};
use tokio::net::UdpSocket;
use utl::udp::EndpointUdp;

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let socka = UdpSocket::bind("127.0.0.1:25000").await?;
    let sockb = UdpSocket::bind("127.0.0.1:25001").await?;

    let mut epa = EndpointUdp::new(socka, 1500);
    let mut epb = EndpointUdp::new(sockb, 1500);

    loop {
        let (from_a, frame) = tokio::select! {
            f = epa.next() => (true, f),
            f = epb.next() => (false, f),
        };

        let frame = frame.unwrap();
        if let Err(e) = frame {
            eprintln!("Error: {:?}", e);
            continue;
        }

        let frame = frame.unwrap();

        let ep;
        if from_a {
            ep = &mut epb;
        } else {
            ep = &mut epa;
        }

        let _ = ep.feed(frame).await;
        let _ = ep.flush().await;
    }
}
