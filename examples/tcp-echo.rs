use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use utl::tcp::EndpointTcp;

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let server = TcpListener::bind("127.0.0.1:25000").await?;

    loop {
        let (conn, addr) = server.accept().await?;
        println!("Inbound connection from {:?}", addr);

        tokio::spawn(async move {
            let mut ep = EndpointTcp::new(conn, 1500);            

            loop {
                let ret = ep.next().await.unwrap();
                if let Err(e) = ret {
                    eprintln!("Error: {:?}", e);
                    continue;
                }

                let ret = ret.unwrap();

                println!("GOT: {:?}", ret.data);
                let _ = ep.feed(ret).await;
                let _ = ep.flush().await;
            }
        });
    }
}
