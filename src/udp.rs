use crate::frame::*;
use tokio::{io::ReadBuf, net::UdpSocket};
use std::{net::SocketAddr, task::Poll, io::ErrorKind};
use futures::{Stream, Sink};

pub struct EndpointUdp {
    sock: UdpSocket,
    buff: Vec<u8>,
    peer: Option<SocketAddr>,
    pending_frame: Option<Frame>,
}

impl EndpointUdp {
    pub fn new(sock: UdpSocket, bufsize: usize) -> Self {
        let buff = vec![0; bufsize];
        let peer = None;
        let pending_frame = None;

        Self { sock, buff, peer, pending_frame }
    }
}

fn unwrap_poll<T>(p: Poll<T>) -> T {
    match p {
        Poll::Pending => panic!("Tried to unwrap pending poll"),
        Poll::Ready(v) => v,
    }
}

impl Stream for EndpointUdp {
    type Item = Result<Frame, Error>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let mut buf = ReadBuf::new(&mut this.buff);

            let ret = this.sock.poll_recv_from(cx, &mut buf);
            if ret.is_pending() {
                return Poll::Pending;
            }

            let res = unwrap_poll(ret);
            if let Err(e) = res {
                if e.kind() == ErrorKind::ConnectionReset {
                    continue;
                } else {
                    return Poll::Ready(Some(Err(e)));
                }
            }

            let addr = res.unwrap();
            this.peer = Some(addr);

            let data = buf.filled();

            return Poll::Ready(
                Some(Ok(Frame{
                    data: bytes::Bytes::copy_from_slice(data),
                }))
            );
        }
    }
}

impl Sink<Frame> for EndpointUdp {
    type Error = Error;

    fn poll_ready(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.peer.is_none() {
            return Poll::Ready(Err(Error::from(ErrorKind::NotConnected)));
        }

        if self.pending_frame.is_some() {
            return Poll::Ready(Err(Error::from(ErrorKind::ResourceBusy)));
        } else {
            return Poll::Ready(Ok(()));
        }
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        // Should not be called before poll_ready, so we overwrite pending frame 
        let this = self.get_mut();
        this.pending_frame = Some(item);
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        if this.pending_frame.is_none() {
            return Poll::Ready(Ok(()));
        } 

        let data = &this.pending_frame
            .as_ref()
            .unwrap()
            .data;

        let ret = this.sock.poll_send_to(cx, data, this.peer.unwrap())
            .map(|v| { 
                match v {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
            }
        });

        this.pending_frame = None;
        return ret;
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

use super::*;
    use std::assert_matches;

    #[tokio::test]
    async fn creation() {
        let sock = UdpSocket::bind("127.0.0.1:25000").await;
        assert_matches!(sock, Ok(_));

        let ep = EndpointUdp::new(sock.unwrap(), 128);
        assert_matches!(ep.pending_frame, None);
        assert_matches!(ep.peer, None);
    }

    #[tokio::test]
    async fn receiving() {
        let socka = UdpSocket::bind("127.0.0.1:25000").await;
        let sockb = UdpSocket::bind("127.0.0.1:25001").await;
        assert_matches!(socka, Ok(_));
        assert_matches!(sockb, Ok(_));

        let mut ep = EndpointUdp::new(socka.unwrap(), 128);
        assert_matches!(sockb.unwrap().send_to("Hello, world!".as_bytes(), "127.0.0.1:25000").await, Ok(_));
        let frame = ep.next().await;
        assert_eq!(frame.unwrap().unwrap().data, "Hello, world!".as_bytes());
    } 
}
