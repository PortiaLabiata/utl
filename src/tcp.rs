use std::{io::ErrorKind, task::Poll};
use tokio::net::TcpStream;
use futures::{Stream, Sink};
use crate::frame::*;

pub struct EndpointTcp {
    stream: TcpStream,
    buff: Vec<u8>,
    pending_frame: Option<Frame>,
}

impl Stream for EndpointTcp {
    type Item = Result<Frame, Error>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let ret = this.stream.poll_read_ready(cx);

        if ret.is_pending() {
            return Poll::Pending;
        }

        let ret = this.stream.try_read(&mut this.buff);
        if ret.is_err() {
            return Poll::Ready(Some(Err(ret.unwrap_err())));
        }

        let data = &this.buff[..ret.unwrap()];

        return Poll::Ready(Some(Ok(Frame {
            data: bytes::Bytes::copy_from_slice(data),
        })))
    }
}

impl Sink<Frame> for EndpointTcp {
    type Error = Error;

    fn poll_ready(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.pending_frame.is_some() {
            return Poll::Ready(Err(Error::from(ErrorKind::ResourceBusy)));
        }

        return self.stream.poll_write_ready(cx);
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        self.get_mut().pending_frame = Some(item);
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        if this.pending_frame.is_none() {
            return Poll::Ready(Ok(()));
        }

        let data = &this.pending_frame
            .as_ref()
            .unwrap()
            .data;

        let ret = this.stream.try_write(data);
        if ret.is_err() {
            return Poll::Ready(Err(ret.unwrap_err())); 
        }

        return Poll::Ready(Ok(()));
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
