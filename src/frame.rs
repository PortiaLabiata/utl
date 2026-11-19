pub type Error = std::io::Error;

#[derive(Debug)]
pub struct FrameMeta {
    pub timestamp: std::time::Instant,
}

#[derive(Debug)]
pub struct Frame {
    pub data: bytes::Bytes,
}

pub trait Endpoint :
    futures::Stream<Item = Result<Frame, Error>>
    + futures::Sink<Frame, Error = Error>
    + Send
{}

impl<T> Endpoint for T where
    T: futures::Stream<Item = Result<Frame, Error>>
        + futures::Sink<Frame, Error = Error>
        + Send
{}

pub type BoxEndpoint = std::pin::Pin<Box<dyn Endpoint>>;
