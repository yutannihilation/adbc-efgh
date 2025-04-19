// Based on this h3's example code: https://github.com/hyperium/h3/blob/master/examples/server.rs

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use hyper::{server::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use tracing::{info, trace_span};

use http_body_util::{Full, StreamBody};

const SERVER_CERT: &[u8] = include_bytes!("cert/localhost+2.pem");
const SERVER_KEY: &[u8] = include_bytes!("cert/localhost+2-key.pem");
const ADDRESS: &str = "[::1]";
const PORT: &str = "443";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .init();

    let cert = CertificateDer::from_pem_slice(SERVER_CERT).unwrap();
    let key = PrivateKeyDer::from_pem_slice(SERVER_KEY)?;

    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    tls_config.max_early_data_size = u32::MAX;
    tls_config.alpn_protocols = vec![b"h2".into()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

    let address: SocketAddr = format!("{ADDRESS}:{PORT}").parse()?;
    let endpoint_http2 = TcpListener::bind(address).await?;

    info!("listening on {ADDRESS}:{PORT}");

    // handle incoming connections and requests

    loop {
        while let Ok((stream, _)) = endpoint_http2.accept().await {
            let tls_acceptor = tls_acceptor.clone();

            trace_span!("New HTTP/2 connection being attempted");

            // handle_http2_request
            tokio::task::spawn(async move {
                let io = match tls_acceptor.accept(stream).await {
                    Ok(tls_stream) => TokioIo::new(tls_stream),
                    Err(err) => {
                        eprintln!("failed to perform tls handshake: {err:#}");
                        return;
                    }
                };

                // Handle the connection from the client using HTTP/2 with an executor and pass any
                // HTTP requests received on that connection to the `hello` function
                if let Err(err) = http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service_fn(handle_http2_request))
                    .await
                {
                    eprintln!("Error serving connection: {}", err);
                }
            });
        }
    }
}

struct Hello<I: Iterator<Item = Bytes>> {
    msg: I,
}

// In order to use self.msg as ref mut, this is necessary.
impl<I: Iterator<Item = Bytes>> Unpin for Hello<I> {}

impl<I: Iterator<Item = Bytes>> hyper::body::Body for Hello<I> {
    type Data = Bytes;

    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.msg.next() {
            Some(bytes) => std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes)))),
            None => std::task::Poll::Ready(None),
        }
    }
}

impl<I: Iterator<Item = Bytes>> Hello<I> {
    fn new(msg: I) -> Self {
        Self { msg }
    }
}

async fn handle_http2_request(
    _: http::Request<hyper::body::Incoming>,
) -> Result<http::Response<Hello<std::vec::IntoIter<bytes::Bytes>>>, std::convert::Infallible> {
    let body = Hello::new(
        vec![
            Bytes::from_static(b"Hello, "),
            Bytes::from_static(b"world "),
            Bytes::from_static(b"of HTTP/2!"),
        ]
        .into_iter(),
    );

    let response = http::Response::builder()
        .body(body)
        // .body(Full::new(Bytes::from("Hello, world of HTTP/2!")))
        .unwrap();

    Ok(response)
}
