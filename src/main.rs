// Based on this h3's example code: https://github.com/hyperium/h3/blob/master/examples/server.rs

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use http::StatusCode;
use hyper::{server::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::{io::AsyncReadExt, net::TcpListener};
use tokio_rustls::TlsAcceptor;

use tracing::{error, info, trace_span};

use h3::server::RequestResolver;
use h3_quinn::quinn::{self, crypto::rustls::QuicServerConfig};
use http_body_util::Full;

const SERVER_CERT: &[u8] = include_bytes!("cert/server.cert");
const SERVER_KEY: &[u8] = include_bytes!("cert/server.key");
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

    let cert = CertificateDer::from(SERVER_CERT);
    let key = PrivateKeyDer::try_from(SERVER_KEY)?;

    let mut tls_config_http3 = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    tls_config_http3.max_early_data_size = u32::MAX;

    let mut tls_config_http2 = tls_config_http3.clone();

    tls_config_http3.alpn_protocols = vec![b"h3".into()];
    let server_config_http3 =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls_config_http3)?));

    tls_config_http2.alpn_protocols = vec![b"h2".into()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config_http2));

    let address = format!("{ADDRESS}:{PORT}").parse()?;

    let endpoint_http3 = quinn::Endpoint::server(server_config_http3, address)?;
    let endpoint_http2 = TcpListener::bind(address).await?;

    info!("listening on {ADDRESS}");

    // handle incoming connections and requests

    loop {
        tokio::select! {
            Ok((stream, _)) = endpoint_http2.accept() => {

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
            // HTTP/3
            Some(new_conn) = endpoint_http3.accept() => {
                trace_span!("New HTTP/3 connection being attempted");

                tokio::spawn(async move {
                    match new_conn.await {
                        Ok(conn) => {
                            info!("new connection established");

                            let mut h3_conn = h3::server::Connection::new(h3_quinn::Connection::new(conn))
                                .await
                                .unwrap();

                            loop {
                                match h3_conn.accept().await {
                                    Ok(Some(resolver)) => {
                                        tokio::spawn(async {
                                            if let Err(e) = handle_http3_request(resolver).await {
                                                error!("handling request failed: {}", e);
                                            }
                                        });
                                    }
                                    // indicating that the remote sent a goaway frame
                                    // all requests have been processed
                                    Ok(None) => {
                                        break;
                                    }
                                    Err(err) => {
                                        error!("error on accept {}", err);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!("accepting connection failed: {:?}", err);
                        }
                    }
                });
            }
        }
    }

    // shut down gracefully
    // wait for connections to be closed before exiting
    endpoint_http3.wait_idle().await;

    Ok(())
}

async fn handle_http3_request<C>(
    resolver: RequestResolver<C, Bytes>,
) -> Result<(), Box<dyn std::error::Error>>
where
    C: h3::quic::Connection<Bytes>,
{
    let (req, mut stream) = resolver.resolve_request().await?;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .body(())
        .unwrap();

    match stream.send_response(resp).await {
        Ok(_) => {
            info!("successfully respond to connection");
        }
        Err(err) => {
            error!("unable to send response to connection peer: {:?}", err);
        }
    }

    let mut reader = tokio_util::io::StreamReader::new(tokio_stream::iter(vec![
        tokio::io::Result::Ok(Bytes::from_static(b"Hello, ")),
        tokio::io::Result::Ok(Bytes::from_static(b"world ")),
        tokio::io::Result::Ok(Bytes::from_static(b"of HTTP/3!")),
    ]));

    loop {
        let mut buf = BytesMut::with_capacity(4096 * 10);
        if reader.read_buf(&mut buf).await? == 0 {
            break;
        }
        stream.send_data(buf.freeze()).await?;
    }

    Ok(stream.finish().await?)
}

async fn handle_http2_request(
    _: http::Request<hyper::body::Incoming>,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    let response = http::Response::builder()
        .header("Alt-Svc", format!(r#"h3=":{PORT}"; ma=60"#))
        .body(Full::new(Bytes::from("Hello, world of HTTP/2!")))
        .unwrap();

    Ok(response)
}
