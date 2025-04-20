// Based on this h3's example code: https://github.com/hyperium/h3/blob/master/examples/server.rs

use std::{net::SocketAddr, sync::Arc};

use adbc_core::{Connection as _, Statement as _};
use bytes::Bytes;
use duckdb::RecordBatchBody;
use hyper::{server::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use tracing::{info, trace_span};

const SERVER_CERT: &[u8] = include_bytes!("cert/localhost+2.pem");
const SERVER_KEY: &[u8] = include_bytes!("cert/localhost+2-key.pem");
const ADDRESS: &str = "[::1]";
const PORT: &str = "443";

mod duckdb;

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

            tokio::task::spawn(async move {
                let io = match tls_acceptor.accept(stream).await {
                    Ok(tls_stream) => TokioIo::new(tls_stream),
                    Err(err) => {
                        eprintln!("failed to perform tls handshake: {err:#}");
                        return;
                    }
                };

                if let Err(err) = http2::Builder::new(TokioExecutor::new())
                    .serve_connection(
                        io,
                        service_fn(|req| async move {
                            if req.uri().path() != "/" {
                                return Ok::<_, std::convert::Infallible>(simple_error(404));
                            }

                            let conn = duckdb::get_duckdb_connection().unwrap();
                            let mut guard = conn.lock().await;

                            let mut stmt = match guard.new_statement() {
                                Ok(stmt) => stmt,
                                Err(e) => {
                                    info!("Error: {e:?}");
                                    return Ok::<_, std::convert::Infallible>(simple_error(503));
                                }
                            };

                            match stmt.set_sql_query("FROM 'tmp.csv'") {
                                Ok(_) => {}
                                Err(e) => {
                                    info!("Error: {e:?}");
                                    return Ok::<_, std::convert::Infallible>(simple_error(503));
                                }
                            };

                            let record_batch_reader = match stmt.execute() {
                                Ok(result) => result,
                                Err(e) => {
                                    info!("Error: {e:?}");
                                    return Ok::<_, std::convert::Infallible>(simple_error(503));
                                }
                            };

                            let mut result_bytes = 0usize;
                            let mut batches = Vec::new();
                            for b in record_batch_reader {
                                match b {
                                    Ok(b) => {
                                        result_bytes += b.get_array_memory_size();
                                        batches.push(b);
                                    }
                                    Err(e) => {
                                        info!("Error: {e:?}");
                                        return Ok::<_, std::convert::Infallible>(simple_error(
                                            503,
                                        ));
                                    }
                                }
                            }

                            let batches = batches.into_iter();

                            let body = RecordBatchBody {
                                result_bytes,
                                batches,
                            };

                            let response = http::Response::builder().body(body).unwrap();

                            Ok::<_, std::convert::Infallible>(response)
                        }),
                    )
                    .await
                {
                    eprintln!("Error serving connection: {}", err);
                }
            });
        }
    }
}

fn simple_error(status_code: u16) -> http::Response<RecordBatchBody> {
    http::Response::builder()
        .status(status_code)
        .body(RecordBatchBody::empty_body())
        .unwrap()
}
