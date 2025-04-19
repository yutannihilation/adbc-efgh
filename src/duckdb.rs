use std::sync::Arc;

use adbc_core::{Database as _, Driver as _, driver_manager::ManagedConnection};
use tokio::sync::Mutex;

#[cfg(target_os = "windows")]
pub(crate) const DUCKDB_DYLIB: &str = "duckdb/duckdb.dll";

#[cfg(target_os = "macos")]
pub(crate) const DUCKDB_DYLIB: &str = "duckdb/libduckdb.dylib";

#[cfg(target_os = "linux")]
pub(crate) const DUCKDB_DYLIB: &str = "duckdb/libduckdb.so";

#[derive(serde::Deserialize)]
pub(crate) struct RunQueryParams {
    // query: String,
}

pub(crate) fn get_duckdb_connection()
-> Result<Arc<Mutex<ManagedConnection>>, Box<dyn std::error::Error>> {
    match std::fs::exists(DUCKDB_DYLIB) {
        Ok(true) => {}
        Ok(false) => return Err("Please download the duckdb and copy it to {DUCKDB_DYLIB}".into()),
        Err(e) => return Err("Unexpected error: {e:?}".into()),
    }

    let mut driver = adbc_core::driver_manager::ManagedDriver::load_dynamic_from_filename(
        DUCKDB_DYLIB,
        Some(b"duckdb_adbc_init"),
        adbc_core::options::AdbcVersion::default(),
    )?;

    let mut db = driver.new_database()?;

    let conn = db.new_connection()?;
    Ok(Arc::new(Mutex::new(conn)))
}

struct RecordBatchBody<'a, T: arrow::record_batch::RecordBatchReader>{
    reader: T,
    buf: &'a mut [u8],
    writer: arrow_ipc::writer::StreamWriter<&'a mut Vec<u8>>,
}

impl<T: arrow::record_batch::RecordBatchReader> hyper::body::Body for RecordBatchBody<T> {
    type Data = bytes::Bytes;

    type Error = arrow::error::ArrowError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.reader.next() {
            Some(bytes) => {
                let mut bytes = 
                std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes))))
            }
            None => std::task::Poll::Ready(None),
        }
    }
}

struct DuckDBService {
    conn: Arc<ManagedConnection>,
}

impl hyper::service::Service<http::Request<hyper::body::Incoming>> for DuckDBService {
    type Response;

    type Error;

    type Future;

    fn call(&self, req: Request) -> Self::Future {
        todo!()
    }
}
