use std::sync::{Arc, OnceLock};

use adbc_core::{
    Connection as _, Database as _, Driver as _, Statement as _, driver_manager::ManagedConnection,
};
use arrow::{
    array::{RecordBatch, RecordBatchReader},
    record_batch,
};
use arrow_ipc::writer::StreamWriter;
use tokio::sync::Mutex;

static DUCKDB_CONN: OnceLock<Arc<Mutex<ManagedConnection>>> = OnceLock::new();

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
    // get_init() cannot return error from inside the closure, so use get() and set().
    // This risks the double init, but assuming it's not critical (the second connection is just closed).
    // But, there might be better way...
    if let Some(conn) = DUCKDB_CONN.get() {
        return Ok(conn.clone());
    }

    match std::fs::exists(DUCKDB_DYLIB) {
        Ok(true) => {}
        Ok(false) => {
            return Err("Please download the duckdb and copy it to {DUCKDB_DYLIB}".into());
        }
        Err(e) => return Err("Unexpected error: {e:?}".into()),
    }

    let mut driver = adbc_core::driver_manager::ManagedDriver::load_dynamic_from_filename(
        DUCKDB_DYLIB,
        Some(b"duckdb_adbc_init"),
        adbc_core::options::AdbcVersion::default(),
    )?;

    let mut db = driver.new_database()?;

    let conn = Arc::new(Mutex::new(db.new_connection()?));
    match DUCKDB_CONN.set(conn.clone()) {
        Ok(_) => {}
        Err(_) => return Err("Failed to initialize the connection".into()),
    };

    Ok(conn)
}

pub struct RecordBatchBody {
    // batches: Vec<RecordBatch>,
    pub reader: Box<dyn RecordBatchReader + Send>,
}

impl hyper::body::Body for RecordBatchBody {
    type Data = bytes::Bytes;

    type Error = arrow::error::ArrowError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.reader.next() {
            Some(result) => {
                let batch = result?;

                // TODO: this is super inefficient to create a buffer and writer for every record batch, but let's start from here...
                let mut bytes: Vec<u8> = Vec::with_capacity(batch.get_array_memory_size());
                let mut writer = StreamWriter::try_new(&mut bytes, &*batch.schema()).unwrap();
                writer.write(&batch)?;
                drop(writer);

                std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes.into()))))
            }
            None => std::task::Poll::Ready(None),
        }
    }
}

struct DuckDBService {
    conn: Arc<Mutex<ManagedConnection>>,
}

// impl hyper::service::Service<http::Request<hyper::body::Incoming>> for DuckDBService {
//     type Response = http::Response<RecordBatchBody>;

//     type Error = std::convert::Infallible;

//     type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

//     fn call(&self, req: http::Request<hyper::body::Incoming>) -> Self::Future {
//         let fut = async move {
//             let mut guard = self.conn.lock().await;

//             let mut stmt = match guard.new_statement() {
//                 Ok(stmt) => stmt,
//                 Err(e) => todo!(),
//             };

//             match stmt.set_sql_query("FROM 'tmp.csv'") {
//                 Ok(_) => {}
//                 Err(e) => todo!(),
//             };

//             let record_batch_reader = match stmt.execute() {
//                 Ok(result) => result,
//                 Err(e) => todo!(),
//             };

//             let mut result_bytes = 0usize;
//             let mut batches = Vec::new();
//             for b in record_batch_reader {
//                 match b {
//                     Ok(b) => {
//                         result_bytes += b.get_array_memory_size();
//                         batches.push(b);
//                     }
//                     Err(e) => {
//                         todo!()
//                         // return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
//                     }
//                 }
//             }

//             let body = RecordBatchBody { batches };

//             Ok(http::Response::builder().body(body).unwrap())
//         };

//         Box::pin(fut)
//     }
// }
