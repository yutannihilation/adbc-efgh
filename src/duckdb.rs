use std::{
    sync::{Arc, OnceLock},
    vec::IntoIter,
};

use adbc_core::{Database as _, Driver as _, driver_manager::ManagedConnection};
use arrow::array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use ringbuf::{
    HeapCons, HeapProd, HeapRb,
    traits::{Consumer, Split},
};
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
            return Err(format!("Please download the duckdb and copy it to {DUCKDB_DYLIB}").into());
        }
        Err(e) => return Err(format!("Unexpected error: {e:?}").into()),
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
    // pub reader: Box<dyn RecordBatchReader + Send>,
    pub writer: StreamWriter<HeapProd<u8>>,
    pub reader: HeapCons<u8>,
    pub batches: IntoIter<RecordBatch>,
}

impl RecordBatchBody {
    pub fn empty_body() -> Self {
        todo!()
        // Self {
        //     buf: vec![],
        //     batches: vec![].into_iter(),
        // }
    }

    pub fn new(buffer_size: usize, batches: Vec<RecordBatch>) -> Self {
        let schema = &*batches[0].schema();
        let batches = batches.into_iter();

        let rb = Arc::new(HeapRb::<u8>::new(buffer_size));
        let (prod, cons) = rb.split();

        let writer = StreamWriter::try_new(prod, schema).unwrap();

        Self {
            writer,
            reader: cons,
            batches,
        }
    }
}

impl hyper::body::Body for RecordBatchBody {
    type Data = bytes::Bytes;

    type Error = arrow::error::ArrowError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.batches.next() {
            Some(batch) => {
                let size = batch.get_array_memory_size();

                // Assuming this isn't buffered, so no flush is needed.
                self.writer.write(&batch)?;

                let (left, right) = self.reader.as_slices();
                let bytes = if left.len() >= size {
                    bytes::Bytes::copy_from_slice(&left[..size])
                } else {
                    let size_rest = size - left.len();
                    let both = [left, &right[..size_rest]].concat();
                    bytes::Bytes::from(both)
                };

                unsafe {
                    self.reader.advance_read_index(size);
                }

                std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes))))
            }
            None => std::task::Poll::Ready(None),
        }
    }
}
