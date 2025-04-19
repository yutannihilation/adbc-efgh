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
