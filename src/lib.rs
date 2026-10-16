use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use parking_lot::RwLock;
use pyo3::exceptions::{PyKeyError, PyIOError};
use pyo3::prelude::*;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::io::{BufReader, BufWriter, Write};


#[derive(Serialize, Deserialize)]
enum LogOp {
    Set(String, Vec<u8>),
    Delete(String),
}

#[pyclass]
struct MiniKVS {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    wal_writer: Arc<parking_lot::Mutex<BufWriter<File>>>,
}

#[pymethods]
impl MiniKVS {
    #[new]
    fn new(filepath: String) -> PyResult<Self> {
        let path = PathBuf::from(filepath);
        let mut map = HashMap::new();

        if path.exists() {
            let file = File::open(&path).map_err(|e| PyIOError::new_err(e.to_string()))?;
            let mut reader = BufReader::new(file);
            while let Ok(op) = bincode::deserialize_from::<_, LogOp>(&mut reader) {
                match op {
                    LogOp::Set(k, v) => { map.insert(k, v); }
                    LogOp::Delete(k) => { map.remove(&k); }
                }
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(MiniKVS {
            data: Arc::new(RwLock::new(map)),
            wal_writer: Arc::new(parking_lot::Mutex::new(BufWriter::new(file))),
        })
    }

    fn set(&self, py: Python<'_>, key: String, value: Vec<u8>) -> PyResult<()> {
        py.allow_threads(|| {
            let op = LogOp::Set(key.clone(), value.clone());
            let encoded = bincode::serialize(&op).map_err(|e| PyIOError::new_err(e.to_string()))?;
            
            let mut writer = self.wal_writer.lock();
            writer.write_all(&encoded).map_err(|e| PyIOError::new_err(e.to_string()))?;
            writer.flush().map_err(|e| PyIOError::new_err(e.to_string()))?;

            self.data.write().insert(key, value);
            Ok(())
        })
    }

    /// Retrieve a value by key. Releases GIL for concurrent reads.
    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Vec<u8>> {
        py.allow_threads(|| {
            let reader = self.data.read();
            match reader.get(key) {
                Some(val) => Ok(val.clone()),
                None => Err(PyKeyError::new_err(format!("Key not found: {key}"))),
            }
        })
    }

    /// Delete a key
    fn delete(&self, py: Python<'_>, key: String) -> PyResult<()> {
        py.allow_threads(|| {
            let op = LogOp::Delete(key.clone());
            let encoded = bincode::serialize(&op).map_err(|e| PyIOError::new_err(e.to_string()))?;
            
            let mut writer = self.wal_writer.lock();
            writer.write_all(&encoded).map_err(|e| PyIOError::new_err(e.to_string()))?;
            writer.flush().map_err(|e| PyIOError::new_err(e.to_string()))?;

            let mut write_guard = self.data.write();
            if write_guard.remove(&key).is_none() {
                return Err(PyKeyError::new_err(format!("Key not found: {key}")));
            }
            Ok(())
        })
    }

    // Python Dict-like Protocol
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Vec<u8>> {
        self.get(py, key)
    }

    fn __setitem__(&self, py: Python<'_>, key: String, value: Vec<u8>) -> PyResult<()> {
        self.set(py, key, value)
    }

    fn __delitem__(&self, py: Python<'_>, key: String) -> PyResult<()> {
        self.delete(py, key)
    }

    fn __contains__(&self, key: &str) -> bool {
        self.data.read().contains_key(key)
    }

    fn __len__(&self) -> usize {
        self.data.read().len()
    }
}


#[pymodule]
fn mini_kvs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MiniKVS>()?;
    Ok(())
}





