use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict},
};

fn main() {
    Python::with_gil(|py| -> PyResult<()> {
        let locals = PyDict::new(py);
        py.run(
            r#"
import sys
from time import time
print(sys.path)
print(time())
print(sys.executable)
    "#,
            None,
            Some(locals),
        )
        .unwrap();
        Ok(())
    });
    println!("abczy", );
}
