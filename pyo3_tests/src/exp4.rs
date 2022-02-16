use pyo3::{prelude::*, types::PyDict};


fn main() {
    Python::with_gil(|py| {
        let locals = PyDict::new(py);
        let custom_code = r#"
import pandas as pd
import numpy as np
from time import time

start_t = time()
for i in range(1000000000):
    pass
print(time() - start_t)

        "#;
        py.run(custom_code, None, Some(locals)).unwrap();
    });
}
