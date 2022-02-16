use pyo3::{prelude::*, py_run, types::PyDict};

fn transform_with_custom_code(custom_code: &str) {
    let field_transformer_code: &str = r#"

import hashlib
class FieldTransformer:
    def to_upper(self, value: str) -> str:
        return value.upper()
field_transformer = FieldTransformer()
    "#; 

    let run_code = format!("{}\n{}", field_transformer_code, custom_code);
    Python::with_gil(|py| {
        let locals = PyDict::new(py);
        py.run(&run_code, None, Some(locals)).unwrap();
        let output = locals.get_item("output").unwrap();
        println!("{}", output)
    });
}

fn main() {
    let custom_code = r#"
print(field_transformer)
input = "Hello"
print(input)
output = field_transformer.to_upper(input)
"#;
    transform_with_custom_code(custom_code);
}