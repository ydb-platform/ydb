use pyo3::prelude::*;

#[derive(Clone)]
#[pyclass(module = "complexipy", get_all)]
pub struct FunctionComplexity {
    pub name: String,
    pub complexity: u64,
}

#[derive(Clone)]
#[pyclass(module = "complexipy", get_all)]
pub struct FileComplexity {
    pub path: String,
    pub file_name: String,
    pub functions: Vec<FunctionComplexity>,
    pub complexity: u64,
}
