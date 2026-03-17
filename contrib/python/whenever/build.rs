fn main() {
    for cfg in pyo3_build_config::get().build_script_outputs() {
        println!("{cfg}");
    }
}
