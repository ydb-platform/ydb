fn main() {
    if std::env::var("DOCS_RS").is_ok() {
        // prevent linking libraries to avoid documentation failure
        return;
    }

    if let Err(s) = system_deps::Config::new().probe() {
        println!("cargo:warning={s}");
        std::process::exit(1);
    }
}
