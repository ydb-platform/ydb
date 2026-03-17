extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let changed = cbindgen::Builder::new()
        .with_cpp_compat(true)
        .with_crate(crate_dir)
        .with_pragma_once(true)
        .with_language(cbindgen::Language::C)
        .with_item_prefix("TemporalCore")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("include/temporal-sdk-core-c-bridge.h");

    // If this changed and an env var disallows change, error
    if let Ok(env_val) = env::var("TEMPORAL_SDK_BRIDGE_DISABLE_HEADER_CHANGE")
        && changed
        && env_val == "true"
    {
        println!("cargo:warning=bridge's header file changed unexpectedly from what's on disk");
        std::process::exit(1);
    }
}
