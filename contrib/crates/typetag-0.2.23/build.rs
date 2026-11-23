use std::env;
use std::fs;
use std::path::PathBuf;

const PRIVATE: &str = "\
#[doc(hidden)]
pub mod __private$$ {
    #[doc(hidden)]
    pub use crate::private::*;
}
";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let patch_version = env::var("CARGO_PKG_VERSION_PATCH").unwrap();
    let module = PRIVATE.replace("$$", &patch_version);
    fs::write(out_dir.join("private.rs"), module).unwrap();
}
