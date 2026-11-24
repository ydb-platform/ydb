fn main() {
    // Avoid unnecessary re-building.
    println!("cargo:rerun-if-changed=build.rs");

    // Quiet rustc's helpful lint.  reprocheck is defined in CI.
    println!("cargo:rustc-check-cfg=cfg(reprocheck)");
}
