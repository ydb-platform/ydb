fn main() {
    println!("cargo::rustc-check-cfg=cfg(target_os, values(\"multics\"))");
}
