# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(objc2_io_kit)

CRATE_VERSION(0.3.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/consumes_argument.rs
    src/generated/graphics.rs
    src/generated/hid.rs
    src/generated/hidsystem.rs
    src/generated/mod.rs
    src/generated/network.rs
    src/generated/ps.rs
    src/generated/pwr_mgt.rs
    src/generated/serial.rs
    src/generated/usb/AppleUSBDefinitions.rs
    src/generated/usb/IOUSBHostFamilyDefinitions.rs
    src/generated/usb/IOUSBLib.rs
    src/generated/usb/USB.rs
    src/generated/usb/USBSpec.rs
    src/generated/usb/mod.rs
    src/macros.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    README.md
    translation-config.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    libc
    std
)

PEERDIR(
    contrib/crates/libc-0.2.191
    contrib/crates/objc2-core-foundation-0.3.2
)

LICENSE("Zlib OR Apache-2.0 OR MIT")
VERSION(0.3.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/objc2-io-kit/objc2-io-kit-0.3.2.crate)

CRATE_SOURCES(
    src/generated/graphics.rs
    src/generated/hid.rs
    src/generated/hidsystem.rs
    src/generated/network.rs
    src/generated/ps.rs
    src/generated/pwr_mgt.rs
    src/generated/serial.rs
    src/generated/usb/AppleUSBDefinitions.rs
    src/generated/usb/IOUSBHostFamilyDefinitions.rs
    src/generated/usb/IOUSBLib.rs
    src/generated/usb/USB.rs
    src/generated/usb/USBSpec.rs
    src/generated/usb/mod.rs
)

CRATE_SOURCES(
    src/consumes_argument.rs
    src/generated/mod.rs
    src/lib.rs
    src/macros.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
