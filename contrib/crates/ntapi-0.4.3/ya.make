# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(ntapi)

CRATE_VERSION(0.4.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/macros.rs
    src/ntapi_base.rs
    src/ntdbg.rs
    src/ntexapi.rs
    src/ntgdi.rs
    src/ntioapi.rs
    src/ntkeapi.rs
    src/ntldr.rs
    src/ntlpcapi.rs
    src/ntmisc.rs
    src/ntmmapi.rs
    src/ntnls.rs
    src/ntobapi.rs
    src/ntpebteb.rs
    src/ntpfapi.rs
    src/ntpnpapi.rs
    src/ntpoapi.rs
    src/ntpsapi.rs
    src/ntregapi.rs
    src/ntrtl.rs
    src/ntsam.rs
    src/ntseapi.rs
    src/ntsmss.rs
    src/nttmapi.rs
    src/nttp.rs
    src/ntwow64.rs
    src/ntxcapi.rs
    src/ntzwapi.rs
    src/string.rs
    src/subprocesstag.rs
    src/winapi_local.rs
    src/winapi_local/um.rs
    src/winapi_local/um/winioctl.rs
    src/winapi_local/um/winnt.rs
    src/winsta.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    user
)

PEERDIR(
    contrib/crates/winapi-0.3.9
)

CRATE_BUILD_SCRIPT(contrib/crates/ntapi-0.4.3/build_script)

LICENSE("Apache-2.0 OR MIT")
VERSION(0.4.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/ntapi/ntapi-0.4.3.crate)

CRATE_SOURCES(
    src/lib.rs
    src/macros.rs
    src/ntapi_base.rs
    src/ntdbg.rs
    src/ntexapi.rs
    src/ntgdi.rs
    src/ntioapi.rs
    src/ntkeapi.rs
    src/ntldr.rs
    src/ntlpcapi.rs
    src/ntmisc.rs
    src/ntmmapi.rs
    src/ntnls.rs
    src/ntobapi.rs
    src/ntpebteb.rs
    src/ntpfapi.rs
    src/ntpnpapi.rs
    src/ntpoapi.rs
    src/ntpsapi.rs
    src/ntregapi.rs
    src/ntrtl.rs
    src/ntsam.rs
    src/ntseapi.rs
    src/ntsmss.rs
    src/nttmapi.rs
    src/nttp.rs
    src/ntwow64.rs
    src/ntxcapi.rs
    src/ntzwapi.rs
    src/string.rs
    src/subprocesstag.rs
    src/winapi_local.rs
    src/winapi_local/um.rs
    src/winapi_local/um/winioctl.rs
    src/winapi_local/um/winnt.rs
    src/winsta.rs
)

END()

RECURSE(build_script)
