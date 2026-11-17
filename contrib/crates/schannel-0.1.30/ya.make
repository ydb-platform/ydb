# Native Rust build with dependencies shared through contrib/crates.

CRATE(schannel)

CRATE_VERSION(0.1.30)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/alpn_list.rs
    src/cert_chain.rs
    src/cert_context.rs
    src/cert_store.rs
    src/context_buffer.rs
    src/crypt_key.rs
    src/crypt_prov.rs
    src/ctl_context.rs
    src/key_handle.rs
    src/ncrypt_key.rs
    src/schannel_cred.rs
    src/security_context.rs
    src/test.rs
    src/tls_stream.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE.md
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/windows-sys-0.61.2
)

LICENSE("MIT")
VERSION(0.1.30)
ORIGINAL_SOURCE(https://static.crates.io/crates/schannel/schannel-0.1.30.crate)

END()
