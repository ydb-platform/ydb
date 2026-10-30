# Native Rust build with dependencies shared through contrib/crates.

CRATE(webpki_roots)

CRATE_VERSION(1.0.11)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/rustls-pki-types-1.15.1
)

CRATE_DEPENDENCY_ALIAS(pki_types contrib/crates/rustls-pki-types-1.15.1)
# Trust anchors retain the shipped Community Data License Agreement.
LICENSE("CDLA-Permissive-2.0")
VERSION(1.0.11)
ORIGINAL_SOURCE(https://static.crates.io/crates/webpki-roots/webpki-roots-1.0.11.crate)

END()
