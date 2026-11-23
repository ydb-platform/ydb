# rust-asn1

[![Dependency Status][deps-rs-image]][deps-rs-link]
[![Documentation][docs-rs-image]][docs-rs-link]

This is a Rust library for parsing and generating ASN.1 data (DER only).

## Installation

Add `asn1` to the `[dependencies]` section of your `Cargo.toml`:

```toml
[dependencies]
asn1 = "0.15"
```

Builds on Rust 1.56.0 and newer.

`rust-asn1` is compatible with `#![no_std]` environments:

```toml
asn1 = { version = "0.15", default-features = false }
```

[deps-rs-image]: https://deps.rs/repo/github/alex/rust-asn1/status.svg
[deps-rs-link]: https://deps.rs/repo/github/alex/rust-asn1
[docs-rs-image]: https://docs.rs/asn1/badge.svg
[docs-rs-link]: https://docs.rs/asn1/
