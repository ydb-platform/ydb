# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(bitflags)

CRATE_VERSION(2.13.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/example_generated.rs
    src/external.rs
    src/external/arbitrary.rs
    src/external/bytemuck.rs
    src/external/serde.rs
    src/internal.rs
    src/iter.rs
    src/parser.rs
    src/public.rs
    src/tests.rs
    src/tests/all.rs
    src/tests/all_named.rs
    src/tests/bitflags_match.rs
    src/tests/bits.rs
    src/tests/clear.rs
    src/tests/complement.rs
    src/tests/contains.rs
    src/tests/difference.rs
    src/tests/empty.rs
    src/tests/eq.rs
    src/tests/extend.rs
    src/tests/flag_name.rs
    src/tests/flags.rs
    src/tests/fmt.rs
    src/tests/from_bits.rs
    src/tests/from_bits_retain.rs
    src/tests/from_bits_truncate.rs
    src/tests/from_name.rs
    src/tests/insert.rs
    src/tests/intersection.rs
    src/tests/intersects.rs
    src/tests/is_all.rs
    src/tests/is_empty.rs
    src/tests/iter.rs
    src/tests/iter_equal_names.rs
    src/tests/known_bits.rs
    src/tests/parser.rs
    src/tests/remove.rs
    src/tests/symmetric_difference.rs
    src/tests/truncate.rs
    src/tests/union.rs
    src/tests/unknown.rs
    src/tests/unknown_bits.rs
    src/traits.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    CODE_OF_CONDUCT.md
    CONTRIBUTING.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    SECURITY.md
    spec.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(2.13.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/bitflags/bitflags-2.13.1.crate)

END()
