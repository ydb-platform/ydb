# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(chrono)

CRATE_VERSION(0.4.45)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/date.rs
    src/datetime/mod.rs
    src/datetime/serde.rs
    src/datetime/tests.rs
    src/format/formatting.rs
    src/format/locales.rs
    src/format/mod.rs
    src/format/parse.rs
    src/format/parsed.rs
    src/format/scan.rs
    src/format/strftime.rs
    src/month.rs
    src/naive/date/mod.rs
    src/naive/date/tests.rs
    src/naive/datetime/mod.rs
    src/naive/datetime/serde.rs
    src/naive/datetime/tests.rs
    src/naive/internals.rs
    src/naive/isoweek.rs
    src/naive/mod.rs
    src/naive/time/mod.rs
    src/naive/time/serde.rs
    src/naive/time/tests.rs
    src/offset/fixed.rs
    src/offset/local/mod.rs
    src/offset/local/tz_data.rs
    src/offset/local/tz_info/mod.rs
    src/offset/local/tz_info/parser.rs
    src/offset/local/tz_info/rule.rs
    src/offset/local/tz_info/timezone.rs
    src/offset/local/unix.rs
    src/offset/local/win_bindings.rs
    src/offset/local/windows.rs
    src/offset/mod.rs
    src/offset/utc.rs
    src/round.rs
    src/time_delta.rs
    src/traits.rs
    src/weekday.rs
    src/weekday_set.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CITATION.cff
    Cargo.lock
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    serde
)

PEERDIR(
    contrib/crates/num-traits-0.2.19
    contrib/crates/serde-1.0.229
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.45)
ORIGINAL_SOURCE(https://static.crates.io/crates/chrono/chrono-0.4.45.crate)

CRATE_FEATURES(
    clock
    default
    iana-time-zone
    js-sys
    now
    oldtime
    std
    wasm-bindgen
    wasmbind
    winapi
    windows-link
)

IF (OS_DARWIN AND ARCH_ARM64)
    PEERDIR(
    contrib/crates/iana-time-zone-0.1.65
)

ENDIF()

IF (OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL)
    PEERDIR(
    contrib/crates/iana-time-zone-0.1.65
)

ENDIF()

IF (OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/iana-time-zone-0.1.65
)

ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/iana-time-zone-0.1.65
)

ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/windows-link-0.2.1
)

ENDIF()

IF (OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL)
    PEERDIR(
    contrib/crates/iana-time-zone-0.1.65
)

ENDIF()

IF (OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/iana-time-zone-0.1.65
)

ENDIF()

CRATE_SOURCES(
    src/offset/local/unix.rs
    src/offset/local/windows.rs
)

CRATE_SOURCES(
    src/date.rs
    src/datetime/mod.rs
    src/datetime/serde.rs
    src/datetime/tests.rs
    src/format/formatting.rs
    src/format/locales.rs
    src/format/mod.rs
    src/format/parse.rs
    src/format/parsed.rs
    src/format/scan.rs
    src/format/strftime.rs
    src/lib.rs
    src/month.rs
    src/naive/date/mod.rs
    src/naive/date/tests.rs
    src/naive/datetime/mod.rs
    src/naive/datetime/serde.rs
    src/naive/datetime/tests.rs
    src/naive/internals.rs
    src/naive/isoweek.rs
    src/naive/mod.rs
    src/naive/time/mod.rs
    src/naive/time/serde.rs
    src/naive/time/tests.rs
    src/offset/fixed.rs
    src/offset/local/mod.rs
    src/offset/local/tz_data.rs
    src/offset/local/tz_info/mod.rs
    src/offset/local/tz_info/parser.rs
    src/offset/local/tz_info/rule.rs
    src/offset/local/tz_info/timezone.rs
    src/offset/local/win_bindings.rs
    src/offset/mod.rs
    src/offset/utc.rs
    src/round.rs
    src/time_delta.rs
    src/traits.rs
    src/weekday.rs
    src/weekday_set.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
