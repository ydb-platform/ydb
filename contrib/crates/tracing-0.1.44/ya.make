# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tracing)

CRATE_VERSION(0.1.44)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/dispatcher.rs
    src/field.rs
    src/instrument.rs
    src/level_filters.rs
    src/macros.rs
    src/span.rs
    src/subscriber.rs
    test-macros/tests/debug.rs
    test-macros/tests/debug_n.rs
    test-macros/tests/debug_np.rs
    test-macros/tests/debug_nt.rs
    test-macros/tests/debug_ntp.rs
    test-macros/tests/debug_p.rs
    test-macros/tests/debug_t.rs
    test-macros/tests/debug_tp.rs
    test-macros/tests/error.rs
    test-macros/tests/error_n.rs
    test-macros/tests/error_np.rs
    test-macros/tests/error_nt.rs
    test-macros/tests/error_ntp.rs
    test-macros/tests/error_p.rs
    test-macros/tests/error_t.rs
    test-macros/tests/error_tp.rs
    test-macros/tests/event.rs
    test-macros/tests/event_n.rs
    test-macros/tests/event_np.rs
    test-macros/tests/event_nt.rs
    test-macros/tests/event_ntp.rs
    test-macros/tests/event_p.rs
    test-macros/tests/event_t.rs
    test-macros/tests/event_tp.rs
    test-macros/tests/info.rs
    test-macros/tests/info_n.rs
    test-macros/tests/info_np.rs
    test-macros/tests/info_nt.rs
    test-macros/tests/info_ntp.rs
    test-macros/tests/info_p.rs
    test-macros/tests/info_t.rs
    test-macros/tests/info_tp.rs
    test-macros/tests/trace.rs
    test-macros/tests/trace_n.rs
    test-macros/tests/trace_np.rs
    test-macros/tests/trace_nt.rs
    test-macros/tests/trace_ntp.rs
    test-macros/tests/trace_p.rs
    test-macros/tests/trace_t.rs
    test-macros/tests/trace_tp.rs
    test-macros/tests/warn.rs
    test-macros/tests/warn_n.rs
    test-macros/tests/warn_np.rs
    test-macros/tests/warn_nt.rs
    test-macros/tests/warn_ntp.rs
    test-macros/tests/warn_p.rs
    test-macros/tests/warn_t.rs
    test-macros/tests/warn_tp.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    test-macros/bin/macro-results.sh
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    attributes
    default
    std
    tracing-attributes
)

PEERDIR(
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/tracing-core-0.1.36
)

LICENSE("MIT")
VERSION(0.1.44)
ORIGINAL_SOURCE(https://static.crates.io/crates/tracing/tracing-0.1.44.crate)

CRATE_FEATURES(
    log
)

PEERDIR(
    contrib/crates/log-0.4.34
)

CRATE_FEATURES(
    valuable
)

CRATE_SOURCES(
    src/dispatcher.rs
    src/field.rs
    src/instrument.rs
    src/level_filters.rs
    src/lib.rs
    src/macros.rs
    src/span.rs
    src/subscriber.rs
)

CRATE_PROC_MACROS(
    contrib/crates/tracing-attributes-0.1.31
)

END()
