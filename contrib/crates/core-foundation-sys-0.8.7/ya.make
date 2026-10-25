# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(core_foundation_sys)

CRATE_VERSION(0.8.7)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/array.rs
    src/attributed_string.rs
    src/bag.rs
    src/base.rs
    src/binary_heap.rs
    src/bit_vector.rs
    src/bundle.rs
    src/calendar.rs
    src/characterset.rs
    src/data.rs
    src/date.rs
    src/date_formatter.rs
    src/dictionary.rs
    src/error.rs
    src/file_security.rs
    src/filedescriptor.rs
    src/locale.rs
    src/mach_port.rs
    src/messageport.rs
    src/notification_center.rs
    src/number.rs
    src/number_formatter.rs
    src/plugin.rs
    src/preferences.rs
    src/propertylist.rs
    src/runloop.rs
    src/set.rs
    src/socket.rs
    src/stream.rs
    src/string.rs
    src/string_tokenizer.rs
    src/timezone.rs
    src/tree.rs
    src/url.rs
    src/url_enumerator.rs
    src/user_notification.rs
    src/uuid.rs
    src/xml_node.rs
    src/xml_parser.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    link
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/core-foundation-sys/core-foundation-sys-0.8.7.crate)

CRATE_SOURCES(
    src/array.rs
    src/attributed_string.rs
    src/bag.rs
    src/base.rs
    src/binary_heap.rs
    src/bit_vector.rs
    src/bundle.rs
    src/calendar.rs
    src/characterset.rs
    src/data.rs
    src/date.rs
    src/date_formatter.rs
    src/dictionary.rs
    src/error.rs
    src/file_security.rs
    src/filedescriptor.rs
    src/lib.rs
    src/locale.rs
    src/mach_port.rs
    src/messageport.rs
    src/notification_center.rs
    src/number.rs
    src/number_formatter.rs
    src/plugin.rs
    src/preferences.rs
    src/propertylist.rs
    src/runloop.rs
    src/set.rs
    src/socket.rs
    src/stream.rs
    src/string.rs
    src/string_tokenizer.rs
    src/timezone.rs
    src/tree.rs
    src/url.rs
    src/url_enumerator.rs
    src/user_notification.rs
    src/uuid.rs
    src/xml_node.rs
    src/xml_parser.rs
)

END()
