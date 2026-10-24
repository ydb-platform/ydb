//! Antithesis SDK integration for invariant testing.
//!
//! This module provides assertion macros that integrate with the Antithesis
//! testing platform to detect invariant violations during fuzz testing.

use std::sync::OnceLock;

/// Ensure Antithesis is initialized exactly once.
pub(crate) fn ensure_init() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        ::antithesis_sdk::antithesis_init();
    });
}

/// Assert that a condition is always true during Antithesis fuzz testing.
/// Use `false` as the condition to log an invariant violation.
macro_rules! assert_always {
    ($condition:expr, $message:literal, $details:expr) => {{
        $crate::antithesis::ensure_init();
        let details: ::serde_json::Value = $details;
        ::antithesis_sdk::assert_always!($condition, $message, &details);
    }};
    ($condition:expr, $message:literal) => {{
        $crate::antithesis::ensure_init();
        ::antithesis_sdk::assert_always!($condition, $message);
    }};
}

/// Assert that a condition is sometimes true during Antithesis fuzz testing.
/// This checks that the condition occurs at least once across the entire test session.
macro_rules! assert_sometimes {
    ($condition:expr, $message:literal, $details:expr) => {{
        $crate::antithesis::ensure_init();
        let details: ::serde_json::Value = $details;
        ::antithesis_sdk::assert_sometimes!($condition, $message, &details);
    }};
    ($condition:expr, $message:literal) => {{
        $crate::antithesis::ensure_init();
        ::antithesis_sdk::assert_sometimes!($condition, $message);
    }};
}

/// Assert that a code location is unreachable during Antithesis fuzz testing.
/// Use this for code paths that should never be reached (bugs, invariant violations).
macro_rules! assert_unreachable {
    ($message:literal, $details:expr) => {{
        $crate::antithesis::ensure_init();
        let details: ::serde_json::Value = $details;
        ::antithesis_sdk::assert_unreachable!($message, &details);
    }};
    ($message:literal) => {{
        $crate::antithesis::ensure_init();
        ::antithesis_sdk::assert_unreachable!($message);
    }};
}

pub(crate) use assert_always;
pub(crate) use assert_sometimes;
pub(crate) use assert_unreachable;
