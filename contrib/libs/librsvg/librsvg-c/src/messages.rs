//! Logging functions, plus Rust versions of `g_return_if_fail()`.
//!
//! Glib's `g_return_if_fail()`, `g_warning()`, etc. are all C macros, so they cannot be
//! used from Rust.  This module defines equivalent functions or macros with an `rsvg_`
//! prefix, to be clear that they should only be used from the implementation of the C API
//! and not from the main Rust code of the library.

use glib::ffi::{G_LOG_LEVEL_CRITICAL, G_LOG_LEVEL_WARNING, GLogField, g_log_structured_array};
use glib::translate::*;

/*
  G_LOG_LEVEL_CRITICAL          = 1 << 3,
  G_LOG_LEVEL_WARNING           = 1 << 4,

#define g_critical(...) g_log_structured_standard (G_LOG_DOMAIN, G_LOG_LEVEL_CRITICAL, \
                                                   __FILE__, G_STRINGIFY (__LINE__), \
                                                   G_STRFUNC, __VA_ARGS__)
#define g_warning(...)  g_log_structured_standard (G_LOG_DOMAIN, G_LOG_LEVEL_WARNING, \
                                                   __FILE__, G_STRINGIFY (__LINE__), \
                                                   G_STRFUNC, __VA_ARGS__)
  GLogField fields[] =
    {
      { "PRIORITY", log_level_to_priority (log_level), -1 },
      { "CODE_FILE", file, -1 },
      { "CODE_LINE", line, -1 },
      { "CODE_FUNC", func, -1 },
      /* Filled in later: */
      { "MESSAGE", NULL, -1 },
      /* If @log_domain is %NULL, we will not pass this field: */
      { "GLIB_DOMAIN", log_domain, -1 },
    };

  g_log_structured_array (log_level, fields, n_fields);
 */

/// Helper function for converting string literals to C char pointers.
#[macro_export]
macro_rules! rsvg_c_str {
    ($txt:expr) => {
        // SAFETY: it's important that the type we pass to `from_bytes_with_nul` is 'static,
        // so that the storage behind the returned pointer doesn't get freed while it's still
        // being used. We get that by only allowing string literals.
        std::ffi::CStr::from_bytes_with_nul(concat!($txt, "\0").as_bytes())
            .unwrap()
            .as_ptr()
    };
}

/// Helper for `rsvg_g_warning` and `rsvg_g_critical`
///
/// This simulates what in C would be a call to the g_warning() or g_critical()
/// macros, but with the underlying function g_log_structured_array().
///
/// If the implementation of g_warning() or g_critical() changes, we'll have
/// to change this function.
fn rsvg_g_log(level: glib::ffi::GLogLevelFlags, msg: &str) {
    // stolen from gmessages.c:log_level_to_priority()
    let priority = match level {
        G_LOG_LEVEL_WARNING | G_LOG_LEVEL_CRITICAL => c"4".as_ptr(),
        _ => unreachable!("please add another log level priority to rsvg_g_log()"),
    };

    let c_msg = msg.to_glib_none();
    let c_char_msg: *const libc::c_char = c_msg.0;

    // Glib's g_log_structured_standard() adds a few more fields for the source
    // file, line number, etc., but those are not terribly useful without a stack
    // trace.  So, we'll omit them.
    let fields = [
        GLogField {
            key: c"PRIORITY".as_ptr(),
            value: priority.cast(),
            length: -1,
        },
        GLogField {
            key: c"MESSAGE".as_ptr(),
            value: c_char_msg.cast(),
            length: msg.len() as _,
        },
        // This is the G_LOG_DOMAIN set from the Makefile
        GLogField {
            key: c"GLIB_DOMAIN".as_ptr(),
            value: c"librsvg".as_ptr().cast(),
            length: -1,
        },
    ];

    unsafe {
        g_log_structured_array(level, fields.as_ptr(), fields.len());
    }
}

/// Replacement for `g_warning()`.
///
/// Use this to signal an error condition in the following situations:
///
/// * The C API does not have an adequate error code for the error in question (and cannot
///   be changed to have one, for ABI compatibility reasons).
///
/// * Applications using the C API would be prone to ignoring an important error,
///   so it's best to have a warning on the console to at least have a hope of someone
///   noticing the error.
pub(crate) fn rsvg_g_warning(msg: &str) {
    rsvg_g_log(glib::ffi::G_LOG_LEVEL_WARNING, msg);
}

/// Replacement for `g_critical()`.
///
/// Use this to signal a programming error from the caller of the C API, like passing
/// incorrect arguments or calling the API out of order.  Rust code conventionally panics
/// in such situations, but C/Glib code does not, so it's best to "do nothing", print a
/// critical message, and return.  Development versions of GNOME will crash the program
/// if this happens; release versions will ignore the error.
pub(crate) fn rsvg_g_critical(msg: &str) {
    rsvg_g_log(glib::ffi::G_LOG_LEVEL_CRITICAL, msg);
}

/// Replacement for `g_return_if_fail()`.
// Once Rust has a function! macro that gives us the current function name, we
// can remove the $func_name argument.
#[macro_export]
macro_rules! rsvg_return_if_fail {
    {
        $func_name:ident;
        $($condition:expr,)+
    } => {
        $(
            if !$condition {
                unsafe {
                    glib::ffi::g_return_if_fail_warning(
                        c"librsvg".as_ptr(),
                        rsvg_c_str!(stringify!($func_name)),
                        rsvg_c_str!(stringify!($condition)),
                    );
                }
                return;
            }
        )+
    }
}

/// Replacement for `g_return_val_if_fail()`.
#[macro_export]
macro_rules! rsvg_return_val_if_fail {
    {
        $func_name:ident => $retval:expr;
        $($condition:expr,)+
    } => {
        $(
            if !$condition {
                unsafe {
                    glib::ffi::g_return_if_fail_warning(
                        c"librsvg".as_ptr(),
                        rsvg_c_str!(stringify!($func_name)),
                        rsvg_c_str!(stringify!($condition)),
                    );
                }
                return $retval;
            }
        )+
    }
}
