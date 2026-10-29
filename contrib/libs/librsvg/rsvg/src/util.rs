//! Miscellaneous utilities.

use std::borrow::Cow;
use std::ffi::CStr;
use std::str;

/// Converts a `char *` which is known to be valid UTF-8 into a `&str`
///
/// The usual `from_glib_none(s)` allocates an owned String.  The
/// purpose of `utf8_cstr()` is to get a temporary string slice into a
/// C string which is already known to be valid UTF-8; for example,
/// as for strings which come from `libxml2`.
///
/// Safety: `s` must be a nul-terminated, valid UTF-8 string of bytes.
pub unsafe fn utf8_cstr<'a>(s: *const libc::c_char) -> &'a str {
    assert!(!s.is_null());

    unsafe { str::from_utf8_unchecked(CStr::from_ptr(s).to_bytes()) }
}

/// Converts a `char *` which is known to be valid UTF-8 into an `Option<&str>`
///
/// NULL pointers get converted to None.
///
/// Safety: `s` must be null, or a nul-terminated, valid UTF-8 string of bytes.
pub unsafe fn opt_utf8_cstr<'a>(s: *const libc::c_char) -> Option<&'a str> {
    if s.is_null() {
        None
    } else {
        unsafe { Some(utf8_cstr(s)) }
    }
}

/// Gets a known-to-be valid UTF-8 string given start/end-exclusive pointers to its bounds.
///
/// Safety: `start` must be a valid pointer, and `end` must be the same for a zero-length string,
/// or greater than `start`.  All the bytes between them must be valid UTF-8.
pub unsafe fn utf8_cstr_bounds<'a>(
    start: *const libc::c_char,
    end: *const libc::c_char,
) -> &'a str {
    unsafe {
        let len = end.offset_from(start);
        assert!(len >= 0);

        utf8_cstr_len(start, len as usize)
    }
}

/// Gets a known-to-be valid UTF-8 string given a pointer to its start and a length.
///
/// Safety: `start` must be a valid pointer, and `len` bytes starting from it must be
/// valid UTF-8.
pub unsafe fn utf8_cstr_len<'a>(start: *const libc::c_char, len: usize) -> &'a str {
    unsafe {
        // Convert from libc::c_char to u8.  Why cast like this?  Because libc::c_char
        // is of different signedness depending on the architecture (u8 on aarch64,
        // i8 on x86_64).  If one just uses "start as *const u8", it triggers a
        // trivial_casts warning.
        #[allow(trivial_casts)]
        let start = start as *const u8;
        let value_slice = std::slice::from_raw_parts(start, len);

        str::from_utf8_unchecked(value_slice)
    }
}

/// Error-tolerant C string import
pub unsafe fn cstr<'a>(s: *const libc::c_char) -> Cow<'a, str> {
    if s.is_null() {
        return Cow::Borrowed("(null)");
    }
    unsafe { CStr::from_ptr(s).to_string_lossy() }
}

pub fn clamp<T: PartialOrd>(val: T, low: T, high: T) -> T {
    if val < low {
        low
    } else if val > high {
        high
    } else {
        val
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(trivial_casts)]
    #[test]
    fn utf8_cstr_works() {
        unsafe {
            let hello = b"hello\0".as_ptr() as *const libc::c_char;

            assert_eq!(utf8_cstr(hello), "hello");
        }
    }

    #[allow(trivial_casts)]
    #[test]
    fn opt_utf8_cstr_works() {
        unsafe {
            let hello = b"hello\0".as_ptr() as *const libc::c_char;

            assert_eq!(opt_utf8_cstr(hello), Some("hello"));
            assert_eq!(opt_utf8_cstr(std::ptr::null()), None);
        }
    }

    #[allow(trivial_casts)]
    #[test]
    fn utf8_cstr_bounds_works() {
        unsafe {
            let hello: *const libc::c_char = b"hello\0" as *const _ as *const _;

            assert_eq!(utf8_cstr_bounds(hello, hello.offset(5)), "hello");
            assert_eq!(utf8_cstr_bounds(hello, hello), "");
        }
    }

    #[allow(trivial_casts)]
    #[test]
    fn utf8_cstr_len_works() {
        unsafe {
            let hello: *const libc::c_char = b"hello\0" as *const _ as *const _;

            assert_eq!(utf8_cstr_len(hello, 5), "hello");
        }
    }

    #[allow(trivial_casts)]
    #[test]
    fn cstr_works() {
        unsafe {
            let hello: *const libc::c_char = b"hello\0" as *const _ as *const _;
            let invalid_utf8: *const libc::c_char = b"hello\xff\0" as *const _ as *const _;

            assert_eq!(cstr(hello).as_ref(), "hello");
            assert_eq!(cstr(std::ptr::null()).as_ref(), "(null)");
            assert_eq!(cstr(invalid_utf8).as_ref(), "hello\u{fffd}");
        }
    }
}
