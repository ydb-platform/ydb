#![allow(
    // We choose to have narrow "unsafe" blocks instead of marking entire
    // functions as unsafe. Even the example in clippy's docs at
    // https://rust-lang.github.io/rust-clippy/master/index.html#not_unsafe_ptr_arg_deref
    // cause a rustc warning for unnecessary inner-unsafe when marked on fn.
    // This check only applies to "pub" functions which are all exposed via C
    // API.
    clippy::not_unsafe_ptr_arg_deref,
)]

pub mod client;
pub mod metric;
pub mod random;
pub mod runtime;
pub mod testing;
pub mod worker;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

#[repr(C)]
pub struct ByteArrayRef {
    pub data: *const u8,
    pub size: libc::size_t,
}

impl ByteArrayRef {
    pub fn empty() -> ByteArrayRef {
        static EMPTY: &str = "";
        EMPTY.into()
    }

    pub fn to_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.size) }
    }

    pub fn to_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data as *mut u8, self.size) }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.to_slice().to_vec()
    }

    pub fn to_str(&self) -> &str {
        // Trust caller to send UTF8. Even if we did do a checked call here with
        // error, the caller can still have a bad pointer or something else
        // wrong. Therefore we trust the caller implicitly.
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(self.data, self.size)) }
    }

    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        self.to_str().to_string()
    }

    pub fn to_option_slice(&self) -> Option<&[u8]> {
        if self.size == 0 {
            None
        } else {
            Some(self.to_slice())
        }
    }

    pub fn to_option_vec(&self) -> Option<Vec<u8>> {
        if self.size == 0 {
            None
        } else {
            Some(self.to_vec())
        }
    }

    pub fn to_option_str(&self) -> Option<&str> {
        if self.size == 0 {
            None
        } else {
            Some(self.to_str())
        }
    }

    pub fn to_option_string(&self) -> Option<String> {
        self.to_option_str().map(str::to_string)
    }

    pub fn to_str_map_on_newlines(&self) -> HashMap<&str, &str> {
        let strs: Vec<&str> = self.to_str().split('\n').collect();
        strs.chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect()
    }

    pub fn to_string_map_on_newlines(&self) -> HashMap<String, String> {
        self.to_str_map_on_newlines()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}

impl From<&str> for ByteArrayRef {
    fn from(value: &str) -> ByteArrayRef {
        ByteArrayRef {
            data: value.as_ptr(),
            size: value.len(),
        }
    }
}

impl From<&[u8]> for ByteArrayRef {
    fn from(value: &[u8]) -> ByteArrayRef {
        ByteArrayRef {
            data: value.as_ptr(),
            size: value.len(),
        }
    }
}

impl<T> From<Option<T>> for ByteArrayRef
where
    T: Into<ByteArrayRef>,
{
    fn from(value: Option<T>) -> ByteArrayRef {
        value.map(Into::into).unwrap_or(ByteArrayRef::empty())
    }
}

#[repr(C)]
pub struct ByteArrayRefArray {
    pub data: *const ByteArrayRef,
    pub size: libc::size_t,
}

impl ByteArrayRefArray {
    pub fn to_str_vec(&self) -> Vec<&str> {
        if self.size == 0 {
            vec![]
        } else {
            let raw = unsafe { std::slice::from_raw_parts(self.data, self.size) };
            raw.iter().map(ByteArrayRef::to_str).collect()
        }
    }
}

/// Metadata is <key1>\n<value1>\n<key2>\n<value2>. Metadata keys or
/// values cannot contain a newline within.
pub type MetadataRef = ByteArrayRef;

#[repr(C)]
pub struct ByteArray {
    pub data: *const u8,
    pub size: libc::size_t,
    /// For internal use only.
    cap: libc::size_t,
    /// For internal use only.
    disable_free: bool,
}

impl ByteArray {
    pub fn from_utf8(str: String) -> ByteArray {
        ByteArray::from_vec(str.into_bytes())
    }

    pub fn from_vec(vec: Vec<u8>) -> ByteArray {
        // Mimics Vec::into_raw_parts that's only available in nightly
        let mut vec = std::mem::ManuallyDrop::new(vec);
        ByteArray {
            data: vec.as_mut_ptr(),
            size: vec.len(),
            cap: vec.capacity(),
            disable_free: false,
        }
    }

    pub fn from_vec_disable_free(vec: Vec<u8>) -> ByteArray {
        let mut b = ByteArray::from_vec(vec);
        b.disable_free = true;
        b
    }

    pub fn into_raw(self) -> *mut ByteArray {
        Box::into_raw(Box::new(self))
    }

    pub fn as_ref(&self) -> ByteArrayRef {
        ByteArrayRef {
            data: self.data,
            size: self.size,
        }
    }
}

// Required because these instances are used by lazy_static and raw pointers are
// not usually safe for send/sync.
unsafe impl Send for ByteArray {}
unsafe impl Sync for ByteArray {}

impl Drop for ByteArray {
    fn drop(&mut self) {
        // In cases where freeing is disabled (or technically some other
        // drop-but-not-freed situation though we don't expect any), the bytes
        // remain non-null so we re-own them here. See "byte_array_free" in
        // runtime.rs.
        if !self.data.is_null() {
            unsafe { Vec::from_raw_parts(self.data as *mut u8, self.size, self.cap) };
        }
    }
}

/// Used for maintaining pointer to user data across threads. See
/// https://doc.rust-lang.org/nomicon/send-and-sync.html.
struct UserDataHandle(*mut libc::c_void);
unsafe impl Send for UserDataHandle {}
unsafe impl Sync for UserDataHandle {}

impl From<UserDataHandle> for *mut libc::c_void {
    fn from(v: UserDataHandle) -> Self {
        v.0
    }
}

pub struct CancellationToken {
    token: tokio_util::sync::CancellationToken,
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_cancellation_token_new() -> *mut CancellationToken {
    Box::into_raw(Box::new(CancellationToken {
        token: tokio_util::sync::CancellationToken::new(),
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_cancellation_token_cancel(token: *mut CancellationToken) {
    let token = unsafe { &*token };
    token.token.cancel();
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_cancellation_token_free(token: *mut CancellationToken) {
    unsafe {
        let _ = Box::from_raw(token);
    }
}
