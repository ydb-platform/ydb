//! Miscellaneous utility functions and constants.
use super::{args::*, base::*, exc::*, refs::*, types::*};
use core::{
    ffi::{CStr, c_int, c_void},
    ptr::null_mut as NULL,
};
use pyo3_ffi::*;

pub(crate) fn none() -> Owned<PyObj> {
    // SAFETY: Py_None is a valid pointer
    unsafe { PyObj::from_ptr_unchecked(Py_None()) }.newref()
}

pub(crate) fn identity1(_: PyType, slf: PyObj) -> Owned<PyObj> {
    slf.newref()
}

pub(crate) fn __copy__(_: PyType, slf: PyObj) -> Owned<PyObj> {
    slf.newref()
}

pub(crate) fn __deepcopy__(_: PyType, slf: PyObj, _: PyObj) -> Owned<PyObj> {
    slf.newref()
}

pub(crate) fn import(module: &CStr) -> PyReturn {
    unsafe { PyImport_ImportModule(module.as_ptr()) }.rust_owned()
}

pub(crate) fn __get_pydantic_core_schema__<T: PyWrapped>(
    cls: HeapType<T>,
    _: &[PyObj],
    _: &mut IterKwargs,
) -> PyReturn {
    cls.state().get_pydantic_schema.get()?.call1(cls)
}

pub(crate) fn not_implemented() -> PyReturn {
    Ok(Owned::new(
        // SAFETY: Py_NotImplemented is always non-null
        unsafe {
            PyObj::from_ptr_unchecked({
                let ptr = Py_NotImplemented();
                Py_INCREF(ptr);
                ptr
            })
        },
    ))
}

/// Pack various types into a byte array. Used for pickling.
macro_rules! pack {
    [$x:expr, $($xs:expr),*] => {{
        // OPTIMIZE: use Vec::with_capacity, or a fixed-size array
        // since we know the size at compile time
        let mut result = Vec::new();
        result.extend_from_slice(&$x.to_le_bytes());
        $(
            result.extend_from_slice(&$xs.to_le_bytes());
        )*
        result
    }}
}

/// Unpack a single value from a byte array. Used for unpickling.
macro_rules! unpack_one {
    ($arr:ident, $t:ty) => {{
        const SIZE: usize = std::mem::size_of::<$t>();
        let data = <$t>::from_le_bytes($arr[..SIZE].try_into().unwrap());
        #[allow(unused_assignments)]
        {
            $arr = &$arr[SIZE..];
        }
        data
    }};
}

#[derive(Debug)]
pub(crate) struct LazyImport {
    module: &'static CStr,
    name: &'static CStr,
    obj: std::cell::UnsafeCell<*mut PyObject>,
}

impl LazyImport {
    pub(crate) fn new(module: &'static CStr, name: &'static CStr) -> Self {
        Self {
            module,
            name,
            obj: std::cell::UnsafeCell::new(NULL()),
        }
    }

    /// Get the object, importing it if necessary.
    pub(crate) fn get(&self) -> PyResult<PyObj> {
        unsafe {
            let obj = *self.obj.get();
            if obj.is_null() {
                let t = import(self.module)?.getattr(self.name)?.py_owned();
                self.obj.get().write(t.as_ptr());
                Ok(t)
            } else {
                Ok(PyObj::from_ptr_unchecked(obj))
            }
        }
    }

    /// Ensure Python's GC can traverse this object.
    pub(crate) fn traverse(&self, visit: visitproc, arg: *mut c_void) -> TraverseResult {
        let obj = unsafe { *self.obj.get() };
        traverse(obj, visit, arg)
    }
}

impl Drop for LazyImport {
    fn drop(&mut self) {
        unsafe {
            let obj = self.obj.get();
            if !(*obj).is_null() {
                Py_CLEAR(obj);
            }
        }
    }
}

// FUTURE: a more efficient way for specific cases?
pub(crate) const fn hashmask(hash: Py_hash_t) -> Py_hash_t {
    if hash == -1 {
        return -2;
    }
    hash
}

/// fast, safe way to combine hash values, from stackoverflow.com/questions/5889238
#[inline]
pub(crate) const fn hash_combine(lhs: Py_hash_t, rhs: Py_hash_t) -> Py_hash_t {
    #[cfg(target_pointer_width = "64")]
    {
        lhs ^ (rhs
            .wrapping_add(0x517cc1b727220a95)
            .wrapping_add(lhs << 6)
            .wrapping_add(lhs >> 2))
    }
    #[cfg(target_pointer_width = "32")]
    {
        lhs ^ (rhs
            .wrapping_add(-0x61c88647)
            .wrapping_add(lhs << 6)
            .wrapping_add(lhs >> 2))
    }
}

/// Result from traversing a Python object for garbage collection.
pub(crate) type TraverseResult = Result<(), c_int>;

pub(crate) fn traverse(
    target: *mut PyObject,
    visit: visitproc,
    arg: *mut c_void,
) -> TraverseResult {
    if target.is_null() {
        Ok(())
    } else {
        match unsafe { (visit)(target, arg) } {
            0 => Ok(()),
            n => Err(n),
        }
    }
}

#[allow(unused_imports)]
pub(crate) use {pack, unpack_one};
