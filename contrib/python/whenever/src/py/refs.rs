//! Functionality relating to ownership and references
use super::{base::*, exc::*};
use core::mem::ManuallyDrop;
use core::ptr::null_mut as NULL;
use pyo3_ffi::*;

/// A wrapper for Python objects that have a reference owned by Rust.
/// They are decreferred on drop.
#[derive(Debug)]
pub(crate) struct Owned<T: PyBase> {
    inner: T,
}

impl<T: PyBase> Owned<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self { inner }
    }

    pub(crate) fn py_owned(self) -> T {
        // By transferring ownership to Python, we essentially say
        // Rust is no longer responsible for the memory (i.e. Drop)
        let this = ManuallyDrop::new(self);
        unsafe {
            std::ptr::read(&this.inner) // Read the inner object without dropping it
        }
    }

    pub(crate) fn borrow(&self) -> T {
        self.inner
    }

    /// Apply a function to the inner object while retaining ownership.
    pub(crate) fn map<U, F>(self, f: F) -> Owned<U>
    where
        F: FnOnce(T) -> U,
        U: PyBase,
    {
        Owned::new(f(self.py_owned()))
    }
}

impl<T: PyBase> Owned<T> {
    pub(crate) fn cast<U: PyStaticType>(self) -> Option<Owned<U>> {
        let inner = self.py_owned();
        inner.as_py_obj().cast().map(Owned::new).or_else(|| {
            // Casting failed, but don't forget to decref the original object
            unsafe { Py_DECREF(inner.as_ptr()) };
            None
        })
    }

    pub(crate) fn cast_allow_subclass<U: PyStaticType>(self) -> Option<Owned<U>> {
        let inner = self.py_owned();
        inner
            .as_py_obj()
            .cast_allow_subclass()
            .map(Owned::new)
            .or_else(|| {
                // Casting failed, but don't forget to decref the original object
                unsafe { Py_DECREF(inner.as_ptr()) };
                None
            })
    }

    pub(crate) unsafe fn cast_unchecked<U: PyBase>(self) -> Owned<U> {
        // SAFETY: the caller guarantees the type
        Owned::new(unsafe { self.py_owned().as_py_obj().cast_unchecked() })
    }
}

impl<T: PyBase> Drop for Owned<T> {
    fn drop(&mut self) {
        unsafe {
            // SAFETY: we hold a reference to the object, so it's guaranteed to be valid
            Py_DECREF(self.inner.as_ptr());
        }
    }
}

impl<T: PyBase> std::ops::Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(crate) trait PyObjectExt {
    fn rust_owned(self) -> PyResult<Owned<PyObj>>;
}

impl PyObjectExt for *mut PyObject {
    fn rust_owned(self) -> PyResult<Owned<PyObj>> {
        PyObj::new(self).map(Owned::new)
    }
}

pub(crate) trait ToPyOwnedPtr {
    fn to_py_owned_ptr(self) -> *mut PyObject;
}

impl ToPyOwnedPtr for *mut PyObject {
    fn to_py_owned_ptr(self) -> *mut PyObject {
        self
    }
}

impl<T: PyBase> ToPyOwnedPtr for PyResult<Owned<T>> {
    fn to_py_owned_ptr(self) -> *mut PyObject {
        match self.map(|x| x.py_owned()) {
            Ok(x) => x.as_ptr(),
            Err(_) => NULL(),
        }
    }
}

impl<T: PyBase> ToPyOwnedPtr for Owned<T> {
    fn to_py_owned_ptr(self) -> *mut PyObject {
        self.py_owned().as_ptr()
    }
}
