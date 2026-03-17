//! Basic types and traits for PyObject and its subtypes.
use super::{exc::*, refs::*, string::*, tuple::*, types::*};
use core::{ffi::CStr, fmt::Debug, ptr::NonNull, ptr::null_mut as NULL};
use pyo3_ffi::*;

pub(crate) trait FromPy: Sized + Copy {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self;
}

pub(crate) trait ToPy: Sized {
    fn to_py(self) -> PyReturn;
}

pub(crate) trait PyStaticType: PyBase {
    fn isinstance_exact(obj: impl PyBase) -> bool;
    fn isinstance(obj: impl PyBase) -> bool;
}

/// A minimal wrapper for the PyObject pointer.
/// Transparent to PyObject to allow casting to/from PyObject.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PyObj {
    inner: NonNull<PyObject>,
}

impl PyObj {
    pub(crate) fn new(ptr: *mut PyObject) -> PyResult<Self> {
        match NonNull::new(ptr) {
            Some(x) => Ok(Self { inner: x }),
            None => Err(PyErrMarker()),
        }
    }

    pub(crate) fn wrap(inner: NonNull<PyObject>) -> Self {
        Self { inner }
    }

    /// The PyType of this object.
    pub(crate) fn type_(&self) -> PyType {
        unsafe { PyType::from_ptr_unchecked(Py_TYPE(self.inner.as_ptr()).cast()) }
    }

    /// Convert into anything that converts from PyObject.
    /// Useful for passing Pyobjects into functions that may convert them.
    pub(crate) unsafe fn into_unchecked<T: FromPy>(self) -> T {
        unsafe { T::from_ptr_unchecked(self.as_ptr()) }
    }

    pub(crate) unsafe fn assume_heaptype<T: PyWrapped>(&self) -> (HeapType<T>, T) {
        (
            unsafe { HeapType::from_ptr_unchecked(self.type_().as_ptr()) },
            unsafe { T::from_obj(self.inner.as_ptr()) },
        )
    }

    pub(crate) fn extract<T: PyWrapped>(&self, t: HeapType<T>) -> Option<T> {
        (self.type_() == t.inner()).then(
            // SAFETY: we've just checked the type, so this is safe
            || unsafe { T::from_obj(self.inner.as_ptr()) },
        )
    }

    /// Downcast to a specific type *exactly*. Cannot be used for heap types,
    /// use `extract` instead.
    pub(crate) fn cast<T: PyStaticType>(self) -> Option<T> {
        T::isinstance_exact(self)
            .then_some(unsafe { T::from_ptr_unchecked(self.as_py_obj().inner.as_ptr()) })
    }

    /// Like `cast`, but allows subclasses.
    pub(crate) fn cast_allow_subclass<T: PyStaticType>(self) -> Option<T> {
        T::isinstance(self)
            .then_some(unsafe { T::from_ptr_unchecked(self.as_py_obj().inner.as_ptr()) })
    }

    /// Like `cast`, but does not check the type.
    pub(crate) unsafe fn cast_unchecked<T: PyBase>(self) -> T {
        unsafe { T::from_ptr_unchecked(self.as_ptr()) }
    }

    pub(crate) fn is_none(&self) -> bool {
        self.as_ptr() == unsafe { Py_None() }
    }
}

impl PyBase for PyObj {
    fn as_py_obj(&self) -> PyObj {
        *self
    }
}

impl FromPy for PyObj {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            inner: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyObj {
    fn isinstance_exact(_: impl PyBase) -> bool {
        true
    }

    fn isinstance(_: impl PyBase) -> bool {
        true
    }
}

/// A trait for all PyObject and subtypes.
pub(crate) trait PyBase: FromPy {
    fn as_py_obj(&self) -> PyObj;

    /// Create a new, owned, reference to this object.
    fn newref(self) -> Owned<Self> {
        unsafe { Py_INCREF(self.as_py_obj().as_ptr()) }
        Owned::new(self)
    }

    /// Get the PyObject pointer.
    fn as_ptr(&self) -> *mut PyObject {
        self.as_py_obj().inner.as_ptr()
    }

    /// Write the repr of the object to the given formatter.
    fn write_repr<T: std::fmt::Write>(&self, f: &mut T) -> std::fmt::Result {
        let Ok(repr_obj) = unsafe { PyObject_Repr(self.as_ptr()) }.rust_owned() else {
            // i.e. repr() raised an exception
            unsafe { PyErr_Clear() };
            return f.write_str("<repr() failed>");
        };
        let Some(py_str) = repr_obj.cast::<PyStr>() else {
            // i.e. repr() didn't return a string
            return f.write_str("<repr() failed>");
        };
        let Ok(utf8) = py_str.as_utf8() else {
            // i.e. repr() returned a non-UTF-8 string
            unsafe { PyErr_Clear() };
            return f.write_str("<repr() failed>");
        };
        // SAFETY: Python emits valid UTF-8 strings
        f.write_str(unsafe { std::str::from_utf8_unchecked(utf8) })
    }

    /// Call `getattr()` on the object
    fn getattr(&self, name: &CStr) -> PyReturn {
        unsafe { PyObject_GetAttrString(self.as_ptr(), name.as_ptr()) }.rust_owned()
    }

    /// Call __getitem__ of the object
    fn getitem(&self, key: PyObj) -> PyReturn {
        unsafe { PyObject_GetItem(self.as_ptr(), key.as_ptr()) }.rust_owned()
    }

    /// Get the attribute of the object.
    fn setattr(&self, name: &CStr, v: PyObj) -> PyResult<()> {
        if unsafe { PyObject_SetAttrString(self.as_ptr(), name.as_ptr(), v.as_ptr()) } == 0 {
            Ok(())
        } else {
            Err(PyErrMarker())
        }
    }

    /// Call the object with one argument.
    fn call1(&self, arg: impl PyBase) -> PyReturn {
        unsafe { PyObject_CallOneArg(self.as_ptr(), arg.as_ptr()) }.rust_owned()
    }

    /// Call the object with no arguments.
    fn call0(&self) -> PyReturn {
        unsafe { PyObject_CallNoArgs(self.as_ptr()) }.rust_owned()
    }

    /// Call the object with a tuple of arguments.
    fn call(&self, args: PyTuple) -> PyReturn {
        // OPTIMIZE: use vectorcall?
        unsafe { PyObject_Call(self.as_ptr(), args.as_ptr(), NULL()) }.rust_owned()
    }

    /// Determine if the object is equal to another object, according to Python's
    /// `__eq__` method.
    fn py_eq(&self, other: impl PyBase) -> PyResult<bool> {
        // SAFETY: calling CPython API with valid arguments
        match unsafe { PyObject_RichCompareBool(self.as_ptr(), other.as_ptr(), Py_EQ) } {
            1 => Ok(true),
            0 => Ok(false),
            _ => Err(PyErrMarker()),
        }
    }

    /// Determine if the object is *exactly equal* to `True`.
    fn is_true(&self) -> bool {
        unsafe { self.as_ptr() == Py_True() }
    }
}

impl std::fmt::Display for PyObj {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_repr(f)
    }
}
