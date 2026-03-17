//! Functions for handling arguments and keyword arguments in Python
use crate::py::*;
use pyo3_ffi::*;

pub(crate) struct IterKwargs {
    keys: *mut PyObject,
    values: *const *mut PyObject,
    size: isize,
    pos: isize,
}

impl IterKwargs {
    pub(crate) unsafe fn new(keys: *mut PyObject, values: *const *mut PyObject) -> Self {
        Self {
            keys,
            values,
            size: if keys.is_null() {
                0
            } else {
                // SAFETY: calling C API with valid arguments
                unsafe { PyTuple_GET_SIZE(keys) as isize }
            },
            pos: 0,
        }
    }

    pub(crate) fn len(&self) -> isize {
        self.size
    }
}

impl Iterator for IterKwargs {
    type Item = (PyObj, PyObj);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.size {
            return None;
        }
        let item = unsafe {
            (
                PyObj::from_ptr_unchecked(PyTuple_GET_ITEM(self.keys, self.pos)),
                PyObj::from_ptr_unchecked(*self.values.offset(self.pos)),
            )
        };
        self.pos += 1;
        Some(item)
    }
}

macro_rules! parse_args_kwargs {
    ($args:ident, $kwargs:ident, $fmt:expr, $($var:ident),* $(,)?) => {
        // SAFETY: calling CPython API with valid arguments
        unsafe {
            const _ARGNAMES: *mut *const std::ffi::c_char = [
                $(
                    concat!(stringify!($var), "\0").as_ptr() as *const std::ffi::c_char,
                )*
                std::ptr::null(),
            ].as_ptr() as *mut _;
            if PyArg_ParseTupleAndKeywords(
                $args.as_ptr(),
                $kwargs.map_or(NULL(), |d| d.as_ptr()),
                $fmt.as_ptr(),
                {
                    // This API was changed in Python 3.13
                    #[cfg(Py_3_13)]
                    {
                        _ARGNAMES
                    }
                    #[cfg(not(Py_3_13))]
                    {
                        _ARGNAMES as *mut *mut _
                    }
                },
                $(&mut $var,)*
            ) == 0 {
                return Err(PyErrMarker());
            }
        }
    };
}

#[inline]
fn ptr_eq(a: PyObj, b: PyObj) -> bool {
    a == b
}

#[inline]
fn value_eq(a: PyObj, b: PyObj) -> bool {
    unsafe { PyObject_RichCompareBool(a.as_ptr(), b.as_ptr(), Py_EQ) == 1 }
}

pub(crate) fn handle_kwargs<F, K>(fname: &str, kwargs: K, mut handler: F) -> PyResult<()>
where
    F: FnMut(PyObj, PyObj, fn(PyObj, PyObj) -> bool) -> PyResult<bool>,
    K: IntoIterator<Item = (PyObj, PyObj)>,
{
    for (key, value) in kwargs {
        // First we try to match *all kwargs* on pointer equality.
        // This is actually the common case, as static strings are interned.
        // In the rare case they aren't, we fall back to value comparison.
        // Doing it this way is faster than always doing value comparison outright.
        if !handler(key, value, ptr_eq)? && !handler(key, value, value_eq)? {
            return raise_type_err(format!(
                "{fname}() got an unexpected keyword argument: {key}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn match_interned_str<T, F>(name: &str, value: PyObj, mut handler: F) -> PyResult<T>
where
    F: FnMut(PyObj, fn(PyObj, PyObj) -> bool) -> Option<T>,
{
    handler(value, ptr_eq)
        .or_else(|| handler(value, value_eq))
        .ok_or_else_value_err(|| format!("Invalid value for {name}: {value}"))
}

pub(crate) use parse_args_kwargs;
