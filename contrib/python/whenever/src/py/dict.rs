//! Functions for manipulating Python dictionaries.
use super::{base::*, exc::*};
use core::{ffi::CStr, ptr::null_mut as NULL};
use pyo3_ffi::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PyDict {
    obj: PyObj,
}

impl PyBase for PyDict {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyDict {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyDict {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyDict_CheckExact(obj.as_ptr()) != 0 }
    }

    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyDict_Check(obj.as_ptr()) != 0 }
    }
}

impl PyDict {
    pub(crate) fn set_item_str(&self, key: &CStr, value: PyObj) -> PyResult<()> {
        if unsafe { PyDict_SetItemString(self.obj.as_ptr(), key.as_ptr(), value.as_ptr()) } == -1 {
            return Err(PyErrMarker());
        }
        Ok(())
    }

    pub(crate) fn len(&self) -> Py_ssize_t {
        unsafe { PyDict_Size(self.obj.as_ptr()) }
    }

    pub(crate) fn iteritems(&self) -> PyDictIterItems {
        PyDictIterItems {
            obj: self.obj.as_ptr(),
            pos: 0,
        }
    }
}

pub(crate) struct PyDictIterItems {
    obj: *mut PyObject,
    pos: Py_ssize_t,
}

impl Iterator for PyDictIterItems {
    type Item = (PyObj, PyObj);

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = NULL();
        let mut value = NULL();
        (unsafe { PyDict_Next(self.obj, &mut self.pos, &mut key, &mut value) } != 0).then(
            || unsafe {
                (
                    PyObj::from_ptr_unchecked(key),
                    PyObj::from_ptr_unchecked(value),
                )
            },
        )
    }
}
