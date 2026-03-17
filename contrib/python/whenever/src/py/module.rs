//! Functions for working with the module object.

use super::{base::*, exc::*, types::*};
use crate::pymodule::State;
use core::mem::MaybeUninit;
use pyo3_ffi::*;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PyModule {
    obj: PyObj,
}

impl PyBase for PyModule {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyModule {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyModule {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyModule_CheckExact(obj.as_ptr()) != 0 }
    }

    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyModule_Check(obj.as_ptr()) != 0 }
    }
}

impl PyModule {
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn state<'a>(&self) -> &'a mut MaybeUninit<Option<State>> {
        // SAFETY: calling CPython API with valid arguments
        unsafe {
            PyModule_GetState(self.as_ptr())
                .cast::<MaybeUninit<Option<State>>>()
                .as_mut()
        }
        .unwrap()
    }

    pub(crate) fn add_type(&self, cls: PyType) -> PyResult<()> {
        // SAFETY: calling CPython API with valid arguments
        if unsafe { PyModule_AddType(self.as_ptr(), cls.as_ptr().cast()) } == 0 {
            Ok(())
        } else {
            Err(PyErrMarker())
        }
    }
}
