//! Functions for dealing with Python tuples.
use super::{base::*, exc::*, refs::*};
use pyo3_ffi::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PyTuple {
    obj: PyObj,
}

impl PyBase for PyTuple {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyTuple {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyTuple {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyTuple_CheckExact(obj.as_ptr()) != 0 }
    }

    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyTuple_Check(obj.as_ptr()) != 0 }
    }
}

impl PyTuple {
    pub(crate) fn len(&self) -> Py_ssize_t {
        unsafe { PyTuple_GET_SIZE(self.obj.as_ptr()) }
    }

    pub(crate) fn iter(&self) -> PyTupleIter {
        PyTupleIter {
            obj: self.as_ptr(),
            index: 0,
            size: self.len(),
        }
    }
}

pub(crate) struct PyTupleIter {
    obj: *mut PyObject,
    index: Py_ssize_t,
    size: Py_ssize_t,
}

impl Iterator for PyTupleIter {
    type Item = PyObj;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.size {
            return None;
        }
        let result = unsafe { PyObj::from_ptr_unchecked(PyTuple_GET_ITEM(self.obj, self.index)) };
        self.index += 1;
        Some(result)
    }
}

pub(crate) trait IntoPyTuple {
    fn into_pytuple(self) -> PyResult<Owned<PyTuple>>;
}

impl<T: PyBase> IntoPyTuple for (Owned<T>,) {
    fn into_pytuple(self) -> PyResult<Owned<PyTuple>> {
        let tuple = unsafe { PyTuple_New(1).rust_owned()?.cast_unchecked::<PyTuple>() };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 0, self.0.py_owned().as_ptr()) };
        Ok(tuple)
    }
}

impl<T: PyBase, U: PyBase> IntoPyTuple for (Owned<T>, Owned<U>) {
    fn into_pytuple(self) -> PyResult<Owned<PyTuple>> {
        let tuple = unsafe { PyTuple_New(2).rust_owned()?.cast_unchecked::<PyTuple>() };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 0, self.0.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 1, self.1.py_owned().as_ptr()) };
        Ok(tuple)
    }
}

impl<T: PyBase, U: PyBase, V: PyBase> IntoPyTuple for (Owned<T>, Owned<U>, Owned<V>) {
    fn into_pytuple(self) -> PyResult<Owned<PyTuple>> {
        let tuple = unsafe { PyTuple_New(3).rust_owned()?.cast_unchecked::<PyTuple>() };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 0, self.0.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 1, self.1.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 2, self.2.py_owned().as_ptr()) };
        Ok(tuple)
    }
}

impl<T: PyBase, U: PyBase, V: PyBase, W: PyBase> IntoPyTuple
    for (Owned<T>, Owned<U>, Owned<V>, Owned<W>)
{
    fn into_pytuple(self) -> PyResult<Owned<PyTuple>> {
        let tuple = unsafe { PyTuple_New(4).rust_owned()?.cast_unchecked::<PyTuple>() };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 0, self.0.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 1, self.1.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 2, self.2.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 3, self.3.py_owned().as_ptr()) };
        Ok(tuple)
    }
}

impl<T: PyBase, U: PyBase, V: PyBase, W: PyBase, X: PyBase> IntoPyTuple
    for (Owned<T>, Owned<U>, Owned<V>, Owned<W>, Owned<X>)
{
    fn into_pytuple(self) -> PyResult<Owned<PyTuple>> {
        let tuple = unsafe { PyTuple_New(5).rust_owned()?.cast_unchecked::<PyTuple>() };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 0, self.0.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 1, self.1.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 2, self.2.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 3, self.3.py_owned().as_ptr()) };
        unsafe { PyTuple_SET_ITEM(tuple.as_ptr(), 4, self.4.py_owned().as_ptr()) };
        Ok(tuple)
    }
}
