//! Functionality related to Python type objects
use super::{base::*, exc::*, module::*, refs::*};
use crate::pymodule::State;
use core::{
    ffi::CStr,
    mem::{self, MaybeUninit},
    ptr::NonNull,
};
use pyo3_ffi::*;

/// Wrapper around PyTypeObject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PyType {
    obj: PyObj,
}

impl PyBase for PyType {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyType {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr.cast()) },
        }
    }
}

impl PyStaticType for PyType {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyType_CheckExact(obj.as_ptr()) != 0 }
    }
    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyType_Check(obj.as_ptr()) != 0 }
    }
}

impl PyType {
    /// Get the module state if both types are from the whenever module.
    pub(crate) fn same_module(&self, other: PyType) -> Option<&State> {
        let Some(mod_a) = NonNull::new(unsafe { PyType_GetModule(self.as_ptr().cast()) }) else {
            unsafe { PyErr_Clear() };
            return None;
        };
        let Some(mod_b) = NonNull::new(unsafe { PyType_GetModule(other.as_ptr().cast()) }) else {
            unsafe { PyErr_Clear() };
            return None;
        };
        if mod_a == mod_b {
            // SAFETY: we only use this function after module initialization
            unsafe {
                PyModule::from_ptr_unchecked(mod_a.as_ptr())
                    .state()
                    .assume_init_ref()
                    .as_ref()
            }
        } else {
            None
        }
    }

    /// Associate the type with the given Rust type.
    pub(crate) unsafe fn link_type<T: PyWrapped>(self) -> HeapType<T> {
        // SAFETY: we assume the pointer is valid and points to a PyType object
        unsafe { HeapType::from_ptr_unchecked(self.as_ptr()) }
    }
}

impl std::fmt::Display for PyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_repr(f)
    }
}

/// A PyTypeObject that is linked to a Rust struct in whenever.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct HeapType<T: PyWrapped> {
    type_py: PyType,
    type_rust: std::marker::PhantomData<T>,
}

impl<T: PyWrapped> HeapType<T> {
    /// Get the module state
    pub(crate) fn state<'a>(&self) -> &'a State {
        // SAFETY: the type pointer is valid, and the retrieved module
        // state is valid once the Python module is initialized.
        unsafe {
            PyType_GetModuleState(self.type_py.as_ptr().cast())
                .cast::<MaybeUninit<Option<State>>>()
                .as_ref()
                .unwrap()
                .assume_init_ref()
                .as_ref()
                .unwrap()
        }
    }

    pub(crate) fn inner(&self) -> PyType {
        self.type_py
    }
}

impl<T: PyWrapped> PyBase for HeapType<T> {
    fn as_py_obj(&self) -> PyObj {
        self.type_py.as_py_obj()
    }
}

impl<T: PyWrapped> FromPy for HeapType<T> {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            type_py: unsafe { PyType::from_ptr_unchecked(ptr) },
            type_rust: std::marker::PhantomData,
        }
    }
}

impl<T: PyWrapped> From<HeapType<T>> for PyType {
    fn from(t: HeapType<T>) -> Self {
        t.type_py
    }
}

/// A trait for types that can be wrapped in a `whenever` Python class.
pub(crate) trait PyWrapped: FromPy {
    #[inline]
    unsafe fn from_obj(obj: *mut PyObject) -> Self {
        unsafe { (*obj.cast::<PyWrap<Self>>()).data }
    }

    #[inline]
    fn to_obj(self, type_: HeapType<Self>) -> PyReturn {
        generic_alloc(type_.type_py, self)
    }
}

impl<T: PyWrapped> FromPy for T {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> T {
        unsafe { T::from_obj(ptr) }
    }
}

/// The shape of PyObjects that wrap a `whenever` Rust type.
#[repr(C)]
struct PyWrap<T: PyWrapped> {
    _ob_base: PyObject,
    data: T,
}

pub(crate) const fn type_spec<T: PyWrapped>(
    name: &CStr,
    slots: &'static [PyType_Slot],
) -> PyType_Spec {
    PyType_Spec {
        name: name.as_ptr().cast(),
        basicsize: mem::size_of::<PyWrap<T>>() as _,
        itemsize: 0,
        // NOTE: IMMUTABLETYPE flag is required to prevent additional refcycles
        // between the class and the instance.
        // This allows us to keep our types GC-free.
        #[cfg(Py_3_10)]
        flags: (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE) as _,
        // XXX: implement a way to prevent refcycles on Python 3.9
        // without Py_TPFLAGS_IMMUTABLETYPE.
        // Not a pressing concern, because this only will be triggered
        // if users themselves decide to add instances to the class
        // namespace.
        // Even so, this will just result in a minor memory leak
        // preventing the module from being GC'ed,
        // since subinterpreters aren't a concern.
        #[cfg(not(Py_3_10))]
        flags: Py_TPFLAGS_DEFAULT as _,
        slots: slots.as_ptr().cast_mut(),
    }
}

pub(crate) extern "C" fn generic_dealloc(slf: PyObj) {
    let cls = slf.type_().as_ptr().cast::<PyTypeObject>();
    unsafe {
        let tp_free = PyType_GetSlot(cls, Py_tp_free);
        debug_assert_ne!(tp_free, core::ptr::null_mut());
        let tp_free: freefunc = std::mem::transmute(tp_free);
        tp_free(slf.as_ptr().cast());
        Py_DECREF(cls.cast());
    }
}

#[inline]
pub(crate) fn generic_alloc<T: PyWrapped>(type_: PyType, d: T) -> PyReturn {
    let type_ptr = type_.as_ptr().cast::<PyTypeObject>();
    unsafe {
        let slf = (*type_ptr).tp_alloc.unwrap()(type_ptr, 0).cast::<PyWrap<T>>();
        match slf.cast::<PyObject>().as_mut() {
            Some(r) => {
                (&raw mut (*slf).data).write(d);
                Ok(Owned::new(PyObj::from_ptr_unchecked(r)))
            }
            None => Err(PyErrMarker()),
        }
    }
}
