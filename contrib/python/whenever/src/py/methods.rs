//! Macros for defining methods in Python C extensions.
macro_rules! method0(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: {
                    use crate::py::*;
                    unsafe extern "C" fn _wrap(slf_ptr: *mut PyObject, _: *mut PyObject) -> *mut PyObject {
                        let slf = unsafe { PyObj::from_ptr_unchecked(slf_ptr) };
                        $meth(
                            unsafe {slf.type_().link_type::<$typ>().into()},
                            unsafe {slf.into_unchecked() }
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_NOARGS,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! method1(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: {
                    use crate::py::*;
                    unsafe extern "C" fn _wrap(slf_ptr: *mut PyObject, arg_obj: *mut PyObject) -> *mut PyObject {
                        let slf = unsafe {PyObj::from_ptr_unchecked(slf_ptr)};
                        let arg = unsafe {PyObj::from_ptr_unchecked(arg_obj)};
                        $meth(
                            unsafe {slf.type_().link_type::<$typ>().into()},
                            unsafe {slf.into_unchecked()},
                            unsafe {arg.into_unchecked()},
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_O,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! modmethod1(
    ($meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: {
                    use crate::py::*;
                    unsafe extern "C" fn _wrap(mod_ptr: *mut PyObject, arg_obj: *mut PyObject) -> *mut PyObject {
                        // SAFETY: valid pointers are passed to this function
                        let mod_obj = unsafe{ PyModule::from_ptr_unchecked(mod_ptr) };
                        let arg = unsafe {PyObj::from_ptr_unchecked(arg_obj)};
                        $meth(
                            // SAFETY: module state is initialized before this function is called
                            unsafe { mod_obj
                                .state()
                                .assume_init_mut()
                                .as_mut()
                                .unwrap()
                            },
                            arg
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_O,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! modmethod0(
    ($meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: {
                    use crate::py::*;
                    unsafe extern "C" fn _wrap(mod_ptr: *mut PyObject, _: *mut PyObject) -> *mut PyObject {
                        // SAFETY: valid pointers are passed to this function
                        let mod_obj = unsafe{ PyModule::from_ptr_unchecked(mod_ptr) };
                        $meth(
                            // SAFETY: module state is initialized before this function is called
                            unsafe { mod_obj
                                .state()
                                .assume_init_mut()
                                .as_mut()
                                .unwrap()
                            },
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_NOARGS,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! classmethod1(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: {
                    use crate::py::*;
                    unsafe extern "C" fn _wrap(cls: *mut PyObject, arg: *mut PyObject) -> *mut PyObject {
                        $meth(
                            unsafe {PyType::from_ptr_unchecked(cls).link_type::<$typ>().into()},
                            unsafe {PyObj::from_ptr_unchecked(arg)},
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_O | METH_CLASS,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! classmethod0(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunction: {
                    use crate::py::*;
                    unsafe extern "C" fn _wrap(cls: *mut PyObject, _: *mut PyObject) -> *mut PyObject {
                        $meth(
                            unsafe {PyType::from_ptr_unchecked(cls).link_type::<$typ>().into()},
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_NOARGS | METH_CLASS,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! method_vararg(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunctionFast: {
                    unsafe extern "C" fn _wrap(
                        slf_obj: *mut PyObject,
                        args: *mut *mut PyObject,
                        nargs: Py_ssize_t,
                    ) -> *mut PyObject {
                        let slf = unsafe {PyObj::from_ptr_unchecked(slf_obj)};
                        $meth(
                            unsafe {slf.type_().link_type::<$typ>().into()},
                            unsafe {slf.into_unchecked()},
                            unsafe {std::slice::from_raw_parts(args.cast::<PyObj>(), nargs as usize)},
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_FASTCALL,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! modmethod_vararg(
    ($meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCFunctionFast: {
                    unsafe extern "C" fn _wrap(
                        mod_ptr: *mut PyObject,
                        args: *mut *mut PyObject,
                        nargs: Py_ssize_t,
                    ) -> *mut PyObject {
                        // SAFETY: valid pointers are passed to this function
                        let mod_obj = unsafe{ PyModule::from_ptr_unchecked(mod_ptr) };
                        $meth(
                            // SAFETY: module state is initialized before this function is called
                            unsafe { mod_obj
                                .state()
                                .assume_init_mut()
                                .as_mut()
                                .unwrap()
                            },
                            unsafe {std::slice::from_raw_parts(args.cast::<PyObj>(), nargs as usize)},
                        )
                        .to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_FASTCALL,
            ml_doc: $doc.as_ptr()
        }
    };
);
macro_rules! method_kwargs(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCMethod: {
                    unsafe extern "C" fn _wrap(
                        slf: *mut PyObject,
                        cls: *mut PyTypeObject,
                        args_raw: *const *mut PyObject,
                        nargsf: Py_ssize_t,
                        kwnames: *mut PyObject,
                    ) -> *mut PyObject {
                        let nargs = unsafe {PyVectorcall_NARGS(nargsf as usize)};
                        $meth(
                            unsafe {PyType::from_ptr_unchecked(cls.cast()).link_type::<$typ>().into()},
                            unsafe {PyObj::from_ptr_unchecked(slf).into_unchecked()},
                            unsafe {std::slice::from_raw_parts(args_raw.cast::<PyObj>(), nargs as usize)},
                            &mut unsafe {IterKwargs::new(kwnames, args_raw.offset(nargs as isize))},
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_METHOD | METH_FASTCALL | METH_KEYWORDS,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! classmethod_kwargs(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyMethodDef {
            ml_name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            ml_meth: PyMethodDefPointer {
                PyCMethod: {
                    unsafe extern "C" fn _wrap(
                        _: *mut PyObject,
                        cls: *mut PyTypeObject,
                        args_raw: *const *mut PyObject,
                        nargsf: Py_ssize_t,
                        kwnames: *mut PyObject,
                    ) -> *mut PyObject {
                        let nargs = unsafe {PyVectorcall_NARGS(nargsf as usize)};
                        $meth(
                            unsafe {PyType::from_ptr_unchecked(cls.cast()).link_type::<$typ>().into()},
                            unsafe {std::slice::from_raw_parts(args_raw.cast::<PyObj>(), nargs as usize)},
                            &mut unsafe {IterKwargs::new(kwnames, args_raw.offset(nargs as isize))},
                        ).to_py_owned_ptr()
                    }
                    _wrap
                },
            },
            ml_flags: METH_METHOD | METH_FASTCALL | METH_KEYWORDS | METH_CLASS,
            ml_doc: $doc.as_ptr()
        }
    };
);

macro_rules! slotmethod {
    ($typ:ident, Py_tp_new, $name:ident) => {
        PyType_Slot {
            slot: Py_tp_new,
            pfunc: {
                unsafe extern "C" fn _wrap(
                    cls: *mut PyTypeObject,
                    args: *mut PyObject,
                    kwargs: *mut PyObject,
                ) -> *mut PyObject {
                    $name(
                        unsafe { HeapType::<$typ>::from_ptr_unchecked(cls.cast()) },
                        unsafe { PyTuple::from_ptr_unchecked(args) },
                        (!kwargs.is_null()).then(|| unsafe { PyDict::from_ptr_unchecked(kwargs) }),
                    )
                    .to_py_owned_ptr()
                }
                _wrap as *mut c_void
            },
        }
    };

    ($typ:ident, Py_tp_richcompare, $name:ident) => {
        PyType_Slot {
            slot: Py_tp_richcompare,
            pfunc: {
                unsafe extern "C" fn _wrap(
                    a: *mut PyObject,
                    b: *mut PyObject,
                    op: c_int,
                ) -> *mut PyObject {
                    let a = unsafe { PyObj::from_ptr_unchecked(a) };
                    $name(
                        unsafe { a.type_().link_type::<$typ>().into() },
                        unsafe { a.into_unchecked() },
                        unsafe { PyObj::from_ptr_unchecked(b) },
                        op,
                    )
                    .to_py_owned_ptr()
                }
                _wrap as *mut c_void
            },
        }
    };
    ($slot:ident, $name:ident, 2) => {
        PyType_Slot {
            slot: $slot,
            pfunc: {
                unsafe extern "C" fn _wrap(a: *mut PyObject, b: *mut PyObject) -> *mut PyObject {
                    $name(unsafe { PyObj::from_ptr_unchecked(a) }, unsafe {
                        PyObj::from_ptr_unchecked(b)
                    })
                    .to_py_owned_ptr()
                }
                _wrap as *mut c_void
            },
        }
    };

    ($typ:ident, $slot:ident, $name:ident, 1) => {
        PyType_Slot {
            slot: $slot,
            pfunc: {
                unsafe extern "C" fn _wrap(slf: *mut PyObject) -> *mut PyObject {
                    let slf = unsafe { PyObj::from_ptr_unchecked(slf) };
                    $name(unsafe { slf.type_().link_type::<$typ>().into() }, unsafe {
                        slf.into_unchecked()
                    })
                    .to_py_owned_ptr()
                }
                _wrap as *mut c_void
            },
        }
    };
}

macro_rules! getter(
    ($typ:ident, $meth:ident, $doc:expr) => {
        PyGetSetDef {
            name: concat!(stringify!($meth), "\0").as_ptr().cast(),
            get: Some({
                unsafe extern "C" fn _wrap(
                    slf_obj: *mut PyObject,
                    _: *mut c_void,
                ) -> *mut PyObject {
                    let slf = unsafe {PyObj::from_ptr_unchecked(slf_obj)};
                    $meth(
                        unsafe {slf.type_().link_type::<$typ>().into()},
                        unsafe {slf.into_unchecked()},
                    ).to_py_owned_ptr()
                }
                _wrap
            }),
            set: None,
            doc: concat!($doc, "\0").as_ptr().cast(),
            closure: core::ptr::null_mut(),
        }
    };
);

#[allow(unused_imports)]
pub(crate) use {
    classmethod_kwargs, classmethod0, classmethod1, getter, method_kwargs, method_vararg, method0,
    method1, modmethod_vararg, modmethod0, modmethod1, slotmethod,
};
