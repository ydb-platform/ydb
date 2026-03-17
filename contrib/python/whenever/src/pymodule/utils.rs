//! Miscellaneous utilities for the `whenever` module definition.
use crate::py::*;
use pyo3_ffi::*;
use std::{ffi::CStr, ptr::null_mut as NULL};

/// Create and add a new enum type to the module
pub(crate) fn new_enum(
    module: PyModule,
    module_name: PyObj,
    name: &str,
    members: &str, // space-separated list of members
) -> PyResult<Owned<PyType>> {
    let tp = (name.to_py()?, members.to_py()?).into_pytuple()?;
    let enum_cls = import(c"enum")?
        .getattr(c"Enum")?
        .call(tp.borrow())?
        .cast_allow_subclass::<PyType>()
        .unwrap();

    enum_cls.setattr(c"__module__", module_name)?;

    module.add_type(enum_cls.borrow())?;
    Ok(enum_cls)
}

/// Create and add a new exception type to the module
pub(crate) fn new_exception(
    module: PyModule,
    name: &CStr,
    doc: &CStr,
    base: *mut PyObject,
) -> PyResult<Owned<PyObj>> {
    // SAFETY: calling C API with valid arguments
    let e = unsafe { PyErr_NewExceptionWithDoc(name.as_ptr(), doc.as_ptr(), base, NULL()) }
        .rust_owned()?;
    module.add_type(e.borrow().cast::<PyType>().unwrap())?;
    Ok(e)
}

/// Create a new class in the module, including configuring the
/// unpickler and setting the module name
pub(crate) fn new_class<T: PyWrapped>(
    module: PyModule,
    module_nameobj: PyObj,
    spec: &mut PyType_Spec,
    unpickle_name: &CStr,
    singletons: &[(&CStr, T)],
) -> PyResult<(Owned<HeapType<T>>, Owned<PyObj>)> {
    let cls = unsafe { PyType_FromModuleAndSpec(module.as_ptr(), spec, NULL()) }
        .rust_owned()?
        .cast_allow_subclass::<PyType>()
        .unwrap()
        .map(|t| unsafe { t.link_type::<T>() });
    module.add_type(cls.borrow().into())?;

    // SAFETY: each type is guaranteed to have tp_dict
    let cls_dict =
        unsafe { PyDict::from_ptr_unchecked((*cls.as_ptr().cast::<PyTypeObject>()).tp_dict) };
    for (name, value) in singletons {
        let pyvalue = value.to_obj(cls.borrow())?;
        cls_dict
            // NOTE: We drop the value here, but count on the class dict to
            // keep the reference alive. This is safe since the dict is blocked
            // from mutation by the Py_TPFLAGS_IMMUTABLETYPE flag.
            .set_item_str(name, pyvalue.borrow())?;
    }

    let unpickler = module.getattr(unpickle_name)?;
    unpickler.setattr(c"__module__", module_nameobj)?;
    Ok((cls, unpickler))
}

/// Intern a string in the Python interpreter
pub(crate) fn intern(s: &CStr) -> PyReturn {
    unsafe { PyUnicode_InternFromString(s.as_ptr()) }.rust_owned()
}
