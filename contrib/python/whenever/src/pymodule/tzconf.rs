//! Functions in the `whenever` module that manage the TZ cache and search path
use crate::{py::*, pymodule::State};
use std::path::PathBuf;

pub(crate) fn _set_tzpath(state: &mut State, to: PyObj) -> PyReturn {
    let Some(py_tuple) = to.cast::<PyTuple>() else {
        raise_type_err("Argument must be a tuple")?
    };

    let mut result = Vec::with_capacity(py_tuple.len() as _);

    for path in py_tuple.iter() {
        result.push(PathBuf::from(
            path.cast::<PyStr>()
                .ok_or_type_err("Path must be a string")?
                .as_str()?,
        ))
    }
    state.tz_store.paths = result;
    Ok(none())
}

pub(crate) fn _clear_tz_cache(state: &mut State) -> PyReturn {
    state.tz_store.clear_all();
    Ok(none())
}

pub(crate) fn _clear_tz_cache_by_keys(state: &mut State, keys_obj: PyObj) -> PyReturn {
    let Some(py_tuple) = keys_obj.cast::<PyTuple>() else {
        raise_type_err("Argument must be a tuple")?
    };
    let mut keys = Vec::with_capacity(py_tuple.len() as _);
    for k in py_tuple.iter() {
        keys.push(
            k.cast::<PyStr>()
                .ok_or_type_err("Key must be a string")?
                .as_str()?
                // FUTURE: We should be able to use string slices here, but
                // making this work with lifetimes requires a bit of work.
                // Since this isn't performance critical, we leave it for now.
                .to_string(),
        );
    }
    state.tz_store.clear_only(&keys);
    Ok(none())
}
