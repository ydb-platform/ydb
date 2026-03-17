//! Functionality for working with Python's datetime module.
use super::{base::*, exc::*};
use crate::common::scalar::{DeltaSeconds, SubSecNanos};
use pyo3_ffi::*;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PyDate {
    obj: PyObj,
}

impl PyDate {
    pub fn year(&self) -> i32 {
        unsafe { PyDateTime_GET_YEAR(self.obj.as_ptr()) }
    }

    pub fn month(&self) -> i32 {
        unsafe { PyDateTime_GET_MONTH(self.obj.as_ptr()) }
    }

    pub fn day(&self) -> i32 {
        unsafe { PyDateTime_GET_DAY(self.obj.as_ptr()) }
    }
}

impl PyBase for PyDate {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyDate {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyDate {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyDate_CheckExact(obj.as_ptr()) != 0 }
    }

    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyDate_Check(obj.as_ptr()) != 0 }
    }
}

impl std::fmt::Display for PyDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_repr(f)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PyDateTime {
    obj: PyObj,
}

impl PyDateTime {
    pub(crate) fn year(&self) -> i32 {
        unsafe { PyDateTime_GET_YEAR(self.obj.as_ptr()) }
    }

    pub(crate) fn month(&self) -> i32 {
        unsafe { PyDateTime_GET_MONTH(self.obj.as_ptr()) }
    }

    pub(crate) fn day(&self) -> i32 {
        unsafe { PyDateTime_GET_DAY(self.obj.as_ptr()) }
    }

    pub(crate) fn hour(&self) -> i32 {
        unsafe { PyDateTime_DATE_GET_HOUR(self.obj.as_ptr()) }
    }

    pub(crate) fn minute(&self) -> i32 {
        unsafe { PyDateTime_DATE_GET_MINUTE(self.obj.as_ptr()) }
    }

    pub(crate) fn second(&self) -> i32 {
        unsafe { PyDateTime_DATE_GET_SECOND(self.obj.as_ptr()) }
    }

    pub(crate) fn microsecond(&self) -> i32 {
        unsafe { PyDateTime_DATE_GET_MICROSECOND(self.obj.as_ptr()) }
    }

    /// Get a borrowed reference to the tzinfo object. Only valid so
    /// long as the PyDateTime object itself is alive.
    pub(crate) fn tzinfo(&self) -> PyObj {
        // SAFETY: calling CPython API with valid arguments
        unsafe {
            PyObj::from_ptr_unchecked({
                #[cfg(Py_3_10)]
                {
                    PyDateTime_DATE_GET_TZINFO(self.as_ptr())
                }
                #[cfg(not(Py_3_10))]
                {
                    // NOTE: We intentionally let the reference be decreffed
                    // here. This is safe because the PyDateTime object
                    // will keep at least one reference alive
                    self.getattr(c"tzinfo").unwrap().as_ptr()
                }
            })
        }
    }

    pub(crate) fn date(&self) -> PyDate {
        // SAFETY: Date has the same layout
        unsafe { PyDate::from_ptr_unchecked(self.obj.as_ptr()) }
    }

    pub(crate) fn utcoffset(&self) -> PyReturn {
        self.getattr(c"utcoffset")?.call0()
    }
}

impl PyBase for PyDateTime {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyDateTime {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyDateTime {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyDateTime_CheckExact(obj.as_ptr()) != 0 }
    }

    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyDateTime_Check(obj.as_ptr()) != 0 }
    }
}

impl std::fmt::Display for PyDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_repr(f)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PyTimeDelta {
    obj: PyObj,
}

impl PyTimeDelta {
    pub(crate) fn days_component(&self) -> i32 {
        unsafe { PyDateTime_DELTA_GET_DAYS(self.obj.as_ptr()) }
    }

    pub(crate) fn seconds_component(&self) -> i32 {
        unsafe { PyDateTime_DELTA_GET_SECONDS(self.obj.as_ptr()) }
    }

    pub(crate) fn microseconds_component(&self) -> i32 {
        unsafe { PyDateTime_DELTA_GET_MICROSECONDS(self.obj.as_ptr()) }
    }

    pub(crate) fn whole_seconds(self) -> Option<DeltaSeconds> {
        DeltaSeconds::new(
            // SAFETY: timedelta.max days (in seconds) are safely within i64
            i64::from(self.days_component()) * 86400 + i64::from(self.seconds_component()),
        )
    }

    pub(crate) fn subsec(self) -> SubSecNanos {
        // SAFETY: microseconds are always less than 1_000_000
        SubSecNanos::new_unchecked(self.microseconds_component() * 1_000)
    }
}

impl PyBase for PyTimeDelta {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyTimeDelta {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyTimeDelta {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyDelta_CheckExact(obj.as_ptr()) != 0 }
    }

    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyDelta_Check(obj.as_ptr()) != 0 }
    }
}

impl std::fmt::Display for PyTimeDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_repr(f)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PyTime {
    obj: PyObj,
}

impl PyTime {
    pub(crate) fn hour(&self) -> i32 {
        unsafe { PyDateTime_TIME_GET_HOUR(self.obj.as_ptr()) }
    }

    pub(crate) fn minute(&self) -> i32 {
        unsafe { PyDateTime_TIME_GET_MINUTE(self.obj.as_ptr()) }
    }

    pub(crate) fn second(&self) -> i32 {
        unsafe { PyDateTime_TIME_GET_SECOND(self.obj.as_ptr()) }
    }

    pub(crate) fn microsecond(&self) -> i32 {
        unsafe { PyDateTime_TIME_GET_MICROSECOND(self.obj.as_ptr()) }
    }
}

impl PyBase for PyTime {
    fn as_py_obj(&self) -> PyObj {
        self.obj
    }
}

impl FromPy for PyTime {
    unsafe fn from_ptr_unchecked(ptr: *mut PyObject) -> Self {
        Self {
            obj: unsafe { PyObj::from_ptr_unchecked(ptr) },
        }
    }
}

impl PyStaticType for PyTime {
    fn isinstance_exact(obj: impl PyBase) -> bool {
        unsafe { PyTime_CheckExact(obj.as_ptr()) != 0 }
    }

    fn isinstance(obj: impl PyBase) -> bool {
        unsafe { PyTime_Check(obj.as_ptr()) != 0 }
    }
}

impl std::fmt::Display for PyTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_repr(f)
    }
}
