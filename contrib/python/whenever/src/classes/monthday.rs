use core::ffi::{CStr, c_int, c_long, c_void};
use core::ptr::null_mut as NULL;
use pyo3_ffi::*;
use std::fmt::{self, Display, Formatter};

use crate::{
    classes::date::Date,
    common::{parse::extract_2_digits, scalar::*},
    docstrings as doc,
    py::*,
    pymodule::State,
};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub(crate) struct MonthDay {
    pub(crate) month: Month,
    pub(crate) day: u8,
}

pub(crate) const SINGLETONS: &[(&CStr, MonthDay); 2] = &[
    (c"MIN", MonthDay::new_unchecked(Month::January, 1)),
    (c"MAX", MonthDay::new_unchecked(Month::December, 31)),
];

const LEAP_YEAR: Year = Year::new_unchecked(4);

impl MonthDay {
    pub(crate) const fn hash(self) -> i32 {
        ((self.month as i32) << 8) | self.day as i32
    }

    pub(crate) fn from_longs(m: c_long, d: c_long) -> Option<Self> {
        let month = Month::from_long(m)?;
        if d >= 1 && d <= LEAP_YEAR.days_in_month(month) as _ {
            Some(MonthDay { month, day: d as _ })
        } else {
            None
        }
    }

    pub(crate) const fn new(month: Month, day: u8) -> Option<Self> {
        if day > 0 && day <= LEAP_YEAR.days_in_month(month) {
            Some(MonthDay { month, day })
        } else {
            None
        }
    }

    pub(crate) const fn new_unchecked(month: Month, day: u8) -> Self {
        debug_assert!(day > 0 && day <= LEAP_YEAR.days_in_month(month));
        MonthDay { month, day }
    }

    pub(crate) fn parse(s: &[u8]) -> Option<Self> {
        if &s[..2] != b"--" {
            return None;
        }
        if s.len() == 7 && s[4] == b'-' {
            MonthDay::new(
                extract_2_digits(s, 2).and_then(Month::new)?,
                extract_2_digits(s, 5)?,
            )
        } else if s.len() == 6 {
            MonthDay::new(
                extract_2_digits(s, 2).and_then(Month::new)?,
                extract_2_digits(s, 4)?,
            )
        } else {
            None
        }
    }
}

impl PyWrapped for MonthDay {}

impl Display for MonthDay {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "--{:02}-{:02}", self.month.get(), self.day)
    }
}

fn __new__(cls: HeapType<MonthDay>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    let mut month: c_long = 0;
    let mut day: c_long = 0;
    parse_args_kwargs!(args, kwargs, c"ll:MonthDay", month, day);
    MonthDay::from_longs(month, day)
        .ok_or_value_err("Invalid month/day component value")?
        .to_obj(cls)
}

fn __repr__(_: PyType, slf: MonthDay) -> PyReturn {
    format!("MonthDay({slf})").to_py()
}

extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    // SAFETY: we know self is passed to this method
    unsafe { slf.assume_heaptype::<MonthDay>() }.1.hash() as Py_hash_t
}

fn __str__(_: PyType, slf: MonthDay) -> PyReturn {
    format!("{slf}").to_py()
}

fn __richcmp__(cls: HeapType<MonthDay>, a: MonthDay, b_obj: PyObj, op: c_int) -> PyReturn {
    match b_obj.extract(cls) {
        Some(b) => match op {
            pyo3_ffi::Py_LT => a < b,
            pyo3_ffi::Py_LE => a <= b,
            pyo3_ffi::Py_EQ => a == b,
            pyo3_ffi::Py_NE => a != b,
            pyo3_ffi::Py_GT => a > b,
            pyo3_ffi::Py_GE => a >= b,
            _ => unreachable!(),
        }
        .to_py(),
        None => not_implemented(),
    }
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(MonthDay, Py_tp_new, __new__),
    slotmethod!(MonthDay, Py_tp_str, __str__, 1),
    slotmethod!(MonthDay, Py_tp_repr, __repr__, 1),
    slotmethod!(MonthDay, Py_tp_richcompare, __richcmp__),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::MONTHDAY.as_ptr() as *mut c_void,
    },
    PyType_Slot {
        slot: Py_tp_methods,
        pfunc: unsafe { METHODS.as_ptr() as *mut c_void },
    },
    PyType_Slot {
        slot: Py_tp_getset,
        pfunc: unsafe { GETSETTERS.as_ptr() as *mut c_void },
    },
    PyType_Slot {
        slot: Py_tp_hash,
        pfunc: __hash__ as *mut c_void,
    },
    PyType_Slot {
        slot: Py_tp_dealloc,
        pfunc: generic_dealloc as *mut c_void,
    },
    PyType_Slot {
        slot: 0,
        pfunc: NULL(),
    },
];

fn format_common_iso(cls: PyType, slf: MonthDay) -> PyReturn {
    __str__(cls, slf)
}

fn parse_common_iso(cls: HeapType<MonthDay>, s: PyObj) -> PyReturn {
    MonthDay::parse(
        s.cast::<PyStr>()
            .ok_or_type_err("argument must be str")?
            .as_utf8()?,
    )
    .ok_or_else_value_err(|| format!("Invalid format: {s}"))?
    .to_obj(cls)
}

fn __reduce__(
    cls: HeapType<MonthDay>,
    MonthDay { month, day }: MonthDay,
) -> PyResult<Owned<PyTuple>> {
    (
        cls.state().unpickle_monthday.newref(),
        (pack![month.get(), day].to_py()?,).into_pytuple()?,
    )
        .into_pytuple()
}

fn replace(
    cls: HeapType<MonthDay>,
    md: MonthDay,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let &State {
        str_month, str_day, ..
    } = cls.state();
    if !args.is_empty() {
        raise_type_err("replace() takes no positional arguments")
    } else {
        let mut month = md.month.get().into();
        let mut day = md.day.into();
        handle_kwargs("replace", kwargs, |key, value, eq| {
            if eq(key, str_month) {
                month = value
                    .cast::<PyInt>()
                    .ok_or_type_err("month must be an integer")?
                    .to_long()?;
            } else if eq(key, str_day) {
                day = value
                    .cast::<PyInt>()
                    .ok_or_type_err("day must be an integer")?
                    .to_long()?;
            } else {
                return Ok(false);
            }
            Ok(true)
        })?;
        MonthDay::from_longs(month, day)
            .ok_or_value_err("Invalid month/day components")?
            .to_obj(cls)
    }
}

fn in_year(
    cls: HeapType<MonthDay>,
    MonthDay { month, day }: MonthDay,
    year_obj: PyObj,
) -> PyReturn {
    let year = Year::from_long(
        year_obj
            .cast::<PyInt>()
            .ok_or_type_err("year must be an integer")?
            .to_long()?,
    )
    .ok_or_value_err("year out of range")?;
    // OPTIMIZE: we don't need to check the validity of the month again
    Date::new(year, month, day)
        .ok_or_value_err("Invalid date components")?
        .to_obj(cls.state().date_type)
}

fn is_leap(_: PyType, MonthDay { month, day }: MonthDay) -> PyReturn {
    (day == 29 && month == Month::February).to_py()
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(MonthDay, __reduce__, c""),
    method0!(MonthDay, __copy__, c""),
    method1!(MonthDay, __deepcopy__, c""),
    method0!(MonthDay, format_common_iso, doc::MONTHDAY_FORMAT_COMMON_ISO),
    classmethod1!(MonthDay, parse_common_iso, doc::MONTHDAY_PARSE_COMMON_ISO),
    method1!(MonthDay, in_year, doc::MONTHDAY_IN_YEAR),
    method0!(MonthDay, is_leap, doc::MONTHDAY_IS_LEAP),
    method_kwargs!(MonthDay, replace, doc::MONTHDAY_REPLACE),
    classmethod_kwargs!(MonthDay, __get_pydantic_core_schema__, doc::PYDANTIC_SCHEMA),
    PyMethodDef::zeroed(),
];

pub(crate) fn unpickle(state: &State, arg: PyObj) -> PyReturn {
    let py_bytes = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;
    let mut packed = py_bytes.as_bytes()?;
    if packed.len() != 2 {
        raise_value_err("Invalid pickle data")?
    }
    MonthDay {
        month: Month::new_unchecked(unpack_one!(packed, u8)),
        day: unpack_one!(packed, u8),
    }
    .to_obj(state.monthday_type)
}

fn month(_: PyType, slf: MonthDay) -> PyReturn {
    slf.month.get().to_py()
}

fn day(_: PyType, slf: MonthDay) -> PyReturn {
    slf.day.to_py()
}

static mut GETSETTERS: &[PyGetSetDef] = &[
    getter!(MonthDay, month, "The month component"),
    getter!(MonthDay, day, "The day component"),
    PyGetSetDef {
        name: NULL(),
        get: None,
        set: None,
        doc: NULL(),
        closure: NULL(),
    },
];

pub(crate) static mut SPEC: PyType_Spec =
    type_spec::<MonthDay>(c"whenever.MonthDay", unsafe { SLOTS });
