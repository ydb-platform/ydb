use core::{
    ffi::{CStr, c_int, c_long, c_void},
    ptr::null_mut as NULL,
};
use pyo3_ffi::*;
use std::fmt::{self, Display, Formatter};

use crate::{
    classes::date::{Date, extract_year},
    common::{parse::extract_2_digits, scalar::*},
    docstrings as doc,
    py::*,
    pymodule::State,
};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub(crate) struct YearMonth {
    pub(crate) year: Year,
    pub(crate) month: Month,
}

pub(crate) const SINGLETONS: &[(&CStr, YearMonth); 2] = &[
    (c"MIN", YearMonth::new_unchecked(1, 1)),
    (c"MAX", YearMonth::new_unchecked(9999, 12)),
];

impl YearMonth {
    pub(crate) const fn new(year: Year, month: Month) -> Self {
        YearMonth { year, month }
    }

    pub(crate) const fn new_unchecked(year: u16, month: u8) -> Self {
        debug_assert!(year != 0);
        debug_assert!(year <= Year::MAX.get() as _);
        debug_assert!(month >= 1 && month <= 12);
        YearMonth {
            year: Year::new_unchecked(year),
            month: Month::new_unchecked(month),
        }
    }
    pub(crate) fn from_longs(y: c_long, m: c_long) -> Option<Self> {
        Some(YearMonth {
            year: Year::from_long(y)?,
            month: Month::from_long(m)?,
        })
    }

    pub(crate) const fn hash(self) -> i32 {
        ((self.year.get() as i32) << 4) | self.month as i32
    }

    pub(crate) fn parse(s: &[u8]) -> Option<Self> {
        if s.len() == 7 && s[4] == b'-' {
            Some(YearMonth::new(
                extract_year(s, 0)?,
                extract_2_digits(s, 5).and_then(Month::new)?,
            ))
        } else if s.len() == 6 {
            Some(YearMonth::new(
                extract_year(s, 0)?,
                extract_2_digits(s, 4).and_then(Month::new)?,
            ))
        } else {
            None
        }
    }
}

impl PyWrapped for YearMonth {}

impl Display for YearMonth {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.year.get(), self.month.get())
    }
}

fn __new__(cls: HeapType<YearMonth>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    let mut year: c_long = 0;
    let mut month: c_long = 0;
    parse_args_kwargs!(args, kwargs, c"ll:YearMonth", year, month);
    YearMonth::from_longs(year, month)
        .ok_or_value_err("Invalid year/month component value")?
        .to_obj(cls)
}

fn __repr__(_: PyType, slf: YearMonth) -> PyReturn {
    format!("YearMonth({slf})").to_py()
}

fn __str__(_: PyType, slf: YearMonth) -> PyReturn {
    format!("{slf}").to_py()
}

extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    // SAFETY: we know self is passed to this method
    unsafe { slf.assume_heaptype::<YearMonth>() }.1.hash() as Py_hash_t
}

fn __richcmp__(cls: HeapType<YearMonth>, a: YearMonth, b_obj: PyObj, op: c_int) -> PyReturn {
    match b_obj.extract(cls) {
        Some(b) => match op {
            pyo3_ffi::Py_EQ => a == b,
            pyo3_ffi::Py_NE => a != b,
            pyo3_ffi::Py_LT => a < b,
            pyo3_ffi::Py_LE => a <= b,
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
    slotmethod!(YearMonth, Py_tp_new, __new__),
    slotmethod!(YearMonth, Py_tp_str, __str__, 1),
    slotmethod!(YearMonth, Py_tp_repr, __repr__, 1),
    slotmethod!(YearMonth, Py_tp_richcompare, __richcmp__),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::YEARMONTH.as_ptr() as *mut c_void,
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

fn format_common_iso(cls: PyType, slf: YearMonth) -> PyReturn {
    __str__(cls, slf)
}

fn parse_common_iso(cls: HeapType<YearMonth>, arg: PyObj) -> PyReturn {
    let py_str = arg.cast::<PyStr>().ok_or_type_err("argument must be str")?;
    YearMonth::parse(py_str.as_utf8()?)
        .ok_or_else_value_err(|| format!("Invalid format: {arg}"))?
        .to_obj(cls)
}

fn __reduce__(cls: HeapType<YearMonth>, slf: YearMonth) -> PyResult<Owned<PyTuple>> {
    let YearMonth { year, month } = slf;
    (
        cls.state().unpickle_yearmonth.newref(),
        (pack![year.get(), month.get()].to_py()?,).into_pytuple()?,
    )
        .into_pytuple()
}

fn replace(
    cls: HeapType<YearMonth>,
    slf: YearMonth,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let &State {
        str_year,
        str_month,
        ..
    } = cls.state();
    if !args.is_empty() {
        raise_type_err("replace() takes no positional arguments")
    } else {
        let mut year = slf.year.get().into();
        let mut month = slf.month.get().into();
        handle_kwargs("replace", kwargs, |key, value, eq| {
            if eq(key, str_year) {
                year = value
                    .cast::<PyInt>()
                    .ok_or_type_err("year must be an integer")?
                    .to_long()?;
            } else if eq(key, str_month) {
                month = value
                    .cast::<PyInt>()
                    .ok_or_type_err("month must be an integer")?
                    .to_long()?;
            } else {
                return Ok(false);
            }
            Ok(true)
        })?;
        YearMonth::from_longs(year, month)
            .ok_or_value_err("Invalid year/month components")?
            .to_obj(cls)
    }
}

fn on_day(cls: HeapType<YearMonth>, slf: YearMonth, arg: PyObj) -> PyReturn {
    let YearMonth { year, month } = slf;
    let day = arg
        .cast::<PyInt>()
        .ok_or_type_err("day must be an integer")?
        .to_long()?
        .try_into()
        .ok()
        .ok_or_value_err("day out of range")?;
    // OPTIMIZE: we don't need to check the validity of the year and month again
    Date::new(year, month, day)
        .ok_or_value_err("Invalid date components")?
        .to_obj(cls.state().date_type)
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(YearMonth, __copy__, c""),
    method1!(YearMonth, __deepcopy__, c""),
    method0!(YearMonth, __reduce__, c""),
    method0!(
        YearMonth,
        format_common_iso,
        doc::YEARMONTH_FORMAT_COMMON_ISO
    ),
    classmethod1!(YearMonth, parse_common_iso, doc::YEARMONTH_PARSE_COMMON_ISO),
    method1!(YearMonth, on_day, doc::YEARMONTH_ON_DAY),
    method_kwargs!(YearMonth, replace, doc::YEARMONTH_REPLACE),
    classmethod_kwargs!(
        YearMonth,
        __get_pydantic_core_schema__,
        doc::PYDANTIC_SCHEMA
    ),
    PyMethodDef::zeroed(),
];

pub(crate) fn unpickle(state: &State, arg: PyObj) -> PyReturn {
    let py_bytes = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;
    let mut packed = py_bytes.as_bytes()?;
    if packed.len() != 3 {
        raise_value_err("Invalid pickle data")?
    }
    YearMonth {
        year: Year::new_unchecked(unpack_one!(packed, u16)),
        month: Month::new_unchecked(unpack_one!(packed, u8)),
    }
    .to_obj(state.yearmonth_type)
}

fn year(_: PyType, slf: YearMonth) -> PyReturn {
    slf.year.get().to_py()
}

fn month(_: PyType, slf: YearMonth) -> PyReturn {
    slf.month.get().to_py()
}

static mut GETSETTERS: &[PyGetSetDef] = &[
    getter!(YearMonth, year, "The year component"),
    getter!(YearMonth, month, "The month component"),
    PyGetSetDef {
        name: NULL(),
        get: None,
        set: None,
        doc: NULL(),
        closure: NULL(),
    },
];

pub(crate) static mut SPEC: PyType_Spec =
    type_spec::<YearMonth>(c"whenever.YearMonth", unsafe { SLOTS });
