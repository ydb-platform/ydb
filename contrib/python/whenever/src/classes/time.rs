use core::ffi::{CStr, c_int, c_long, c_void};
use pyo3_ffi::*;
use std::fmt::{self, Display, Formatter};
use std::ptr::null_mut as NULL;

use crate::{
    classes::plain_datetime::DateTime,
    common::{parse::Scan, round, scalar::*},
    docstrings as doc,
    py::*,
    pymodule::State,
};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct Time {
    pub(crate) hour: u8,
    pub(crate) minute: u8,
    pub(crate) second: u8,
    pub(crate) subsec: SubSecNanos,
}

impl Time {
    pub(crate) const fn pyhash(&self) -> Py_hash_t {
        #[cfg(target_pointer_width = "64")]
        {
            ((self.hour as Py_hash_t) << 48)
                | ((self.minute as Py_hash_t) << 40)
                | ((self.second as Py_hash_t) << 32)
                | (self.subsec.get() as Py_hash_t)
        }
        #[cfg(target_pointer_width = "32")]
        {
            hash_combine(
                (self.hour as Py_hash_t) << 16
                    | (self.minute as Py_hash_t) << 8
                    | (self.second as Py_hash_t),
                self.subsec.get() as Py_hash_t,
            )
        }
    }

    pub(crate) const fn total_seconds(&self) -> u32 {
        self.hour as u32 * 3600 + self.minute as u32 * 60 + self.second as u32
    }

    pub(crate) const fn from_sec_subsec(sec: u32, subsec: SubSecNanos) -> Self {
        Time {
            hour: (sec / 3600) as u8,
            minute: ((sec % 3600) / 60) as u8,
            second: (sec % 60) as u8,
            subsec,
        }
    }

    pub(crate) const fn total_nanos(&self) -> u64 {
        self.subsec.get() as u64 + self.total_seconds() as u64 * 1_000_000_000
    }

    pub(crate) fn from_total_nanos_unchecked(nanos: u64) -> Self {
        Time {
            hour: (nanos / 3_600_000_000_000) as u8,
            minute: ((nanos % 3_600_000_000_000) / 60_000_000_000) as u8,
            second: ((nanos % 60_000_000_000) / 1_000_000_000) as u8,
            subsec: SubSecNanos::from_remainder(nanos),
        }
    }

    pub(crate) fn from_longs(
        hour: c_long,
        minute: c_long,
        second: c_long,
        nanos: c_long,
    ) -> Option<Self> {
        if (0..24).contains(&hour) && (0..60).contains(&minute) && (0..60).contains(&second) {
            Some(Time {
                hour: hour as u8,
                minute: minute as u8,
                second: second as u8,
                subsec: SubSecNanos::from_long(nanos)?,
            })
        } else {
            None
        }
    }

    /// Read a time string in the ISO 8601 extended format (i.e. with ':' separators)
    pub(crate) fn read_iso_extended(s: &mut Scan) -> Option<Self> {
        // FUTURE: potential double checks coming from some callers
        let hour = s.digits00_23()?;
        let (minute, second, subsec) = match s.advance_on(b':') {
            // The "extended" format with mandatory ':' between components
            Some(true) => {
                let min = s.digits00_59()?;
                // seconds are still optional at this point
                let (sec, subsec) = match s.advance_on(b':') {
                    Some(true) => s.digits00_59().zip(s.subsec())?,
                    _ => (0, SubSecNanos::MIN),
                };
                (min, sec, subsec)
            }
            // No components besides hour
            _ => (0, 0, SubSecNanos::MIN),
        };
        Some(Time {
            hour,
            minute,
            second,
            subsec,
        })
    }

    pub(crate) fn read_iso_basic(s: &mut Scan) -> Option<Self> {
        let hour = s.digits00_23()?;
        let (minute, second, subsec) = match s.digits00_59() {
            Some(m) => {
                let (sec, sub) = match s.digits00_59() {
                    Some(n) => (n, s.subsec().unwrap_or(SubSecNanos::MIN)),
                    None => (0, SubSecNanos::MIN),
                };
                (m, sec, sub)
            }
            None => (0, 0, SubSecNanos::MIN),
        };
        Some(Time {
            hour,
            minute,
            second,
            subsec,
        })
    }

    pub(crate) fn read_iso(s: &mut Scan) -> Option<Self> {
        match s.get(2) {
            Some(b':') => Self::read_iso_extended(s),
            _ => Self::read_iso_basic(s),
        }
    }

    pub(crate) fn parse_iso(s: &[u8]) -> Option<Self> {
        Scan::new(s).parse_all(Self::read_iso)
    }

    /// Round the time to the specified increment
    ///
    /// Returns the rounded time and whether it has wrapped around to the next day (0 or 1)
    /// The increment is given in ns must be a divisor of 24 hours
    pub(crate) fn round(self, increment: u64, mode: round::Mode) -> (Self, u64) {
        debug_assert!(86_400_000_000_000 % increment == 0);
        let total_nanos = self.total_nanos();
        let quotient = total_nanos / increment;
        let remainder = total_nanos % increment;

        let threshold = match mode {
            round::Mode::HalfEven => 1.max(increment / 2 + (quotient % 2 == 0) as u64),
            round::Mode::Ceil => 1,
            round::Mode::Floor => increment + 1,
            round::Mode::HalfFloor => increment / 2 + 1,
            round::Mode::HalfCeil => 1.max(increment / 2),
        };
        let round_up = remainder >= threshold;
        let ns_since_midnight = (quotient + round_up as u64) * increment;
        (
            Self::from_total_nanos_unchecked(ns_since_midnight % 86_400_000_000_000),
            ns_since_midnight / 86_400_000_000_000,
        )
    }

    pub(crate) fn from_py(t: PyTime) -> Self {
        Time {
            hour: t.hour() as _,
            minute: t.minute() as _,
            second: t.second() as _,
            // SAFETY: microseconds are always sub-second
            subsec: SubSecNanos::new_unchecked((t.microsecond() * 1_000) as _),
        }
    }

    pub(crate) fn from_py_dt_with_subsec(dt: PyDateTime, subsec: SubSecNanos) -> Self {
        Time {
            hour: dt.hour() as _,
            minute: dt.minute() as _,
            second: dt.second() as _,
            subsec,
        }
    }

    pub(crate) fn from_py_dt(dt: PyDateTime) -> Self {
        Time {
            hour: dt.hour() as _,
            minute: dt.minute() as _,
            second: dt.second() as _,
            // SAFETY: microseconds are always sub-second
            subsec: SubSecNanos::new_unchecked((dt.microsecond() * 1_000) as _),
        }
    }

    pub(crate) const MIDNIGHT: Time = Time {
        hour: 0,
        minute: 0,
        second: 0,
        subsec: SubSecNanos::MIN,
    };
}

impl PyWrapped for Time {}

// FUTURE: a trait for faster formatting since timestamp are small and
// limited in length?
impl Display for Time {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02}:{:02}:{:02}{}",
            self.hour, self.minute, self.second, self.subsec
        )
    }
}

pub(crate) const SINGLETONS: &[(&CStr, Time); 4] = &[
    (c"MIN", Time::MIDNIGHT),
    (c"MIDNIGHT", Time::MIDNIGHT),
    (
        c"NOON",
        Time {
            hour: 12,
            minute: 0,
            second: 0,
            subsec: SubSecNanos::MIN,
        },
    ),
    (
        c"MAX",
        Time {
            hour: 23,
            minute: 59,
            second: 59,
            subsec: SubSecNanos::MAX,
        },
    ),
];

fn __new__(cls: HeapType<Time>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    let mut hour: c_long = 0;
    let mut minute: c_long = 0;
    let mut second: c_long = 0;
    let mut nanosecond: c_long = 0;

    parse_args_kwargs!(
        args,
        kwargs,
        c"|lll$l:Time",
        hour,
        minute,
        second,
        nanosecond
    );

    Time::from_longs(hour, minute, second, nanosecond)
        .ok_or_value_err("Invalid time component value")?
        .to_obj(cls)
}

fn __repr__(_: PyType, slf: Time) -> PyReturn {
    format!("Time({slf})").to_py()
}

extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    // SAFETY: self type is always passed to __hash__
    hashmask(unsafe { slf.assume_heaptype::<Time>() }.1.pyhash())
}

fn __richcmp__(cls: HeapType<Time>, slf: Time, arg: PyObj, op: c_int) -> PyReturn {
    match arg.extract(cls) {
        Some(b) => match op {
            pyo3_ffi::Py_EQ => slf == b,
            pyo3_ffi::Py_NE => slf != b,
            pyo3_ffi::Py_LT => slf < b,
            pyo3_ffi::Py_LE => slf <= b,
            pyo3_ffi::Py_GT => slf > b,
            pyo3_ffi::Py_GE => slf >= b,
            _ => unreachable!(),
        }
        .to_py(),
        None => not_implemented(),
    }
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(Time, Py_tp_new, __new__),
    slotmethod!(Time, Py_tp_str, format_common_iso, 1),
    slotmethod!(Time, Py_tp_repr, __repr__, 1),
    slotmethod!(Time, Py_tp_richcompare, __richcmp__),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::TIME.as_ptr() as *mut c_void,
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

fn py_time(cls: HeapType<Time>, slf: Time) -> PyReturn {
    let Time {
        hour,
        minute,
        second,
        subsec,
    } = slf;
    let &PyDateTime_CAPI {
        Time_FromTime,
        TimeType,
        ..
    } = cls.state().py_api;
    // SAFETY: calling C API with valid arguments
    unsafe {
        Time_FromTime(
            hour.into(),
            minute.into(),
            second.into(),
            (subsec.get() / 1_000) as c_int,
            Py_None(),
            TimeType,
        )
    }
    .rust_owned()
}

fn from_py_time(cls: HeapType<Time>, arg: PyObj) -> PyReturn {
    Time::from_py(
        arg.cast_allow_subclass::<PyTime>()
            .ok_or_type_err("argument must be a datetime.time")?,
    )
    .to_obj(cls)
}

fn format_common_iso(_: PyType, slf: Time) -> PyReturn {
    format!("{slf}").to_py()
}

fn __reduce__(cls: HeapType<Time>, slf: Time) -> PyResult<Owned<PyTuple>> {
    let Time {
        hour,
        minute,
        second,
        subsec: nanos,
    } = slf;
    let data = pack![hour, minute, second, nanos.get()];
    (
        cls.state().unpickle_time.newref(),
        (data.to_py()?,).into_pytuple()?,
    )
        .into_pytuple()
}

fn parse_common_iso(cls: HeapType<Time>, s: PyObj) -> PyReturn {
    Time::parse_iso(
        s.cast::<PyStr>()
            .ok_or_type_err("Argument must be a string")?
            .as_utf8()?,
    )
    .ok_or_else_value_err(|| format!("Invalid format: {s}"))?
    .to_obj(cls)
}

fn on(cls: HeapType<Time>, slf: Time, arg: PyObj) -> PyReturn {
    let &State {
        plain_datetime_type,
        date_type,
        ..
    } = cls.state();

    if let Some(date) = arg.extract(date_type) {
        DateTime { date, time: slf }.to_obj(plain_datetime_type)
    } else {
        raise_type_err("argument must be a date")
    }
}

fn replace(cls: HeapType<Time>, slf: Time, args: &[PyObj], kwargs: &mut IterKwargs) -> PyReturn {
    let &State {
        str_hour,
        str_minute,
        str_second,
        str_nanosecond,
        ..
    } = cls.state();
    if !args.is_empty() {
        raise_type_err("replace() takes no positional arguments")
    } else {
        let mut hour = slf.hour.into();
        let mut minute = slf.minute.into();
        let mut second = slf.second.into();
        let mut nanos = slf.subsec.get() as _;
        handle_kwargs("replace", kwargs, |key, value, eq| {
            if eq(key, str_hour) {
                hour = value
                    .cast::<PyInt>()
                    .ok_or_type_err("hour must be an integer")?
                    .to_long()?;
            } else if eq(key, str_minute) {
                minute = value
                    .cast::<PyInt>()
                    .ok_or_type_err("minute must be an integer")?
                    .to_long()?;
            } else if eq(key, str_second) {
                second = value
                    .cast::<PyInt>()
                    .ok_or_type_err("second must be an integer")?
                    .to_long()?;
            } else if eq(key, str_nanosecond) {
                nanos = value
                    .cast::<PyInt>()
                    .ok_or_type_err("nanosecond must be an integer")?
                    .to_long()?;
            } else {
                return Ok(false);
            }
            Ok(true)
        })?;
        Time::from_longs(hour, minute, second, nanos)
            .ok_or_value_err("Invalid time component value")?
            .to_obj(cls)
    }
}

fn round(cls: HeapType<Time>, slf: Time, args: &[PyObj], kwargs: &mut IterKwargs) -> PyReturn {
    let (unit, increment, mode) = round::parse_args(cls.state(), args, kwargs, false, false)?;
    if unit == round::Unit::Day {
        raise_value_err("Cannot round Time to day")?;
    } else if unit == round::Unit::Hour && 86_400_000_000_000 % increment != 0 {
        raise_value_err("increment must be a divisor of 24")?;
    }
    slf.round(increment as u64, mode).0.to_obj(cls)
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(Time, __copy__, c""),
    method1!(Time, __deepcopy__, c""),
    method0!(Time, __reduce__, c""),
    method0!(Time, py_time, doc::TIME_PY_TIME),
    method_kwargs!(Time, replace, doc::TIME_REPLACE),
    method0!(Time, format_common_iso, doc::TIME_FORMAT_COMMON_ISO),
    classmethod1!(Time, parse_common_iso, doc::TIME_PARSE_COMMON_ISO),
    classmethod1!(Time, from_py_time, doc::TIME_FROM_PY_TIME),
    method1!(Time, on, doc::TIME_ON),
    method_kwargs!(Time, round, doc::TIME_ROUND),
    classmethod_kwargs!(Time, __get_pydantic_core_schema__, doc::PYDANTIC_SCHEMA),
    PyMethodDef::zeroed(),
];

pub(crate) fn unpickle(state: &State, arg: PyObj) -> PyReturn {
    let py_bytes = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;

    let mut data = py_bytes.as_bytes()?;
    if data.len() != 7 {
        raise_type_err("Invalid pickle data")?
    }
    Time {
        hour: unpack_one!(data, u8),
        minute: unpack_one!(data, u8),
        second: unpack_one!(data, u8),
        subsec: SubSecNanos::new_unchecked(unpack_one!(data, i32)),
    }
    .to_obj(state.time_type)
}

fn hour(_: PyType, slf: Time) -> PyReturn {
    slf.hour.to_py()
}

fn minute(_: PyType, slf: Time) -> PyReturn {
    slf.minute.to_py()
}

fn second(_: PyType, slf: Time) -> PyReturn {
    slf.second.to_py()
}

fn nanosecond(_: PyType, slf: Time) -> PyReturn {
    slf.subsec.get().to_py()
}

static mut GETSETTERS: &[PyGetSetDef] = &[
    getter!(Time, hour, "The hour component"),
    getter!(Time, minute, "The minute component"),
    getter!(Time, second, "The second component"),
    getter!(Time, nanosecond, "The nanosecond component"),
    PyGetSetDef {
        name: NULL(),
        get: None,
        set: None,
        doc: NULL(),
        closure: NULL(),
    },
];

pub(crate) static mut SPEC: PyType_Spec = type_spec::<Time>(c"whenever.Time", unsafe { SLOTS });
