use core::ffi::{CStr, c_int, c_void};
use pyo3_ffi::*;
use std::fmt;
use std::ops::Neg;
use std::ptr::null_mut as NULL;

use crate::{
    classes::{
        date_delta::{self, DateDelta, InitError, Unit as DateUnit, parse_prefix},
        time_delta::{
            self, MAX_HOURS, MAX_MICROSECONDS, MAX_MILLISECONDS, MAX_MINUTES, MAX_SECS, TimeDelta,
        },
    },
    common::scalar::*,
    docstrings as doc,
    py::*,
    pymodule::State,
};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) struct DateTimeDelta {
    // invariant: these never have opposite signs
    pub(crate) ddelta: DateDelta,
    pub(crate) tdelta: TimeDelta,
}

impl DateTimeDelta {
    pub(crate) fn pyhash(self) -> Py_hash_t {
        hash_combine(self.ddelta.pyhash(), self.tdelta.pyhash())
    }

    pub(crate) fn new(ddelta: DateDelta, tdelta: TimeDelta) -> Option<Self> {
        if ddelta.months.get() >= 0 && ddelta.days.get() >= 0 && tdelta.secs.get() >= 0
            || ddelta.months.get() <= 0 && ddelta.days.get() <= 0 && tdelta.secs.get() <= 0
        {
            Some(Self { ddelta, tdelta })
        } else {
            None
        }
    }

    pub(crate) fn checked_mul(self, factor: i32) -> Option<Self> {
        let Self { ddelta, tdelta } = self;
        ddelta
            .checked_mul(factor)
            .zip(tdelta.checked_mul(factor.into()))
            // Safe: multiplication can't result in different signs
            .map(|(ddelta, tdelta)| Self { ddelta, tdelta })
    }

    pub(crate) fn checked_add(self, other: Self) -> Result<Self, InitError> {
        let ddelta = self.ddelta.checked_add(other.ddelta)?;
        let tdelta = self
            .tdelta
            .checked_add(other.tdelta)
            .ok_or(InitError::TooBig)?;
        // Confirm the signs of date- and timedelta didn't get out of sync
        if ddelta.months.get() >= 0 && ddelta.days.get() >= 0 && tdelta.secs.get() >= 0
            || ddelta.months.get() <= 0 && ddelta.days.get() <= 0 && tdelta.secs.get() <= 0
        {
            Ok(Self { ddelta, tdelta })
        } else {
            Err(InitError::MixedSign)
        }
    }

    fn fmt_iso(self) -> String {
        let mut s = String::with_capacity(8);
        let DateTimeDelta { ddelta, tdelta } = if self.tdelta.secs.get() < 0
            || self.ddelta.months.get() < 0
            || self.ddelta.days.get() < 0
        {
            s.push('-');
            -self
        } else if self.tdelta.is_zero() && self.ddelta.is_zero() {
            return "P0D".to_string();
        } else {
            self
        };
        s.push('P');

        if !ddelta.is_zero() {
            date_delta::format_components(ddelta, &mut s);
        }
        if !tdelta.is_zero() {
            s.push('T');
            time_delta::fmt_components_abs(tdelta, &mut s);
        }
        s
    }
}

impl PyWrapped for DateTimeDelta {}

impl Neg for DateTimeDelta {
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            ddelta: -self.ddelta,
            tdelta: -self.tdelta,
        }
    }
}

#[inline]
pub(crate) fn handle_exact_unit(
    value: PyObj,
    max: i64,
    name: &str,
    factor: i128,
) -> PyResult<i128> {
    if let Some(int) = value.cast::<PyInt>() {
        let i = int.to_i64()?;
        (-max..=max)
            .contains(&i)
            .then(|| i as i128 * factor)
            .ok_or_else_value_err(|| format!("{name} out of range"))
    } else if let Some(py_float) = value.cast::<PyFloat>() {
        let f = py_float.to_f64()?;
        (-max as f64..=max as f64)
            .contains(&f)
            .then_some((f * factor as f64) as i128)
            .ok_or_else_value_err(|| format!("{name} out of range"))
    } else {
        raise_value_err(format!("{name} must be an integer or float"))?
    }
}

#[inline]
pub(crate) fn set_units_from_kwargs(
    key: PyObj,
    value: PyObj,
    months: &mut i32,
    days: &mut i32,
    nanos: &mut i128,
    state: &State,
    eq: fn(PyObj, PyObj) -> bool,
) -> PyResult<bool> {
    if eq(key, state.str_years) {
        *months = value
            .cast::<PyInt>()
            .ok_or_value_err("years must be an integer")?
            .to_long()?
            .checked_mul(12)
            .and_then(|y| y.try_into().ok())
            .and_then(|y| months.checked_add(y))
            .ok_or_value_err("total years out of range")?;
    } else if eq(key, state.str_months) {
        *months = value
            .cast::<PyInt>()
            .ok_or_value_err("months must be an integer")?
            .to_long()?
            .try_into()
            .ok()
            .and_then(|m| months.checked_add(m))
            .ok_or_value_err("total months out of range")?;
    } else if eq(key, state.str_weeks) {
        *days = value
            .cast::<PyInt>()
            .ok_or_value_err("weeks must be an integer")?
            .to_long()?
            .checked_mul(7)
            .and_then(|d| d.try_into().ok())
            .and_then(|d| days.checked_add(d))
            .ok_or_value_err("total days out of range")?;
    } else if eq(key, state.str_days) {
        *days = value
            .cast::<PyInt>()
            .ok_or_value_err("days must be an integer")?
            .to_long()?
            .try_into()
            .ok()
            .and_then(|d| days.checked_add(d))
            .ok_or_value_err("total days out of range")?;
    } else if eq(key, state.str_hours) {
        *nanos += handle_exact_unit(value, MAX_HOURS, "hours", 3_600_000_000_000)?;
    } else if eq(key, state.str_minutes) {
        *nanos += handle_exact_unit(value, MAX_MINUTES, "minutes", 60_000_000_000)?;
    } else if eq(key, state.str_seconds) {
        *nanos += handle_exact_unit(value, MAX_SECS, "seconds", 1_000_000_000)?;
    } else if eq(key, state.str_milliseconds) {
        *nanos += handle_exact_unit(value, MAX_MILLISECONDS, "milliseconds", 1_000_000)?;
    } else if eq(key, state.str_microseconds) {
        *nanos += handle_exact_unit(value, MAX_MICROSECONDS, "microseconds", 1_000)?;
    } else if eq(key, state.str_nanoseconds) {
        *nanos = value
            .cast::<PyInt>()
            .ok_or_value_err("nanoseconds must be an integer")?
            .to_i128()?
            .checked_add(*nanos)
            .ok_or_value_err("total nanoseconds out of range")?;
    } else {
        return Ok(false);
    }
    Ok(true)
}

pub(crate) const SINGLETONS: &[(&CStr, DateTimeDelta); 1] = &[(
    c"ZERO",
    DateTimeDelta {
        ddelta: DateDelta::ZERO,
        tdelta: TimeDelta::ZERO,
    },
)];

impl fmt::Display for DateTimeDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A bit inefficient, but this isn't performance-critical
        let mut isofmt = self.fmt_iso().into_bytes();
        // Safe: we know the string is valid ASCII
        for c in isofmt.iter_mut().skip(2) {
            if *c != b'T' {
                *c = c.to_ascii_lowercase();
            }
        }
        f.write_str(
            // SAFETY: we've built the string ourselves out of ASCII
            unsafe { std::str::from_utf8_unchecked(&isofmt) },
        )
    }
}

fn __new__(cls: HeapType<DateTimeDelta>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    let nargs = args.len();
    let nkwargs = kwargs.map_or(0, |k| k.len());

    let mut months: i32 = 0;
    let mut days: i32 = 0;
    let mut nanos: i128 = 0;
    let state = cls.state();
    match (nargs, nkwargs) {
        (0, 0) => DateTimeDelta {
            ddelta: DateDelta {
                months: DeltaMonths::ZERO,
                days: DeltaDays::ZERO,
            },
            tdelta: TimeDelta {
                secs: DeltaSeconds::ZERO,
                subsec: SubSecNanos::MIN,
            },
        }, // OPTIMIZE: return the singleton
        (0, _) => {
            handle_kwargs(
                "DateTimeDelta",
                // SAFETY: if nkwargs > 0, kwargs is Some
                kwargs.unwrap().iteritems(),
                |key, value, eq| {
                    set_units_from_kwargs(key, value, &mut months, &mut days, &mut nanos, state, eq)
                },
            )?;
            if months >= 0 && days >= 0 && nanos >= 0 || months <= 0 && days <= 0 && nanos <= 0 {
                DateTimeDelta {
                    ddelta: DeltaMonths::new(months)
                        .zip(DeltaDays::new(days))
                        .map(|(m, d)| DateDelta { months: m, days: d })
                        .ok_or_value_err("Out of range")?,
                    tdelta: TimeDelta::from_nanos(nanos)
                        .ok_or_value_err("TimeDelta out of range")?,
                }
            } else {
                raise_value_err("Mixed sign in DateTimeDelta")?
            }
        }
        _ => raise_value_err("TimeDelta() takes no positional arguments")?,
    }
    .to_obj(cls)
}

fn __richcmp__(
    cls: HeapType<DateTimeDelta>,
    a: DateTimeDelta,
    b_obj: PyObj,
    op: c_int,
) -> PyReturn {
    match b_obj.extract(cls) {
        Some(b) => match op {
            pyo3_ffi::Py_EQ => (a == b).to_py(),
            pyo3_ffi::Py_NE => (a != b).to_py(),
            _ => not_implemented(),
        },
        None => not_implemented(),
    }
}

extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    hashmask(
        // SAFETY: first argument guaranteed to be self type
        unsafe { slf.assume_heaptype::<DateTimeDelta>() }.1.pyhash(),
    )
}

fn __neg__(cls: HeapType<DateTimeDelta>, d: DateTimeDelta) -> PyReturn {
    (-d).to_obj(cls)
}

extern "C" fn __bool__(slf: PyObj) -> c_int {
    // SAFETY: first argument guaranteed to be self type
    let (_, DateTimeDelta { ddelta, tdelta }) = unsafe { slf.assume_heaptype() };
    (!(ddelta.is_zero() && tdelta.is_zero())).into()
}

fn __repr__(_: PyType, d: DateTimeDelta) -> PyReturn {
    format!("DateTimeDelta({d})").to_py()
}

fn __str__(_: PyType, d: DateTimeDelta) -> PyReturn {
    d.fmt_iso().to_py()
}

fn __mul__(a: PyObj, b: PyObj) -> PyReturn {
    // These checks are needed because the args could be reversed!
    let (delta_obj, factor) = if let Some(i) = b.cast::<PyInt>() {
        (a, i.to_long()?)
    } else if let Some(i) = a.cast::<PyInt>() {
        (b, i.to_long()?)
    } else {
        return not_implemented();
    };

    if factor == 1 {
        return Ok(delta_obj.newref());
    };

    // SAFETY: at this point we know that delta_obj is a DateTimeDelta
    let (delta_type, delta) = unsafe { delta_obj.assume_heaptype::<DateTimeDelta>() };
    i32::try_from(factor)
        .ok()
        .and_then(|f| delta.checked_mul(f))
        .ok_or_value_err("Multiplication factor or result out of bounds")?
        .to_obj(delta_type)
}

fn __add__(a_obj: PyObj, b_obj: PyObj) -> PyReturn {
    _add_method(a_obj, b_obj, false)
}

fn __sub__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    _add_method(obj_a, obj_b, true)
}

#[inline]
fn _add_method(obj_a: PyObj, obj_b: PyObj, negate: bool) -> PyReturn {
    // FUTURE: optimize zero cases
    let type_a = obj_a.type_();
    let type_b = obj_b.type_();
    // The easy case: DateTimeDelta + DateTimeDelta
    let (delta_type, a, mut b) = if type_a == type_b {
        // SAFETY: Both are the same type, and one of them *must* be a DateTimeDelta
        let (delta_type, a) = unsafe { obj_a.assume_heaptype::<DateTimeDelta>() };
        let (_, b) = unsafe { obj_b.assume_heaptype::<DateTimeDelta>() };
        (delta_type, a, b)
    // Other cases are more difficult, as they can be triggered
    // by reflexive operations with arbitrary types.
    // We need to eliminate them carefully.
    } else if let Some(state) = type_a.same_module(type_b) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (delta_type, a) = unsafe { obj_a.assume_heaptype::<DateTimeDelta>() };
        let delta_b = if let Some(ddelta) = obj_b.extract(state.date_delta_type) {
            DateTimeDelta {
                ddelta,
                tdelta: TimeDelta::ZERO,
            }
        } else if let Some(tdelta) = obj_b.extract(state.time_delta_type) {
            DateTimeDelta {
                ddelta: DateDelta::ZERO,
                tdelta,
            }
        } else {
            // We can safely discount other types within our module
            return raise_value_err(format!(
                "unsupported operand type(s) for +/-: {type_a} and {type_b}"
            ));
        };
        // SAFETY: at least one of the objects is a DateTimeDelta
        (delta_type, a, delta_b)
    } else {
        return not_implemented();
    };
    if negate {
        b = -b;
    };
    a.checked_add(b)
        .map_err(|e| {
            value_err(match e {
                InitError::TooBig => "Addition result out of bounds",
                InitError::MixedSign => "Mixed sign in DateTimeDelta",
            })
        })?
        .to_obj(delta_type)
}

fn __abs__(
    cls: HeapType<DateTimeDelta>,
    DateTimeDelta { ddelta, tdelta }: DateTimeDelta,
) -> PyReturn {
    // FUTURE: optimize case where self is already positive
    DateTimeDelta {
        ddelta: ddelta.abs(),
        tdelta: tdelta.abs(),
    }
    .to_obj(cls)
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(DateTimeDelta, Py_tp_new, __new__),
    slotmethod!(DateTimeDelta, Py_tp_richcompare, __richcmp__),
    slotmethod!(DateTimeDelta, Py_nb_negative, __neg__, 1),
    slotmethod!(DateTimeDelta, Py_tp_repr, __repr__, 1),
    slotmethod!(DateTimeDelta, Py_tp_str, __str__, 1),
    slotmethod!(DateTimeDelta, Py_nb_positive, identity1, 1),
    slotmethod!(DateTimeDelta, Py_nb_absolute, __abs__, 1),
    slotmethod!(Py_nb_multiply, __mul__, 2),
    slotmethod!(Py_nb_add, __add__, 2),
    slotmethod!(Py_nb_subtract, __sub__, 2),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::DATETIMEDELTA.as_ptr() as *mut c_void,
    },
    PyType_Slot {
        slot: Py_tp_methods,
        pfunc: unsafe { METHODS.as_ptr() as *mut c_void },
    },
    PyType_Slot {
        slot: Py_tp_hash,
        pfunc: __hash__ as *mut c_void,
    },
    PyType_Slot {
        slot: Py_nb_bool,
        pfunc: __bool__ as *mut c_void,
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

fn format_common_iso(_: PyType, d: DateTimeDelta) -> PyReturn {
    d.fmt_iso().to_py()
}

pub(crate) fn parse_date_components(s: &mut &[u8]) -> Option<DateDelta> {
    let mut months = 0;
    let mut days = 0;
    let mut prev_unit: Option<DateUnit> = None;

    while !s.is_empty() && !s[0].eq_ignore_ascii_case(&b'T') {
        let (value, unit) = date_delta::parse_component(s)?;
        match (unit, prev_unit.replace(unit)) {
            // Note: We prevent overflow by limiting how many digits we parse
            (DateUnit::Years, None) => {
                months += value * 12;
            }
            (DateUnit::Months, None | Some(DateUnit::Years)) => {
                months += value;
            }
            (DateUnit::Weeks, None | Some(DateUnit::Years | DateUnit::Months)) => {
                days += value * 7;
            }
            (DateUnit::Days, _) => {
                days += value;
                break;
            }
            _ => None?, // i.e. the order of the components is wrong
        }
    }
    DeltaMonths::new(months)
        .zip(DeltaDays::new(days))
        .map(|(months, days)| DateDelta { months, days })
}

fn parse_common_iso(cls: HeapType<DateTimeDelta>, arg: PyObj) -> PyReturn {
    let binding = arg
        .cast::<PyStr>()
        .ok_or_value_err("argument must be str")?;

    let s = &mut binding.as_utf8()?;
    let err = || format!("Invalid format or out of range: {arg}");
    if s.len() < 3 {
        // at least `P0D`
        raise_value_err(err())?
    }

    let negated = parse_prefix(s).ok_or_else_value_err(err)?;
    // Safe: we checked the string is at least 3 bytes long
    if s[s.len() - 1].eq_ignore_ascii_case(&b'T') {
        // catch 'empty' cases
        raise_value_err(err())?
    }
    let mut ddelta = parse_date_components(s).ok_or_else_value_err(err)?;
    let mut tdelta = if s.is_empty() {
        TimeDelta::ZERO
    } else if s[0].eq_ignore_ascii_case(&b'T') {
        *s = &s[1..];
        let (nanos, _) = time_delta::parse_all_components(s).ok_or_else_value_err(err)?;
        TimeDelta::from_nanos(nanos).ok_or_else_value_err(err)?
    } else {
        raise_value_err(err())?
    };
    if negated {
        ddelta = -ddelta;
        tdelta = -tdelta;
    }
    DateTimeDelta { ddelta, tdelta }.to_obj(cls)
}

fn in_months_days_secs_nanos(
    _: PyType,
    DateTimeDelta {
        ddelta: DateDelta { months, days },
        tdelta: TimeDelta { secs, subsec },
    }: DateTimeDelta,
) -> PyResult<Owned<PyTuple>> {
    let mut secs = secs.get();
    let nanos = if secs < 0 && subsec.get() > 0 {
        secs += 1;
        subsec.get() - 1_000_000_000
    } else {
        subsec.get()
    };
    (
        months.get().to_py()?,
        days.get().to_py()?,
        secs.to_py()?,
        nanos.to_py()?,
    )
        .into_pytuple()
}

fn date_part(cls: HeapType<DateTimeDelta>, slf: DateTimeDelta) -> PyReturn {
    slf.ddelta.to_obj(cls.state().date_delta_type)
}

fn time_part(cls: HeapType<DateTimeDelta>, slf: DateTimeDelta) -> PyReturn {
    slf.tdelta.to_obj(cls.state().time_delta_type)
}

fn __reduce__(
    cls: HeapType<DateTimeDelta>,
    DateTimeDelta {
        ddelta: DateDelta { months, days },
        tdelta: TimeDelta { secs, subsec },
    }: DateTimeDelta,
) -> PyResult<Owned<PyTuple>> {
    (
        cls.state().unpickle_datetime_delta.newref(),
        // We don't do our own bit packing because the numbers are usually small
        // and Python's pickle protocol handles them more efficiently.
        (
            months.get().to_py()?,
            days.get().to_py()?,
            secs.get().to_py()?,
            subsec.get().to_py()?,
        )
            .into_pytuple()?,
    )
        .into_pytuple()
}

pub(crate) fn unpickle(state: &State, args: &[PyObj]) -> PyReturn {
    match args {
        &[months, days, secs, nanos] => DateTimeDelta {
            ddelta: DateDelta {
                months: DeltaMonths::new_unchecked(
                    months
                        .cast::<PyInt>()
                        .ok_or_type_err("Invalid pickle data")?
                        .to_long()? as _,
                ),
                days: DeltaDays::new_unchecked(
                    days.cast::<PyInt>()
                        .ok_or_type_err("Invalid pickle data")?
                        .to_long()? as _,
                ),
            },
            tdelta: TimeDelta {
                secs: DeltaSeconds::new_unchecked(
                    secs.cast::<PyInt>()
                        .ok_or_type_err("Invalid pickle data")?
                        .to_long()? as _,
                ),
                subsec: SubSecNanos::new_unchecked(
                    nanos
                        .cast::<PyInt>()
                        .ok_or_type_err("Invalid pickle data")?
                        .to_long()? as _,
                ),
            },
        }
        .to_obj(state.datetime_delta_type),
        _ => raise_type_err("Invalid pickle data")?,
    }
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(DateTimeDelta, __copy__, c""),
    method1!(DateTimeDelta, __deepcopy__, c""),
    method0!(
        DateTimeDelta,
        format_common_iso,
        doc::DATETIMEDELTA_FORMAT_COMMON_ISO
    ),
    method0!(DateTimeDelta, date_part, doc::DATETIMEDELTA_DATE_PART),
    method0!(DateTimeDelta, time_part, doc::DATETIMEDELTA_TIME_PART),
    classmethod1!(
        DateTimeDelta,
        parse_common_iso,
        doc::DATETIMEDELTA_PARSE_COMMON_ISO
    ),
    method0!(DateTimeDelta, __reduce__, c""),
    method0!(
        DateTimeDelta,
        in_months_days_secs_nanos,
        doc::DATETIMEDELTA_IN_MONTHS_DAYS_SECS_NANOS
    ),
    classmethod_kwargs!(
        DateTimeDelta,
        __get_pydantic_core_schema__,
        doc::PYDANTIC_SCHEMA
    ),
    PyMethodDef::zeroed(),
];

pub(crate) static mut SPEC: PyType_Spec =
    type_spec::<DateTimeDelta>(c"whenever.DateTimeDelta", unsafe { SLOTS });
