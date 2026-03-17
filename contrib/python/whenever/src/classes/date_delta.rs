use core::ffi::{CStr, c_int, c_long, c_void};
use pyo3_ffi::*;
use std::fmt;
use std::ops::Neg;
use std::ptr::null_mut as NULL;

use crate::{
    classes::{datetime_delta::DateTimeDelta, time_delta::TimeDelta},
    common::scalar::*,
    docstrings as doc,
    py::*,
    pymodule::State,
};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) struct DateDelta {
    // invariant: these never have opposite signs
    pub(crate) months: DeltaMonths,
    pub(crate) days: DeltaDays,
}

pub(crate) enum InitError {
    TooBig,
    MixedSign,
}

impl DateDelta {
    pub(crate) fn pyhash(self) -> Py_hash_t {
        #[cfg(target_pointer_width = "64")]
        {
            self.months.get() as Py_hash_t | ((self.days.get() as Py_hash_t) << 32)
        }
        #[cfg(target_pointer_width = "32")]
        {
            hash_combine(self.months.get() as Py_hash_t, self.days.get() as Py_hash_t)
        }
    }

    /// Construct a new `DateDelta` from the given months and days.
    /// Returns `None` if the signs are mixed.
    pub(crate) fn new(months: DeltaMonths, days: DeltaDays) -> Option<Self> {
        same_sign(months, days).then_some(Self { months, days })
    }

    pub(crate) fn from_months(months: DeltaMonths) -> Self {
        Self {
            months,
            days: DeltaDays::ZERO,
        }
    }

    pub(crate) fn from_days(days: DeltaDays) -> Self {
        Self {
            months: DeltaMonths::ZERO,
            days,
        }
    }

    pub(crate) fn checked_mul(self, factor: i32) -> Option<Self> {
        let Self { months, days } = self;
        months
            .mul(factor)
            .zip(days.mul(factor))
            // Safety: multiplication can't result in different signs
            .map(|(months, days)| Self { months, days })
    }

    pub(crate) fn checked_add(self, other: Self) -> Result<Self, InitError> {
        let Self { months, days } = self;
        let (month_sum, day_sum) = months
            .add(other.months)
            .zip(days.add(other.days))
            .ok_or(InitError::TooBig)?;
        // Note: addition *can* result in different signs
        Self::new(month_sum, day_sum).ok_or(InitError::MixedSign)
    }

    pub(crate) fn is_zero(self) -> bool {
        self.months.get() == 0 && self.days.get() == 0
    }

    pub(crate) fn abs(self) -> Self {
        Self {
            months: self.months.abs(),
            days: self.days.abs(),
        }
    }

    pub(crate) const ZERO: Self = Self {
        months: DeltaMonths::ZERO,
        days: DeltaDays::ZERO,
    };

    fn fmt_iso(self) -> String {
        let mut s = String::with_capacity(8);
        let Self { months, days } = self;
        let self_abs = if months.get() < 0 || days.get() < 0 {
            s.push('-');
            -self
        } else if months.get() == 0 && days.get() == 0 {
            return "P0D".to_string();
        } else {
            self
        };
        s.push('P');
        format_components(self_abs, &mut s);
        s
    }
}

impl PyWrapped for DateDelta {}

impl Neg for DateDelta {
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            // Arithmetic overflow is impossible due to the ranges
            months: -self.months,
            days: -self.days,
        }
    }
}

fn same_sign(months: DeltaMonths, days: DeltaDays) -> bool {
    months.get() >= 0 && days.get() >= 0 || months.get() <= 0 && days.get() <= 0
}

pub(crate) const SINGLETONS: &[(&CStr, DateDelta); 1] = &[(c"ZERO", DateDelta::ZERO)];

impl fmt::Display for DateDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A bit wasteful, but this isn't performance critical
        let mut isofmt = self.fmt_iso().into_bytes();
        // Safe: we know the string is valid ASCII
        for c in isofmt.iter_mut().skip(2) {
            *c = c.to_ascii_lowercase();
        }
        f.write_str(&
            // SAFETY: We've built the string ourselves from ASCII
            unsafe { String::from_utf8_unchecked(isofmt) })
    }
}

// NOTE: delta must be positive
pub(crate) fn format_components(delta: DateDelta, s: &mut String) {
    let mut months = delta.months.get();
    let days = delta.days.get();
    debug_assert!(months >= 0 && days >= 0);
    debug_assert!(months > 0 || days > 0);
    let years = months / 12;
    months %= 12;
    if years != 0 {
        s.push_str(&format!("{years}Y"));
    }
    if months != 0 {
        s.push_str(&format!("{months}M"));
    }
    if days != 0 {
        s.push_str(&format!("{days}D"));
    }
}

pub(crate) fn handle_init_kwargs<T>(
    fname: &str,
    kwargs: T,
    str_years: PyObj,
    str_months: PyObj,
    str_days: PyObj,
    str_weeks: PyObj,
) -> PyResult<(DeltaMonths, DeltaDays)>
where
    T: IntoIterator<Item = (PyObj, PyObj)>,
{
    let mut days: c_long = 0;
    let mut months: c_long = 0;
    handle_kwargs(fname, kwargs, |key, value, eq| {
        if eq(key, str_days) {
            days = value
                .cast::<PyInt>()
                .ok_or_type_err("days must be an integer")?
                .to_long()?
                .checked_add(days)
                .ok_or_value_err("days out of range")?;
        } else if eq(key, str_months) {
            months = value
                .cast::<PyInt>()
                .ok_or_type_err("months must be an integer")?
                .to_long()?
                .checked_add(months)
                .ok_or_value_err("months out of range")?;
        } else if eq(key, str_years) {
            months = value
                .cast::<PyInt>()
                .ok_or_type_err("years must be an integer")?
                .to_long()?
                .checked_mul(12)
                .and_then(|m| m.checked_add(months))
                .ok_or_value_err("years out of range")?;
        } else if eq(key, str_weeks) {
            days = value
                .cast::<PyInt>()
                .ok_or_type_err("weeks must be an integer")?
                .to_long()?
                .checked_mul(7)
                .and_then(|d| d.checked_add(days))
                .ok_or_value_err("weeks out of range")?;
        } else {
            return Ok(false);
        }
        Ok(true)
    })?;
    Ok((
        DeltaMonths::from_long(months).ok_or_value_err("months out of range")?,
        DeltaDays::from_long(days).ok_or_value_err("days out of range")?,
    ))
}

fn __new__(cls: HeapType<DateDelta>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    if args.len() != 0 {
        return raise_type_err("DateDelta() takes no positional arguments");
    }
    let &State {
        str_years,
        str_months,
        str_days,
        str_weeks,
        ..
    } = cls.state();
    match kwargs {
        None => DateDelta::ZERO,
        Some(kwarg_dict) => {
            let (months, days) = handle_init_kwargs(
                "DateDelta",
                kwarg_dict.iteritems(),
                str_years,
                str_months,
                str_days,
                str_weeks,
            )?;
            DateDelta::new(months, days).ok_or_value_err("Mixed sign in DateDelta")?
        }
    }
    .to_obj(cls)
}

pub(crate) fn years(state: &State, amount: PyObj) -> PyReturn {
    amount
        .cast::<PyInt>()
        .ok_or_type_err("argument must be int")?
        .to_long()?
        .checked_mul(12)
        .and_then(DeltaMonths::from_long)
        .map(DateDelta::from_months)
        .ok_or_value_err("value out of bounds")?
        .to_obj(state.date_delta_type)
}

pub(crate) fn months(state: &State, amount: PyObj) -> PyReturn {
    DeltaMonths::from_long(
        amount
            .cast::<PyInt>()
            .ok_or_type_err("argument must be int")?
            .to_long()?,
    )
    .map(DateDelta::from_months)
    .ok_or_value_err("value out of bounds")?
    .to_obj(state.date_delta_type)
}

pub(crate) fn weeks(state: &State, amount: PyObj) -> PyReturn {
    amount
        .cast::<PyInt>()
        .ok_or_type_err("argument must be int")?
        .to_long()?
        .checked_mul(7)
        .and_then(DeltaDays::from_long)
        .map(DateDelta::from_days)
        .ok_or_value_err("value out of bounds")?
        .to_obj(state.date_delta_type)
}

pub(crate) fn days(state: &State, amount: PyObj) -> PyReturn {
    DeltaDays::from_long(
        amount
            .cast::<PyInt>()
            .ok_or_type_err("argument must be int")?
            .to_long()?,
    )
    .map(DateDelta::from_days)
    .ok_or_value_err("value out of bounds")?
    .to_obj(state.date_delta_type)
}

fn __richcmp__(cls: HeapType<DateDelta>, a: DateDelta, b_obj: PyObj, op: c_int) -> PyReturn {
    match b_obj.extract(cls) {
        Some(b) => match op {
            pyo3_ffi::Py_EQ => a == b,
            pyo3_ffi::Py_NE => a != b,
            _ => return not_implemented(),
        }
        .to_py(),
        None => not_implemented(),
    }
}

fn __neg__(cls: HeapType<DateDelta>, d: DateDelta) -> PyReturn {
    (-d).to_obj(cls)
}

fn __repr__(_: PyType, d: DateDelta) -> PyReturn {
    format!("DateDelta({d})").to_py()
}

fn __str__(_: PyType, d: DateDelta) -> PyReturn {
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

    // SAFETY: At this point we know that delta_obj is a DateDelta
    let (delta_type, delta) = unsafe { delta_obj.assume_heaptype::<DateDelta>() };
    i32::try_from(factor)
        .ok()
        .and_then(|f| delta.checked_mul(f))
        .ok_or_value_err("Multiplication factor or result out of bounds")?
        .to_obj(delta_type)
}

fn __add__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    _add_method(obj_a, obj_b, false)
}

fn __sub__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    _add_method(obj_a, obj_b, true)
}

#[inline]
fn _add_method(obj_a: PyObj, obj_b: PyObj, negate: bool) -> PyReturn {
    let type_a = obj_a.type_();
    let type_b = obj_b.type_();

    // Case: both are DateDelta
    if type_a == type_b {
        // SAFETY: The only way to get here is if *both* are DateDelta
        let (ddelta_type, a) = unsafe { obj_a.assume_heaptype::<DateDelta>() };
        let (_, mut b) = unsafe { obj_b.assume_heaptype::<DateDelta>() };
        if negate {
            b = -b;
        }
        a.checked_add(b)
            .map_err(|e| {
                value_err(match e {
                    InitError::TooBig => "Addition result out of bounds",
                    InitError::MixedSign => "Mixed sign in DateDelta",
                })
            })?
            .to_obj(ddelta_type)
    // Case: two `whenever` types
    } else if let Some(state) = type_a.same_module(type_b) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (_, ddelta) = unsafe { obj_a.assume_heaptype::<DateDelta>() };
        if let Some(mut tdelta) = obj_b.extract(state.time_delta_type) {
            if negate {
                tdelta = -tdelta;
            }
            DateTimeDelta::new(ddelta, tdelta).ok_or_value_err("Mixed sign in delta")?
        } else if let Some(mut dtdelta) = obj_b.extract(state.datetime_delta_type) {
            if negate {
                dtdelta = -dtdelta;
            }
            dtdelta
                .checked_add(DateTimeDelta {
                    // SAFETY: At least one of the two is a DateDelta
                    ddelta,
                    tdelta: TimeDelta::ZERO,
                })
                .map_err(|e| {
                    value_err(match e {
                        InitError::TooBig => "Addition result out of bounds",
                        InitError::MixedSign => "Mixed sign in DateTimeDelta",
                    })
                })?
        } else {
            raise_type_err(format!(
                "unsupported operand type(s) for +/-: {type_a} and {type_b}"
            ))?
        }
        .to_obj(state.datetime_delta_type)
    } else {
        not_implemented()
    }
}

fn __abs__(cls: HeapType<DateDelta>, slf: PyObj) -> PyReturn {
    // SAFETY: self argument to __abs__ is always a DateDelta
    let (_, DateDelta { months, days }) = unsafe { slf.assume_heaptype() };
    if months.get() >= 0 && days.get() >= 0 {
        Ok(slf.newref())
    } else {
        DateDelta {
            // SAFETY: No overflow is possible due to the ranges
            months: -months,
            days: -days,
        }
        .to_obj(cls)
    }
}

extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    hashmask(
        // SAFETY: self argument is always the DateDelta type
        unsafe { slf.assume_heaptype::<DateDelta>() }.1.pyhash(),
    )
}

extern "C" fn __bool__(slf: PyObj) -> c_int {
    // SAFETY: self argument is always the DateDelta type
    (!unsafe { slf.assume_heaptype::<DateDelta>() }.1.is_zero()).into()
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(DateDelta, Py_tp_new, __new__),
    slotmethod!(DateDelta, Py_tp_richcompare, __richcmp__),
    slotmethod!(DateDelta, Py_nb_negative, __neg__, 1),
    slotmethod!(DateDelta, Py_tp_repr, __repr__, 1),
    slotmethod!(DateDelta, Py_tp_str, __str__, 1),
    slotmethod!(DateDelta, Py_nb_positive, identity1, 1),
    slotmethod!(DateDelta, Py_nb_absolute, __abs__, 1),
    slotmethod!(Py_nb_multiply, __mul__, 2),
    slotmethod!(Py_nb_add, __add__, 2),
    slotmethod!(Py_nb_subtract, __sub__, 2),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::DATEDELTA.as_ptr() as *mut c_void,
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

fn format_common_iso(_: PyType, slf: DateDelta) -> PyReturn {
    slf.fmt_iso().to_py()
}

// parse the prefix of an ISO8601 duration, e.g. `P`, `-P`, `+P`,
pub(crate) fn parse_prefix(s: &mut &[u8]) -> Option<bool> {
    debug_assert!(s.len() >= 2);
    match s[0] {
        b'P' | b'p' => {
            let result = Some(false);
            *s = &s[1..];
            result
        }
        b'-' if s[1].eq_ignore_ascii_case(&b'P') => {
            let result = Some(true);
            *s = &s[2..];
            result
        }
        b'+' if s[1].eq_ignore_ascii_case(&b'P') => {
            let result = Some(false);
            *s = &s[2..];
            result
        }
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub(crate) enum Unit {
    Years,
    Months,
    Weeks,
    Days,
}

fn finish_parsing_component(s: &mut &[u8], mut value: i32) -> Option<(i32, Unit)> {
    // We limit parsing to a number of digits to prevent overflow
    for i in 1..s.len().min(7) {
        match s[i] {
            c if c.is_ascii_digit() => value = value * 10 + i32::from(c - b'0'),
            b'D' | b'd' => {
                *s = &s[i + 1..];
                return Some((value, Unit::Days));
            }
            b'W' | b'w' => {
                *s = &s[i + 1..];
                return Some((value, Unit::Weeks));
            }
            b'M' | b'm' => {
                *s = &s[i + 1..];
                return Some((value, Unit::Months));
            }
            b'Y' | b'y' => {
                *s = &s[i + 1..];
                return Some((value, Unit::Years));
            }
            _ => {
                return None;
            }
        }
    }
    None
}

// parse a component of a ISO8601 duration, e.g. `6Y`, `56M`, `2W`, `0D`
pub(crate) fn parse_component(s: &mut &[u8]) -> Option<(i32, Unit)> {
    if s.len() >= 2 && s[0].is_ascii_digit() {
        finish_parsing_component(s, (s[0] - b'0').into())
    } else {
        None
    }
}

fn parse_common_iso(cls: HeapType<DateDelta>, arg: PyObj) -> PyReturn {
    let py_str = arg.cast::<PyStr>().ok_or_type_err("argument must be str")?;
    let s = &mut py_str.as_utf8()?;
    let err = || format!("Invalid format: {arg}");
    if s.len() < 3 {
        // at least `P0D`
        raise_value_err(err())?
    }
    let mut months = 0;
    let mut days = 0;
    let mut prev_unit: Option<Unit> = None;

    let negated = parse_prefix(s).ok_or_else_value_err(err)?;

    while !s.is_empty() {
        let (value, unit) = parse_component(s).ok_or_else_value_err(err)?;
        match (unit, prev_unit.replace(unit)) {
            // NOTE: overflows are prevented by limiting the number
            // of digits that are parsed.
            (Unit::Years, None) => {
                months += value * 12;
            }
            (Unit::Months, None | Some(Unit::Years)) => {
                months += value;
            }
            (Unit::Weeks, None | Some(Unit::Years | Unit::Months)) => {
                days += value * 7;
            }
            (Unit::Days, _) => {
                days += value;
                if s.is_empty() {
                    break;
                }
                // i.e. there's more after the days component
                raise_value_err(err())?;
            }
            _ => {
                // i.e. the order of the components is wrong
                raise_value_err(err())?;
            }
        }
    }

    // i.e. there must be at least one component (`P` alone is invalid)
    if prev_unit.is_none() {
        raise_value_err(err())?;
    }

    if negated {
        months = -months;
        days = -days;
    }
    DeltaMonths::new(months)
        .zip(DeltaDays::new(days))
        .map(|(months, days)| DateDelta { months, days })
        .ok_or_value_err("DateDelta out of range")?
        .to_obj(cls)
}

fn in_months_days(_: PyType, DateDelta { months, days }: DateDelta) -> PyResult<Owned<PyTuple>> {
    (months.get().to_py()?, days.get().to_py()?).into_pytuple()
}

// FUTURE: maybe also return the sign?
fn in_years_months_days(
    _: PyType,
    DateDelta { months, days }: DateDelta,
) -> PyResult<Owned<PyTuple>> {
    let years = months.get() / 12;
    let months = months.get() % 12;
    (years.to_py()?, months.to_py()?, days.get().to_py()?).into_pytuple()
}

fn __reduce__(
    cls: HeapType<DateDelta>,
    DateDelta { months, days }: DateDelta,
) -> PyResult<Owned<PyTuple>> {
    (
        cls.state().unpickle_date_delta.newref(),
        (months.get().to_py()?, days.get().to_py()?).into_pytuple()?,
    )
        .into_pytuple()
}

pub(crate) fn unpickle(state: &State, args: &[PyObj]) -> PyReturn {
    match args {
        &[months_obj, days_obj] => {
            let months = DeltaMonths::new_unchecked(
                months_obj
                    .cast::<PyInt>()
                    .ok_or_type_err("Invalid pickle data")?
                    .to_long()? as _,
            );
            let days = DeltaDays::new_unchecked(
                days_obj
                    .cast::<PyInt>()
                    .ok_or_type_err("Invalid pickle data")?
                    .to_long()? as _,
            );
            DateDelta::new(months, days)
                .ok_or_value_err("Invalid pickle data")?
                .to_obj(state.date_delta_type)
        }
        _ => raise_type_err("Invalid pickle data")?,
    }
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(DateDelta, __copy__, c""),
    method1!(DateDelta, __deepcopy__, c""),
    method0!(
        DateDelta,
        format_common_iso,
        doc::DATEDELTA_FORMAT_COMMON_ISO
    ),
    classmethod1!(DateDelta, parse_common_iso, doc::DATEDELTA_PARSE_COMMON_ISO),
    method0!(DateDelta, in_months_days, doc::DATEDELTA_IN_MONTHS_DAYS),
    method0!(
        DateDelta,
        in_years_months_days,
        doc::DATEDELTA_IN_YEARS_MONTHS_DAYS
    ),
    method0!(DateDelta, __reduce__, c""),
    classmethod_kwargs!(
        DateDelta,
        __get_pydantic_core_schema__,
        doc::PYDANTIC_SCHEMA
    ),
    PyMethodDef::zeroed(),
];

pub(crate) static mut SPEC: PyType_Spec =
    type_spec::<DateDelta>(c"whenever.DateDelta", unsafe { SLOTS });
