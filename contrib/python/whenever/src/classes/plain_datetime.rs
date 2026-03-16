use core::ffi::{CStr, c_int, c_long, c_void};
use core::ptr::null_mut as NULL;
use pyo3_ffi::*;

use crate::{
    classes::{
        date::Date,
        date_delta::DateDelta,
        datetime_delta::set_units_from_kwargs,
        instant::Instant,
        offset_datetime::{OffsetDateTime, check_ignore_dst_kwarg},
        time::Time,
        zoned_datetime::ZonedDateTime,
    },
    common::{ambiguity::*, parse::Scan, round, scalar::*},
    docstrings as doc,
    py::*,
    pymodule::State,
};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct DateTime {
    pub(crate) date: Date,
    pub(crate) time: Time,
}

pub(crate) const SINGLETONS: &[(&CStr, DateTime); 2] = &[
    (
        c"MIN",
        DateTime {
            date: Date {
                year: Year::new(1).unwrap(),
                month: Month::January,
                day: 1,
            },
            time: Time {
                hour: 0,
                minute: 0,
                second: 0,
                subsec: SubSecNanos::MIN,
            },
        },
    ),
    (
        c"MAX",
        DateTime {
            date: Date {
                year: Year::new(9999).unwrap(),
                month: Month::December,
                day: 31,
            },
            time: Time {
                hour: 23,
                minute: 59,
                second: 59,
                subsec: SubSecNanos::MAX,
            },
        },
    ),
];

impl DateTime {
    pub(crate) fn shift_date(self, months: DeltaMonths, days: DeltaDays) -> Option<Self> {
        let DateTime { date, time } = self;
        date.shift(months, days).map(|date| DateTime { date, time })
    }

    pub(crate) fn shift_nanos(self, nanos: i128) -> Option<Self> {
        let DateTime { mut date, time } = self;
        let new_time = i128::from(time.total_nanos()).checked_add(nanos)?;
        let days_delta = i32::try_from(new_time.div_euclid(NS_PER_DAY)).ok()?;
        let nano_delta = new_time.rem_euclid(NS_PER_DAY) as u64;
        if days_delta != 0 {
            date = DeltaDays::new(days_delta).and_then(|d| date.shift_days(d))?;
        }
        Some(DateTime {
            date,
            time: Time::from_total_nanos_unchecked(nano_delta),
        })
    }

    // FUTURE: is this actually worth it?
    pub(crate) fn change_offset(self, s: OffsetDelta) -> Option<Self> {
        let Self { date, time } = self;
        // Safety: both values sufficiently within i32 range
        let secs_since_midnight = time.total_seconds() as i32 + s.get();
        Some(Self {
            date: match secs_since_midnight.div_euclid(S_PER_DAY) {
                0 => date,
                1 => date.tomorrow()?,
                -1 => date.yesterday()?,
                // more than 1 day difference is highly unlikely--but possible
                2 => date.tomorrow()?.tomorrow()?,
                -2 => date.yesterday()?.yesterday()?,
                // OffsetDelta is <48 hours, so this is safe
                _ => unreachable!(),
            },
            time: Time::from_sec_subsec(
                secs_since_midnight.rem_euclid(S_PER_DAY) as u32,
                time.subsec,
            ),
        })
    }

    pub(crate) fn read_iso(s: &mut Scan) -> Option<Self> {
        // Minimal length is 11 (YYYYMMDDTHH)
        if s.len() < 11 {
            return None;
        }
        let date = if is_datetime_sep(s[10]) {
            Date::parse_iso_extended(s.take_unchecked(10).try_into().unwrap())
        } else if is_datetime_sep(s[8]) {
            Date::parse_iso_basic(s.take_unchecked(8).try_into().unwrap())
        } else {
            return None;
        }?;
        let time = Time::read_iso(s.skip(1))?;
        Some(DateTime { date, time })
    }

    pub fn parse(s: &[u8]) -> Option<Self> {
        Scan::new(s).parse_all(Self::read_iso)
    }

    fn from_py(dt: PyDateTime) -> PyResult<Self> {
        let tzinfo = dt.tzinfo();
        if !tzinfo.is_none() {
            raise_value_err(format!("datetime must be naive, but got tzinfo={tzinfo}"))?
        }
        Ok(DateTime {
            date: Date::from_py(dt.date()),
            time: Time::from_py_dt(dt),
        })
    }
}

impl PyWrapped for DateTime {}

impl std::fmt::Display for DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}T{}", self.date, self.time)
    }
}

fn __new__(cls: HeapType<DateTime>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    let mut year: c_long = 0;
    let mut month: c_long = 0;
    let mut day: c_long = 0;
    let mut hour: c_long = 0;
    let mut minute: c_long = 0;
    let mut second: c_long = 0;
    let mut nanosecond: c_long = 0;

    parse_args_kwargs!(
        args,
        kwargs,
        c"lll|lll$l:PlainDateTime",
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
    );

    DateTime {
        date: Date::from_longs(year, month, day).ok_or_type_err("Invalid date")?,
        time: Time::from_longs(hour, minute, second, nanosecond).ok_or_type_err("Invalid time")?,
    }
    .to_obj(cls)
}

fn __repr__(_: PyType, slf: DateTime) -> PyReturn {
    let DateTime { date, time } = slf;
    format!("PlainDateTime({date} {time})").to_py()
}

fn __str__(_: PyType, slf: DateTime) -> PyReturn {
    format!("{slf}").to_py()
}

fn format_common_iso(cls: PyType, slf: DateTime) -> PyReturn {
    __str__(cls, slf)
}

fn __richcmp__(cls: HeapType<DateTime>, slf: DateTime, other: PyObj, op: c_int) -> PyReturn {
    if let Some(dt) = other.extract(cls) {
        match op {
            pyo3_ffi::Py_LT => slf < dt,
            pyo3_ffi::Py_LE => slf <= dt,
            pyo3_ffi::Py_EQ => slf == dt,
            pyo3_ffi::Py_NE => slf != dt,
            pyo3_ffi::Py_GT => slf > dt,
            pyo3_ffi::Py_GE => slf >= dt,
            _ => unreachable!(),
        }
        .to_py()
    } else {
        not_implemented()
    }
}

extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    // SAFETY: self type is always passed to __hash__
    let (_, DateTime { date, time }) = unsafe { slf.assume_heaptype() };
    hashmask(hash_combine(date.hash() as Py_hash_t, time.pyhash()))
}

fn __add__(a: PyObj, b: PyObj) -> PyReturn {
    _shift_operator(a, b, false)
}

fn __sub__(a: PyObj, b: PyObj) -> PyReturn {
    // easy case: subtracting two PlainDateTime objects
    if a.type_() == b.type_() {
        // SAFETY: at least one of the args is a PlainDateTime so both are.
        let (dt_type, _) = unsafe { a.assume_heaptype::<DateTime>() };
        raise(
            dt_type.state().exc_implicitly_ignoring_dst.as_ptr(),
            doc::DIFF_OPERATOR_LOCAL_MSG,
        )?
    } else {
        _shift_operator(a, b, true)
    }
}

#[inline]
fn _shift_operator(obj_a: PyObj, obj_b: PyObj, negate: bool) -> PyReturn {
    let opname = if negate { "-" } else { "+" };
    let type_a = obj_a.type_();
    let type_b = obj_b.type_();

    if let Some(state) = type_a.same_module(type_b) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (dt_type, a) = unsafe { obj_a.assume_heaptype::<DateTime>() };

        if let Some(DateDelta {
            mut months,
            mut days,
        }) = obj_b.extract(state.date_delta_type)
        {
            if negate {
                months = -months;
                days = -days;
            }
            a.shift_date(months, days)
                .ok_or_else_value_err(|| format!("Result of {opname} out of range"))?
                .to_obj(dt_type)
        } else if type_b == state.datetime_delta_type.into()
            || type_b == state.time_delta_type.into()
        {
            raise(
                state.exc_implicitly_ignoring_dst.as_ptr(),
                doc::SHIFT_LOCAL_MSG,
            )?
        } else {
            raise_type_err(format!(
                "unsupported operand type(s) for {opname}: 'PlainDateTime' and {type_b}"
            ))?
        }
    } else {
        not_implemented()
    }
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(DateTime, Py_tp_new, __new__),
    slotmethod!(DateTime, Py_tp_repr, __repr__, 1),
    slotmethod!(DateTime, Py_tp_str, __str__, 1),
    slotmethod!(DateTime, Py_tp_richcompare, __richcmp__),
    slotmethod!(Py_nb_add, __add__, 2),
    slotmethod!(Py_nb_subtract, __sub__, 2),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::PLAINDATETIME.as_ptr() as *mut c_void,
    },
    PyType_Slot {
        slot: Py_tp_hash,
        pfunc: __hash__ as *mut c_void,
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
        slot: Py_tp_dealloc,
        pfunc: generic_dealloc as *mut c_void,
    },
    PyType_Slot {
        slot: 0,
        pfunc: NULL(),
    },
];

#[inline]
#[allow(clippy::too_many_arguments)]
pub(crate) fn set_components_from_kwargs(
    key: PyObj,
    value: PyObj,
    year: &mut c_long,
    month: &mut c_long,
    day: &mut c_long,
    hour: &mut c_long,
    minute: &mut c_long,
    second: &mut c_long,
    nanos: &mut c_long,
    str_year: PyObj,
    str_month: PyObj,
    str_day: PyObj,
    str_hour: PyObj,
    str_minute: PyObj,
    str_second: PyObj,
    str_nanosecond: PyObj,
    eq: fn(PyObj, PyObj) -> bool,
) -> PyResult<bool> {
    if eq(key, str_year) {
        *year = value
            .cast::<PyInt>()
            .ok_or_type_err("year must be an integer")?
            .to_long()?;
    } else if eq(key, str_month) {
        *month = value
            .cast::<PyInt>()
            .ok_or_type_err("month must be an integer")?
            .to_long()?;
    } else if eq(key, str_day) {
        *day = value
            .cast::<PyInt>()
            .ok_or_type_err("day must be an integer")?
            .to_long()?;
    } else if eq(key, str_hour) {
        *hour = value
            .cast::<PyInt>()
            .ok_or_type_err("hour must be an integer")?
            .to_long()?;
    } else if eq(key, str_minute) {
        *minute = value
            .cast::<PyInt>()
            .ok_or_type_err("minute must be an integer")?
            .to_long()?;
    } else if eq(key, str_second) {
        *second = value
            .cast::<PyInt>()
            .ok_or_type_err("second must be an integer")?
            .to_long()?;
    } else if eq(key, str_nanosecond) {
        *nanos = value
            .cast::<PyInt>()
            .ok_or_type_err("nanosecond must be an integer")?
            .to_long()?;
    } else {
        return Ok(false);
    }
    Ok(true)
}

fn replace(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    if !args.is_empty() {
        raise_type_err("replace() takes no positional arguments")?
    }
    let &State {
        str_year,
        str_month,
        str_day,
        str_hour,
        str_minute,
        str_second,
        str_nanosecond,
        ..
    } = cls.state();
    let mut year = slf.date.year.get().into();
    let mut month = slf.date.month.get().into();
    let mut day = slf.date.day.into();
    let mut hour = slf.time.hour.into();
    let mut minute = slf.time.minute.into();
    let mut second = slf.time.second.into();
    let mut nanos = slf.time.subsec.get() as _;
    handle_kwargs("replace", kwargs, |key, value, eq| {
        set_components_from_kwargs(
            key,
            value,
            &mut year,
            &mut month,
            &mut day,
            &mut hour,
            &mut minute,
            &mut second,
            &mut nanos,
            str_year,
            str_month,
            str_day,
            str_hour,
            str_minute,
            str_second,
            str_nanosecond,
            eq,
        )
    })?;
    DateTime {
        date: Date::from_longs(year, month, day).ok_or_value_err("Invalid date")?,
        time: Time::from_longs(hour, minute, second, nanos).ok_or_value_err("Invalid time")?,
    }
    .to_obj(cls)
}

fn add(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    _shift_method(cls, slf, args, kwargs, false)
}

fn subtract(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    _shift_method(cls, slf, args, kwargs, true)
}

#[inline]
fn _shift_method(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
    negate: bool,
) -> PyReturn {
    let fname = if negate { "subtract" } else { "add" };
    // FUTURE: get fields all at once from State (this is faster)
    let state = cls.state();
    let mut months = DeltaMonths::ZERO;
    let mut days = DeltaDays::ZERO;
    let mut nanos = 0;
    let mut ignore_dst = false;

    match *args {
        [arg] => {
            match kwargs.next() {
                Some((key, value)) if kwargs.len() == 1 && key.py_eq(state.str_ignore_dst)? => {
                    ignore_dst = value.is_true();
                }
                Some(_) => raise_type_err(format!(
                    "{fname}() can't mix positional and keyword arguments"
                ))?,
                None => {}
            };
            if let Some(tdelta) = arg.extract(state.time_delta_type) {
                nanos = tdelta.total_nanos();
            } else if let Some(ddelta) = arg.extract(state.date_delta_type) {
                months = ddelta.months;
                days = ddelta.days;
            } else if let Some(dt) = arg.extract(state.datetime_delta_type) {
                months = dt.ddelta.months;
                days = dt.ddelta.days;
                nanos = dt.tdelta.total_nanos();
            } else {
                raise_type_err(format!("{fname}() argument must be a delta"))?
            }
        }
        [] => {
            let mut raw_months = 0;
            let mut raw_days = 0;
            handle_kwargs(fname, kwargs, |key, value, eq| {
                if eq(key, state.str_ignore_dst) {
                    ignore_dst = value.is_true();
                    Ok(true)
                } else {
                    set_units_from_kwargs(
                        key,
                        value,
                        &mut raw_months,
                        &mut raw_days,
                        &mut nanos,
                        state,
                        eq,
                    )
                }
            })?;
            months = DeltaMonths::new(raw_months).ok_or_value_err("Months out of range")?;
            days = DeltaDays::new(raw_days).ok_or_value_err("Days out of range")?;
        }
        _ => raise_type_err(format!(
            "{}() takes at most 1 positional argument, got {}",
            fname,
            args.len()
        ))?,
    }

    if negate {
        months = -months;
        days = -days;
        nanos = -nanos;
    }
    if nanos != 0 && !ignore_dst {
        raise(
            state.exc_implicitly_ignoring_dst.as_ptr(),
            doc::ADJUST_LOCAL_DATETIME_MSG,
        )?
    }
    slf.shift_date(months, days)
        .and_then(|dt| dt.shift_nanos(nanos))
        .ok_or_else_value_err(|| format!("Result of {fname}() out of range"))?
        .to_obj(cls)
}

fn difference(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    check_ignore_dst_kwarg(kwargs, state, doc::DIFF_LOCAL_MSG)?;
    let [arg] = *args else {
        raise_type_err("difference() takes exactly 1 argument")?
    };
    if let Some(dt) = arg.extract(cls) {
        Instant::from_datetime(slf.date, slf.time)
            .diff(Instant::from_datetime(dt.date, dt.time))
            .to_obj(state.time_delta_type)
    } else {
        raise_type_err("difference() argument must be a PlainDateTime")?
    }
}

fn __reduce__(cls: HeapType<DateTime>, slf: DateTime) -> PyResult<Owned<PyTuple>> {
    let DateTime {
        date: Date { year, month, day },
        time:
            Time {
                hour,
                minute,
                second,
                subsec,
            },
    } = slf;
    let data = pack![
        year.get(),
        month.get(),
        day,
        hour,
        minute,
        second,
        subsec.get()
    ];
    (
        cls.state().unpickle_plain_datetime.newref(),
        (data.to_py()?,).into_pytuple()?,
    )
        .into_pytuple()
}

pub(crate) fn unpickle(state: &State, arg: PyObj) -> PyReturn {
    let py_bytes = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;

    let mut packed = py_bytes.as_bytes()?;
    if packed.len() != 11 {
        raise_type_err("Invalid pickle data")?
    }
    DateTime {
        date: Date {
            year: Year::new_unchecked(unpack_one!(packed, u16)),
            month: Month::new_unchecked(unpack_one!(packed, u8)),
            day: unpack_one!(packed, u8),
        },
        time: Time {
            hour: unpack_one!(packed, u8),
            minute: unpack_one!(packed, u8),
            second: unpack_one!(packed, u8),
            subsec: SubSecNanos::new_unchecked(unpack_one!(packed, i32)),
        },
    }
    .to_obj(state.plain_datetime_type)
}

fn from_py_datetime(cls: HeapType<DateTime>, arg: PyObj) -> PyReturn {
    let Some(dt) = arg.cast_allow_subclass::<PyDateTime>() else {
        raise_type_err("argument must be datetime.datetime")?
    };
    DateTime::from_py(dt)?.to_obj(cls)
}

fn py_datetime(cls: HeapType<DateTime>, slf: DateTime) -> *mut PyObject {
    let DateTime {
        date: Date { year, month, day },
        time:
            Time {
                hour,
                minute,
                second,
                subsec,
            },
    } = slf;
    let &PyDateTime_CAPI {
        DateTime_FromDateAndTime,
        DateTimeType,
        ..
    } = cls.state().py_api;
    // SAFETY: calling C API with valid arguments
    unsafe {
        DateTime_FromDateAndTime(
            year.get().into(),
            month.get().into(),
            day.into(),
            hour.into(),
            minute.into(),
            second.into(),
            (subsec.get() / 1_000) as _,
            Py_None(),
            DateTimeType,
        )
    }
}

fn date(cls: HeapType<DateTime>, slf: DateTime) -> PyReturn {
    slf.date.to_obj(cls.state().date_type)
}

fn time(cls: HeapType<DateTime>, slf: DateTime) -> PyReturn {
    slf.time.to_obj(cls.state().time_type)
}

fn is_datetime_sep(c: u8) -> bool {
    c == b'T' || c == b' ' || c == b't'
}

fn parse_common_iso(cls: HeapType<DateTime>, arg: PyObj) -> PyReturn {
    DateTime::parse(
        arg.cast::<PyStr>()
            .ok_or_type_err("Expected a string")?
            .as_utf8()?,
    )
    .ok_or_else_value_err(|| format!("Invalid format: {arg}"))?
    .to_obj(cls)
}

fn parse_strptime(cls: HeapType<DateTime>, args: &[PyObj], kwargs: &mut IterKwargs) -> PyReturn {
    let &State {
        str_format,
        strptime,
        ..
    } = cls.state();
    let format_obj = match kwargs.next() {
        Some((key, value)) if kwargs.len() == 1 && key.py_eq(str_format)? => value,
        _ => raise_type_err("parse_strptime() requires exactly one keyword argument `format`")?,
    };
    let &[arg_obj] = args else {
        raise_type_err(format!(
            "parse_strptime() takes exactly 1 positional argument, got {}",
            args.len()
        ))?
    };

    let args = (arg_obj.newref(), format_obj.newref()).into_pytuple()?;
    let parsed = strptime
        .call(*args)?
        .cast::<PyDateTime>()
        .ok_or_type_err("strptime() returned non-datetime")?;

    DateTime::from_py(*parsed)?.to_obj(cls)
}

fn assume_utc(cls: HeapType<DateTime>, DateTime { date, time }: DateTime) -> PyReturn {
    Instant::from_datetime(date, time).to_obj(cls.state().instant_type)
}

fn assume_fixed_offset(cls: HeapType<DateTime>, slf: DateTime, arg: PyObj) -> PyReturn {
    let &State {
        time_delta_type,
        offset_datetime_type,
        ..
    } = cls.state();
    slf.with_offset(Offset::from_obj(arg, time_delta_type)?)
        .ok_or_value_err("Datetime out of range")?
        .to_obj(offset_datetime_type)
}

fn assume_tz(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let &State {
        str_disambiguate,
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
        zoned_datetime_type,
        exc_skipped,
        exc_repeated,
        exc_tz_notfound,
        ref tz_store,
        ..
    } = cls.state();

    let DateTime { date, time } = slf;
    let &[tz_obj] = args else {
        raise_type_err(format!(
            "assume_tz() takes 1 positional argument but {} were given",
            args.len()
        ))?
    };

    let dis = Disambiguate::from_only_kwarg(
        kwargs,
        str_disambiguate,
        "assume_tz",
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
    )?
    .unwrap_or(Disambiguate::Compatible);
    let tzif = tz_store.obj_get(tz_obj, exc_tz_notfound)?;
    ZonedDateTime::resolve_using_disambiguate(date, time, tzif, dis, exc_repeated, exc_skipped)?
        .to_obj(zoned_datetime_type)
}

fn assume_system_tz(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let &State {
        py_api,
        str_disambiguate,
        system_datetime_type,
        exc_skipped,
        exc_repeated,
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
        ..
    } = cls.state();
    let DateTime { date, time } = slf;
    if !args.is_empty() {
        raise_type_err("assume_system_tz() takes no positional arguments")?
    }

    let dis = Disambiguate::from_only_kwarg(
        kwargs,
        str_disambiguate,
        "assume_system_tz",
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
    )?;
    OffsetDateTime::resolve_system_tz_using_disambiguate(
        py_api,
        date,
        time,
        dis.unwrap_or(Disambiguate::Compatible),
        exc_repeated,
        exc_skipped,
    )?
    .to_obj(system_datetime_type)
}

fn replace_date(cls: HeapType<DateTime>, slf: DateTime, arg: PyObj) -> PyReturn {
    let Some(date) = arg.extract(cls.state().date_type) else {
        raise_type_err("argument must be a whenever.Date")?
    };
    DateTime { date, ..slf }.to_obj(cls)
}

fn replace_time(cls: HeapType<DateTime>, slf: DateTime, arg: PyObj) -> PyReturn {
    let Some(time) = arg.extract(cls.state().time_type) else {
        raise_type_err("argument must be a whenever.Time")?
    };
    DateTime { time, ..slf }.to_obj(cls)
}

fn round(
    cls: HeapType<DateTime>,
    slf: DateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let (_, increment, mode) = round::parse_args(cls.state(), args, kwargs, false, false)?;
    let DateTime { mut date, time } = slf;
    let (time_rounded, next_day) = time.round(increment as u64, mode);
    if next_day == 1 {
        date = date
            .tomorrow()
            .ok_or_value_err("Resulting date out of range")?;
    }
    DateTime {
        date,
        time: time_rounded,
    }
    .to_obj(cls)
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(DateTime, __copy__, c""),
    method1!(DateTime, __deepcopy__, c""),
    method0!(DateTime, __reduce__, c""),
    classmethod1!(
        DateTime,
        from_py_datetime,
        doc::PLAINDATETIME_FROM_PY_DATETIME
    ),
    method0!(DateTime, py_datetime, doc::BASICCONVERSIONS_PY_DATETIME),
    method0!(DateTime, date, doc::LOCALTIME_DATE),
    method0!(DateTime, time, doc::LOCALTIME_TIME),
    method0!(
        DateTime,
        format_common_iso,
        doc::PLAINDATETIME_FORMAT_COMMON_ISO
    ),
    classmethod1!(
        DateTime,
        parse_common_iso,
        doc::PLAINDATETIME_PARSE_COMMON_ISO
    ),
    classmethod_kwargs!(DateTime, parse_strptime, doc::PLAINDATETIME_PARSE_STRPTIME),
    method_kwargs!(DateTime, replace, doc::PLAINDATETIME_REPLACE),
    method0!(DateTime, assume_utc, doc::PLAINDATETIME_ASSUME_UTC),
    method1!(
        DateTime,
        assume_fixed_offset,
        doc::PLAINDATETIME_ASSUME_FIXED_OFFSET
    ),
    method_kwargs!(DateTime, assume_tz, doc::PLAINDATETIME_ASSUME_TZ),
    method_kwargs!(
        DateTime,
        assume_system_tz,
        doc::PLAINDATETIME_ASSUME_SYSTEM_TZ
    ),
    method1!(DateTime, replace_date, doc::PLAINDATETIME_REPLACE_DATE),
    method1!(DateTime, replace_time, doc::PLAINDATETIME_REPLACE_TIME),
    method_kwargs!(DateTime, add, doc::PLAINDATETIME_ADD),
    method_kwargs!(DateTime, subtract, doc::PLAINDATETIME_SUBTRACT),
    method_kwargs!(DateTime, difference, doc::PLAINDATETIME_DIFFERENCE),
    method_kwargs!(DateTime, round, doc::PLAINDATETIME_ROUND),
    classmethod_kwargs!(DateTime, __get_pydantic_core_schema__, doc::PYDANTIC_SCHEMA),
    PyMethodDef::zeroed(),
];

fn year(_: PyType, slf: DateTime) -> PyReturn {
    slf.date.year.get().to_py()
}

fn month(_: PyType, slf: DateTime) -> PyReturn {
    slf.date.month.get().to_py()
}

fn day(_: PyType, slf: DateTime) -> PyReturn {
    slf.date.day.to_py()
}

fn hour(_: PyType, slf: DateTime) -> PyReturn {
    slf.time.hour.to_py()
}

fn minute(_: PyType, slf: DateTime) -> PyReturn {
    slf.time.minute.to_py()
}

fn second(_: PyType, slf: DateTime) -> PyReturn {
    slf.time.second.to_py()
}

fn nanosecond(_: PyType, slf: DateTime) -> PyReturn {
    slf.time.subsec.get().to_py()
}

static mut GETSETTERS: &[PyGetSetDef] = &[
    getter!(DateTime, year, "The year component"),
    getter!(DateTime, month, "The month component"),
    getter!(DateTime, day, "The day component"),
    getter!(DateTime, hour, "The hour component"),
    getter!(DateTime, minute, "The minute component"),
    getter!(DateTime, second, "The second component"),
    getter!(DateTime, nanosecond, "The nanosecond component"),
    PyGetSetDef {
        name: NULL(),
        get: None,
        set: None,
        doc: NULL(),
        closure: NULL(),
    },
];

pub(crate) static mut SPEC: PyType_Spec =
    type_spec::<DateTime>(c"whenever.PlainDateTime", unsafe { SLOTS });

#[cfg(test)]
mod tests {
    use super::*;

    fn mkdate(year: u16, month: u8, day: u8) -> Date {
        Date {
            year: Year::new_unchecked(year),
            month: Month::new_unchecked(month),
            day,
        }
    }

    #[test]
    fn test_parse_valid() {
        let cases = &[
            (&b"2023-03-02 02:09:09"[..], 2023, 3, 2, 2, 9, 9, 0),
            (
                b"2023-03-02 02:09:09.123456789",
                2023,
                3,
                2,
                2,
                9,
                9,
                123_456_789,
            ),
        ];
        for &(str, y, m, d, h, min, s, ns) in cases {
            assert_eq!(
                DateTime::parse(str),
                Some(DateTime {
                    date: mkdate(y, m, d),
                    time: Time {
                        hour: h,
                        minute: min,
                        second: s,
                        subsec: SubSecNanos::new_unchecked(ns),
                    },
                })
            );
        }
    }

    #[test]
    fn test_parse_invalid() {
        // dot but no fractional digits
        assert_eq!(DateTime::parse(b"2023-03-02 02:09:09."), None);
        // too many fractions
        assert_eq!(DateTime::parse(b"2023-03-02 02:09:09.1234567890"), None);
        // invalid minute
        assert_eq!(DateTime::parse(b"2023-03-02 02:69:09.123456789"), None);
        // invalid date
        assert_eq!(DateTime::parse(b"2023-02-29 02:29:09.123456789"), None);
    }

    #[test]
    fn test_change_offset() {
        let d = DateTime {
            date: mkdate(2023, 3, 2),
            time: Time {
                hour: 2,
                minute: 9,
                second: 9,
                subsec: SubSecNanos::MIN,
            },
        };
        assert_eq!(d.change_offset(OffsetDelta::ZERO).unwrap(), d);
        assert_eq!(
            d.change_offset(OffsetDelta::new_unchecked(1)).unwrap(),
            DateTime {
                date: mkdate(2023, 3, 2),
                time: Time {
                    hour: 2,
                    minute: 9,
                    second: 10,
                    subsec: SubSecNanos::MIN,
                }
            }
        );
        assert_eq!(
            d.change_offset(OffsetDelta::new_unchecked(-1)).unwrap(),
            DateTime {
                date: mkdate(2023, 3, 2),
                time: Time {
                    hour: 2,
                    minute: 9,
                    second: 8,
                    subsec: SubSecNanos::MIN,
                }
            }
        );
        assert_eq!(
            d.change_offset(OffsetDelta::new_unchecked(86_400)).unwrap(),
            DateTime {
                date: mkdate(2023, 3, 3),
                time: Time {
                    hour: 2,
                    minute: 9,
                    second: 9,
                    subsec: SubSecNanos::MIN,
                }
            }
        );
        assert_eq!(
            d.change_offset(OffsetDelta::new_unchecked(-86_400))
                .unwrap(),
            DateTime {
                date: mkdate(2023, 3, 1),
                time: Time {
                    hour: 2,
                    minute: 9,
                    second: 9,
                    subsec: SubSecNanos::MIN,
                }
            }
        );
        let midnight = DateTime {
            date: mkdate(2023, 3, 2),
            time: Time {
                hour: 0,
                minute: 0,
                second: 0,
                subsec: SubSecNanos::MIN,
            },
        };
        assert_eq!(midnight.change_offset(OffsetDelta::ZERO).unwrap(), midnight);
        assert_eq!(
            midnight
                .change_offset(OffsetDelta::new_unchecked(-1))
                .unwrap(),
            DateTime {
                date: mkdate(2023, 3, 1),
                time: Time {
                    hour: 23,
                    minute: 59,
                    second: 59,
                    subsec: SubSecNanos::MIN,
                }
            }
        );
        assert_eq!(
            midnight
                .change_offset(OffsetDelta::new_unchecked(-86_400))
                .unwrap(),
            DateTime {
                date: mkdate(2023, 3, 1),
                time: Time {
                    hour: 0,
                    minute: 0,
                    second: 0,
                    subsec: SubSecNanos::MIN,
                }
            }
        );
        assert_eq!(
            midnight
                .change_offset(OffsetDelta::new_unchecked(-86_401))
                .unwrap(),
            DateTime {
                date: mkdate(2023, 2, 28),
                time: Time {
                    hour: 23,
                    minute: 59,
                    second: 59,
                    subsec: SubSecNanos::MIN,
                }
            }
        );
        assert_eq!(
            DateTime {
                date: mkdate(2023, 1, 1),
                time: Time {
                    hour: 0,
                    minute: 0,
                    second: 0,
                    subsec: SubSecNanos::MIN,
                }
            }
            .change_offset(OffsetDelta::new_unchecked(-1))
            .unwrap(),
            DateTime {
                date: mkdate(2022, 12, 31),
                time: Time {
                    hour: 23,
                    minute: 59,
                    second: 59,
                    subsec: SubSecNanos::MIN,
                }
            }
        )
    }
}
