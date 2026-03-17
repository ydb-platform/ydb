use core::ffi::{CStr, c_int, c_long, c_void};
use core::ptr::{NonNull, null_mut as NULL};
use pyo3_ffi::*;
use std::fmt::{self, Display, Formatter};

use crate::{
    classes::{
        date::Date,
        datetime_delta::set_units_from_kwargs,
        instant::Instant,
        plain_datetime::{DateTime, set_components_from_kwargs},
        time::Time,
        time_delta::TimeDelta,
    },
    common::{parse::Scan, rfc2822, round, scalar::*},
    docstrings as doc,
    py::*,
    pymodule::State,
    tz::tzif::is_valid_key,
};

/// A date and time with a fixed offset from UTC.
/// Invariant: the instant represented by the date and time is always within range.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub(crate) struct OffsetDateTime {
    pub(crate) date: Date,
    pub(crate) time: Time,
    pub(crate) offset: Offset,
}

pub(crate) const SINGLETONS: &[(&CStr, OffsetDateTime); 0] = &[];

impl OffsetDateTime {
    pub(crate) const fn new_unchecked(date: Date, time: Time, offset: Offset) -> Self {
        OffsetDateTime { date, time, offset }
    }

    pub(crate) fn new(date: Date, time: Time, offset: Offset) -> Option<Self> {
        // Check: the instant represented by the date and time is within range
        date.epoch_at(time).offset(-offset)?;
        Some(Self { date, time, offset })
    }

    pub(crate) fn instant(self) -> Instant {
        Instant::from_datetime(self.date, self.time)
            .offset(-self.offset)
            // Safe: we know the instant of an OffsetDateTime is in range
            .unwrap()
    }

    pub(crate) const fn without_offset(self) -> DateTime {
        DateTime {
            date: self.date,
            time: self.time,
        }
    }

    pub(crate) fn parse(s: &[u8]) -> Option<Self> {
        Scan::new(s).parse_all(Self::read_iso)
    }

    pub(crate) fn read_iso(s: &mut Scan) -> Option<Self> {
        DateTime::read_iso(s)?
            .with_offset(Offset::read_iso(s)?)
            .and_then(|d| {
                skip_tzname(s)?;
                Some(d)
            })
    }

    pub(crate) fn to_py(
        self,
        &PyDateTime_CAPI {
            DateTime_FromDateAndTime,
            DateTimeType,
            TimeZone_FromTimeZone,
            Delta_FromDelta,
            DeltaType,
            ..
        }: &PyDateTime_CAPI,
    ) -> PyResult<Owned<PyDateTime>> {
        let OffsetDateTime {
            date: Date { year, month, day },
            time:
                Time {
                    hour,
                    minute,
                    second,
                    subsec,
                },
            offset,
            ..
        } = self;
        // SAFETY: calling CPython API with valid arguments
        let delta = unsafe {
            Delta_FromDelta(
                // Important that we normalize so seconds >= 0
                offset.get().div_euclid(S_PER_DAY),
                offset.get().rem_euclid(S_PER_DAY),
                0,
                0,
                DeltaType,
            )
        }
        .rust_owned()?;
        let tz = unsafe { TimeZone_FromTimeZone(delta.as_ptr(), NULL()) }.rust_owned()?;
        unsafe {
            DateTime_FromDateAndTime(
                year.get().into(),
                month.get().into(),
                day.into(),
                hour.into(),
                minute.into(),
                second.into(),
                (subsec.get() / 1_000) as _,
                tz.as_ptr(),
                DateTimeType,
            )
        }
        .rust_owned()
        // SAFETY: safe to assume result of C API function is the proper type
        .map(|d| unsafe { d.cast_unchecked::<PyDateTime>() })
    }

    pub(crate) fn from_py(dt: PyDateTime) -> PyResult<Self> {
        let date = Date::from_py(dt.date());
        let time = Time::from_py_dt(dt);
        OffsetDateTime::new(date, time, Offset::from_py(dt)?)
            .ok_or_value_err("Datetime is out of range")
    }

    pub(crate) fn from_py_with_subsec(dt: PyDateTime, subsec: SubSecNanos) -> PyResult<Self> {
        OffsetDateTime::new(
            Date::from_py(dt.date()),
            Time::from_py_dt_with_subsec(dt, subsec),
            Offset::from_py(dt)?,
        )
        .ok_or_value_err("Datetime is out of range")
    }
}

impl DateTime {
    pub(crate) fn with_offset(self, offset: Offset) -> Option<OffsetDateTime> {
        OffsetDateTime::new(self.date, self.time, offset)
    }

    pub(crate) const fn with_offset_unchecked(self, offset: Offset) -> OffsetDateTime {
        OffsetDateTime {
            date: self.date,
            time: self.time,
            offset,
        }
    }
}

impl Instant {
    pub(crate) fn to_offset(self, secs: Offset) -> Option<OffsetDateTime> {
        Some(
            self.offset(secs)?
                .to_datetime()
                // Safety: at this point, we know the instant and local date
                // are in range
                .with_offset_unchecked(secs),
        )
    }
}

impl Offset {
    pub(crate) fn read_iso(s: &mut Scan) -> Option<Self> {
        let sign = match s.next() {
            Some(b'+') => Sign::Plus,
            Some(b'-') => Sign::Minus,
            Some(b'Z' | b'z') => return Some(Offset::ZERO),
            _ => None?, // sign is required
        };
        let mut total = 0;

        // hours (required)
        total += s.digits00_23()? as i32 * 3600;

        match s.advance_on(b':') {
            // we're parsing: HH:MM[:SS]
            Some(true) => {
                // minutes (required after the ':')
                total += s.digits00_59()? as i32 * 60;
                // Let's see if we have seconds too (optional)
                if let Some(true) = s.advance_on(b':') {
                    total += s.digits00_59()? as i32;
                }
            }
            // we *may* be parsing HHMM[SS]
            Some(false) => {
                // Let's see if we have minutes (optional)
                if let Some(n) = s.digits00_59() {
                    total += n as i32 * 60;
                    // Let's see if we have seconds too (optional)
                    if let Some(n) = s.digits00_59() {
                        total += n as i32;
                    }
                }
            }
            // end of string. We're done
            None => {}
        }
        // Safe: we've bounded the values on parsing so we're in range
        Some(Offset::new_unchecked(total).with_sign(sign))
    }

    /// Get the offset from a Python datetime
    pub(crate) fn from_py(dt: PyDateTime) -> PyResult<Self> {
        Ok({
            let offset = dt.utcoffset()?;
            if let Some(py_delta) = offset.borrow().cast::<PyTimeDelta>() {
                if py_delta.microseconds_component() != 0 {
                    raise_value_err("Sub-second offset precision not supported")?
                }
                // SAFETY: Python datetime offsets are limited to +/- 24 hours
                Offset::new_unchecked(
                    py_delta.days_component() * S_PER_DAY + py_delta.seconds_component(),
                )
            } else if offset.is_none() {
                raise_value_err("Datetime is naive")?
            } else {
                raise_value_err("Datetime utcoffset() returned non-delta value")?
            }
        })
    }

    pub(crate) fn from_obj(obj: PyObj, tdelta_cls: HeapType<TimeDelta>) -> PyResult<Self> {
        if let Some(py_int) = obj.cast::<PyInt>() {
            Offset::from_hours(py_int.to_long()?)
                .ok_or_value_err("offset must be between -24 and 24 hours")
        } else if let Some(TimeDelta { secs, subsec }) = obj.extract(tdelta_cls) {
            if subsec.get() == 0 {
                Offset::from_i64(secs.get())
                    .ok_or_value_err("offset must be between -24 and 24 hours")
            } else {
                raise_value_err("offset must be a whole number of seconds")?
            }
        } else {
            raise_type_err(format!(
                "offset must be an integer or TimeDelta instance, got {obj}"
            ))?
        }
    }
}

/// Skip over the timezone in the string, if present.
fn skip_tzname(s: &mut Scan) -> Option<()> {
    if let Some(true) = s.advance_on(b'[') {
        match s.take_until_inclusive(|c| c == b']') {
            Some(tz) if is_valid_key(std::str::from_utf8(&tz[..tz.len() - 1]).ok()?) => (),
            _ => None?,
        }
    };
    Some(())
}

impl PyWrapped for OffsetDateTime {}

impl Display for OffsetDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let &OffsetDateTime { date, time, offset } = self;
        write!(f, "{date}T{time}{offset}")
    }
}

fn __new__(cls: HeapType<OffsetDateTime>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    let mut year: c_long = 0;
    let mut month: c_long = 0;
    let mut day: c_long = 0;
    let mut hour: c_long = 0;
    let mut minute: c_long = 0;
    let mut second: c_long = 0;
    let mut nanosecond: c_long = 0;
    let mut offset: *mut PyObject = NULL();

    parse_args_kwargs!(
        args,
        kwargs,
        c"lll|lll$lO:OffsetDateTime",
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
        offset
    );

    let Some(offset_obj) = NonNull::new(offset).map(PyObj::wrap) else {
        raise_type_err("Missing required keyword argument: 'offset'")?
    };
    let date = Date::from_longs(year, month, day).ok_or_value_err("Invalid date")?;
    let time =
        Time::from_longs(hour, minute, second, nanosecond).ok_or_value_err("Invalid time")?;
    let offset = Offset::from_obj(offset_obj, cls.state().time_delta_type)?;
    OffsetDateTime::new(date, time, offset)
        .ok_or_value_err("Time is out of range")?
        .to_obj(cls)
}

fn __repr__(_: PyType, slf: OffsetDateTime) -> PyReturn {
    let OffsetDateTime { date, time, offset } = slf;
    format!("OffsetDateTime({date} {time}{offset})").to_py()
}

fn __str__(_: PyType, slf: OffsetDateTime) -> PyReturn {
    format!("{slf}").to_py()
}

fn __richcmp__(
    cls: HeapType<OffsetDateTime>,
    a: OffsetDateTime,
    b_obj: PyObj,
    op: c_int,
) -> PyReturn {
    let inst_a = a.instant();
    let inst_b = if let Some(odt) = b_obj.extract(cls) {
        odt.instant()
    } else {
        let &State {
            instant_type,
            zoned_datetime_type,
            system_datetime_type,
            ..
        } = cls.state();

        if let Some(inst) = b_obj.extract(instant_type) {
            inst
        } else if let Some(zdt) = b_obj.extract(zoned_datetime_type) {
            zdt.instant()
        } else if let Some(odt) = b_obj.extract(system_datetime_type) {
            odt.instant()
        } else {
            return not_implemented();
        }
    };
    match op {
        pyo3_ffi::Py_EQ => inst_a == inst_b,
        pyo3_ffi::Py_NE => inst_a != inst_b,
        pyo3_ffi::Py_LT => inst_a < inst_b,
        pyo3_ffi::Py_LE => inst_a <= inst_b,
        pyo3_ffi::Py_GT => inst_a > inst_b,
        pyo3_ffi::Py_GE => inst_a >= inst_b,
        _ => unreachable!(),
    }
    .to_py()
}

pub(crate) extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    hashmask(
        // SAFETY: self type is always passed to __hash__
        unsafe { slf.assume_heaptype::<OffsetDateTime>() }
            .1
            .instant()
            .pyhash(),
    )
}

fn __sub__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    let type_a = obj_a.type_();
    let type_b = obj_b.type_();

    // Easy case: OffsetDT - OffsetDT
    let (state, inst_a, inst_b) = if type_a == type_b {
        // SAFETY: at least one of the objects is an OffsetDateTime, so both are
        let (odt_type, slf) = unsafe { obj_a.assume_heaptype::<OffsetDateTime>() };
        let (_, other) = unsafe { obj_b.assume_heaptype::<OffsetDateTime>() };
        (odt_type.state(), slf.instant(), other.instant())
    // Other cases are more difficult, as they can be triggered
    // by reflexive operations with arbitrary types.
    // We need to eliminate them carefully.
    } else if let Some(state) = type_a.same_module(type_b) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (_, slf) = unsafe { obj_a.assume_heaptype::<OffsetDateTime>() };
        let inst_b = if let Some(inst) = obj_b.extract(state.instant_type) {
            inst
        } else if let Some(zdt) = obj_b.extract(state.zoned_datetime_type) {
            zdt.instant()
        } else if let Some(odt) = obj_b.extract(state.system_datetime_type) {
            odt.instant()
        } else if type_b == state.time_delta_type.into()
            || type_b == state.date_delta_type.into()
            || type_b == state.datetime_delta_type.into()
        {
            raise(
                state.exc_implicitly_ignoring_dst.as_ptr(),
                doc::ADJUST_OFFSET_DATETIME_MSG,
            )?
        } else {
            raise_type_err(format!(
                "unsupported operand type(s) for +/-: {type_a} and {type_b}"
            ))?
        };
        (state, slf.instant(), inst_b)
    } else {
        return not_implemented();
    };
    inst_a.diff(inst_b).to_obj(state.time_delta_type)
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(OffsetDateTime, Py_tp_new, __new__),
    slotmethod!(OffsetDateTime, Py_tp_str, __str__, 1),
    slotmethod!(OffsetDateTime, Py_tp_repr, __repr__, 1),
    slotmethod!(OffsetDateTime, Py_tp_richcompare, __richcmp__),
    slotmethod!(Py_nb_subtract, __sub__, 2),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::OFFSETDATETIME.as_ptr() as *mut c_void,
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

fn exact_eq(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime, obj_b: PyObj) -> PyReturn {
    if let Some(odt) = obj_b.extract(cls) {
        (slf == odt).to_py()
    } else {
        raise_type_err("Can't compare different types")?
    }
}

pub(crate) fn to_instant(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    slf.instant().to_obj(cls.state().instant_type)
}

pub(crate) fn instant(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    // SAFETY: calling CPython API with valid arguments
    unsafe {
        PyErr_WarnEx(
            PyExc_DeprecationWarning,
            c"instant() method is deprecated. Use to_instant() instead".as_ptr(),
            1,
        )
    };
    to_instant(cls, slf)
}

fn to_fixed_offset(cls: HeapType<OffsetDateTime>, slf_obj: PyObj, args: &[PyObj]) -> PyReturn {
    match *args {
        [] => Ok(slf_obj.newref()),
        [offset_obj] => {
            // SAFETY: self argument is always of OffsetDateTime type
            unsafe { slf_obj.assume_heaptype::<OffsetDateTime>() }
                .1
                .instant()
                .to_offset(Offset::from_obj(offset_obj, cls.state().time_delta_type)?)
                .ok_or_value_err("Resulting date is out of range")?
                .to_obj(cls)
        }
        _ => raise_type_err("to_fixed_offset() takes at most 1 argument"),
    }
}

fn to_tz(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime, tz_obj: PyObj) -> PyReturn {
    let &State {
        zoned_datetime_type,
        exc_tz_notfound,
        ref tz_store,
        ..
    } = cls.state();
    let tz = tz_store.obj_get(tz_obj, exc_tz_notfound)?;
    slf.instant()
        .to_tz(tz)
        .ok_or_value_err("Resulting datetime is out of range")?
        .to_obj(zoned_datetime_type)
}

fn to_system_tz(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    let &State {
        py_api,
        system_datetime_type,
        ..
    } = cls.state();
    slf.instant()
        .to_system_tz(py_api)?
        .to_obj(system_datetime_type)
}

pub(crate) fn unpickle(state: &State, arg: PyObj) -> PyReturn {
    let py_bytes = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;
    let mut packed = py_bytes.as_bytes()?;
    if packed.len() != 15 {
        raise_value_err("Invalid pickle data")?;
    }
    OffsetDateTime::new_unchecked(
        Date {
            year: Year::new_unchecked(unpack_one!(packed, u16)),
            month: Month::new_unchecked(unpack_one!(packed, u8)),
            day: unpack_one!(packed, u8),
        },
        Time {
            hour: unpack_one!(packed, u8),
            minute: unpack_one!(packed, u8),
            second: unpack_one!(packed, u8),
            subsec: SubSecNanos::new_unchecked(unpack_one!(packed, i32)),
        },
        Offset::new_unchecked(unpack_one!(packed, i32)),
    )
    .to_obj(state.offset_datetime_type)
}

fn py_datetime(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyResult<Owned<PyDateTime>> {
    slf.to_py(cls.state().py_api)
}

fn date(cls: HeapType<OffsetDateTime>, OffsetDateTime { date, .. }: OffsetDateTime) -> PyReturn {
    date.to_obj(cls.state().date_type)
}

fn time(cls: HeapType<OffsetDateTime>, OffsetDateTime { time, .. }: OffsetDateTime) -> PyReturn {
    time.to_obj(cls.state().time_type)
}

#[inline]
pub(crate) fn check_ignore_dst_kwarg(
    kwargs: &mut IterKwargs,
    &State {
        str_ignore_dst,
        exc_implicitly_ignoring_dst,
        ..
    }: &State,
    msg: &str,
) -> PyResult<()> {
    match kwargs.next() {
        Some((key, value))
            if kwargs.len() == 1 && key.py_eq(str_ignore_dst)? && value.is_true() =>
        {
            Ok(())
        }
        Some((key, _)) => raise_type_err(format!("Unknown keyword argument: {key}")),
        _ => raise(exc_implicitly_ignoring_dst.as_ptr(), msg),
    }
}

fn replace_date(
    cls: HeapType<OffsetDateTime>,
    OffsetDateTime { time, offset, .. }: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    check_ignore_dst_kwarg(kwargs, state, doc::ADJUST_OFFSET_DATETIME_MSG)?;
    let &[arg] = args else {
        raise_type_err("replace() takes exactly 1 positional argument")?
    };
    if let Some(date) = arg.extract(state.date_type) {
        OffsetDateTime::new(date, time, offset)
            .ok_or_value_err("New datetime is out of range")?
            .to_obj(cls)
    } else {
        raise_type_err("date must be a whenever.Date instance")
    }
}

fn replace_time(
    cls: HeapType<OffsetDateTime>,
    OffsetDateTime { date, offset, .. }: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    check_ignore_dst_kwarg(kwargs, state, doc::ADJUST_OFFSET_DATETIME_MSG)?;
    let &[arg] = args else {
        raise_type_err("replace() takes exactly 1 positional argument")?
    };
    if let Some(time) = arg.extract(state.time_type) {
        OffsetDateTime::new(date, time, offset)
            .ok_or_value_err("New datetime is out of range")?
            .to_obj(cls)
    } else {
        raise_type_err("date must be a whenever.Time instance")
    }
}

fn format_common_iso(cls: PyType, slf: OffsetDateTime) -> PyReturn {
    __str__(cls, slf)
}

fn replace(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    if !args.is_empty() {
        raise_type_err("replace() takes no positional arguments")?
    }
    let &State {
        str_ignore_dst,
        str_offset,
        str_year,
        str_month,
        str_day,
        str_hour,
        str_minute,
        str_second,
        str_nanosecond,
        time_delta_type,
        exc_implicitly_ignoring_dst,
        ..
    } = cls.state();
    let OffsetDateTime {
        date,
        time,
        mut offset,
    } = slf;
    let mut year = date.year.get().into();
    let mut month = date.month.get().into();
    let mut day = date.day.into();
    let mut hour = time.hour.into();
    let mut minute = time.minute.into();
    let mut second = time.second.into();
    let mut nanos = time.subsec.get() as _;
    let mut ignore_dst = false;

    handle_kwargs("replace", kwargs, |key, value, eq| {
        if eq(key, str_ignore_dst) {
            ignore_dst = value.is_true();
        } else if eq(key, str_offset) {
            offset = Offset::from_obj(value, time_delta_type)?;
        } else {
            return set_components_from_kwargs(
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
            );
        }
        Ok(true)
    })?;

    if !ignore_dst {
        raise(
            exc_implicitly_ignoring_dst.as_ptr(),
            doc::ADJUST_OFFSET_DATETIME_MSG,
        )?
    }

    let date = Date::from_longs(year, month, day).ok_or_value_err("Invalid date")?;
    let time = Time::from_longs(hour, minute, second, nanos).ok_or_value_err("Invalid time")?;
    OffsetDateTime::new(date, time, offset)
        .ok_or_value_err("Resulting datetime is out of range")?
        .to_obj(cls)
}

fn now(cls: HeapType<OffsetDateTime>, args: &[PyObj], kwargs: &mut IterKwargs) -> PyReturn {
    let state = cls.state();
    let &[offset_obj] = args else {
        raise_type_err("now() takes exactly 1 positional argument")?
    };
    check_ignore_dst_kwarg(kwargs, state, doc::OFFSET_NOW_DST_MSG)?;
    let offset = Offset::from_obj(offset_obj, state.time_delta_type)?;
    state
        .time_ns()?
        .to_offset(offset)
        // SAFETY: Exception types are safe to reference during Python runtime
        .ok_or_raise(unsafe { PyExc_OSError }, "Date is out of range")?
        .to_obj(cls)
}

fn from_py_datetime(cls: HeapType<OffsetDateTime>, arg: PyObj) -> PyReturn {
    if let Some(py_dt) = arg.cast_allow_subclass::<PyDateTime>() {
        OffsetDateTime::from_py(py_dt)?.to_obj(cls)
    } else {
        raise_type_err("Argument must be a datetime.datetime instance")?
    }
}

pub(crate) fn to_plain(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    slf.without_offset().to_obj(cls.state().plain_datetime_type)
}

pub(crate) fn local(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    // SAFETY: calling CPython API with valid arguments
    unsafe {
        PyErr_WarnEx(
            PyExc_DeprecationWarning,
            c"local() method is deprecated. Use to_plain() instead".as_ptr(),
            1,
        )
    };
    to_plain(cls, slf)
}

pub(crate) fn timestamp(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.instant().epoch.get().to_py()
}

pub(crate) fn timestamp_millis(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.instant().timestamp_millis().to_py()
}

pub(crate) fn timestamp_nanos(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.instant().timestamp_nanos().to_py()
}

fn add(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    _shift_method(cls, slf, args, kwargs, false)
}

fn subtract(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    _shift_method(cls, slf, args, kwargs, true)
}

#[inline]
fn _shift_method(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
    negate: bool,
) -> PyReturn {
    let fname = if negate { "subtract" } else { "add" };
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
                None => {}
                _ => raise_type_err(format!(
                    "{fname}() can't mix positional and keyword arguments"
                ))?,
            }
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
            // FUTURE: some redundancy in checks
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
    if !ignore_dst {
        raise(
            state.exc_implicitly_ignoring_dst.as_ptr(),
            doc::ADJUST_OFFSET_DATETIME_MSG,
        )?
    }
    let OffsetDateTime { date, time, offset } = slf;
    DateTime { date, time }
        .shift_date(months, days)
        .and_then(|dt| dt.shift_nanos(nanos))
        .and_then(|dt| dt.with_offset(offset))
        .ok_or_else_value_err(|| format!("Result of {fname}() out of range"))?
        .to_obj(cls)
}

fn difference(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime, arg: PyObj) -> PyReturn {
    let state = cls.state();
    let other_inst = if let Some(odt) = arg.extract(cls) {
        odt.instant()
    } else if let Some(inst) = arg.extract(state.instant_type) {
        inst
    } else if let Some(zdt) = arg.extract(state.zoned_datetime_type) {
        zdt.instant()
    } else if let Some(odt) = arg.extract(state.system_datetime_type) {
        odt.instant()
    } else {
        raise_type_err(
            "difference() argument must be an OffsetDateTime, 
                Instant, ZonedDateTime, or SystemDateTime",
        )?
    };

    slf.instant().diff(other_inst).to_obj(state.time_delta_type)
}

fn __reduce__(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyResult<Owned<PyTuple>> {
    let OffsetDateTime {
        date: Date { year, month, day },
        time:
            Time {
                hour,
                minute,
                second,
                subsec,
            },
        offset,
    } = slf;
    let data = pack![
        year.get(),
        month.get(),
        day,
        hour,
        minute,
        second,
        subsec.get(),
        offset.get()
    ];
    (
        cls.state().unpickle_offset_datetime.newref(),
        (data.to_py()?,).into_pytuple()?,
    )
        .into_pytuple()
}

/// checks the args comply with (ts: ?, /, *, offset: ?, ignore_dst: true)
fn check_from_timestamp_args_return_offset(
    fname: &str,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
    &State {
        str_offset,
        str_ignore_dst,
        time_delta_type,
        exc_implicitly_ignoring_dst,
        ..
    }: &State,
) -> PyResult<Offset> {
    let mut ignore_dst = false;
    let mut offset = None;
    if args.len() != 1 {
        raise_type_err(format!(
            "{}() takes 1 positional argument but {} were given",
            fname,
            args.len()
        ))?
    }

    handle_kwargs("from_timestamp", kwargs, |key, value, eq| {
        if eq(key, str_ignore_dst) {
            ignore_dst = value.is_true();
        } else if eq(key, str_offset) {
            offset = Some(Offset::from_obj(value, time_delta_type)?);
        } else {
            return Ok(false);
        }
        Ok(true)
    })?;

    if !ignore_dst {
        raise(exc_implicitly_ignoring_dst.as_ptr(), doc::TIMESTAMP_DST_MSG)?
    }

    offset.ok_or_type_err("Missing required keyword argument: 'offset'")
}

fn from_timestamp(
    cls: HeapType<OffsetDateTime>,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    let offset = check_from_timestamp_args_return_offset("from_timestamp", args, kwargs, state)?;

    if let Some(py_int) = args[0].cast::<PyInt>() {
        Instant::from_timestamp(py_int.to_i64()?)
    } else if let Some(py_float) = args[0].cast::<PyFloat>() {
        Instant::from_timestamp_f64(py_float.to_f64()?)
    } else {
        raise_type_err("Timestamp must be an integer or float")?
    }
    .ok_or_value_err("Timestamp is out of range")?
    .to_offset(offset)
    .ok_or_value_err("Resulting date is out of range")?
    .to_obj(cls)
}

fn from_timestamp_millis(
    cls: HeapType<OffsetDateTime>,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    let offset =
        check_from_timestamp_args_return_offset("from_timestamp_millis", args, kwargs, state)?;
    Instant::from_timestamp_millis(
        args[0]
            .cast::<PyInt>()
            .ok_or_type_err("Timestamp must be an integer")?
            .to_i64()?,
    )
    .ok_or_value_err("timestamp is out of range")?
    .to_offset(offset)
    .ok_or_value_err("Resulting date is out of range")?
    .to_obj(cls)
}

fn from_timestamp_nanos(
    cls: HeapType<OffsetDateTime>,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    let offset =
        check_from_timestamp_args_return_offset("from_timestamp_nanos", args, kwargs, state)?;
    Instant::from_timestamp_nanos(
        args[0]
            .cast::<PyInt>()
            .ok_or_type_err("timestamp must be an integer")?
            .to_i128()?,
    )
    .ok_or_value_err("Timestamp is out of range")?
    .to_offset(offset)
    .ok_or_value_err("Resulting date is out of range")?
    .to_obj(cls)
}

fn parse_common_iso(cls: HeapType<OffsetDateTime>, arg: PyObj) -> PyReturn {
    OffsetDateTime::parse(
        arg.cast::<PyStr>()
            .ok_or_type_err("Expected a string")?
            .as_utf8()?,
    )
    .ok_or_else_value_err(|| format!("Invalid format: {arg}"))?
    .to_obj(cls)
}

fn parse_strptime(
    cls: HeapType<OffsetDateTime>,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    let format_obj = match kwargs.next() {
        Some((key, value)) if kwargs.len() == 1 && key.py_eq(state.str_format)? => value,
        _ => raise_type_err("parse_strptime() requires exactly one keyword argument `format`")?,
    };
    let &[arg_obj] = args else {
        raise_type_err(format!(
            "parse_strptime() takes exactly 1 positional argument, got {}",
            args.len()
        ))?
    };

    let args = (arg_obj.newref(), format_obj.newref()).into_pytuple()?;
    let parsed = state
        .strptime
        .call(*args)?
        .cast::<PyDateTime>()
        .ok_or_type_err("strptime() returned non-datetime")?;

    OffsetDateTime::from_py(*parsed)?.to_obj(cls)
}

fn format_rfc2822(_: PyType, slf: OffsetDateTime) -> PyReturn {
    let fmt = rfc2822::write(slf);
    // SAFETY: we know the format is ASCII only
    unsafe { std::str::from_utf8_unchecked(&fmt[..]) }.to_py()
}

fn parse_rfc2822(cls: HeapType<OffsetDateTime>, arg: PyObj) -> PyReturn {
    let s = arg.cast::<PyStr>().ok_or_type_err("Expected a string")?;
    let (date, time, offset) =
        rfc2822::parse(s.as_utf8()?).ok_or_else_value_err(|| format!("Invalid format: {arg}"))?;
    OffsetDateTime::new(date, time, offset)
        .ok_or_value_err("Instant out of range")?
        .to_obj(cls)
}

fn round(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let (_, increment, mode) = round::parse_args(cls.state(), args, kwargs, false, true)?;
    let OffsetDateTime {
        mut date,
        time,
        offset,
    } = slf;
    let (time_rounded, next_day) = time.round(increment as u64, mode);
    if next_day == 1 {
        date = date
            .tomorrow()
            .ok_or_value_err("Resulting datetime out of range")?;
    }
    OffsetDateTime {
        date,
        time: time_rounded,
        offset,
    }
    .to_obj(cls)
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(OffsetDateTime, __copy__, c""),
    method1!(OffsetDateTime, __deepcopy__, c""),
    method0!(OffsetDateTime, __reduce__, c""),
    classmethod_kwargs!(OffsetDateTime, now, doc::OFFSETDATETIME_NOW),
    method1!(OffsetDateTime, exact_eq, doc::EXACTTIME_EXACT_EQ),
    method0!(
        OffsetDateTime,
        py_datetime,
        doc::BASICCONVERSIONS_PY_DATETIME
    ),
    classmethod1!(
        OffsetDateTime,
        from_py_datetime,
        doc::OFFSETDATETIME_FROM_PY_DATETIME
    ),
    method0!(
        OffsetDateTime,
        to_instant,
        doc::EXACTANDLOCALTIME_TO_INSTANT
    ),
    method0!(OffsetDateTime, instant, c""), // deprecated alias
    method0!(OffsetDateTime, to_plain, doc::EXACTANDLOCALTIME_TO_PLAIN),
    method0!(OffsetDateTime, local, c""), // deprecated alias
    method1!(OffsetDateTime, to_tz, doc::EXACTTIME_TO_TZ),
    method_vararg!(
        OffsetDateTime,
        to_fixed_offset,
        doc::EXACTTIME_TO_FIXED_OFFSET
    ),
    method0!(OffsetDateTime, to_system_tz, doc::EXACTTIME_TO_SYSTEM_TZ),
    method0!(OffsetDateTime, date, doc::LOCALTIME_DATE),
    method0!(OffsetDateTime, time, doc::LOCALTIME_TIME),
    method0!(
        OffsetDateTime,
        format_rfc2822,
        doc::OFFSETDATETIME_FORMAT_RFC2822
    ),
    classmethod1!(
        OffsetDateTime,
        parse_rfc2822,
        doc::OFFSETDATETIME_PARSE_RFC2822
    ),
    method0!(
        OffsetDateTime,
        format_common_iso,
        doc::OFFSETDATETIME_FORMAT_COMMON_ISO
    ),
    classmethod1!(
        OffsetDateTime,
        parse_common_iso,
        doc::OFFSETDATETIME_PARSE_COMMON_ISO
    ),
    method0!(OffsetDateTime, timestamp, doc::EXACTTIME_TIMESTAMP),
    method0!(
        OffsetDateTime,
        timestamp_millis,
        doc::EXACTTIME_TIMESTAMP_MILLIS
    ),
    method0!(
        OffsetDateTime,
        timestamp_nanos,
        doc::EXACTTIME_TIMESTAMP_NANOS
    ),
    classmethod_kwargs!(
        OffsetDateTime,
        from_timestamp,
        doc::OFFSETDATETIME_FROM_TIMESTAMP
    ),
    classmethod_kwargs!(
        OffsetDateTime,
        from_timestamp_millis,
        doc::OFFSETDATETIME_FROM_TIMESTAMP_MILLIS
    ),
    classmethod_kwargs!(
        OffsetDateTime,
        from_timestamp_nanos,
        doc::OFFSETDATETIME_FROM_TIMESTAMP_NANOS
    ),
    method_kwargs!(OffsetDateTime, replace, doc::OFFSETDATETIME_REPLACE),
    method_kwargs!(
        OffsetDateTime,
        replace_date,
        doc::OFFSETDATETIME_REPLACE_DATE
    ),
    method_kwargs!(
        OffsetDateTime,
        replace_time,
        doc::OFFSETDATETIME_REPLACE_TIME
    ),
    classmethod_kwargs!(
        OffsetDateTime,
        parse_strptime,
        doc::OFFSETDATETIME_PARSE_STRPTIME
    ),
    method_kwargs!(OffsetDateTime, add, doc::OFFSETDATETIME_ADD),
    method_kwargs!(OffsetDateTime, subtract, doc::OFFSETDATETIME_SUBTRACT),
    method1!(OffsetDateTime, difference, doc::EXACTTIME_DIFFERENCE),
    method_kwargs!(OffsetDateTime, round, doc::OFFSETDATETIME_ROUND),
    classmethod_kwargs!(
        OffsetDateTime,
        __get_pydantic_core_schema__,
        doc::PYDANTIC_SCHEMA
    ),
    PyMethodDef::zeroed(),
];

fn year(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.date.year.get().to_py()
}

fn month(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.date.month.get().to_py()
}

fn day(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.date.day.to_py()
}

fn hour(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.time.hour.to_py()
}

fn minute(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.time.minute.to_py()
}

fn second(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.time.second.to_py()
}

fn nanosecond(_: PyType, slf: OffsetDateTime) -> PyReturn {
    slf.time.subsec.get().to_py()
}

fn offset(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    TimeDelta::from_offset(slf.offset).to_obj(cls.state().time_delta_type)
}

static mut GETSETTERS: &[PyGetSetDef] = &[
    getter!(OffsetDateTime, year, "The year component"),
    getter!(OffsetDateTime, month, "The month component"),
    getter!(OffsetDateTime, day, "The day component"),
    getter!(OffsetDateTime, hour, "The hour component"),
    getter!(OffsetDateTime, minute, "The minute component"),
    getter!(OffsetDateTime, second, "The second component"),
    getter!(OffsetDateTime, nanosecond, "The nanosecond component"),
    getter!(OffsetDateTime, offset, "The offset from UTC"),
    PyGetSetDef {
        name: NULL(),
        get: None,
        set: None,
        doc: NULL(),
        closure: NULL(),
    },
];

pub(crate) static mut SPEC: PyType_Spec =
    type_spec::<OffsetDateTime>(c"whenever.OffsetDateTime", unsafe { SLOTS });
