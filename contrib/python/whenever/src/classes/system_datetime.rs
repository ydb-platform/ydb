use core::ffi::{CStr, c_int, c_void};
use core::ptr::null_mut as NULL;
use pyo3_ffi::*;

use crate::{
    classes::{
        date::Date,
        datetime_delta::set_units_from_kwargs,
        instant::Instant,
        offset_datetime::{
            self, OffsetDateTime, instant, local, timestamp, timestamp_millis, timestamp_nanos,
            to_instant, to_plain,
        },
        plain_datetime::{DateTime, set_components_from_kwargs},
        time::Time,
        time_delta::TimeDelta,
    },
    common::{ambiguity::*, round, scalar::*},
    docstrings as doc,
    py::*,
    pymodule::State,
};

pub(crate) const SINGLETONS: &[(&CStr, OffsetDateTime); 0] = &[];

impl OffsetDateTime {
    pub(crate) fn resolve_system_tz(
        py_api: &PyDateTime_CAPI,
        date: Date,
        time: Time,
        dis: Option<Disambiguate>,
        preferred_offset: Offset,
        exc_repeated: PyObj,
        exc_skipped: PyObj,
    ) -> PyResult<Self> {
        match dis {
            Some(dis) => Self::resolve_system_tz_using_disambiguate(
                py_api,
                date,
                time,
                dis,
                exc_repeated,
                exc_skipped,
            ),
            None => Self::resolve_system_tz_using_offset(py_api, date, time, preferred_offset),
        }
    }

    pub(crate) fn resolve_system_tz_using_disambiguate(
        py_api: &PyDateTime_CAPI,
        date: Date,
        time: Time,
        dis: Disambiguate,
        exc_repeated: PyObj,
        exc_skipped: PyObj,
    ) -> PyResult<Self> {
        use Ambiguity::*;
        Ok(match offset_for_system_tz(py_api, date, time)? {
            Unambiguous(offset) => OffsetDateTime::new_unchecked(date, time, offset),
            Fold(offset0, offset1) => {
                let offset = match dis {
                    Disambiguate::Compatible | Disambiguate::Earlier => offset0,
                    Disambiguate::Later => offset1,
                    Disambiguate::Raise => raise(
                        exc_repeated.as_ptr(),
                        format!("{date} {time} is repeated in the system timezone"),
                    )?,
                };
                OffsetDateTime::new_unchecked(date, time, offset)
            }
            Gap(offset0, offset1) => {
                let (offset, shift) = match dis {
                    Disambiguate::Compatible | Disambiguate::Later => {
                        (offset1, offset1.sub(offset0))
                    }
                    Disambiguate::Earlier => (offset0, offset0.sub(offset1)),
                    Disambiguate::Raise => raise(
                        exc_skipped.as_ptr(),
                        format!("{date} {time} is skipped in the system timezone"),
                    )?,
                };
                DateTime { date, time }
                    .change_offset(shift)
                    .ok_or_value_err("Resulting date is out of range")?
                    .with_offset_unchecked(offset)
            }
        })
    }

    fn resolve_system_tz_using_offset(
        py_api: &PyDateTime_CAPI,
        date: Date,
        time: Time,
        target: Offset,
    ) -> PyResult<Self> {
        use Ambiguity::*;
        match offset_for_system_tz(py_api, date, time)? {
            Unambiguous(offset) => OffsetDateTime::new(date, time, offset),
            Fold(offset0, offset1) => OffsetDateTime::new(
                date,
                time,
                if target == offset1 { offset1 } else { offset0 },
            ),
            Gap(offset0, offset1) => {
                let (offset, shift) = if target == offset0 {
                    (offset0, offset0.sub(offset1))
                } else {
                    (offset1, offset1.sub(offset0))
                };
                DateTime { date, time }
                    .change_offset(shift)
                    .ok_or_value_err("Resulting date is out of range")?
                    .with_offset(offset)
            }
        }
        .ok_or_value_err("Resulting datetime is out of range")
    }

    pub(crate) fn to_system_tz(self, py_api: &PyDateTime_CAPI) -> PyResult<Self> {
        let dt_offset = self.to_py(py_api)?;
        let dt_local = dt_offset
            .getattr(c"astimezone")?
            .call0()?
            .cast::<PyDateTime>()
            .ok_or_raise(
                unsafe { PyExc_RuntimeError },
                "Unexpected result from astimezone()",
            )?;
        OffsetDateTime::from_py_with_subsec(dt_local.borrow(), self.time.subsec)
    }

    #[allow(clippy::too_many_arguments)]
    fn shift_in_system_tz(
        self,
        py_api: &PyDateTime_CAPI,
        months: DeltaMonths,
        days: DeltaDays,
        delta: TimeDelta,
        dis: Option<Disambiguate>,
        exc_repeated: PyObj,
        exc_skipped: PyObj,
    ) -> PyResult<Self> {
        let slf = if !months.is_zero() || !days.is_zero() {
            Self::resolve_system_tz(
                py_api,
                self.date
                    .shift(months, days)
                    .ok_or_value_err("Resulting date is out of range")?,
                self.time,
                dis,
                self.offset,
                exc_repeated,
                exc_skipped,
            )?
        } else {
            self
        };
        slf.instant()
            .shift(delta)
            .ok_or_value_err("Result is out of range")?
            .to_system_tz(py_api)
    }
}

fn offset_for_system_tz(py_api: &PyDateTime_CAPI, date: Date, time: Time) -> PyResult<Ambiguity> {
    let (offset0, shifted) = system_offset(date, time, 0, py_api)?;
    let (offset1, _) = system_offset(date, time, 1, py_api)?;

    Ok(if offset0 == offset1 {
        Ambiguity::Unambiguous(offset0)
    } else if shifted {
        // Before Python 3.12, the fold of system times was erroneously reversed
        // in case of gaps. See cpython/issues/83861
        #[cfg(Py_3_12)]
        {
            Ambiguity::Gap(offset1, offset0)
        }
        #[cfg(not(Py_3_12))]
        {
            Ambiguity::Gap(offset0, offset1)
        }
    } else {
        Ambiguity::Fold(offset0, offset1)
    })
}

fn system_offset(
    date: Date,
    time: Time,
    fold: i32,
    &PyDateTime_CAPI {
        DateTime_FromDateAndTimeAndFold,
        DateTime_FromDateAndTime,
        DateTimeType,
        ..
    }: &PyDateTime_CAPI,
) -> PyResult<(Offset, bool)> {
    // SAFETY: calling C API with valid arguments
    let naive = unsafe {
        DateTime_FromDateAndTimeAndFold(
            date.year.get().into(),
            date.month.get().into(),
            date.day.into(),
            time.hour.into(),
            time.minute.into(),
            time.second.into(),
            0, // assume no sub-second system offsets
            Py_None(),
            fold,
            DateTimeType,
        )
    }
    .rust_owned()?
    .cast::<PyDateTime>()
    .unwrap(); // Safe to assume result of C API is a datetime
    let aware = naive
        .getattr(c"astimezone")?
        .call0()?
        .cast::<PyDateTime>()
        .unwrap(); // Safe to assume result is a datetime

    // SAFETY: calling C API with valid arguments
    let shifted_naive = unsafe {
        DateTime_FromDateAndTime(
            aware.year(),
            aware.month(),
            aware.day(),
            aware.hour(),
            aware.minute(),
            aware.second(),
            0,
            Py_None(),
            DateTimeType,
        )
    }
    .rust_owned()?;
    Ok((
        Offset::from_py(aware.borrow())?,
        !naive.py_eq(*shifted_naive)?,
    ))
}

impl Instant {
    #[inline]
    pub(crate) fn to_system_tz(self, py_api: &PyDateTime_CAPI) -> PyResult<OffsetDateTime> {
        let dt_utc = self.to_py(py_api)?;
        let dt_new = dt_utc
            .getattr(c"astimezone")?
            .call0()?
            .cast::<PyDateTime>()
            // SAFETY: we can be sure that the result is a datetime
            .unwrap();
        Ok(OffsetDateTime::new_unchecked(
            Date::from_py(dt_new.date()),
            Time {
                hour: dt_new.hour() as _,
                minute: dt_new.minute() as _,
                second: dt_new.second() as _,
                // NOTE: to keep nanosecond precision, we need to use
                //       the subsec field of the original instant.
                //       This is OK since system offsets are always whole seconds.
                subsec: self.subsec,
            },
            Offset::from_py(dt_new.borrow())?,
        ))
    }
}

fn __new__(cls: HeapType<OffsetDateTime>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
    let &State {
        py_api,
        exc_repeated,
        exc_skipped,
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
        ..
    } = cls.state();
    let mut year = 0;
    let mut month = 0;
    let mut day = 0;
    let mut hour = 0;
    let mut minute = 0;
    let mut second = 0;
    let mut nanosecond = 0;
    let mut disambiguate: *mut PyObject = str_compatible.as_ptr();

    parse_args_kwargs!(
        args,
        kwargs,
        c"lll|lll$lU:SystemDateTime",
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
        disambiguate
    );

    let date = Date::from_longs(year, month, day).ok_or_value_err("Invalid date")?;
    let time =
        Time::from_longs(hour, minute, second, nanosecond).ok_or_value_err("Invalid time")?;
    let dis = Disambiguate::from_py(
        // SAFETY: initialized as non-null
        unsafe { PyObj::from_ptr_unchecked(disambiguate) },
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
    )?;
    OffsetDateTime::resolve_system_tz_using_disambiguate(
        py_api,
        date,
        time,
        dis,
        exc_repeated,
        exc_skipped,
    )?
    .to_obj(cls)
}

fn __repr__(_: PyType, slf: OffsetDateTime) -> PyReturn {
    let OffsetDateTime { date, time, offset } = slf;
    format!("SystemDateTime({date} {time}{offset})").to_py()
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
            offset_datetime_type,
            ..
        } = cls.state();

        if let Some(inst) = b_obj.extract(instant_type) {
            inst
        } else if let Some(zdt) = b_obj.extract(zoned_datetime_type) {
            zdt.instant()
        } else if let Some(odt) = b_obj.extract(offset_datetime_type) {
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

fn __add__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    if let Some(state) = obj_a.type_().same_module(obj_b.type_()) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (sdt_type, slf) = unsafe { obj_a.assume_heaptype::<OffsetDateTime>() };
        _shift_operator(state, sdt_type, slf, obj_b, false)
    } else {
        not_implemented()
    }
}

fn __sub__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    let type_a = obj_a.type_();
    let type_b = obj_b.type_();

    // Easy case: both are self type
    let (state, inst_a, inst_b) = if type_a == type_b {
        // SAFETY: at least one of the objects is an OffsetDateTime, so both are
        let (sdt_type, slf) = unsafe { obj_a.assume_heaptype::<OffsetDateTime>() };
        let (_, other) = unsafe { obj_b.assume_heaptype::<OffsetDateTime>() };
        (sdt_type.state(), slf.instant(), other.instant())
    // Other cases are more difficult, as they can be triggered
    // by reflexive operations with arbitrary types.
    // We need to eliminate them carefully.
    } else if let Some(state) = type_a.same_module(type_b) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (cls, slf) = unsafe { obj_a.assume_heaptype::<OffsetDateTime>() };
        let inst_b = if let Some(inst) = obj_b.extract(state.instant_type) {
            inst
        } else if let Some(zdt) = obj_b.extract(state.zoned_datetime_type) {
            zdt.instant()
        } else if let Some(odt) = obj_b.extract(state.offset_datetime_type) {
            odt.instant()
        } else {
            return _shift_operator(state, cls, slf, obj_b, true);
        };
        (state, slf.instant(), inst_b)
    } else {
        return not_implemented();
    };
    inst_a.diff(inst_b).to_obj(state.time_delta_type)
}

#[inline]
fn _shift_operator(
    state: &State,
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    arg: PyObj,
    negate: bool,
) -> PyReturn {
    let &State {
        time_delta_type,
        date_delta_type,
        datetime_delta_type,
        py_api,
        exc_repeated,
        exc_skipped,
        ..
    } = state;

    let mut months = DeltaMonths::ZERO;
    let mut days = DeltaDays::ZERO;
    let mut tdelta = TimeDelta::ZERO;

    if let Some(x) = arg.extract(time_delta_type) {
        tdelta = x;
    } else if let Some(x) = arg.extract(date_delta_type) {
        months = x.months;
        days = x.days;
    } else if let Some(x) = arg.extract(datetime_delta_type) {
        months = x.ddelta.months;
        days = x.ddelta.days;
        tdelta = x.tdelta;
    } else {
        raise_type_err(format!(
            "unsupported operand type for SystemDateTime and {}",
            arg.type_()
        ))?
    };

    if negate {
        months = -months;
        days = -days;
        tdelta = -tdelta;
    }

    slf.shift_in_system_tz(
        py_api,
        months,
        days,
        tdelta,
        None,
        exc_repeated,
        exc_skipped,
    )?
    .to_obj(cls)
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(OffsetDateTime, Py_tp_new, __new__),
    slotmethod!(OffsetDateTime, Py_tp_str, __str__, 1),
    slotmethod!(OffsetDateTime, Py_tp_repr, __repr__, 1),
    slotmethod!(OffsetDateTime, Py_tp_richcompare, __richcmp__),
    slotmethod!(Py_nb_add, __add__, 2),
    slotmethod!(Py_nb_subtract, __sub__, 2),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::SYSTEMDATETIME.as_ptr() as *mut c_void,
    },
    PyType_Slot {
        slot: Py_tp_hash,
        pfunc: offset_datetime::__hash__ as *mut c_void,
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

fn exact_eq(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime, arg: PyObj) -> PyReturn {
    if let Some(other) = arg.extract(cls) {
        (slf == other).to_py()
    } else {
        raise_type_err(format!("Argument must be same type, got {arg}"))
    }
}

pub(crate) fn unpickle(state: &State, arg: PyObj) -> PyReturn {
    let py_bytes = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;
    let mut packed = py_bytes.as_bytes()?;
    if packed.len() != 15 {
        raise_value_err("Invalid pickle data")?
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
    .to_obj(state.system_datetime_type)
}

fn py_datetime(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyResult<Owned<PyDateTime>> {
    slf.to_py(cls.state().py_api)
}

fn date(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    slf.date.to_obj(cls.state().date_type)
}

fn time(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    slf.time.to_obj(cls.state().time_type)
}

fn replace_date(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let &State {
        date_type,
        py_api,
        str_disambiguate,
        exc_skipped,
        exc_repeated,
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
        ..
    } = cls.state();

    let &[arg] = args else {
        raise_type_err(format!(
            "replace_date() takes 1 positional argument but {} were given",
            args.len()
        ))?
    };

    if let Some(date) = arg.extract(date_type) {
        let OffsetDateTime { time, offset, .. } = slf;
        OffsetDateTime::resolve_system_tz(
            py_api,
            date,
            time,
            Disambiguate::from_only_kwarg(
                kwargs,
                str_disambiguate,
                "replace_date",
                str_compatible,
                str_raise,
                str_earlier,
                str_later,
            )?,
            offset,
            exc_repeated,
            exc_skipped,
        )?
        .to_obj(cls)
    } else {
        raise_type_err("date must be a Date instance")
    }
}

fn replace_time(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let &State {
        time_type,
        py_api,
        str_disambiguate,
        exc_skipped,
        exc_repeated,
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
        ..
    } = cls.state();

    let &[arg] = args else {
        raise_type_err(format!(
            "replace_time() takes 1 positional argument but {} were given",
            args.len()
        ))?
    };

    if let Some(time) = arg.extract(time_type) {
        let OffsetDateTime { date, offset, .. } = slf;
        OffsetDateTime::resolve_system_tz(
            py_api,
            date,
            time,
            Disambiguate::from_only_kwarg(
                kwargs,
                str_disambiguate,
                "replace_time",
                str_compatible,
                str_raise,
                str_earlier,
                str_later,
            )?,
            offset,
            exc_repeated,
            exc_skipped,
        )?
        .to_obj(cls)
    } else {
        raise_type_err("time must be a Time instance")
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
        str_disambiguate,
        exc_repeated,
        exc_skipped,
        str_year,
        str_month,
        str_day,
        str_hour,
        str_minute,
        str_second,
        str_nanosecond,
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
        py_api,
        ..
    } = cls.state();
    let OffsetDateTime { date, time, offset } = slf;
    let mut year = date.year.get().into();
    let mut month = date.month.get().into();
    let mut day = date.day.into();
    let mut hour = time.hour.into();
    let mut minute = time.minute.into();
    let mut second = time.second.into();
    let mut nanos = time.subsec.get() as _;
    let mut dis = None;

    handle_kwargs("replace", kwargs, |key, value, eq| {
        if eq(key, str_disambiguate) {
            dis = Some(Disambiguate::from_py(
                value,
                str_compatible,
                str_raise,
                str_earlier,
                str_later,
            )?);
            Ok(true)
        } else {
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
        }
    })?;

    let date = Date::from_longs(year, month, day).ok_or_value_err("Invalid date")?;
    let time = Time::from_longs(hour, minute, second, nanos).ok_or_value_err("Invalid time")?;
    OffsetDateTime::resolve_system_tz(py_api, date, time, dis, offset, exc_repeated, exc_skipped)?
        .to_obj(cls)
}

fn now(cls: HeapType<OffsetDateTime>) -> PyReturn {
    let state = cls.state();
    let instant = state.time_ns()?;
    let dt_utc = instant.to_py(state.py_api)?;
    let dt_local = dt_utc
        .getattr(c"astimezone")?
        .call0()?
        .cast::<PyDateTime>()
        .ok_or_raise(
            unsafe { PyExc_RuntimeError },
            "Unexpected result from astimezone()",
        )?;
    OffsetDateTime::from_py_with_subsec(dt_local.borrow(), instant.subsec)?.to_obj(cls)
}

fn from_py_datetime(cls: HeapType<OffsetDateTime>, arg: PyObj) -> PyReturn {
    if let Some(py_dt) = arg.cast_allow_subclass::<PyDateTime>() {
        OffsetDateTime::from_py(py_dt)?.to_obj(cls)
    } else {
        raise_type_err("Argument must be a datetime.datetime instance")?
    }
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
                ..
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
        cls.state().unpickle_system_datetime.newref(),
        (data.to_py()?,).into_pytuple()?,
    )
        .into_pytuple()
}

fn from_timestamp(cls: HeapType<OffsetDateTime>, arg: PyObj) -> PyReturn {
    if let Some(py_int) = arg.cast::<PyInt>() {
        Instant::from_timestamp(py_int.to_i64()?)
    } else if let Some(py_float) = arg.cast::<PyFloat>() {
        Instant::from_timestamp_f64(py_float.to_f64()?)
    } else {
        raise_type_err("Timestamp must be an integer or float")?
    }
    .ok_or_value_err("Timestamp is out of range")?
    .to_system_tz(cls.state().py_api)?
    .to_obj(cls)
}

fn from_timestamp_millis(cls: HeapType<OffsetDateTime>, arg: PyObj) -> PyReturn {
    Instant::from_timestamp_millis(
        arg.cast::<PyInt>()
            .ok_or_type_err("Timestamp must be an integer")?
            .to_i64()?,
    )
    .ok_or_value_err("timestamp is out of range")?
    .to_system_tz(cls.state().py_api)?
    .to_obj(cls)
}

fn from_timestamp_nanos(cls: HeapType<OffsetDateTime>, arg: PyObj) -> PyReturn {
    Instant::from_timestamp_nanos(
        arg.cast::<PyInt>()
            .ok_or_type_err("Timestamp must be an integer")?
            .to_i128()?,
    )
    .ok_or_value_err("timestamp is out of range")?
    .to_system_tz(cls.state().py_api)?
    .to_obj(cls)
}

fn parse_common_iso(cls: HeapType<OffsetDateTime>, s_obj: PyObj) -> PyReturn {
    OffsetDateTime::parse(
        s_obj
            .cast::<PyStr>()
            .ok_or_type_err("argument must be a string")?
            .as_utf8()?,
    )
    .ok_or_else_value_err(|| format!("Invalid format: {s_obj}"))?
    .to_obj(cls)
}

fn to_fixed_offset(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime, args: &[PyObj]) -> PyReturn {
    match *args {
        [] => slf.to_obj(cls.state().offset_datetime_type),
        [arg] => {
            let &State {
                offset_datetime_type,
                time_delta_type,
                ..
            } = cls.state();
            let offset = Offset::from_obj(arg, time_delta_type)?;
            slf.instant()
                .to_offset(offset)
                .ok_or_value_err("Resulting local date out of range")?
                .to_obj(offset_datetime_type)
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
    slf.to_system_tz(cls.state().py_api)?.to_obj(cls)
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
    let &State {
        str_disambiguate,
        time_delta_type,
        date_delta_type,
        datetime_delta_type,
        str_compatible,
        str_raise,
        str_earlier,
        str_later,
        ..
    } = state;
    let mut dis = None;
    let mut months = DeltaMonths::ZERO;
    let mut days = DeltaDays::ZERO;
    let mut tdelta = TimeDelta::ZERO;

    match *args {
        [arg] => {
            match kwargs.next() {
                Some((key, value)) if kwargs.len() == 1 && key.py_eq(state.str_disambiguate)? => {
                    dis = Some(Disambiguate::from_py(
                        value,
                        str_compatible,
                        str_raise,
                        str_earlier,
                        str_later,
                    )?)
                }
                Some(_) => raise_type_err(format!(
                    "{fname}() can't mix positional and keyword arguments"
                ))?,
                None => {}
            };

            if let Some(x) = arg.extract(time_delta_type) {
                // SAFETY: we know that tdelta is a TimeDelta
                tdelta = x;
            } else if let Some(x) = arg.extract(date_delta_type) {
                months = x.months;
                days = x.days;
            } else if let Some(x) = arg.extract(datetime_delta_type) {
                tdelta = x.tdelta;
                months = x.ddelta.months;
                days = x.ddelta.days;
            } else {
                raise_type_err(format!("{fname}() argument must be a delta"))?
            }
        }
        [] => {
            let mut nanos = 0;
            let mut raw_months = 0;
            let mut raw_days = 0;
            handle_kwargs(fname, kwargs, |key, value, eq| {
                if eq(key, str_disambiguate) {
                    dis = Some(Disambiguate::from_py(
                        value,
                        str_compatible,
                        str_raise,
                        str_earlier,
                        str_later,
                    )?);
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
            tdelta = TimeDelta::from_nanos(nanos).ok_or_value_err("Total duration out of range")?;
            // FUTURE: these range checks are double
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
        tdelta = -tdelta;
    }
    slf.shift_in_system_tz(
        state.py_api,
        months,
        days,
        tdelta,
        dis,
        state.exc_repeated,
        state.exc_skipped,
    )?
    .to_obj(cls)
}

fn difference(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime, arg: PyObj) -> PyReturn {
    let inst_a = slf.instant();
    let state = cls.state();
    let inst_b = if let Some(sdt) = arg.extract(cls) {
        sdt.instant()
    } else if let Some(inst) = arg.extract(state.instant_type) {
        inst
    } else if let Some(zdt) = arg.extract(state.zoned_datetime_type) {
        zdt.instant()
    } else if let Some(odt) = arg.extract(state.offset_datetime_type) {
        odt.instant()
    } else {
        raise_type_err(
            "difference() argument must be an OffsetDateTime, Instant, ZonedDateTime, or SystemDateTime",
        )?
    };
    inst_a.diff(inst_b).to_obj(state.time_delta_type)
}

fn is_ambiguous(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    let OffsetDateTime { date, time, .. } = slf;
    matches!(
        offset_for_system_tz(cls.state().py_api, date, time)?,
        Ambiguity::Fold(_, _)
    )
    .to_py()
}

fn start_of_day(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    let OffsetDateTime { date, .. } = slf;
    let &State {
        py_api,
        exc_repeated,
        exc_skipped,
        ..
    } = cls.state();
    OffsetDateTime::resolve_system_tz_using_disambiguate(
        py_api,
        date,
        Time::MIDNIGHT,
        Disambiguate::Compatible,
        exc_repeated,
        exc_skipped,
    )?
    .to_obj(cls)
}

fn day_length(cls: HeapType<OffsetDateTime>, slf: OffsetDateTime) -> PyReturn {
    let OffsetDateTime { date, .. } = slf;
    let &State {
        py_api,
        exc_repeated,
        exc_skipped,
        time_delta_type,
        ..
    } = cls.state();
    let start_of_day = OffsetDateTime::resolve_system_tz_using_disambiguate(
        py_api,
        date,
        Time::MIDNIGHT,
        Disambiguate::Compatible,
        exc_repeated,
        exc_skipped,
    )?
    .instant();
    let start_of_next_day = OffsetDateTime::resolve_system_tz_using_disambiguate(
        py_api,
        date.tomorrow().ok_or_value_err("Date is out of range")?,
        Time::MIDNIGHT,
        Disambiguate::Compatible,
        exc_repeated,
        exc_skipped,
    )?
    .instant();
    start_of_next_day.diff(start_of_day).to_obj(time_delta_type)
}

fn round(
    cls: HeapType<OffsetDateTime>,
    slf: OffsetDateTime,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let state = cls.state();
    let (unit, increment, mode) = round::parse_args(state, args, kwargs, false, false)?;

    match unit {
        round::Unit::Day => _round_day(slf, state, mode),
        _ => {
            let OffsetDateTime {
                mut date,
                time,
                offset,
            } = slf;
            let (time_rounded, next_day) = time.round(increment as u64, mode);
            if next_day == 1 {
                date = date
                    .tomorrow()
                    .ok_or_value_err("Resulting date out of range")?;
            };
            OffsetDateTime::resolve_system_tz_using_offset(state.py_api, date, time_rounded, offset)
        }
    }?
    .to_obj(cls)
}

fn _round_day(slf: OffsetDateTime, state: &State, mode: round::Mode) -> PyResult<OffsetDateTime> {
    let OffsetDateTime { date, time, .. } = slf;
    let &State {
        py_api,
        exc_repeated,
        exc_skipped,
        ..
    } = state;
    let get_floor = || {
        OffsetDateTime::resolve_system_tz_using_disambiguate(
            py_api,
            date,
            Time::MIDNIGHT,
            Disambiguate::Compatible,
            exc_repeated,
            exc_skipped,
        )
    };
    let get_ceil = || {
        OffsetDateTime::resolve_system_tz_using_disambiguate(
            py_api,
            date.tomorrow().ok_or_value_err("Date out of range")?,
            Time::MIDNIGHT,
            Disambiguate::Compatible,
            exc_repeated,
            exc_skipped,
        )
    };
    match mode {
        round::Mode::Ceil => {
            // Round up anything *except* midnight (which is a no-op)
            if time == Time::MIDNIGHT {
                Ok(slf)
            } else {
                get_ceil()
            }
        }
        round::Mode::Floor => get_floor(),
        _ => {
            let time_ns = time.total_nanos();
            let floor = get_floor()?;
            let ceil = get_ceil()?;
            let day_ns = ceil.instant().diff(floor.instant()).total_nanos() as u64;
            debug_assert!(day_ns > 1);
            let threshold = match mode {
                round::Mode::HalfEven => day_ns / 2 + (time_ns % 2 == 0) as u64,
                round::Mode::HalfFloor => day_ns / 2 + 1,
                round::Mode::HalfCeil => day_ns / 2,
                _ => unreachable!(),
            };
            if time_ns >= threshold {
                Ok(ceil)
            } else {
                Ok(floor)
            }
        }
    }
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(OffsetDateTime, __copy__, c""),
    method1!(OffsetDateTime, __deepcopy__, c""),
    method0!(OffsetDateTime, __reduce__, c""),
    method1!(OffsetDateTime, to_tz, doc::EXACTTIME_TO_TZ),
    method0!(OffsetDateTime, to_system_tz, doc::EXACTTIME_TO_SYSTEM_TZ),
    method_vararg!(
        OffsetDateTime,
        to_fixed_offset,
        doc::EXACTTIME_TO_FIXED_OFFSET
    ),
    method1!(OffsetDateTime, exact_eq, doc::EXACTTIME_EXACT_EQ),
    method0!(
        OffsetDateTime,
        py_datetime,
        doc::BASICCONVERSIONS_PY_DATETIME
    ),
    method0!(
        OffsetDateTime,
        to_instant,
        doc::EXACTANDLOCALTIME_TO_INSTANT
    ),
    method0!(OffsetDateTime, instant, c""), // deprecated alias
    method0!(OffsetDateTime, to_plain, doc::EXACTANDLOCALTIME_TO_PLAIN),
    method0!(OffsetDateTime, local, c""), // deprecated alias
    method0!(OffsetDateTime, date, doc::LOCALTIME_DATE),
    method0!(OffsetDateTime, time, doc::LOCALTIME_TIME),
    method0!(
        OffsetDateTime,
        format_common_iso,
        doc::OFFSETDATETIME_FORMAT_COMMON_ISO
    ),
    classmethod1!(
        OffsetDateTime,
        parse_common_iso,
        doc::SYSTEMDATETIME_PARSE_COMMON_ISO
    ),
    classmethod0!(OffsetDateTime, now, doc::SYSTEMDATETIME_NOW),
    classmethod1!(
        OffsetDateTime,
        from_py_datetime,
        doc::SYSTEMDATETIME_FROM_PY_DATETIME
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
    classmethod1!(
        OffsetDateTime,
        from_timestamp,
        doc::SYSTEMDATETIME_FROM_TIMESTAMP
    ),
    classmethod1!(
        OffsetDateTime,
        from_timestamp_millis,
        doc::SYSTEMDATETIME_FROM_TIMESTAMP_MILLIS
    ),
    classmethod1!(
        OffsetDateTime,
        from_timestamp_nanos,
        doc::SYSTEMDATETIME_FROM_TIMESTAMP_NANOS
    ),
    method_kwargs!(OffsetDateTime, replace, doc::SYSTEMDATETIME_REPLACE),
    method_kwargs!(
        OffsetDateTime,
        replace_date,
        doc::SYSTEMDATETIME_REPLACE_DATE
    ),
    method_kwargs!(
        OffsetDateTime,
        replace_time,
        doc::SYSTEMDATETIME_REPLACE_TIME
    ),
    method_kwargs!(OffsetDateTime, add, doc::SYSTEMDATETIME_ADD),
    method_kwargs!(OffsetDateTime, subtract, doc::SYSTEMDATETIME_SUBTRACT),
    method1!(OffsetDateTime, difference, doc::EXACTTIME_DIFFERENCE),
    method0!(
        OffsetDateTime,
        is_ambiguous,
        doc::SYSTEMDATETIME_IS_AMBIGUOUS
    ),
    method0!(
        OffsetDateTime,
        start_of_day,
        doc::SYSTEMDATETIME_START_OF_DAY
    ),
    method0!(OffsetDateTime, day_length, doc::SYSTEMDATETIME_DAY_LENGTH),
    method_kwargs!(OffsetDateTime, round, doc::SYSTEMDATETIME_ROUND),
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
    type_spec::<OffsetDateTime>(c"whenever.SystemDateTime", unsafe { SLOTS });
