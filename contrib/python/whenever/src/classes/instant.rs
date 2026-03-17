use core::ffi::{CStr, c_int, c_long, c_void};
use core::ptr::null_mut as NULL;
use pyo3_ffi::*;

use crate::{
    classes::{
        date::Date,
        datetime_delta::handle_exact_unit,
        offset_datetime::OffsetDateTime,
        plain_datetime::DateTime,
        time::Time,
        time_delta::{
            MAX_HOURS, MAX_MICROSECONDS, MAX_MILLISECONDS, MAX_MINUTES, MAX_SECS, TimeDelta,
        },
    },
    common::{rfc2822, round, scalar::*},
    docstrings as doc,
    py::*,
    pymodule::State,
};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub(crate) struct Instant {
    pub(crate) epoch: EpochSecs,
    pub(crate) subsec: SubSecNanos,
    // FUTURE: make use of padding to cache something?
}

pub(crate) const SINGLETONS: &[(&CStr, Instant); 2] = &[
    (
        c"MIN",
        Instant {
            epoch: EpochSecs::MIN,
            subsec: SubSecNanos::MIN,
        },
    ),
    (
        c"MAX",
        Instant {
            epoch: EpochSecs::MAX,
            subsec: SubSecNanos::MAX,
        },
    ),
];

impl Instant {
    pub(crate) fn from_datetime(date: Date, time: Time) -> Self {
        Instant {
            epoch: date.epoch_at(time),
            subsec: time.subsec,
        }
    }

    pub(crate) fn to_datetime(self) -> DateTime {
        self.epoch.datetime(self.subsec)
    }

    pub(crate) fn diff(self, other: Self) -> TimeDelta {
        let (extra_sec, subsec) = self.subsec.diff(other.subsec);
        TimeDelta {
            secs: self
                .epoch
                .diff(other.epoch)
                // Safety: we know that the difference between two instants is
                // always within delta range
                .add(extra_sec)
                .unwrap(),
            subsec,
        }
    }

    pub(crate) fn timestamp_millis(&self) -> i64 {
        self.epoch.get() * 1_000 + self.subsec.get() as i64 / 1_000_000
    }

    pub(crate) fn timestamp_nanos(&self) -> i128 {
        self.epoch.get() as i128 * 1_000_000_000 + self.subsec.get() as i128
    }

    pub(crate) fn from_timestamp(timestamp: i64) -> Option<Self> {
        Some(Instant {
            epoch: EpochSecs::new(timestamp)?,
            subsec: SubSecNanos::MIN,
        })
    }

    pub(crate) fn from_timestamp_f64(timestamp: f64) -> Option<Self> {
        (EpochSecs::MIN.get() as f64..=EpochSecs::MAX.get() as f64)
            .contains(&timestamp)
            .then(|| Instant {
                epoch: EpochSecs::new_unchecked(timestamp.floor() as i64),
                subsec: SubSecNanos::from_fract(timestamp),
            })
    }

    pub(crate) fn from_timestamp_millis(millis: i64) -> Option<Self> {
        Some(Instant {
            epoch: EpochSecs::new(millis.div_euclid(1_000))?,
            // Safety: we stay under 1_000_000_000
            subsec: SubSecNanos::new_unchecked(millis.rem_euclid(1_000) as i32 * 1_000_000),
        })
    }

    pub(crate) fn from_timestamp_nanos(timestamp: i128) -> Option<Self> {
        i64::try_from(timestamp.div_euclid(1_000_000_000))
            .ok()
            .and_then(EpochSecs::new)
            .map(|secs| Instant {
                epoch: secs,
                subsec: SubSecNanos::from_remainder(timestamp),
            })
    }

    pub(crate) fn shift(&self, d: TimeDelta) -> Option<Instant> {
        let (extra_sec, subsec) = self.subsec.add(d.subsec);
        Some(Instant {
            epoch: self.epoch.shift(d.secs)?.shift(extra_sec)?,
            subsec,
        })
    }

    pub(crate) fn offset(&self, f: Offset) -> Option<Self> {
        Some(Instant {
            epoch: self.epoch.offset(f)?,
            subsec: self.subsec,
        })
    }

    pub(crate) fn to_py(
        self,
        &PyDateTime_CAPI {
            DateTime_FromDateAndTime,
            TimeZone_UTC,
            DateTimeType,
            ..
        }: &PyDateTime_CAPI,
    ) -> PyReturn {
        let DateTime {
            date: Date { year, month, day },
            time:
                Time {
                    hour,
                    minute,
                    second,
                    subsec,
                },
        } = self.to_datetime();
        unsafe {
            // SAFETY: calling DateTime_FromDateAndTime with valid parameters
            DateTime_FromDateAndTime(
                year.get().into(),
                month.get().into(),
                day.into(),
                hour.into(),
                minute.into(),
                second.into(),
                (subsec.get() / 1_000) as _,
                TimeZone_UTC,
                DateTimeType,
            )
        }
        .rust_owned()
    }

    // Returns None if the datetime is out of range
    fn from_py(dt: PyDateTime) -> PyResult<Option<Self>> {
        let inst = Instant::from_datetime(Date::from_py(dt.date()), Time::from_py_dt(dt));
        Ok({
            let offset = dt.utcoffset()?;
            if let Some(py_delta) = offset.borrow().cast::<PyTimeDelta>() {
                // SAFETY: Python offsets are already bounded to +/- 24 hours: well within TimeDelta range.
                inst.shift(-TimeDelta::from_py_unchecked(py_delta))
            } else if offset.is_none() {
                raise_value_err("datetime is naive")?
            } else {
                raise_value_err("datetime utcoffset() returned non-delta value")?
            }
        })
    }

    pub(crate) const fn pyhash(&self) -> Py_hash_t {
        #[cfg(target_pointer_width = "64")]
        {
            hash_combine(
                self.epoch.get() as Py_hash_t,
                self.subsec.get() as Py_hash_t,
            )
        }
        #[cfg(target_pointer_width = "32")]
        hash_combine(
            self.epoch.get() as Py_hash_t,
            hash_combine(
                (self.epoch.get() >> 32) as Py_hash_t,
                self.subsec.get() as Py_hash_t,
            ),
        )
    }

    fn to_delta(self) -> TimeDelta {
        TimeDelta {
            secs: self.epoch.to_delta(),
            subsec: self.subsec,
        }
    }
}

fn __new__(_: HeapType<Instant>, _: PyTuple, _: Option<PyDict>) -> PyReturn {
    raise_type_err("Instant cannot be instantiated directly. Use .now() or .from_utc()")
}

fn from_utc(cls: HeapType<Instant>, args: PyTuple, kwargs: Option<PyDict>) -> PyReturn {
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
        c"lll|lll$l:Instant.from_utc",
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond
    );

    Instant::from_datetime(
        Date::from_longs(year, month, day).ok_or_value_err("Invalid date")?,
        Time::from_longs(hour, minute, second, nanosecond).ok_or_value_err("Invalid time")?,
    )
    .to_obj(cls)
}

impl PyWrapped for Instant {}

fn __repr__(_: PyType, i: Instant) -> PyReturn {
    let DateTime { date, time } = i.to_datetime();
    format!("Instant({date} {time}Z)").to_py()
}

fn __str__(_: PyType, i: Instant) -> PyReturn {
    let DateTime { date, time } = i.to_datetime();
    format!("{date}T{time}Z").to_py()
}

fn __richcmp__(cls: HeapType<Instant>, inst_a: Instant, b_obj: PyObj, op: c_int) -> PyReturn {
    let inst_b = if let Some(i) = b_obj.extract(cls) {
        i
    } else {
        let state = cls.state();
        if let Some(i) = b_obj.extract(state.zoned_datetime_type) {
            i.instant()
        } else if let Some(odt) = b_obj.extract(state.offset_datetime_type) {
            odt.instant()
        } else if let Some(odt) = b_obj.extract(state.system_datetime_type) {
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

extern "C" fn __hash__(slf: PyObj) -> Py_hash_t {
    hashmask(
        // SAFETY: we know the self object is an Instant
        unsafe { slf.assume_heaptype::<Instant>() }.1.pyhash(),
    )
}

fn __sub__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    let type_a = obj_a.type_();
    let type_b = obj_b.type_();

    // Easy case: Instant - Instant
    let (state, inst_a, inst_b) = if type_a == type_b {
        // SAFETY: one of the operands is guaranteed to be an Instant, so both are.
        let (inst_type, inst_a) = unsafe { obj_a.assume_heaptype::<Instant>() };
        let (_, inst_b) = unsafe { obj_b.assume_heaptype::<Instant>() };
        (inst_type.state(), inst_a, inst_b)
    // Other cases are more difficult, as they can be triggered
    // by reflexive operations with arbitrary types.
    // We need to eliminate them carefully.
    } else if let Some(state) = type_a.same_module(type_b) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (inst_type, inst_a) = unsafe { obj_a.assume_heaptype::<Instant>() };
        let inst_b = if let Some(zdt) = obj_b.extract(state.zoned_datetime_type) {
            zdt.instant()
        } else if let Some(odt) = obj_b.extract(state.offset_datetime_type) {
            odt.instant()
        } else if let Some(odt) = obj_b.extract(state.system_datetime_type) {
            odt.instant()
        } else {
            return _shift(inst_type, inst_a, state.time_delta_type, obj_b, true);
        };
        (state, inst_a, inst_b)
    } else {
        return not_implemented();
    };
    inst_a.diff(inst_b).to_obj(state.time_delta_type)
}

fn __add__(obj_a: PyObj, obj_b: PyObj) -> PyReturn {
    if let Some(state) = obj_a.type_().same_module(obj_b.type_()) {
        // SAFETY: the way we've structured binary operations within whenever
        // ensures that the first operand is the self type.
        let (inst_type, a) = unsafe { obj_a.assume_heaptype::<Instant>() };
        _shift(inst_type, a, state.time_delta_type, obj_b, false)
    } else {
        not_implemented()
    }
}

#[inline]
fn _shift(
    cls: HeapType<Instant>,
    inst: Instant,
    tdelta_cls: HeapType<TimeDelta>,
    obj_b: PyObj,
    negate: bool,
) -> PyReturn {
    let Some(mut delta) = obj_b.extract(tdelta_cls) else {
        return raise_type_err(format!(
            "unsupported operand type for Instant and {:?}",
            obj_b.type_()
        ));
    };
    if negate {
        delta = -delta;
    }
    inst.shift(delta)
        .ok_or_value_err("Resulting datetime is out of range")?
        .to_obj(cls)
}

#[allow(static_mut_refs)]
static mut SLOTS: &[PyType_Slot] = &[
    slotmethod!(Instant, Py_tp_new, __new__),
    slotmethod!(Instant, Py_tp_repr, __repr__, 1),
    slotmethod!(Instant, Py_tp_str, __str__, 1),
    slotmethod!(Instant, Py_tp_richcompare, __richcmp__),
    slotmethod!(Py_nb_subtract, __sub__, 2),
    slotmethod!(Py_nb_add, __add__, 2),
    PyType_Slot {
        slot: Py_tp_doc,
        pfunc: doc::INSTANT.as_ptr() as *mut c_void,
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
        slot: Py_tp_dealloc,
        pfunc: generic_dealloc as *mut c_void,
    },
    PyType_Slot {
        slot: 0,
        pfunc: NULL(),
    },
];

fn exact_eq(cls: HeapType<Instant>, slf: Instant, obj_b: PyObj) -> PyReturn {
    if let Some(i) = obj_b.extract(cls) {
        (slf == i).to_py()
    } else {
        raise_type_err("Can't compare different types")?
    }
}

fn __reduce__(
    cls: HeapType<Instant>,
    Instant { epoch, subsec }: Instant,
) -> PyResult<Owned<PyTuple>> {
    let data = pack![epoch.get(), subsec.get()];
    (
        cls.state().unpickle_instant.newref(),
        (data.to_py()?,).into_pytuple()?,
    )
        .into_pytuple()
}

pub(crate) fn unpickle(state: &State, arg: PyObj) -> PyReturn {
    let binding = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;
    let mut packed = binding.as_bytes()?;
    if packed.len() != 12 {
        raise_value_err("Invalid pickle data")?;
    }
    Instant {
        epoch: EpochSecs::new_unchecked(unpack_one!(packed, i64)),
        subsec: SubSecNanos::new_unchecked(unpack_one!(packed, i32)),
    }
    .to_obj(state.instant_type)
}

// Backwards compatibility: an unpickler for Instants pickled before 0.8.0
pub(crate) fn unpickle_pre_0_8(state: &State, arg: PyObj) -> PyReturn {
    let binding = arg
        .cast::<PyBytes>()
        .ok_or_type_err("Invalid pickle data")?;
    let mut packed = binding.as_bytes()?;
    if packed.len() != 12 {
        raise_value_err("Invalid pickle data")?;
    }
    Instant {
        epoch: EpochSecs::new_unchecked(unpack_one!(packed, i64) + EpochSecs::MIN.get() - 86_400),
        subsec: SubSecNanos::new_unchecked(unpack_one!(packed, i32)),
    }
    .to_obj(state.instant_type)
}

fn timestamp(_: PyType, slf: Instant) -> PyReturn {
    slf.epoch.get().to_py()
}

fn timestamp_millis(_: PyType, slf: Instant) -> PyReturn {
    slf.timestamp_millis().to_py()
}

fn timestamp_nanos(_: PyType, slf: Instant) -> PyReturn {
    slf.timestamp_nanos().to_py()
}

fn from_timestamp(cls: HeapType<Instant>, ts: PyObj) -> PyReturn {
    if let Some(py_int) = ts.cast::<PyInt>() {
        Instant::from_timestamp(py_int.to_i64()?)
    } else if let Some(py_float) = ts.cast::<PyFloat>() {
        Instant::from_timestamp_f64(py_float.to_f64()?)
    } else {
        return raise_type_err("Timestamp must be an integer or float");
    }
    .ok_or_value_err("Timestamp out of range")?
    .to_obj(cls)
}

fn from_timestamp_millis(cls: HeapType<Instant>, ts: PyObj) -> PyReturn {
    if let Some(py_int) = ts.cast::<PyInt>() {
        Instant::from_timestamp_millis(py_int.to_i64()?)
    } else {
        return raise_type_err("Timestamp must be an integer");
    }
    .ok_or_value_err("Timestamp out of range")?
    .to_obj(cls)
}

fn from_timestamp_nanos(cls: HeapType<Instant>, ts: PyObj) -> PyReturn {
    if let Some(py_int) = ts.cast::<PyInt>() {
        Instant::from_timestamp_nanos(py_int.to_i128()?)
    } else {
        return raise_type_err("Timestamp must be an integer");
    }
    .ok_or_value_err("Timestamp out of range")?
    .to_obj(cls)
}

fn py_datetime(cls: HeapType<Instant>, slf: Instant) -> PyReturn {
    slf.to_py(cls.state().py_api)
}

fn from_py_datetime(cls: HeapType<Instant>, obj: PyObj) -> PyReturn {
    if let Some(dt) = obj.cast_allow_subclass::<PyDateTime>() {
        Instant::from_py(dt)?
            .ok_or_else_value_err(|| format!("datetime {dt} out of range"))?
            .to_obj(cls)
    } else {
        raise_type_err("Expected a datetime object")
    }
}

fn now(cls: HeapType<Instant>) -> PyReturn {
    cls.state().time_ns()?.to_obj(cls)
}

fn format_common_iso(cls: PyType, slf: Instant) -> PyReturn {
    __str__(cls, slf)
}

fn parse_common_iso(cls: HeapType<Instant>, s_obj: PyObj) -> PyReturn {
    OffsetDateTime::parse(
        s_obj
            .cast::<PyStr>()
            .ok_or_type_err("Expected a string")?
            .as_utf8()?,
    )
    .ok_or_else_value_err(|| format!("Invalid format: {s_obj}"))?
    .instant()
    .to_obj(cls)
}

fn add(cls: HeapType<Instant>, slf: Instant, args: &[PyObj], kwargs: &mut IterKwargs) -> PyReturn {
    _shift_method(cls, slf, args, kwargs, false)
}

fn subtract(
    cls: HeapType<Instant>,
    slf: Instant,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    _shift_method(cls, slf, args, kwargs, true)
}

#[inline]
fn _shift_method(
    cls: HeapType<Instant>,
    instant: Instant,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
    negate: bool,
) -> PyReturn {
    let fname = if negate { "subtract" } else { "add" };
    let &State {
        str_hours,
        str_minutes,
        str_seconds,
        str_milliseconds,
        str_microseconds,
        str_nanoseconds,
        ..
    } = cls.state();
    let mut nanos: i128 = 0;

    if !args.is_empty() {
        raise_type_err(format!("{fname}() takes no positional arguments"))?;
    }
    handle_kwargs(fname, kwargs, |key, value, eq| {
        if eq(key, str_hours) {
            nanos += handle_exact_unit(value, MAX_HOURS, "hours", 3_600_000_000_000_i128)?;
        } else if eq(key, str_minutes) {
            nanos += handle_exact_unit(value, MAX_MINUTES, "minutes", 60_000_000_000_i128)?;
        } else if eq(key, str_seconds) {
            nanos += handle_exact_unit(value, MAX_SECS, "seconds", 1_000_000_000_i128)?;
        } else if eq(key, str_milliseconds) {
            nanos += handle_exact_unit(value, MAX_MILLISECONDS, "milliseconds", 1_000_000_i128)?;
        } else if eq(key, str_microseconds) {
            nanos += handle_exact_unit(value, MAX_MICROSECONDS, "microseconds", 1_000_i128)?;
        } else if eq(key, str_nanoseconds) {
            nanos = value
                .cast::<PyInt>()
                .ok_or_value_err("nanoseconds must be an integer")?
                .to_i128()?
                .checked_add(nanos)
                .ok_or_value_err("total nanoseconds out of range")?;
        } else {
            return Ok(false);
        }
        Ok(true)
    })?;
    if negate {
        nanos = -nanos;
    }

    instant
        .shift(TimeDelta::from_nanos(nanos).ok_or_value_err("Total duration out of range")?)
        .ok_or_value_err("Resulting datetime is out of range")?
        .to_obj(cls)
}

fn difference(cls: HeapType<Instant>, inst_a: Instant, obj_b: PyObj) -> PyReturn {
    let state = cls.state();

    let inst_b = if let Some(i) = obj_b.extract(cls) {
        i
    } else if let Some(zdt) = obj_b.extract(state.zoned_datetime_type) {
        zdt.instant()
    } else if let Some(odt) = obj_b.extract(state.offset_datetime_type) {
        odt.instant()
    } else if let Some(odt) = obj_b.extract(state.system_datetime_type) {
        odt.instant()
    } else {
        raise_type_err(
            "difference() argument must be an OffsetDateTime, 
             Instant, ZonedDateTime, or SystemDateTime",
        )?
    };
    inst_a.diff(inst_b).to_obj(state.time_delta_type)
}

fn to_tz(cls: HeapType<Instant>, slf: Instant, tz_obj: PyObj) -> PyReturn {
    let &State {
        zoned_datetime_type,
        exc_tz_notfound,
        ref tz_store,
        ..
    } = cls.state();
    let tz = tz_store.obj_get(tz_obj, exc_tz_notfound)?;
    slf.to_tz(tz)
        .ok_or_value_err("Resulting datetime is out of range")?
        .to_obj(zoned_datetime_type)
}

fn to_fixed_offset(cls: HeapType<Instant>, slf: Instant, args: &[PyObj]) -> PyReturn {
    let &State {
        offset_datetime_type,
        time_delta_type,
        ..
    } = cls.state();
    match *args {
        [] => slf.to_datetime().with_offset_unchecked(Offset::ZERO),
        [arg] => slf
            .to_offset(Offset::from_obj(arg, time_delta_type)?)
            .ok_or_value_err("Resulting date is out of range")?,
        _ => raise_type_err("to_fixed_offset() takes at most 1 argument")?,
    }
    .to_obj(offset_datetime_type)
}

fn to_system_tz(cls: HeapType<Instant>, slf: Instant) -> PyReturn {
    let &State {
        py_api,
        system_datetime_type,
        ..
    } = cls.state();
    slf.to_system_tz(py_api)?.to_obj(system_datetime_type)
}

fn format_rfc2822(_: PyType, slf: Instant) -> PyReturn {
    let fmt = rfc2822::write_gmt(slf);
    // SAFETY: we know the bytes are ASCII
    unsafe { std::str::from_utf8_unchecked(&fmt[..]) }.to_py()
}

fn parse_rfc2822(cls: HeapType<Instant>, s_obj: PyObj) -> PyReturn {
    let s = s_obj.cast::<PyStr>().ok_or_type_err("Expected a string")?;
    let (date, time, offset) =
        rfc2822::parse(s.as_utf8()?).ok_or_else_value_err(|| format!("Invalid format: {s_obj}"))?;
    OffsetDateTime::new(date, time, offset)
        .ok_or_value_err("Instant out of range")?
        .instant()
        .to_obj(cls)
}

fn round(
    cls: HeapType<Instant>,
    slf: Instant,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
) -> PyReturn {
    let (unit, increment, mode) = round::parse_args(cls.state(), args, kwargs, false, false)?;
    if unit == round::Unit::Day {
        raise_value_err(doc::CANNOT_ROUND_DAY_MSG)?;
    }
    let TimeDelta { secs, subsec } = slf
        .to_delta()
        .round(increment, mode)
        // SAFETY: TimeDelta has higher range than Instant,
        // so rounding cannot result in out-of-range
        .unwrap();
    Instant {
        epoch: EpochSecs::new(secs.get()).ok_or_value_err("Resulting instant out of range")?,
        subsec,
    }
    .to_obj(cls)
}

static mut METHODS: &[PyMethodDef] = &[
    method0!(Instant, __copy__, c""),
    method1!(Instant, __deepcopy__, c""),
    method0!(Instant, __reduce__, c""),
    method1!(Instant, exact_eq, doc::EXACTTIME_EXACT_EQ),
    method0!(Instant, timestamp, doc::EXACTTIME_TIMESTAMP),
    method0!(Instant, timestamp_millis, doc::EXACTTIME_TIMESTAMP_MILLIS),
    method0!(Instant, timestamp_nanos, doc::EXACTTIME_TIMESTAMP_NANOS),
    classmethod1!(Instant, from_timestamp, doc::INSTANT_FROM_TIMESTAMP),
    classmethod1!(
        Instant,
        from_timestamp_millis,
        doc::INSTANT_FROM_TIMESTAMP_MILLIS
    ),
    classmethod1!(
        Instant,
        from_timestamp_nanos,
        doc::INSTANT_FROM_TIMESTAMP_NANOS
    ),
    // This method is defined different because it
    // makes use of the arg/kwargs processing macro.
    // Other types only use it for the __new__ method.
    PyMethodDef {
        ml_name: c"from_utc".as_ptr(),
        ml_meth: PyMethodDefPointer {
            PyCFunctionWithKeywords: {
                unsafe extern "C" fn _wrap(
                    cls: *mut PyObject,
                    args: *mut PyObject,
                    kwargs: *mut PyObject,
                ) -> *mut PyObject {
                    from_utc(
                        unsafe { HeapType::<Instant>::from_ptr_unchecked(cls.cast()) },
                        unsafe { PyTuple::from_ptr_unchecked(args) },
                        (!kwargs.is_null()).then(|| unsafe { PyDict::from_ptr_unchecked(kwargs) }),
                    )
                    .to_py_owned_ptr()
                }
                _wrap
            },
        },
        ml_flags: METH_CLASS | METH_VARARGS | METH_KEYWORDS,
        ml_doc: doc::INSTANT_FROM_UTC.as_ptr(),
    },
    method0!(Instant, py_datetime, doc::BASICCONVERSIONS_PY_DATETIME),
    classmethod1!(Instant, from_py_datetime, doc::INSTANT_FROM_PY_DATETIME),
    classmethod0!(Instant, now, doc::INSTANT_NOW),
    method0!(Instant, format_rfc2822, doc::INSTANT_FORMAT_RFC2822),
    classmethod1!(Instant, parse_rfc2822, doc::INSTANT_PARSE_RFC2822),
    method0!(Instant, format_common_iso, doc::INSTANT_FORMAT_COMMON_ISO),
    classmethod1!(Instant, parse_common_iso, doc::INSTANT_PARSE_COMMON_ISO),
    method_kwargs!(Instant, add, doc::INSTANT_ADD),
    method_kwargs!(Instant, subtract, doc::INSTANT_SUBTRACT),
    method1!(Instant, to_tz, doc::EXACTTIME_TO_TZ),
    method0!(Instant, to_system_tz, doc::EXACTTIME_TO_SYSTEM_TZ),
    method_vararg!(Instant, to_fixed_offset, doc::EXACTTIME_TO_FIXED_OFFSET),
    method1!(Instant, difference, doc::EXACTTIME_DIFFERENCE),
    method_kwargs!(Instant, round, doc::INSTANT_ROUND),
    classmethod_kwargs!(Instant, __get_pydantic_core_schema__, doc::PYDANTIC_SCHEMA),
    PyMethodDef::zeroed(),
];

pub(crate) static mut SPEC: PyType_Spec =
    type_spec::<Instant>(c"whenever.Instant", unsafe { SLOTS });
