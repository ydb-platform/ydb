//! Functionality for rounding datetime values
use crate::{docstrings as doc, py::*, pymodule::State};

#[derive(Debug, Copy, Clone)]
pub(crate) enum Mode {
    Floor,
    Ceil,
    HalfFloor,
    HalfCeil,
    HalfEven,
}

impl Mode {
    fn from_py(
        s: PyObj,
        str_floor: PyObj,
        str_ceil: PyObj,
        str_half_floor: PyObj,
        str_half_ceil: PyObj,
        str_half_even: PyObj,
    ) -> PyResult<Mode> {
        match_interned_str("mode", s, |v, eq| {
            Some(if eq(v, str_floor) {
                Mode::Floor
            } else if eq(v, str_ceil) {
                Mode::Ceil
            } else if eq(v, str_half_floor) {
                Mode::HalfFloor
            } else if eq(v, str_half_ceil) {
                Mode::HalfCeil
            } else if eq(v, str_half_even) {
                Mode::HalfEven
            } else {
                None?
            })
        })
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum Unit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
}

impl Unit {
    #[allow(clippy::too_many_arguments)]
    fn from_py(
        s: PyObj,
        str_nanosecond: PyObj,
        str_microsecond: PyObj,
        str_millisecond: PyObj,
        str_second: PyObj,
        str_minute: PyObj,
        str_hour: PyObj,
        str_day: PyObj,
    ) -> PyResult<Unit> {
        // OPTIMIZE: run the comparisons in order if likelyhood
        match_interned_str("unit", s, |v, eq| {
            Some(if eq(v, str_nanosecond) {
                Unit::Nanosecond
            } else if eq(v, str_microsecond) {
                Unit::Microsecond
            } else if eq(v, str_millisecond) {
                Unit::Millisecond
            } else if eq(v, str_second) {
                Unit::Second
            } else if eq(v, str_minute) {
                Unit::Minute
            } else if eq(v, str_hour) {
                Unit::Hour
            } else if eq(v, str_day) {
                Unit::Day
            } else {
                None?
            })
        })
    }

    fn increment_from_py(self, v: PyObj, hours_increment_always_ok: bool) -> PyResult<i64> {
        let inc = v
            .cast::<PyInt>()
            .ok_or_type_err("increment must be an integer")?
            .to_i64()?;
        if inc <= 0 || inc >= 1000 {
            raise_value_err("increment must be between 0 and 1000")?;
        }
        match self {
            Unit::Nanosecond => (1_000 % inc == 0)
                .then_some(inc)
                .ok_or_value_err("Increment must be a divisor of 1000"),
            Unit::Microsecond => (1_000 % inc == 0)
                .then_some(inc * 1_000)
                .ok_or_value_err("Increment must be a divisor of 1000"),
            Unit::Millisecond => (1_000 % inc == 0)
                .then_some(inc * 1_000_000)
                .ok_or_value_err("Increment must be a divisor of 1000"),
            Unit::Second => (60 % inc == 0)
                .then_some(inc * 1_000_000_000)
                .ok_or_value_err("Increment must be a divisor of 60"),
            Unit::Minute => (60 % inc == 0)
                .then_some(inc * 60 * 1_000_000_000)
                .ok_or_value_err("Increment must be a divisor of 60"),
            Unit::Hour => (hours_increment_always_ok || 24 % inc == 0)
                .then_some(inc * 3_600 * 1_000_000_000)
                .ok_or_value_err("Increment must be a divisor of 24"),
            Unit::Day => (inc == 1)
                .then_some(86_400 * 1_000_000_000)
                .ok_or_value_err("Increment must be 1 for 'day' unit"),
        }
    }

    const fn default_increment(self) -> i64 {
        match self {
            Unit::Nanosecond => 1,
            Unit::Microsecond => 1_000,
            Unit::Millisecond => 1_000_000,
            Unit::Second => 1_000_000_000,
            Unit::Minute => 60 * 1_000_000_000,
            Unit::Hour => 3_600 * 1_000_000_000,
            Unit::Day => 86_400 * 1_000_000_000,
        }
    }
}

pub(crate) fn parse_args(
    state: &State,
    args: &[PyObj],
    kwargs: &mut IterKwargs,
    hours_largest_unit: bool,
    ignore_dst_kwarg: bool,
) -> PyResult<(Unit, i64, Mode)> {
    let &State {
        str_nanosecond,
        str_microsecond,
        str_millisecond,
        str_second,
        str_minute,
        str_hour,
        str_day,
        str_unit,
        str_mode,
        str_increment,
        str_floor,
        str_ceil,
        str_half_floor,
        str_half_ceil,
        str_half_even,
        str_ignore_dst,
        exc_implicitly_ignoring_dst,
        ..
    } = state;

    let num_argkwargs = args.len() + kwargs.len() as usize;
    if ignore_dst_kwarg {
        if args.len() > 3 {
            raise_type_err(format!(
                "round() takes at most 3 positional arguments, got {}",
                args.len()
            ))?;
        }
        if num_argkwargs > 4 {
            raise_type_err(format!(
                "round() takes at most 4 arguments, got {num_argkwargs}"
            ))?;
        }
    } else if num_argkwargs > 3 {
        raise_type_err(format!(
            "round() takes at most 3 arguments, got {num_argkwargs}"
        ))?;
    }
    let mut ignore_dst = false;
    let mut arg_obj: [Option<PyObj>; 3] = [None, None, None];
    for (i, &obj) in args.iter().enumerate() {
        arg_obj[i] = Some(obj)
    }
    handle_kwargs("round", kwargs, |key, value, eq| {
        for (i, &kwname) in [str_unit, str_increment, str_mode].iter().enumerate() {
            if eq(key, kwname) {
                if arg_obj[i].replace(value).is_some() {
                    raise_type_err(format!("round() got multiple values for argument {kwname}"))?;
                }
                return Ok(true);
            }
        }
        if ignore_dst_kwarg && eq(key, str_ignore_dst) {
            if value.is_true() {
                ignore_dst = true;
            }
            return Ok(true);
        }
        Ok(false)
    })?;

    if ignore_dst_kwarg && !ignore_dst {
        raise(
            exc_implicitly_ignoring_dst.as_ptr(),
            doc::OFFSET_ROUNDING_DST_MSG,
        )?
    }

    let unit = arg_obj[0]
        .map(|v| {
            Unit::from_py(
                v,
                str_nanosecond,
                str_microsecond,
                str_millisecond,
                str_second,
                str_minute,
                str_hour,
                str_day,
            )
        })
        .transpose()?
        .unwrap_or(Unit::Second);
    let increment = arg_obj[1]
        .map(|v| unit.increment_from_py(v, hours_largest_unit))
        .transpose()?
        .unwrap_or_else(|| unit.default_increment());
    let mode = arg_obj[2]
        .map(|v| {
            Mode::from_py(
                v,
                str_floor,
                str_ceil,
                str_half_floor,
                str_half_ceil,
                str_half_even,
            )
        })
        .transpose()?
        .unwrap_or(Mode::HalfEven);

    Ok((unit, increment, mode))
}
