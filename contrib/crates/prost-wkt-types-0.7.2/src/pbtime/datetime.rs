//! A date/time type which exists primarily to convert [`Timestamp`]s into an RFC 3339 formatted
//! string.

////////////////////////////////////////////////////////////////////////////////
/// FROM prost-types/src/datetime.rs
////////////////////////////////////////////////////////////////////////////////
use core::fmt;

use crate::Duration;
use crate::Timestamp;

use super::TimestampError;

/// A point in time, represented as a date and time in the UTC timezone.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DateTime {
    /// The year.
    pub(crate) year: i64,
    /// The month of the year, from 1 to 12, inclusive.
    pub(crate) month: u8,
    /// The day of the month, from 1 to 31, inclusive.
    pub(crate) day: u8,
    /// The hour of the day, from 0 to 23, inclusive.
    pub(crate) hour: u8,
    /// The minute of the hour, from 0 to 59, inclusive.
    pub(crate) minute: u8,
    /// The second of the minute, from 0 to 59, inclusive.
    pub(crate) second: u8,
    /// The nanoseconds, from 0 to 999_999_999, inclusive.
    pub(crate) nanos: u32,
}

impl DateTime {
    /// The minimum representable [`Timestamp`] as a `DateTime`.
    pub(crate) const MIN: DateTime = DateTime {
        year: -292_277_022_657,
        month: 1,
        day: 27,
        hour: 8,
        minute: 29,
        second: 52,
        nanos: 0,
    };

    /// The maximum representable [`Timestamp`] as a `DateTime`.
    pub(crate) const MAX: DateTime = DateTime {
        year: 292_277_026_596,
        month: 12,
        day: 4,
        hour: 15,
        minute: 30,
        second: 7,
        nanos: 999_999_999,
    };

    /// Returns `true` if the `DateTime` is a valid calendar date.
    pub(crate) fn is_valid(&self) -> bool {
        self >= &DateTime::MIN
            && self <= &DateTime::MAX
            && self.month > 0
            && self.month <= 12
            && self.day > 0
            && self.day <= days_in_month(self.year, self.month)
            && self.hour < 24
            && self.minute < 60
            && self.second < 60
            && self.nanos < 1_000_000_000
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Pad years to at least 4 digits.
        if self.year > 9999 {
            write!(f, "+{}", self.year)?;
        } else if self.year < 0 {
            write!(f, "{:05}", self.year)?;
        } else {
            write!(f, "{:04}", self.year)?;
        };

        write!(
            f,
            "-{:02}-{:02}T{:02}:{:02}:{:02}",
            self.month, self.day, self.hour, self.minute, self.second,
        )?;

        // Format subseconds to either nothing, millis, micros, or nanos.
        let nanos = self.nanos;
        if nanos == 0 {
            write!(f, "Z")
        } else if nanos % 1_000_000 == 0 {
            write!(f, ".{:03}Z", nanos / 1_000_000)
        } else if nanos % 1_000 == 0 {
            write!(f, ".{:06}Z", nanos / 1_000)
        } else {
            write!(f, ".{:09}Z", nanos)
        }
    }
}

impl From<Timestamp> for DateTime {
    /// musl's [`__secs_to_tm`][1] converted to Rust via [c2rust][2] and then cleaned up by hand.
    ///
    /// All existing `strftime`-like APIs in Rust are unable to handle the full range of timestamps
    /// representable by `Timestamp`, including `strftime` itself, since tm.tm_year is an int.
    ///
    /// [1]: http://git.musl-libc.org/cgit/musl/tree/src/time/__secs_to_tm.c
    /// [2]: https://c2rust.com/
    fn from(mut timestamp: Timestamp) -> DateTime {
        timestamp.normalize();

        let t = timestamp.seconds;
        let nanos = timestamp.nanos;

        // 2000-03-01 (mod 400 year, immediately after feb29
        const LEAPOCH: i64 = 946_684_800 + 86400 * (31 + 29);
        const DAYS_PER_400Y: i32 = 365 * 400 + 97;
        const DAYS_PER_100Y: i32 = 365 * 100 + 24;
        const DAYS_PER_4Y: i32 = 365 * 4 + 1;
        const DAYS_IN_MONTH: [u8; 12] = [31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 29];

        // Note(dcb): this bit is rearranged slightly to avoid integer overflow.
        let mut days: i64 = (t / 86_400) - (LEAPOCH / 86_400);
        let mut remsecs: i32 = (t % 86_400) as i32;
        if remsecs < 0i32 {
            remsecs += 86_400;
            days -= 1
        }

        let mut qc_cycles: i32 = (days / i64::from(DAYS_PER_400Y)) as i32;
        let mut remdays: i32 = (days % i64::from(DAYS_PER_400Y)) as i32;
        if remdays < 0 {
            remdays += DAYS_PER_400Y;
            qc_cycles -= 1;
        }

        let mut c_cycles: i32 = remdays / DAYS_PER_100Y;
        if c_cycles == 4 {
            c_cycles -= 1;
        }
        remdays -= c_cycles * DAYS_PER_100Y;

        let mut q_cycles: i32 = remdays / DAYS_PER_4Y;
        if q_cycles == 25 {
            q_cycles -= 1;
        }
        remdays -= q_cycles * DAYS_PER_4Y;

        let mut remyears: i32 = remdays / 365;
        if remyears == 4 {
            remyears -= 1;
        }
        remdays -= remyears * 365;

        let mut years: i64 = i64::from(remyears)
            + 4 * i64::from(q_cycles)
            + 100 * i64::from(c_cycles)
            + 400 * i64::from(qc_cycles);

        let mut months: i32 = 0;
        while i32::from(DAYS_IN_MONTH[months as usize]) <= remdays {
            remdays -= i32::from(DAYS_IN_MONTH[months as usize]);
            months += 1
        }

        if months >= 10 {
            months -= 12;
            years += 1;
        }

        let date_time = DateTime {
            year: years + 2000,
            month: (months + 3) as u8,
            day: (remdays + 1) as u8,
            hour: (remsecs / 3600) as u8,
            minute: (remsecs / 60 % 60) as u8,
            second: (remsecs % 60) as u8,
            nanos: nanos as u32,
        };
        debug_assert!(date_time.is_valid());
        date_time
    }
}

/// Returns the number of days in the month.
fn days_in_month(year: i64, month: u8) -> u8 {
    const DAYS_IN_MONTH: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let (_, is_leap) = year_to_seconds(year);
    DAYS_IN_MONTH[usize::from(month - 1)] + u8::from(is_leap && month == 2)
}

macro_rules! ensure {
    ($expr:expr) => {{
        if !$expr {
            return None;
        }
    }};
}

/// Parses a date in RFC 3339 format from ASCII string `b`, returning the year, month, day, and
/// remaining input.
///
/// The date is not validated according to a calendar.
fn parse_date(s: &str) -> Option<(i64, u8, u8, &str)> {
    debug_assert!(s.is_ascii());

    // Smallest valid date is YYYY-MM-DD.
    ensure!(s.len() >= 10);

    // Parse the year in one of three formats:
    //  * +YYYY[Y]+
    //  * -[Y]+
    //  * YYYY
    let (year, s) = match s.as_bytes()[0] {
        b'+' => {
            let (digits, s) = parse_digits(&s[1..]);
            ensure!(digits.len() >= 5);
            let date: i64 = digits.parse().ok()?;
            (date, s)
        }
        b'-' => {
            let (digits, s) = parse_digits(&s[1..]);
            ensure!(digits.len() >= 4);
            let date: i64 = digits.parse().ok()?;
            (-date, s)
        }
        _ => {
            // Parse a 4 digit numeric.
            let (n1, s) = parse_two_digit_numeric(s)?;
            let (n2, s) = parse_two_digit_numeric(s)?;
            (i64::from(n1) * 100 + i64::from(n2), s)
        }
    };

    let s = parse_char(s, b'-')?;
    let (month, s) = parse_two_digit_numeric(s)?;
    let s = parse_char(s, b'-')?;
    let (day, s) = parse_two_digit_numeric(s)?;
    Some((year, month, day, s))
}

/// Parses a time in RFC 3339 format from ASCII string `s`, returning the hour, minute, second, and
/// nanos.
///
/// The date is not validated according to a calendar.
fn parse_time(s: &str) -> Option<(u8, u8, u8, u32, &str)> {
    debug_assert!(s.is_ascii());

    let (hour, s) = parse_two_digit_numeric(s)?;
    let s = parse_char(s, b':')?;
    let (minute, s) = parse_two_digit_numeric(s)?;
    let s = parse_char(s, b':')?;
    let (second, s) = parse_two_digit_numeric(s)?;

    let (nanos, s) = parse_nanos(s)?;

    Some((hour, minute, second, nanos, s))
}

/// Parses an optional nanosecond time from ASCII string `s`, returning the nanos and remaining
/// string.
fn parse_nanos(s: &str) -> Option<(u32, &str)> {
    debug_assert!(s.is_ascii());

    // Parse the nanoseconds, if present.
    let (nanos, s) = if let Some(s) = parse_char(s, b'.') {
        let (mut digits, s) = parse_digits(s);
        if digits.len() > 9 {
            digits = digits.split_at(9).0;
        }
        let nanos = 10u32.pow(9 - digits.len() as u32) * digits.parse::<u32>().ok()?;
        (nanos, s)
    } else {
        (0, s)
    };

    Some((nanos, s))
}

/// Parses a timezone offset in RFC 3339 format from ASCII string `s`, returning the offset hour,
/// offset minute, and remaining input.
fn parse_offset(s: &str) -> Option<(i8, i8, &str)> {
    debug_assert!(s.is_ascii());

    if s.is_empty() {
        // If no timezone specified, assume UTC.
        return Some((0, 0, s));
    }

    // Snowflake's timestamp format contains a space separator before the offset.
    let s = parse_char(s, b' ').unwrap_or(s);

    if let Some(s) = parse_char_ignore_case(s, b'Z') {
        Some((0, 0, s))
    } else {
        let (is_positive, s) = if let Some(s) = parse_char(s, b'+') {
            (true, s)
        } else if let Some(s) = parse_char(s, b'-') {
            (false, s)
        } else {
            return None;
        };

        let (hour, s) = parse_two_digit_numeric(s)?;

        let (minute, s) = if s.is_empty() {
            // No offset minutes are specified, e.g. +00 or +07.
            (0, s)
        } else {
            // Optional colon separator between the hour and minute digits.
            let s = parse_char(s, b':').unwrap_or(s);
            let (minute, s) = parse_two_digit_numeric(s)?;
            (minute, s)
        };

        // '-00:00' indicates an unknown local offset.
        ensure!(is_positive || hour > 0 || minute > 0);

        ensure!(hour < 24 && minute < 60);

        let hour = hour as i8;
        let minute = minute as i8;

        if is_positive {
            Some((hour, minute, s))
        } else {
            Some((-hour, -minute, s))
        }
    }
}

/// Parses a two-digit base-10 number from ASCII string `s`, returning the number and the remaining
/// string.
fn parse_two_digit_numeric(s: &str) -> Option<(u8, &str)> {
    debug_assert!(s.is_ascii());
    if s.len() < 2 {
        return None;
    }
    if s.starts_with('+') {
        return None;
    }
    let (digits, s) = s.split_at(2);
    Some((digits.parse().ok()?, s))
}

/// Splits ASCII string `s` at the first occurrence of a non-digit character.
fn parse_digits(s: &str) -> (&str, &str) {
    debug_assert!(s.is_ascii());

    let idx = s
        .as_bytes()
        .iter()
        .position(|c| !c.is_ascii_digit())
        .unwrap_or(s.len());
    s.split_at(idx)
}

/// Attempts to parse ASCII character `c` from ASCII string `s`, returning the remaining string. If
/// the character can not be parsed, returns `None`.
fn parse_char(s: &str, c: u8) -> Option<&str> {
    debug_assert!(s.is_ascii());

    ensure!(*s.as_bytes().first()? == c);
    Some(&s[1..])
}

/// Attempts to parse ASCII character `c` from ASCII string `s`, ignoring ASCII case, returning the
/// remaining string. If the character can not be parsed, returns `None`.
fn parse_char_ignore_case(s: &str, c: u8) -> Option<&str> {
    debug_assert!(s.is_ascii());

    ensure!(s.as_bytes().first()?.eq_ignore_ascii_case(&c));
    Some(&s[1..])
}

/// Returns the offset in seconds from the Unix epoch of the date time.
///
/// This is musl's [`__tm_to_secs`][1] converted to Rust via [c2rust[2] and then cleaned up by
/// hand.
///
/// [1]: https://git.musl-libc.org/cgit/musl/tree/src/time/__tm_to_secs.c
/// [2]: https://c2rust.com/
fn date_time_to_seconds(tm: &DateTime) -> i64 {
    let (start_of_year, is_leap) = year_to_seconds(tm.year);

    let seconds_within_year = month_to_seconds(tm.month, is_leap)
        + 86400 * u32::from(tm.day - 1)
        + 3600 * u32::from(tm.hour)
        + 60 * u32::from(tm.minute)
        + u32::from(tm.second);

    (start_of_year + i128::from(seconds_within_year)) as i64
}

/// Returns the number of seconds in the year prior to the start of the provided month.
///
/// This is musl's [`__month_to_secs`][1] converted to Rust via c2rust and then cleaned up by hand.
///
/// [1]: https://git.musl-libc.org/cgit/musl/tree/src/time/__month_to_secs.c
fn month_to_seconds(month: u8, is_leap: bool) -> u32 {
    const SECS_THROUGH_MONTH: [u32; 12] = [
        0,
        31 * 86400,
        59 * 86400,
        90 * 86400,
        120 * 86400,
        151 * 86400,
        181 * 86400,
        212 * 86400,
        243 * 86400,
        273 * 86400,
        304 * 86400,
        334 * 86400,
    ];
    let t = SECS_THROUGH_MONTH[usize::from(month - 1)];
    if is_leap && month > 2 {
        t + 86400
    } else {
        t
    }
}

/// Returns the offset in seconds from the Unix epoch of the start of a year.
///
/// musl's [`__year_to_secs`][1] converted to Rust via c2rust and then cleaned up by hand.
///
/// Returns an i128 because the start of the earliest supported year underflows i64.
///
/// [1]: https://git.musl-libc.org/cgit/musl/tree/src/time/__year_to_secs.c
pub(crate) fn year_to_seconds(year: i64) -> (i128, bool) {
    let is_leap;
    let year = year - 1900;

    // Fast path for years 1901 - 2038.
    if (1..=138).contains(&year) {
        let mut leaps: i64 = (year - 68) >> 2;
        if (year - 68).trailing_zeros() >= 2 {
            leaps -= 1;
            is_leap = true;
        } else {
            is_leap = false;
        }
        return (
            i128::from(31_536_000 * (year - 70) + 86400 * leaps),
            is_leap,
        );
    }

    let centuries: i64;
    let mut leaps: i64;

    let mut cycles: i64 = (year - 100) / 400;
    let mut rem: i64 = (year - 100) % 400;

    if rem < 0 {
        cycles -= 1;
        rem += 400
    }
    if rem == 0 {
        is_leap = true;
        centuries = 0;
        leaps = 0;
    } else {
        if rem >= 200 {
            if rem >= 300 {
                centuries = 3;
                rem -= 300;
            } else {
                centuries = 2;
                rem -= 200;
            }
        } else if rem >= 100 {
            centuries = 1;
            rem -= 100;
        } else {
            centuries = 0;
        }
        if rem == 0 {
            is_leap = false;
            leaps = 0;
        } else {
            leaps = rem / 4;
            rem %= 4;
            is_leap = rem == 0;
        }
    }
    leaps += 97 * cycles + 24 * centuries - i64::from(is_leap);

    (
        i128::from((year - 100) * 31_536_000) + i128::from(leaps * 86400 + 946_684_800 + 86400),
        is_leap,
    )
}

/// Parses a timestamp in RFC 3339 format from `s`.
pub(crate) fn parse_timestamp(s: &str) -> Option<Timestamp> {
    // Check that the string is ASCII, since subsequent parsing steps use byte-level indexing.
    ensure!(s.is_ascii());

    let (year, month, day, s) = parse_date(s)?;

    if s.is_empty() {
        // The string only contained a date.
        let date_time = DateTime {
            year,
            month,
            day,
            ..DateTime::default()
        };

        return Timestamp::try_from(date_time).ok();
    }

    // Accept either 'T' or ' ' as delimiter between date and time.
    let s = parse_char_ignore_case(s, b'T').or_else(|| parse_char(s, b' '))?;
    let (hour, minute, mut second, nanos, s) = parse_time(s)?;
    let (offset_hour, offset_minute, s) = parse_offset(s)?;

    ensure!(s.is_empty());

    // Detect whether the timestamp falls in a leap second. If this is the case, roll it back
    // to the previous second. To be maximally conservative, this should be checking that the
    // timestamp is the last second in the UTC day (23:59:60), and even potentially checking
    // that it's the final day of the UTC month, however these checks are non-trivial because
    // at this point we have, in effect, a local date time, since the offset has not been
    // applied.
    if second == 60 {
        second = 59;
    }

    let date_time = DateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanos,
    };

    let Timestamp { seconds, nanos } = Timestamp::try_from(date_time).ok()?;

    let seconds =
        seconds.checked_sub(i64::from(offset_hour) * 3600 + i64::from(offset_minute) * 60)?;

    Some(Timestamp { seconds, nanos })
}

/// Parse a duration in the [Protobuf JSON encoding spec format][1].
///
/// [1]: https://developers.google.com/protocol-buffers/docs/proto3#json
pub(crate) fn parse_duration(s: &str) -> Option<Duration> {
    // Check that the string is ASCII, since subsequent parsing steps use byte-level indexing.
    ensure!(s.is_ascii());

    let (is_negative, s) = match parse_char(s, b'-') {
        Some(s) => (true, s),
        None => (false, s),
    };

    let (digits, s) = parse_digits(s);
    let seconds = digits.parse::<i64>().ok()?;

    let (nanos, s) = parse_nanos(s)?;

    let s = parse_char(s, b's')?;
    ensure!(s.is_empty());
    ensure!(nanos < super::NANOS_PER_SECOND as u32);

    // If the duration is negative, also flip the nanos sign.
    let (seconds, nanos) = if is_negative {
        (-seconds, -(nanos as i32))
    } else {
        (seconds, nanos as i32)
    };

    Some(Duration { seconds, nanos })
}

impl TryFrom<DateTime> for Timestamp {
    type Error = TimestampError;

    fn try_from(date_time: DateTime) -> Result<Timestamp, TimestampError> {
        if !date_time.is_valid() {
            return Err(TimestampError::InvalidDateTime);
        }
        let seconds = date_time_to_seconds(&date_time);
        let nanos = date_time.nanos;
        Ok(Timestamp {
            seconds,
            nanos: nanos as i32,
        })
    }
}
