//! Functionality for working with POSIX TZ strings.
//! Note this includes extensions to the POSIX standard as part of the TZif format.
//!
//! Resources:
//! - [POSIX TZ strings](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap08.html)
//! - [GNU libc manual](https://www.gnu.org/software/libc/manual/html_node/TZ-Variable.html)
use crate::{
    classes::date::Date,
    common::{ambiguity::Ambiguity, parse::Scan, scalar::*},
};
use std::num::{NonZeroU8, NonZeroU16};

const DEFAULT_DST: OffsetDelta = OffsetDelta::new_unchecked(3_600);

// RFC 9636: the transition time may range from -167 to 167 hours! (not just 24)
pub(crate) type TransitionTime = i32;
const DEFAULT_RULE_TIME: i32 = 2 * 3_600; // 2 AM

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tz {
    std: Offset,
    dst: Option<Dst>,
    // We don't store the TZ names since we don't use them (yet)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Dst {
    offset: Offset,
    start: (Rule, TransitionTime),
    end: (Rule, TransitionTime),
}

/// A rule for the date when DST starts or ends
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rule {
    LastWeekday(Weekday, Month),
    NthWeekday(NonZeroU8, Weekday, Month), // N is 1..=4
    DayOfYear(NonZeroU16),                 // 1..=366, accounts for leap days
    JulianDayOfYear(NonZeroU16),           // 1..=365, ignores leap days
}

impl Tz {
    pub(crate) fn offset_for_instant(&self, epoch: EpochSecs) -> Offset {
        match self.dst {
            None => self.std, // No DST rule means a fixed offset
            Some(Dst {
                start: (start_rule, start_time),
                end: (end_rule, end_time),
                offset: dst_offset,
            }) => {
                // To determine the exact instant of DST start/end,
                // we need to know the *local* year.
                // However, this is theoretically difficult to determine
                // since we don't *strictly* know the if DST is active,
                // and thus what the offset should be.
                // However, in practice, we can assume that the year of
                // the transition isn't affected by the DST change.
                // This is what Python's `zoneinfo` does anyway...
                let year = epoch.saturating_offset(self.std).date().year;
                // Below are some saturing_add_i32 calls to prevent overflow.
                // These should only affect DST calculations at the extreme MIN/MAX
                // boundaries. This situation is exceedingly rare, but at least we don't crash.
                let start = start_rule
                    .for_year(year)
                    .epoch()
                    .saturating_add_i32(start_time - self.std.get());
                let end = end_rule
                    .for_year(year)
                    .epoch()
                    .saturating_add_i32(end_time - dst_offset.get());

                // Q: Why so complicated? A: Because end may be before start
                if (epoch >= end || epoch < start) && start < end || epoch < start && epoch >= end {
                    self.std
                } else {
                    dst_offset
                }
            }
        }
    }

    /// Get the offset for a local time, given as the number of seconds since the Unix epoch.
    pub(crate) fn ambiguity_for_local(&self, t: EpochSecs) -> Ambiguity {
        match self.dst {
            None => Ambiguity::Unambiguous(self.std), // No DST
            Some(Dst {
                start: (start_rule, start_time),
                end: (end_rule, end_time),
                offset: dst,
            }) => {
                // Below are some saturing_add_i32 calls to prevent overflow.
                // These should only affect DST calculations at the extreme MIN/MAX
                // boundaries. We just want to avoid crashing.
                let year = t.date().year; // OPTIMIZE: pass the year as an argument
                let start = start_rule
                    .for_year(year)
                    .epoch()
                    .saturating_add_i32(start_time);
                let end = end_rule.for_year(year).epoch().saturating_add_i32(end_time);

                // Compensate for inverted DST setups (e.g. Australia)
                // to ensure the two transition points (t1, t2) are in order.
                let (t1, t2, off1, off2, shift) = if start < end {
                    (start, end, self.std, dst, dst.sub(self.std).get())
                } else {
                    (end, start, dst, self.std, self.std.sub(dst).get())
                };

                // Positive DST: first a gap, then a fold
                if shift >= 0 {
                    if t < t1 {
                        Ambiguity::Unambiguous(off1)
                    } else if t < t1.saturating_add_i32(shift) {
                        Ambiguity::Gap(off2, off1)
                    } else if t < t2.saturating_add_i32(-shift) {
                        Ambiguity::Unambiguous(off2)
                    } else if t < t2 {
                        Ambiguity::Fold(off2, off1)
                    } else {
                        Ambiguity::Unambiguous(off1)
                    }
                // Negative DST: first a fold, then a gap
                } else if t < t1.saturating_add_i32(shift) {
                    Ambiguity::Unambiguous(off1)
                } else if t < t1 {
                    Ambiguity::Fold(off1, off2)
                } else if t < t2 {
                    Ambiguity::Unambiguous(off2)
                } else if t < t2.saturating_add_i32(-shift) {
                    Ambiguity::Gap(off1, off2)
                } else {
                    Ambiguity::Unambiguous(off1)
                }
            }
        }
    }
}

impl Rule {
    fn for_year(self, y: Year) -> Date {
        match self {
            Rule::DayOfYear(d) => y
                .unix_days_at_jan1()
                // Safe: no overflow since it stays within the year
                .add_unchecked(
                    (d.get()
                        // The 366th day will blow up for non-leap years.
                        // It's unlikely that a TZ string would specify this,
                        // so we'll just clamp it to the last day of the year.
                        .min(365 + y.is_leap() as u16)
                        - 1) as _,
                )
                .date(),

            Rule::JulianDayOfYear(d) => y
                .unix_days_at_jan1()
                // Safe: No overflow since it stays within the year
                .add_unchecked((d.get() - 1) as i32 + (y.is_leap() && d.get() > 59) as i32)
                .date(),

            Self::LastWeekday(w, m) => {
                // Try the last day of the month, and adjust from there
                let day_last = Date::last_of_month(y, m);
                Date {
                    day: day_last.day
                        - (day_last.day_of_week().sunday_is_0() + 7 - w.sunday_is_0()) % 7,
                    ..day_last
                }
            }
            Self::NthWeekday(n, w, m) => {
                // Try the first day of the month, and adjust from there
                debug_assert!(n.get() <= 4);
                let day1 = Date::first_of_month(y, m);
                Date {
                    day: ((w.sunday_is_0() + 7 - day1.day_of_week().sunday_is_0()) % 7)
                        + 7 * (n.get() - 1)
                        + 1,
                    ..day1
                }
            }
        }
    }
}

pub fn parse(s: &[u8]) -> Option<Tz> {
    let mut scan = Scan::new(s);
    skip_tzname(&mut scan)?;
    let std = parse_offset(&mut scan)?;

    // If there's nothing else, it's a fixed offset without DST
    if scan.is_done() {
        return Some(Tz { std, dst: None });
    };
    skip_tzname(&mut scan)?;

    let dst_offset = match scan.peek()? {
        // If the offset is omitted, the default is 1 hour ahead
        b',' => {
            scan.take_unchecked(1);
            // It's theoretically possible for this default shift to
            // bump the offset to over 24 hours. We reject these cases here.
            std.shift(DEFAULT_DST)?
        }
        // Otherwise, parse the offset
        _ => {
            let offset = parse_offset(&mut scan)?;
            scan.expect(b',')?;
            offset
        }
    };

    // Expect two rules separated by a comma
    let start = parse_rule(&mut scan)?;
    scan.expect(b',')?;
    let end = parse_rule(&mut scan)?;

    // No content should remain after parsing
    scan.is_done().then_some(Tz {
        std,
        dst: Some(Dst {
            offset: dst_offset,
            start,
            end,
        }),
    })
}

/// Skip the TZ name
fn skip_tzname(s: &mut Scan) -> Option<()> {
    // Note also that in Tzif files, TZ names are limited to 6 characters.
    // This might be useful in the future for optimization
    let tzname = match s.peek() {
        Some(b'<') => {
            let name = s.take_until_inclusive(|c| c == b'>')?;
            &name[1..name.len() - 1]
        }
        _ => s.take_until(|c| matches!(c, b'+' | b'-' | b',' | b'0'..=b'9'))?,
    };
    (!tzname.is_empty() && tzname.is_ascii()).then_some(())
}

/// Parse an offset like `[+|-]h[h][:mm[:ss]]`
fn parse_offset(s: &mut Scan) -> Option<Offset> {
    parse_hms(s, Offset::MAX.get())
        // POSIX offsets are inverted from how we store them
        .map(|s| Offset::new_unchecked(-s))
}

/// Parse a `h[hh][:mm[:ss]]` string into a total number of seconds
fn parse_hms(s: &mut Scan, max: i32) -> Option<i32> {
    let sign = s
        .transform(|c| match c {
            b'+' => Some(1),
            b'-' => Some(-1),
            _ => None,
        })
        .unwrap_or(1);
    let mut total = 0;

    // parse the hours
    let hrs = if max > 99 * 3_600 {
        s.up_to_3_digits()? as i32
    } else {
        s.up_to_2_digits()? as i32
    };
    total += hrs * 3_600;

    // parse the optional minutes and seconds
    if let Some(true) = s.advance_on(b':') {
        total += s.digits00_59()? as i32 * 60;
        if let Some(true) = s.advance_on(b':') {
            total += s.digits00_59()? as i32;
        }
    }
    (total <= max).then_some(total * sign)
}

/// Parse `m[m].w.d` string as part of a DST start/end rule
fn parse_weekday_rule(scan: &mut Scan) -> Option<Rule> {
    let m = scan.up_to_2_digits().and_then(Month::new)?;
    scan.expect(b'.')?;
    let w: NonZeroU8 = scan.digit_ranged(b'1'..=b'5')?.try_into().unwrap(); // safe >0 unwrap
    scan.expect(b'.')?;
    let d = scan.digit_ranged(b'0'..=b'6')?;
    // In Posix TZ strings, Sunday is 0. In ISO, it's 7
    let day_of_week = Weekday::from_iso_unchecked(if d == 0 { 7 } else { d });

    // A "fifth" occurrence of a weekday doesn't always occur.
    // Interpret it as the last weekday, according to the standard.
    Some(if w.get() == 5 {
        Rule::LastWeekday(day_of_week, m)
    } else {
        Rule::NthWeekday(w, day_of_week, m)
    })
}

fn parse_rule(scan: &mut Scan) -> Option<(Rule, TransitionTime)> {
    let rule = match scan.peek()? {
        b'M' => {
            scan.next();
            parse_weekday_rule(scan)
        }
        b'J' => {
            scan.next();
            NonZeroU16::new(scan.up_to_3_digits()?)
                .filter(|&d| d.get() <= 365)
                .map(Rule::JulianDayOfYear)
        }
        _ => NonZeroU16::new(scan.up_to_3_digits()? + 1)
            .filter(|&d| d.get() <= 366)
            .map(Rule::DayOfYear),
    }?;

    Some((
        rule,
        scan.expect(b'/')
            .and_then(|_| parse_hms(scan, 167 * 3_600))
            .unwrap_or(DEFAULT_RULE_TIME),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classes::time::Time;
    use crate::common::ambiguity::Ambiguity::*;

    fn unambig(offset: i32) -> Ambiguity {
        Ambiguity::Unambiguous(offset.try_into().unwrap())
    }

    fn fold(off1: i32, off2: i32) -> Ambiguity {
        Ambiguity::Fold(off1.try_into().unwrap(), off2.try_into().unwrap())
    }

    fn gap(off1: i32, off2: i32) -> Ambiguity {
        Ambiguity::Gap(off1.try_into().unwrap(), off2.try_into().unwrap())
    }

    fn mkdate(year: u16, month: u8, day: u8) -> Date {
        Date {
            year: Year::new_unchecked(year),
            month: Month::new_unchecked(month),
            day,
        }
    }

    #[test]
    fn invalid() {
        let cases: &[&[u8]] = &[
            // empty
            b"",
            // no offset
            b"FOO",
            // invalid tzname (digit)
            b"1T",
            b"<FOO>",
            b"<FOO>>-3",
            // Invalid components
            b"FOO+01:",
            b"FOO+01:9:03",
            b"FOO+01:60:03",
            b"FOO-01:59:60",
            b"FOO-01:59:",
            b"FOO-01:59:4",
            // offset too large
            b"FOO24",
            b"FOO+24",
            b"FOO-24",
            b"FOO-27:00",
            b"FOO+27:00",
            b"FOO-25:45:05",
            b"FOO+27:45:09",
            // invalid trailing data
            b"FOO+01:30M",
            // Unfinished rule
            b"FOO+01:30BAR,J",
            b"FOO+01:30BAR,",
            b"FOO+01:30BAR,M3.2.",
            // Invalid month rule
            b"FOO+01:30BAR,M13.2.1,M1.1.1",
            b"FOO+01:30BAR,M12.6.1,M1.1.1",
            b"FOO+01:30BAR,M12.2.7,M1.1.1",
            b"FOO+01:30BAR,M12.0.2,M1.1.1",
            // Invalid day of year
            b"FOO+01:30BAR,J366,M1.1.1",
            b"FOO+01:30BAR,J0,M1.1.1",
            b"FOO+01:30BAR,-1,M1.1.1",
            b"FOO+01:30BAR,366,M1.1.1",
            // Trailing data
            b"FOO+01:30BAR,M3.2.1,M1.1.1,",
            b"FOO+01:30BAR,M3.2.1,M1.1.1/0/1",
            // std + 1 hr exceeds 24 hours
            b"FOO-23:30BAR,M3.2.1,M1.1.1",
            // --- Below are test cases from python's zoneinfo ---
            b"PST8PDT",
            b"+11",
            b"GMT,M3.2.0/2,M11.1.0/3",
            b"GMT0+11,M3.2.0/2,M11.1.0/3",
            b"PST8PDT,M3.2.0/2",
            // Invalid offsets
            b"STD+25",
            b"STD-25",
            b"STD+374",
            b"STD+374DST,M3.2.0/2,M11.1.0/3",
            b"STD+23DST+25,M3.2.0/2,M11.1.0/3",
            b"STD-23DST-25,M3.2.0/2,M11.1.0/3",
            // Completely invalid dates
            b"AAA4BBB,M1443339,M11.1.0/3",
            b"AAA4BBB,M3.2.0/2,0349309483959c",
            // Invalid months
            b"AAA4BBB,M13.1.1/2,M1.1.1/2",
            b"AAA4BBB,M1.1.1/2,M13.1.1/2",
            b"AAA4BBB,M0.1.1/2,M1.1.1/2",
            b"AAA4BBB,M1.1.1/2,M0.1.1/2",
            // Invalid weeks
            b"AAA4BBB,M1.6.1/2,M1.1.1/2",
            b"AAA4BBB,M1.1.1/2,M1.6.1/2",
            // Invalid weekday
            b"AAA4BBB,M1.1.7/2,M2.1.1/2",
            b"AAA4BBB,M1.1.1/2,M2.1.7/2",
            // Invalid numeric offset
            b"AAA4BBB,-1/2,20/2",
            b"AAA4BBB,1/2,-1/2",
            b"AAA4BBB,367,20/2",
            b"AAA4BBB,1/2,367/2",
            // Invalid julian offset
            b"AAA4BBB,J0/2,J20/2",
            b"AAA4BBB,J20/2,J366/2",
        ];
        for &case in cases {
            assert_eq!(parse(case), None, "parse {:?}", unsafe {
                std::str::from_utf8_unchecked(case)
            });
        }
    }

    #[test]
    fn fixed_offset() {
        fn test(s: &[u8], expected: i32) {
            assert_eq!(
                parse(s).unwrap(),
                Tz {
                    std: expected.try_into().unwrap(),
                    dst: None
                },
                "{:?} -> {}",
                unsafe { std::str::from_utf8_unchecked(s) },
                expected
            );
        }

        let cases: &[(&[u8], i32)] = &[
            (b"FOO1", -3600),
            (b"FOOS0", 0),
            (b"FOO+01", -3600),
            (b"FOO+01:30", -3600 - 30 * 60),
            (b"FOO+01:30:59", -3600 - 30 * 60 - 59),
            (b"FOOM+23:59:59", -86_399),
            (b"FOOS-23:59:59", 86_399),
            (b"FOOBLA-23:59", 23 * 3600 + 59 * 60),
            (b"FOO-23", 23 * 3600),
            (b"FOO-01", 3600),
            (b"FOO-01:30", 3600 + 30 * 60),
            (b"FOO-01:30:59", 3600 + 30 * 60 + 59),
            (b"FOO+23:59:59", -86_399),
            (b"FOO+23:59", -23 * 3600 - 59 * 60),
            (b"FOO+23", -23 * 3600),
            (b"<FOO>-3", 3 * 3600),
        ];

        for &(s, expected) in cases {
            test(s, expected);
        }
    }

    #[test]
    fn with_dst() {
        // Implicit DST offset
        assert_eq!(
            parse(b"FOO-1FOOS,M3.5.0,M10.4.0").unwrap(),
            Tz {
                std: 3600.try_into().unwrap(),
                dst: Some(Dst {
                    offset: 7200.try_into().unwrap(),
                    start: (
                        Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                        DEFAULT_RULE_TIME
                    ),
                    end: (
                        Rule::NthWeekday(
                            4.try_into().unwrap(),
                            Weekday::Sunday,
                            10.try_into().unwrap()
                        ),
                        DEFAULT_RULE_TIME
                    )
                })
            }
        );
        // Explicit DST offset
        assert_eq!(
            parse(b"FOO+1FOOS2:30,M3.5.0,M10.2.0").unwrap(),
            Tz {
                std: (-3600).try_into().unwrap(),
                dst: Some(Dst {
                    offset: (-3600 * 2 - 30 * 60).try_into().unwrap(),
                    start: (
                        Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                        DEFAULT_RULE_TIME
                    ),
                    end: (
                        Rule::NthWeekday(
                            2.try_into().unwrap(),
                            Weekday::Sunday,
                            10.try_into().unwrap()
                        ),
                        DEFAULT_RULE_TIME
                    )
                })
            }
        );
        // Explicit time, weekday rule
        assert_eq!(
            parse(b"FOO+1FOOS2:30,M3.5.0/8,M10.2.0").unwrap(),
            Tz {
                std: (-3600).try_into().unwrap(),
                dst: Some(Dst {
                    offset: (-3600 * 2 - 30 * 60).try_into().unwrap(),
                    start: (
                        Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                        8 * 3_600
                    ),
                    end: (
                        Rule::NthWeekday(
                            2.try_into().unwrap(),
                            Weekday::Sunday,
                            10.try_into().unwrap()
                        ),
                        DEFAULT_RULE_TIME
                    )
                })
            }
        );
        // Explicit time, Julian day rule
        assert_eq!(
            parse(b"FOO+1FOOS2:30,J023/8:34:01,M10.2.0/03").unwrap(),
            Tz {
                std: (-3600).try_into().unwrap(),
                dst: Some(Dst {
                    offset: (-3600 * 2 - 30 * 60).try_into().unwrap(),
                    start: (
                        Rule::JulianDayOfYear(23.try_into().unwrap()),
                        8 * 3_600 + 34 * 60 + 1
                    ),
                    end: (
                        Rule::NthWeekday(
                            2.try_into().unwrap(),
                            Weekday::Sunday,
                            10.try_into().unwrap()
                        ),
                        3 * 3_600
                    )
                })
            }
        );
        // Explicit time, day-of-year rule
        assert_eq!(
            parse(b"FOO+1FOOS2:30,023/8:34:01,J1/0").unwrap(),
            Tz {
                std: (-3600).try_into().unwrap(),
                dst: Some(Dst {
                    offset: (-3600 * 2 - 30 * 60).try_into().unwrap(),
                    start: (
                        Rule::DayOfYear(24.try_into().unwrap()),
                        8 * 3_600 + 34 * 60 + 1
                    ),
                    end: (Rule::JulianDayOfYear(1.try_into().unwrap()), 0)
                })
            }
        );
        // Explicit time, zeroth day of year
        assert_eq!(
            parse(b"FOO+1FOOS2:30,00/8:34:01,J1/0").unwrap(),
            Tz {
                std: (-3600).try_into().unwrap(),
                dst: Some(Dst {
                    offset: (-3600 * 2 - 30 * 60).try_into().unwrap(),
                    start: (
                        Rule::DayOfYear(1.try_into().unwrap()),
                        8 * 3_600 + 34 * 60 + 1
                    ),
                    end: (Rule::JulianDayOfYear(1.try_into().unwrap()), 0)
                })
            }
        );
        // 24:00:00 is a valid time for a rule
        assert_eq!(
            parse(b"FOO+2FOOS+1,M3.5.0/24,M10.2.0").unwrap(),
            Tz {
                std: (-7200).try_into().unwrap(),
                dst: Some(Dst {
                    offset: (-3600).try_into().unwrap(),
                    start: (
                        Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                        86_400
                    ),
                    end: (
                        Rule::NthWeekday(
                            2.try_into().unwrap(),
                            Weekday::Sunday,
                            10.try_into().unwrap()
                        ),
                        DEFAULT_RULE_TIME
                    )
                })
            }
        );
        // Anything between -167 and 167 hours is also valid!
        assert_eq!(
            parse(b"FOO+2FOOS+1,M3.5.0/-89:02,M10.2.0/100").unwrap(),
            Tz {
                std: (-7200).try_into().unwrap(),
                dst: Some(Dst {
                    offset: (-3600).try_into().unwrap(),
                    start: (
                        Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                        -89 * 3_600 - 2 * 60
                    ),
                    end: (
                        Rule::NthWeekday(
                            2.try_into().unwrap(),
                            Weekday::Sunday,
                            10.try_into().unwrap()
                        ),
                        100 * 3_600
                    )
                })
            }
        );
    }

    #[test]
    fn day_of_year_rule_for_year() {
        fn test(year: u16, doy: u16, expected: (u16, u8, u8)) {
            let (y, m, d) = expected;
            assert_eq!(
                Rule::DayOfYear(doy.try_into().unwrap()).for_year(year.try_into().unwrap()),
                mkdate(y, m, d),
                "year: {year}, doy: {doy} -> {expected:?}"
            );
        }
        let cases = [
            // Extremes
            (1, 1, (1, 1, 1)),           // MIN day
            (9999, 366, (9999, 12, 31)), // MAX day
            // no leap year
            (2021, 1, (2021, 1, 1)),     // First day
            (2059, 40, (2059, 2, 9)),    // < Feb 28
            (2221, 59, (2221, 2, 28)),   // Feb 28
            (1911, 60, (1911, 3, 1)),    // Mar 1
            (1900, 124, (1900, 5, 4)),   // > Mar 1
            (2021, 365, (2021, 12, 31)), // Last day
            (2021, 366, (2021, 12, 31)), // Last day (clamped)
            // leap year
            (2024, 1, (2024, 1, 1)),     // First day
            (2060, 40, (2060, 2, 9)),    // < Feb 28
            (2228, 59, (2228, 2, 28)),   // Feb 28
            (2228, 60, (2228, 2, 29)),   // Feb 29
            (1920, 61, (1920, 3, 1)),    // Mar 1
            (2000, 125, (2000, 5, 4)),   // > Mar 1
            (2020, 365, (2020, 12, 30)), // second-to-last day
            (2020, 366, (2020, 12, 31)), // Last day
        ];

        for &(year, day_of_year, expected) in &cases {
            test(year, day_of_year, expected);
        }
    }

    #[test]
    fn julian_day_of_year_rule_for_year() {
        fn test(year: u16, doy: u16, expected: (u16, u8, u8)) {
            let (y, m, d) = expected;
            assert_eq!(
                Rule::JulianDayOfYear(doy.try_into().unwrap()).for_year(year.try_into().unwrap()),
                mkdate(y, m, d),
                "year: {year}, doy: {doy} -> {expected:?}"
            );
        }
        let cases = [
            // Extremes
            (1, 1, (1, 1, 1)),           // MIN day
            (9999, 365, (9999, 12, 31)), // MAX day
            // no leap year
            (2021, 1, (2021, 1, 1)),     // First day
            (2059, 40, (2059, 2, 9)),    // < Feb 28
            (2221, 59, (2221, 2, 28)),   // Feb 28
            (1911, 60, (1911, 3, 1)),    // Mar 1
            (1900, 124, (1900, 5, 4)),   // > Mar 1
            (2021, 365, (2021, 12, 31)), // Last day
            // leap year
            (2024, 1, (2024, 1, 1)),     // First day
            (2060, 40, (2060, 2, 9)),    // < Feb 28
            (2228, 59, (2228, 2, 28)),   // Feb 28
            (1920, 60, (1920, 3, 1)),    // Mar 1
            (2000, 124, (2000, 5, 4)),   // > Mar 1
            (2020, 364, (2020, 12, 30)), // second-to-last day
            (2020, 365, (2020, 12, 31)), // Last day
        ];

        for &(year, day_of_year, expected) in &cases {
            test(year, day_of_year, expected);
        }
    }

    #[test]
    fn last_weekday_rule_for_year() {
        fn test(year: u16, month: u8, weekday: Weekday, expected: (u16, u8, u8)) {
            let (y, m, d) = expected;
            assert_eq!(
                Rule::LastWeekday(weekday, month.try_into().unwrap())
                    .for_year(year.try_into().unwrap()),
                mkdate(y, m, d),
                "year: {year}, month: {month}, {weekday:?} -> {expected:?}"
            );
        }

        let cases = [
            (2024, 3, Weekday::Sunday, (2024, 3, 31)),
            (2024, 3, Weekday::Monday, (2024, 3, 25)),
            (1915, 7, Weekday::Sunday, (1915, 7, 25)),
            (1915, 7, Weekday::Saturday, (1915, 7, 31)),
            (1919, 7, Weekday::Thursday, (1919, 7, 31)),
            (1919, 7, Weekday::Sunday, (1919, 7, 27)),
        ];

        for &(year, month, weekday, expected) in &cases {
            test(year, month, weekday, expected);
        }
    }

    #[test]
    fn nth_weekday_rule_for_year() {
        fn test(year: u16, month: u8, nth: u8, weekday: Weekday, expected: (u16, u8, u8)) {
            let (y, m, d) = expected;
            assert_eq!(
                Rule::NthWeekday(nth.try_into().unwrap(), weekday, month.try_into().unwrap())
                    .for_year(year.try_into().unwrap()),
                mkdate(y, m, d),
                "year: {year}, month: {month}, nth: {nth}, {weekday:?} -> {expected:?}"
            );
        }

        let cases = [
            (1919, 7, 1, Weekday::Sunday, (1919, 7, 6)),
            (2002, 12, 1, Weekday::Sunday, (2002, 12, 1)),
            (2002, 12, 2, Weekday::Sunday, (2002, 12, 8)),
            (2002, 12, 3, Weekday::Saturday, (2002, 12, 21)),
            (1992, 2, 1, Weekday::Saturday, (1992, 2, 1)),
            (1992, 2, 4, Weekday::Saturday, (1992, 2, 22)),
        ];

        for &(year, month, nth, weekday, expected) in &cases {
            test(year, month, nth, weekday, expected);
        }
    }

    #[test]
    fn calculate_offsets() {
        let tz_fixed = Tz {
            std: 1234.try_into().unwrap(),
            dst: None,
        };
        // A TZ with random-ish DST rules
        let tz = Tz {
            std: 4800.try_into().unwrap(),
            dst: Some(Dst {
                offset: 9300.try_into().unwrap(),
                start: (
                    Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                    3600 * 4,
                ),
                end: (
                    Rule::JulianDayOfYear(281.try_into().unwrap()),
                    DEFAULT_RULE_TIME,
                ),
            }),
        };
        // A TZ with DST time rules that are very large, or negative!
        let tz_weirdtime = Tz {
            std: 4800.try_into().unwrap(),
            dst: Some(Dst {
                offset: 9300.try_into().unwrap(),
                start: (
                    Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                    50 * 3_600,
                ),
                end: (Rule::JulianDayOfYear(281.try_into().unwrap()), -2 * 3_600),
            }),
        };
        // A TZ with DST rules that are 00:00:00
        let tz00 = Tz {
            std: 4800.try_into().unwrap(),
            dst: Some(Dst {
                offset: 9300.try_into().unwrap(),
                start: (Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()), 0),
                end: (Rule::JulianDayOfYear(281.try_into().unwrap()), 0),
            }),
        };
        // A TZ with a DST offset smaller than the standard offset (theoretically possible)
        let tz_neg = Tz {
            std: 4800.try_into().unwrap(),
            dst: Some(Dst {
                offset: 1200.try_into().unwrap(),
                start: (
                    Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                    DEFAULT_RULE_TIME,
                ),
                end: (Rule::JulianDayOfYear(281.try_into().unwrap()), 4 * 3_600),
            }),
        };
        // Some timezones have DST end before start
        let tz_inverted = Tz {
            std: 4800.try_into().unwrap(),
            dst: Some(Dst {
                offset: 7200.try_into().unwrap(),
                end: (
                    Rule::LastWeekday(Weekday::Sunday, 3.try_into().unwrap()),
                    DEFAULT_RULE_TIME,
                ),
                start: (Rule::JulianDayOfYear(281.try_into().unwrap()), 4 * 3_600), // oct 8th
            }),
        };
        // Some timezones appear to be "always DST", like Africa/Casablanca
        let tz_always_dst = Tz {
            std: 7200.try_into().unwrap(),
            dst: Some(Dst {
                offset: 3600.try_into().unwrap(),
                start: (Rule::DayOfYear(1.try_into().unwrap()), 0),
                end: (Rule::JulianDayOfYear(365.try_into().unwrap()), 23 * 3600),
            }),
        };

        fn to_epoch_s(d: Date, t: Time, offset: Offset) -> EpochSecs {
            d.unix_days().epoch_at(t).offset(-offset).unwrap()
        }

        fn test(tz: Tz, ymd: (u16, u8, u8), hms: (u8, u8, u8), expected: Ambiguity) {
            let (y, m, d) = ymd;
            let (hour, minute, second) = hms;
            let date = mkdate(y, m, d);
            let time = Time {
                hour,
                minute,
                second,
                subsec: SubSecNanos::MIN,
            };
            assert_eq!(
                tz.ambiguity_for_local(to_epoch_s(date, time, Offset::ZERO)),
                expected,
                "{ymd:?} {hms:?} -> {expected:?} (tz: {tz:?})"
            );
            // Test that the inverse operation (epoch->local) works
            match expected {
                Unambiguous(offset) => {
                    let epoch = to_epoch_s(date, time, offset);
                    assert_eq!(
                        tz.offset_for_instant(epoch),
                        offset,
                        "tz: {tz:?} date: {date}, time: {time}, {offset:?}, {epoch:?}"
                    );
                }
                Fold(a, b) => {
                    let epoch_a = to_epoch_s(date, time, a);
                    let epoch_b = to_epoch_s(date, time, b);
                    assert_eq!(
                        tz.offset_for_instant(epoch_a),
                        a,
                        "(earlier offset) tz: {tz:?} date: {date}, time: {time}, {a:?}, {epoch_a:?}"
                    );
                    assert_eq!(
                        tz.offset_for_instant(epoch_b),
                        b,
                        "(later offset) tz: {tz:?} date: {date}, time: {time}, {b:?}, {epoch_b:?}"
                    );
                }
                Gap(_, _) => {} // Times in a gap aren't reversible
            }
        }

        let cases = [
            // fixed always the same
            (tz_fixed, (2020, 3, 19), (12, 34, 56), unambig(1234)),
            // First second of the year
            (tz, (1990, 1, 1), (0, 0, 0), unambig(4800)),
            // Last second of the year
            (tz, (1990, 12, 31), (23, 59, 59), unambig(4800)),
            // Well before the transition
            (tz, (1990, 3, 13), (12, 34, 56), unambig(4800)),
            // Gap: Before, start, mid, end, after
            (tz, (1990, 3, 25), (3, 59, 59), unambig(4800)),
            (tz, (1990, 3, 25), (4, 0, 0), gap(9300, 4800)),
            (tz, (1990, 3, 25), (5, 10, 0), gap(9300, 4800)),
            (tz, (1990, 3, 25), (5, 14, 59), gap(9300, 4800)),
            (tz, (1990, 3, 25), (5, 15, 0), unambig(9300)),
            // Well after the transition
            (tz, (1990, 6, 26), (8, 0, 0), unambig(9300)),
            // Fold: Before, start, mid, end, after
            (tz, (1990, 10, 8), (0, 44, 59), unambig(9300)),
            (tz, (1990, 10, 8), (0, 45, 0), fold(9300, 4800)),
            (tz, (1990, 10, 8), (1, 33, 59), fold(9300, 4800)),
            (tz, (1990, 10, 8), (1, 59, 59), fold(9300, 4800)),
            (tz, (1990, 10, 8), (2, 0, 0), unambig(4800)),
            // Well after the end of DST
            (tz, (1990, 11, 30), (23, 34, 56), unambig(4800)),
            // time outside 0-24h range is also valid for a rule
            (tz_weirdtime, (1990, 3, 26), (1, 59, 59), unambig(4800)),
            (tz_weirdtime, (1990, 3, 27), (2, 0, 0), gap(9300, 4800)),
            (tz_weirdtime, (1990, 3, 27), (3, 0, 0), gap(9300, 4800)),
            (tz_weirdtime, (1990, 3, 27), (3, 14, 59), gap(9300, 4800)),
            (tz_weirdtime, (1990, 3, 27), (3, 15, 0), unambig(9300)),
            (tz_weirdtime, (1990, 10, 7), (20, 44, 59), unambig(9300)),
            (tz_weirdtime, (1990, 10, 7), (20, 45, 0), fold(9300, 4800)),
            (tz_weirdtime, (1990, 10, 7), (21, 33, 59), fold(9300, 4800)),
            (tz_weirdtime, (1990, 10, 7), (21, 59, 59), fold(9300, 4800)),
            (tz_weirdtime, (1990, 10, 7), (22, 0, 0), unambig(4800)),
            (tz_weirdtime, (1990, 10, 7), (22, 0, 1), unambig(4800)),
            // 00:00:00 is a valid time for a rule
            (tz00, (1990, 3, 24), (23, 59, 59), unambig(4800)),
            (tz00, (1990, 3, 25), (0, 0, 0), gap(9300, 4800)),
            (tz00, (1990, 3, 25), (1, 0, 0), gap(9300, 4800)),
            (tz00, (1990, 3, 25), (1, 14, 59), gap(9300, 4800)),
            (tz00, (1990, 3, 25), (1, 15, 0), unambig(9300)),
            (tz00, (1990, 10, 7), (22, 44, 59), unambig(9300)),
            (tz00, (1990, 10, 7), (22, 45, 0), fold(9300, 4800)),
            (tz00, (1990, 10, 7), (23, 33, 59), fold(9300, 4800)),
            (tz00, (1990, 10, 7), (23, 59, 59), fold(9300, 4800)),
            (tz00, (1990, 10, 8), (0, 0, 0), unambig(4800)),
            (tz00, (1990, 10, 8), (0, 0, 1), unambig(4800)),
            // Negative DST should be handled gracefully. Gap and fold reversed
            // Fold instead of gap
            (tz_neg, (1990, 3, 25), (0, 59, 59), unambig(4800)),
            (tz_neg, (1990, 3, 25), (1, 0, 0), fold(4800, 1200)),
            (tz_neg, (1990, 3, 25), (1, 33, 59), fold(4800, 1200)),
            (tz_neg, (1990, 3, 25), (1, 59, 59), fold(4800, 1200)),
            (tz_neg, (1990, 3, 25), (2, 0, 0), unambig(1200)),
            // Gap instead of fold
            (tz_neg, (1990, 10, 8), (3, 59, 59), unambig(1200)),
            (tz_neg, (1990, 10, 8), (4, 0, 0), gap(4800, 1200)),
            (tz_neg, (1990, 10, 8), (4, 42, 12), gap(4800, 1200)),
            (tz_neg, (1990, 10, 8), (4, 59, 59), gap(4800, 1200)),
            (tz_neg, (1990, 10, 8), (5, 0, 0), unambig(4800)),
            // Always DST
            (tz_always_dst, (1990, 1, 1), (0, 0, 0), unambig(3600)),
            // This is actually incorrect, but ZoneInfo does the same...
            (tz_always_dst, (1992, 12, 31), (23, 0, 0), gap(7200, 3600)),
            // Inverted DST
            (tz_inverted, (1990, 2, 9), (15, 0, 0), unambig(7200)), // DST in effect
            (tz_inverted, (1990, 3, 25), (1, 19, 0), unambig(7200)), // Before fold
            (tz_inverted, (1990, 3, 25), (1, 20, 0), fold(7200, 4800)), // Fold starts
            (tz_inverted, (1990, 3, 25), (1, 59, 0), fold(7200, 4800)), // Fold almost over
            (tz_inverted, (1990, 3, 25), (2, 0, 0), unambig(4800)), // Fold over
            (tz_inverted, (1990, 9, 8), (8, 0, 0), unambig(4800)),  // DST not in effect
            (tz_inverted, (1990, 10, 8), (3, 59, 0), unambig(4800)), // Before gap
            (tz_inverted, (1990, 10, 8), (4, 0, 0), gap(7200, 4800)), // Gap starts
            (tz_inverted, (1990, 10, 8), (4, 39, 0), gap(7200, 4800)), // Gap almost over
            (tz_inverted, (1990, 10, 8), (4, 40, 0), unambig(7200)), // Gap over
            (tz_inverted, (1990, 12, 31), (23, 40, 0), unambig(7200)), // DST not in effect
        ];

        for &(tz, ymd, hms, expected) in &cases {
            test(tz, ymd, hms, expected);
        }

        // At the MIN/MAX epoch boundaries
        assert!(tz.offset_for_instant(EpochSecs::MAX) == Offset::new(4800).unwrap());
    }
}
