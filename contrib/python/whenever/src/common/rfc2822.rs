//! Functionality for parsing/writing RFC 2822 formatted date strings.
use crate::{
    classes::{
        date::{Date, extract_year},
        instant::Instant,
        offset_datetime::OffsetDateTime,
        plain_datetime::DateTime,
        time::Time,
    },
    common::{
        fmt::*,
        parse::{Scan, extract_2_digits, extract_digit},
        scalar::*,
    },
};

const TEMPLATE: [u8; 31] = *b"DDD, 00 MMM 0000 00:00:00 +0000";
const TEMPLATE_GMT: [u8; 29] = *b"DDD, 00 MMM 0000 00:00:00 GMT";

pub(crate) fn write(odt: OffsetDateTime) -> [u8; 31] {
    let OffsetDateTime {
        date,
        time:
            Time {
                hour,
                minute,
                second,
                ..
            },
        offset,
    } = odt;
    let Date { year, month, day } = date;

    let mut buf = TEMPLATE;
    buf[..3].copy_from_slice(WEEKDAY_NAMES[date.day_of_week() as usize - 1]);
    write_2_digits(day, &mut buf[5..7]);
    buf[8..11].copy_from_slice(MONTH_NAMES[month as usize - 1]);
    write_4_digits(year.get(), &mut buf[12..16]);
    write_2_digits(hour, &mut buf[17..19]);
    write_2_digits(minute, &mut buf[20..22]);
    write_2_digits(second, &mut buf[23..25]);
    buf[26] = if offset.get() >= 0 { b'+' } else { b'-' };
    let offset_abs = offset.get().abs();
    write_2_digits((offset_abs / 3600) as u8, &mut buf[27..29]);
    write_2_digits(((offset_abs % 3600) / 60) as u8, &mut buf[29..]);
    buf
}

pub(crate) fn write_gmt(i: Instant) -> [u8; 29] {
    let DateTime {
        date,
        time:
            Time {
                hour,
                minute,
                second,
                ..
            },
    } = i.to_datetime();
    let Date { year, month, day } = date;

    let mut buf = TEMPLATE_GMT;
    buf[..3].copy_from_slice(WEEKDAY_NAMES[date.day_of_week() as usize - 1]);
    write_2_digits(day, &mut buf[5..7]);
    buf[8..11].copy_from_slice(MONTH_NAMES[month as usize - 1]);
    write_4_digits(year.get(), &mut buf[12..16]);
    write_2_digits(hour, &mut buf[17..19]);
    write_2_digits(minute, &mut buf[20..22]);
    write_2_digits(second, &mut buf[23..25]);
    buf
}

const WEEKDAY_NAMES: [&[u8]; 7] = [b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat", b"Sun"];
const MONTH_NAMES: [&[u8]; 12] = [
    b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov", b"Dec",
];

pub(crate) fn parse(s: &[u8]) -> Option<(Date, Time, Offset)> {
    let mut scan = Scan::new(s);
    scan.ascii_whitespace();
    let expected_weekday = match scan.peek()? {
        c if c.is_ascii_alphabetic() => Some(parse_weekday(&mut scan)?),
        _ => None,
    };
    let date = parse_date(&mut scan, expected_weekday)?;
    let time = parse_time(&mut scan)?;
    let offset = parse_offset(&mut scan)?;
    scan.ascii_whitespace();
    scan.is_done().then_some((date, time, offset))
}

fn parse_weekday(s: &mut Scan) -> Option<Weekday> {
    s.take(3)
        .and_then(|day_str| {
            WEEKDAY_NAMES.iter().enumerate().find_map(|(i, &b)| {
                day_str
                    .eq_ignore_ascii_case(b)
                    .then(|| Weekday::from_iso_unchecked(i as u8 + 1))
            })
        })
        .and_then(|day| {
            s.ascii_whitespace();
            s.expect(b',')?;
            s.ascii_whitespace();
            Some(day)
        })
}

fn parse_date(s: &mut Scan, expect_weekday: Option<Weekday>) -> Option<Date> {
    let day = s.up_to_2_digits()?;
    s.ascii_whitespace().then_some(())?;
    let month = s.take(3).and_then(|month_str| {
        MONTH_NAMES.iter().enumerate().find_map(|(i, &b)| {
            month_str
                .eq_ignore_ascii_case(b)
                .then(|| Month::new_unchecked(i as u8 + 1))
        })
    })?;
    s.ascii_whitespace().then_some(())?;
    let year = s
        .take_until(Scan::is_whitespace)
        .and_then(|y_str| match y_str.len() {
            4 => extract_year(y_str, 0),
            2 => extract_2_digits(y_str, 0).map(|y| {
                if y < 50 {
                    Year::new_unchecked(2000 + y as u16)
                } else {
                    Year::new_unchecked(1900 + y as u16)
                }
            }),
            3 => Some(Year::new_unchecked(
                1900 + (extract_digit(y_str, 0)? as u16) * 100
                    + (extract_digit(y_str, 1)? as u16) * 10
                    + (extract_digit(y_str, 2)? as u16),
            )),
            _ => None,
        })?;
    s.ascii_whitespace();
    let date = Date::new(year, month, day)?;
    if let Some(weekday) = expect_weekday {
        if date.day_of_week() != weekday {
            return None;
        }
    }
    Some(date)
}

fn parse_time(s: &mut Scan) -> Option<Time> {
    let hour = s.digits00_23()?;
    s.ascii_whitespace();
    s.expect(b':')?;
    s.ascii_whitespace();
    let minute = s.digits00_59()?;
    let whitespace_after_mins = s.ascii_whitespace();
    let second = match s.peek()? {
        b':' => {
            s.skip(1).ascii_whitespace();
            let val = s.digits00_59()?;
            // Whitespace after seconds is required!
            s.ascii_whitespace().then_some(())?;
            val
        }
        _ if whitespace_after_mins => 0,
        _ => None?,
    };

    Some(Time {
        hour,
        minute,
        second,
        subsec: SubSecNanos::MIN,
    })
}

const TIMEZONES: [(&[u8], i32); 10] = [
    (b"GMT", 0),
    (b"UT", 0),
    (b"EST", -5 * 3600),
    (b"EDT", -4 * 3600),
    (b"CST", -6 * 3600),
    (b"CDT", -5 * 3600),
    (b"MST", -7 * 3600),
    (b"MDT", -6 * 3600),
    (b"PST", -8 * 3600),
    (b"PDT", -7 * 3600),
];

fn parse_offset(s: &mut Scan) -> Option<Offset> {
    Some(Offset::new_unchecked(match s.peek()? {
        b'+' => s.skip(1).digits00_23()? as i32 * 3600 + s.digits00_59()? as i32 * 60,
        b'-' => -(s.skip(1).digits00_23()? as i32 * 3600 + s.digits00_59()? as i32 * 60),
        _ => {
            let tz = match s.take_until(|b| !b.is_ascii_alphabetic()) {
                Some(tz) => tz,
                None => s.drain(),
            };
            if tz.is_empty() {
                return None;
            }
            TIMEZONES
                .iter()
                .find_map(|&(tz_name, offset)| tz.eq_ignore_ascii_case(tz_name).then_some(offset))
                // According to specification, if the timezone is not recognized, it should be
                // treated as GMT.
                .unwrap_or(0)
        }
    }))
}
