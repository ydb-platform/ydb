//! Checked arithmetic for scalar date and time concepts
use crate::{
    classes::date::Date, classes::plain_datetime::DateTime, classes::time::Time, common::round,
};
use std::{ffi::c_long, num::NonZeroU16, ops::Neg};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Sign {
    Plus,
    Minus,
}

/// A UTC offset in seconds, smaller than 24 hours (positive or negative)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Offset(i32);

impl Offset {
    pub(crate) const MIN: Offset = Offset(-86_399);
    pub(crate) const MAX: Offset = Offset(86_399);
    pub(crate) const ZERO: Offset = Offset(0);
    pub(crate) const fn new_unchecked(secs: i32) -> Self {
        debug_assert!(secs >= Self::MIN.0 && secs <= Self::MAX.0);
        Self(secs)
    }

    pub(crate) const fn new(secs: i32) -> Option<Self> {
        if secs >= Self::MIN.0 && secs <= Self::MAX.0 {
            Some(Self(secs))
        } else {
            None
        }
    }

    pub(crate) fn from_hours(hrs: c_long) -> Option<Self> {
        (-23..=23)
            .contains(&hrs)
            .then(|| Self::new_unchecked(hrs as i32 * 3600))
    }

    pub(crate) fn from_i64(secs: i64) -> Option<Self> {
        (secs >= Self::MIN.get() as i64 && secs <= Self::MAX.get() as i64)
            .then(|| Self::new_unchecked(secs as i32))
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }

    pub(crate) const fn shift(self, x: OffsetDelta) -> Option<Self> {
        // Safe since both arguments are constrained far below i32::MAX
        Self::new(self.0 + x.0)
    }

    pub(crate) const fn sub(self, x: Self) -> OffsetDelta {
        OffsetDelta::new_unchecked(self.0 - x.0)
    }

    pub(crate) const fn with_sign(self, sign: Sign) -> Self {
        Self(match sign {
            Sign::Plus => self.0,
            Sign::Minus => -self.0,
        })
    }

    pub(crate) const fn as_offset_delta(self) -> OffsetDelta {
        // Safe: range of Offset fits within OffsetDelta
        OffsetDelta::new_unchecked(self.0)
    }
}

impl Neg for Offset {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

impl std::fmt::Display for Offset {
    /// Format an offset in seconds as a string like "+hh:mm",
    /// adding ":ss" only if needed
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (sign, secs) = if self.0 < 0 {
            ('-', -self.0)
        } else {
            ('+', self.0)
        };
        if secs % 60 == 0 {
            write!(f, "{}{:02}:{:02}", sign, secs / 3600, (secs % 3600) / 60)
        } else {
            write!(
                f,
                "{}{:02}:{:02}:{:02}",
                sign,
                secs / 3600,
                (secs % 3600) / 60,
                secs % 60
            )
        }
    }
}

/// Difference between two offsets in seconds. +/- 48 hours
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub(crate) struct OffsetDelta(i32);

impl OffsetDelta {
    pub(crate) const MIN: OffsetDelta = OffsetDelta(Offset::MIN.get() * 2);
    pub(crate) const MAX: OffsetDelta = OffsetDelta(Offset::MAX.get() * 2);
    pub(crate) const ZERO: OffsetDelta = OffsetDelta(0);
    pub(crate) const fn new_unchecked(secs: i32) -> Self {
        debug_assert!(secs >= Self::MIN.0 && secs <= Self::MAX.0);
        Self(secs)
    }

    #[allow(dead_code)]
    pub(crate) const fn new(secs: i32) -> Option<Self> {
        if secs >= Self::MIN.0 && secs <= Self::MAX.0 {
            Some(Self(secs))
        } else {
            None
        }
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }

    pub(crate) const fn abs(self) -> Self {
        // Safe: Range is well within i32::MAX
        Self(self.0.abs())
    }
}

impl Neg for OffsetDelta {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

/// Number of seconds since 1970-01-01
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct EpochSecs(i64);

impl EpochSecs {
    // 0000-01-01 00:00 to 9999-12-31 23:59
    pub(crate) const MIN: EpochSecs = EpochSecs(-62_135_596_800);
    pub(crate) const MAX: EpochSecs = EpochSecs(253_402_300_799);
    pub const fn new_unchecked(secs: i64) -> Self {
        debug_assert!(secs >= Self::MIN.0 && secs <= Self::MAX.0);
        Self(secs)
    }

    pub const fn new(secs: i64) -> Option<Self> {
        if secs >= Self::MIN.0 && secs <= Self::MAX.0 {
            Some(Self(secs))
        } else {
            None
        }
    }

    pub(crate) fn clamp(secs: i64) -> Self {
        Self(secs.clamp(Self::MIN.0, Self::MAX.0))
    }

    pub(crate) fn from_i32(secs: i32) -> Self {
        // Safe: i32 is always within our epoch seconds range
        Self::new_unchecked(secs.into())
    }

    pub(crate) const fn get(self) -> i64 {
        self.0
    }

    pub(crate) const fn offset(self, x: Offset) -> Option<Self> {
        Self::new(self.0 + x.0 as i64)
    }

    pub(crate) fn saturating_offset(self, x: Offset) -> Self {
        Self::clamp(self.0 + x.get() as i64)
    }

    pub(crate) fn saturating_add_i32(self, x: i32) -> Self {
        // Safe: both arguments are constrained far below i64::MIN/MAX
        Self::clamp(self.0 + x as i64)
    }

    pub(crate) fn shift(self, d: DeltaSeconds) -> Option<Self> {
        // Safe: result of addition is always within i64::MIN/MAX
        Self::new(self.0 + d.get())
    }

    pub(crate) fn as_days(self) -> UnixDays {
        UnixDays::new_unchecked((self.0.div_euclid(i64::from(S_PER_DAY))) as _)
    }

    pub(crate) fn datetime(self, nanos: SubSecNanos) -> DateTime {
        DateTime {
            date: self.date(),
            time: self.time(nanos),
        }
    }

    pub(crate) fn date(self) -> Date {
        self.as_days().date()
    }

    pub(crate) fn time(self, nanos: SubSecNanos) -> Time {
        let time_secs = (self.get().rem_euclid(i64::from(S_PER_DAY))) as i32;
        Time {
            hour: (time_secs / 3600) as u8,
            minute: ((time_secs / 60) % 60) as u8,
            second: (time_secs % 60) as u8,
            subsec: nanos,
        }
    }

    pub(crate) fn diff(self, other: Self) -> DeltaSeconds {
        // Safe: range of DeltaSeconds is large enough to cover all possible differences
        DeltaSeconds::new_unchecked(self.0 - other.0)
    }

    pub(crate) fn to_delta(self) -> DeltaSeconds {
        // Safe: range of DeltaSeconds is large enough to cover all possible differences
        DeltaSeconds::new_unchecked(self.0)
    }
}

/// Number of days since 1970-01-01
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnixDays(i32);

impl UnixDays {
    // 0000-01-01 to 9999-12-31
    pub(crate) const MIN: UnixDays = UnixDays(-719_162);
    pub(crate) const MAX: UnixDays = UnixDays(2_932_896);
    pub fn new_unchecked(days: i32) -> Self {
        debug_assert!((Self::MIN.0..=Self::MAX.0).contains(&days));
        Self(days)
    }

    pub(crate) const fn new(days: i32) -> Option<Self> {
        if days >= Self::MIN.0 && days <= Self::MAX.0 {
            Some(Self(days))
        } else {
            None
        }
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }

    // The Neri-Schneider algorithm
    // From https://github.com/cassioneri/eaf/blob/
    // 684d3cc32d14eee371d0abe4f683d6d6a49ed5c1/algorithms/
    // neri_schneider.hpp#L40C3-L40C34
    // under the MIT license
    pub fn date(self) -> Date {
        // Shift and correction constants.
        const S: u32 = 82;
        const K: u32 = 719468 + 146097 * S;
        const L: u32 = 400 * S;
        // Rata die shift.
        let n = (self.0 as u32).wrapping_add(K);

        // Century.
        let n_1 = 4 * n + 3;
        let c = n_1 / 146097;
        let n_c = n_1 % 146097 / 4;

        // Year.
        let n_2 = 4 * n_c + 3;
        let p_2 = 2939745 * n_2 as u64;
        let z = (p_2 / 4294967296) as u32;
        let n_y = (p_2 % 4294967296) as u32 / 2939745 / 4;
        let y = 100 * c + z;

        // Month and day.
        let n_3 = 2141 * n_y + 197913;
        let m = n_3 / 65536;
        let d = n_3 % 65536 / 2141;

        // Map. (Notice the year correction, including type change.)
        let j = n_y >= 306;
        let y_g = y.wrapping_sub(L).wrapping_add(j as u32);
        let m_g = if j { m - 12 } else { m };
        let d_g = d + 1;
        Date {
            // Safety: so long as unix days are in range, the date is valid
            year: Year::new_unchecked(y_g as _),
            month: Month::new_unchecked(m_g as _),
            day: d_g as _,
        }
    }

    pub(crate) fn add_unchecked(self, days: i32) -> Self {
        Self(self.0 + days)
    }

    pub(crate) fn shift(self, d: DeltaDays) -> Option<Self> {
        // Safety: both values well within i32::MIN/MAX
        Self::new(self.0 + d.get())
    }

    pub(crate) fn diff(self, other: Self) -> DeltaDays {
        // Safe: range of DeltaDays is large enough to cover all possible differences
        DeltaDays::new_unchecked(self.0 - other.0)
    }

    pub(crate) fn epoch_at(self, time: Time) -> EpochSecs {
        // Safe: result is always within bounds
        EpochSecs::new_unchecked(self.0 as i64 * S_PER_DAY as i64 + time.total_seconds() as i64)
    }

    pub(crate) fn day_of_week(self) -> Weekday {
        Weekday::from_iso_unchecked(((self.get() + 3).rem_euclid(7) + 1) as _)
    }
}

const MAX_MONTH_DAYS: [[u8; 13]; 2] = [
    // non-leap year
    [
        0, // 1-indexed
        31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ],
    // leap year
    [
        0, // 1-indexed
        31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ],
];
const DAYS_BEFORE_MONTH: [[u16; 13]; 2] = [
    // non-leap years
    [
        0, // 1-indexed
        0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334,
    ],
    // leap years
    [
        0, // 1-indexed
        0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335,
    ],
];

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Year(NonZeroU16);

impl Year {
    pub(crate) const MIN: Year = Year(NonZeroU16::new(1).unwrap());
    pub(crate) const MAX: Year = Year(NonZeroU16::new(9999).unwrap());
    pub(crate) const fn new(year: u16) -> Option<Self> {
        if year <= Year::MAX.get() {
            match NonZeroU16::new(year) {
                Some(year) => Some(Self(year)),
                None => None,
            }
        } else {
            None
        }
    }

    pub const fn new_unchecked(year: u16) -> Self {
        debug_assert!(year >= Year::MIN.get() && year <= Year::MAX.get());
        Self(unsafe { NonZeroU16::new_unchecked(year) })
    }

    // A slightly faster way to create a Year from a c_long
    // that prevents double-checking the bounds
    pub(crate) fn from_long(y: c_long) -> Option<Self> {
        (y >= Year::MIN.get().into() && y <= Year::MAX.get().into())
            .then(|| Self::new_unchecked(y as u16))
    }

    pub(crate) fn from_i32(y: i32) -> Option<Self> {
        (y >= Year::MIN.get().into() && y <= Year::MAX.get().into())
            .then(|| Self::new_unchecked(y as u16))
    }

    pub(crate) const fn get(self) -> u16 {
        self.0.get()
    }

    pub(crate) const fn is_leap(self) -> bool {
        (self.get() % 4 == 0 && self.get() % 100 != 0) || self.get() % 400 == 0
    }

    pub(crate) fn unix_days_at_jan1(self) -> UnixDays {
        let y = (self.get() - 1) as i32;
        UnixDays::new_unchecked(y * 365 + y / 4 - y / 100 + y / 400 - 719_162)
    }

    pub(crate) const fn days_in_month(self, month: Month) -> u8 {
        MAX_MONTH_DAYS[self.is_leap() as usize][month as usize]
    }

    pub(crate) const fn days_before_month(self, month: Month) -> u16 {
        DAYS_BEFORE_MONTH[self.is_leap() as usize][month as usize]
    }

    pub(crate) const fn days_before(self) -> i32 {
        let y = (self.get() - 1) as i32;
        y * 365 + y / 4 - y / 100 + y / 400
    }
}

impl From<Year> for u16 {
    fn from(x: Year) -> Self {
        x.get()
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Month {
    January = 1,
    February = 2,
    March = 3,
    April = 4,
    May = 5,
    June = 6,
    July = 7,
    August = 8,
    September = 9,
    October = 10,
    November = 11,
    December = 12,
}

impl Month {
    pub(crate) const MIN: Month = Month::January;
    pub(crate) const MAX: Month = Month::December;

    pub(crate) const fn new(n: u8) -> Option<Self> {
        if n >= 1 && n <= 12 {
            Some(Self::new_unchecked(n))
        } else {
            None
        }
    }

    pub(crate) const fn new_unchecked(n: u8) -> Self {
        debug_assert!(n >= 1 && n <= 12);
        // Safety: Month is repr(u8)
        unsafe { std::mem::transmute(n) }
    }

    pub(crate) fn from_long(m: c_long) -> Option<Self> {
        (m >= Month::MIN.get().into() && m <= Month::MAX.get().into())
            .then(|| Self::new_unchecked(m as u8))
    }

    pub(crate) const fn get(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for Month {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Month::new(value).ok_or(())
    }
}

pub(crate) const S_PER_DAY: i32 = 86_400;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaMonths(i32);

impl DeltaMonths {
    pub(crate) const MIN: DeltaMonths = DeltaMonths(-(Year::MAX.get() as i32) * 12);
    pub(crate) const MAX: DeltaMonths = DeltaMonths(Year::MAX.get() as i32 * 12);
    pub(crate) const ZERO: DeltaMonths = DeltaMonths(0);
    pub(crate) const fn new(months: i32) -> Option<Self> {
        if months >= Self::MIN.0 && months <= Self::MAX.0 {
            Some(Self(months))
        } else {
            None
        }
    }

    pub(crate) const fn new_unchecked(months: i32) -> Self {
        Self(months)
    }

    pub(crate) fn from_long(months: c_long) -> Option<Self> {
        (months >= Self::MIN.get() as c_long && months <= Self::MAX.get() as c_long)
            .then(|| Self::new_unchecked(months as i32))
    }

    pub(crate) fn get(self) -> i32 {
        self.0
    }

    pub(crate) fn abs(self) -> Self {
        Self(self.0.abs())
    }

    pub(crate) fn mul(self, n: i32) -> Option<Self> {
        self.0.checked_mul(n).and_then(Self::new)
    }

    pub(crate) fn add(self, d: DeltaMonths) -> Option<Self> {
        // Safety: both values well within i32::MIN/MAX
        Self::new(self.0 + d.get())
    }
    pub(crate) fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl Neg for DeltaMonths {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaDays(i32);

impl DeltaDays {
    pub(crate) const MIN: DeltaDays = DeltaDays(UnixDays::MIN.get() - UnixDays::MAX.get() - 1);
    pub(crate) const MAX: DeltaDays = DeltaDays(UnixDays::MAX.get() - UnixDays::MIN.get() + 1);
    pub(crate) const ZERO: DeltaDays = DeltaDays(0);
    pub(crate) const fn new(days: i32) -> Option<Self> {
        if days >= Self::MIN.0 && days <= Self::MAX.0 {
            Some(Self(days))
        } else {
            None
        }
    }

    pub(crate) const fn new_unchecked(days: i32) -> Self {
        debug_assert!(days >= Self::MIN.0 && days <= Self::MAX.0);
        Self(days)
    }

    pub(crate) fn get(self) -> i32 {
        self.0
    }

    pub(crate) fn from_long(days: c_long) -> Option<Self> {
        (days >= Self::MIN.get() as c_long && days <= Self::MAX.get() as c_long)
            .then(|| Self::new_unchecked(days as i32))
    }

    pub(crate) fn abs(self) -> Self {
        Self(self.0.abs())
    }

    pub(crate) fn mul(self, n: i32) -> Option<Self> {
        self.0.checked_mul(n).and_then(Self::new)
    }

    pub(crate) fn add(self, d: DeltaDays) -> Option<Self> {
        // Safety: both values well within i32::MIN/MAX
        Self::new(self.0 + d.get())
    }

    pub(crate) fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl Neg for DeltaDays {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaSeconds(i64);

impl DeltaSeconds {
    // Bounds sufficiently large to cover all years
    pub(crate) const MIN: DeltaSeconds =
        DeltaSeconds(-(Year::MAX.get() as i64) * 366 * 24 * 60 * 60);
    pub(crate) const MAX: DeltaSeconds = DeltaSeconds(Year::MAX.get() as i64 * 366 * 24 * 60 * 60);
    pub(crate) const ZERO: DeltaSeconds = DeltaSeconds(0);
    pub(crate) const fn new(secs: i64) -> Option<Self> {
        if secs >= Self::MIN.0 && secs <= Self::MAX.0 {
            Some(Self(secs))
        } else {
            None
        }
    }

    pub(crate) const fn new_unchecked(secs: i64) -> Self {
        debug_assert!(secs >= Self::MIN.0 && secs <= Self::MAX.0);
        Self(secs)
    }

    pub(crate) const fn get(self) -> i64 {
        self.0
    }

    pub(crate) fn add(self, d: DeltaSeconds) -> Option<Self> {
        // Safety: both values well within i64::MIN/MAX
        Self::new(self.0 + d.get())
    }

    /// Get the absolute value of the delta in hours, minutes, and seconds
    pub(crate) fn abs_hms(self) -> (i64, u8, u8) {
        let secs = self.0.abs();
        (secs / 3600, ((secs % 3600) / 60) as _, (secs % 60) as _)
    }
}

impl From<DeltaSeconds> for i64 {
    fn from(x: DeltaSeconds) -> Self {
        x.get()
    }
}

impl std::ops::Neg for DeltaSeconds {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaNanos(i128);

impl DeltaNanos {
    pub(crate) const MIN: DeltaNanos = DeltaNanos(DeltaSeconds::MIN.get() as i128 * 1_000_000_000);
    pub(crate) const MAX: DeltaNanos = DeltaNanos(DeltaSeconds::MAX.get() as i128 * 1_000_000_000);
    pub(crate) const fn new(nanos: i128) -> Option<Self> {
        if nanos >= Self::MIN.0 && nanos <= Self::MAX.0 {
            Some(Self(nanos))
        } else {
            None
        }
    }

    pub(crate) fn get(self) -> i128 {
        self.0
    }

    pub(crate) fn sec_subsec(self) -> (DeltaSeconds, SubSecNanos) {
        (
            // Safety: No range check since nanos are already within range
            DeltaSeconds::new_unchecked(self.0.div_euclid(1_000_000_000) as _),
            SubSecNanos::from_remainder(self.get()),
        )
    }
}

/// Number of nanoseconds within a second (< 1_000_000_000)
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
// Even though it's always positive, we use i32 over u32 to simplify arithmetic
pub(crate) struct SubSecNanos(i32);

impl SubSecNanos {
    pub(crate) const MIN: SubSecNanos = SubSecNanos(0);
    pub(crate) const MAX: SubSecNanos = SubSecNanos(999_999_999);

    pub(crate) const fn new_unchecked(nanos: i32) -> Self {
        debug_assert!(nanos >= Self::MIN.0 && nanos <= Self::MAX.0);
        Self(nanos)
    }

    pub(crate) fn from_long(n: c_long) -> Option<Self> {
        (n >= Self::MIN.get() as c_long && n <= Self::MAX.get() as c_long)
            .then(|| Self::new_unchecked(n as i32))
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }

    pub(crate) fn negate(self) -> (DeltaSeconds, Self) {
        Self::MIN.diff(self)
    }

    pub(crate) fn from_remainder<T>(nanos: T) -> Self
    where
        T: Copy + NanosRemainder,
    {
        // Safety: remainder is always in range
        Self::new_unchecked(nanos.subsec_nanos())
    }

    pub(crate) fn from_fract(frac: f64) -> Self {
        // Safety: remainder is always in range
        Self::new_unchecked((frac.fract() * 1_000_000_000_f64).rem_euclid(1_000_000_000_f64) as _)
    }

    /// Get the difference between two nanosecond values,
    /// along with the seconds part of the difference (at most -1) if a < b
    pub(crate) fn diff(self, other: Self) -> (DeltaSeconds, Self) {
        let diff_signed = self.0 - other.0;
        (
            // Safety: No range check since we're dealing with at most -1 second here
            DeltaSeconds::new_unchecked(diff_signed.div_euclid(1_000_000_000) as _),
            SubSecNanos::from_remainder(diff_signed),
        )
    }

    pub(crate) fn add(self, other: Self) -> (DeltaSeconds, Self) {
        let sum = self.0 + other.0;
        (
            // Safety: No range check since we're dealing with at most 1 second here
            DeltaSeconds::new_unchecked(sum.div_euclid(1_000_000_000) as _),
            SubSecNanos::from_remainder(sum),
        )
    }

    pub(crate) fn round(self, increment: i32, mode: round::Mode) -> (DeltaSeconds, Self) {
        debug_assert!(increment < 1_000_000_000);
        debug_assert!(1_000_000_000 % increment == 0);
        let quotient = self.0 / increment;
        let remainder = self.0 % increment;
        let threshold = match mode {
            round::Mode::HalfEven => 1.max(increment / 2 + (quotient % 2 == 0) as i32),
            round::Mode::Ceil => 1,
            round::Mode::Floor => increment + 1,
            round::Mode::HalfFloor => increment / 2 + 1,
            round::Mode::HalfCeil => 1.max(increment / 2),
        };
        let round_up = remainder >= threshold;
        let rounded = (quotient + i32::from(round_up)) * increment;
        (
            // Safety: No range check since we're dealing with at most 1 second here
            DeltaSeconds::new_unchecked((rounded / 1_000_000_000) as _),
            SubSecNanos::from_remainder(rounded),
        )
    }
}

// Display sub-second nanos in a way that:
// - only shows the decimal point when necessary (i.e. nanos > 0)
// - doesn't show trailing zeros
impl std::fmt::Display for SubSecNanos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't write anything if the nanoseconds are zero
        if self.0 == 0 {
            return Ok(());
        }
        f.write_str(format!(".{:09}", self.get()).trim_end_matches('0'))
    }
}

// Private trait to enable a generic from_remainder function
pub(crate) trait NanosRemainder {
    fn subsec_nanos(self) -> i32;
}

impl NanosRemainder for i64 {
    fn subsec_nanos(self) -> i32 {
        self.rem_euclid(1_000_000_000) as _
    }
}

impl NanosRemainder for i32 {
    fn subsec_nanos(self) -> i32 {
        self.rem_euclid(1_000_000_000) as _
    }
}

impl NanosRemainder for u64 {
    fn subsec_nanos(self) -> i32 {
        self.rem_euclid(1_000_000_000) as _
    }
}

impl NanosRemainder for i128 {
    fn subsec_nanos(self) -> i32 {
        self.rem_euclid(1_000_000_000) as _
    }
}

/// Weekday according to ISO numbering
#[repr(u8)]
#[allow(dead_code)] // enum members aren't explicitly constructed
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Weekday {
    Monday = 1,
    Tuesday = 2,
    Wednesday = 3,
    Thursday = 4,
    Friday = 5,
    Saturday = 6,
    Sunday = 7,
}

impl Weekday {
    pub(crate) const fn from_iso_unchecked(n: u8) -> Self {
        // Safety: Weekday is repr(u8)
        unsafe { std::mem::transmute(n) }
    }

    pub(crate) const fn iso(self) -> u8 {
        self as u8
    }

    pub(crate) const fn sunday_is_0(self) -> u8 {
        self.iso() % 7
    }
}

pub(crate) static NS_PER_DAY: i128 = S_PER_DAY as i128 * 1_000_000_000;
