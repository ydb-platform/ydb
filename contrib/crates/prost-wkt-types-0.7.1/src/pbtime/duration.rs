use super::*;

////////////////////////////////////////////////////////////////////////////////
/// FROM prost-types/src/duration.rs
////////////////////////////////////////////////////////////////////////////////

impl Duration {
    /// Normalizes the duration to a canonical format.
    ///
    /// Based on [`google::protobuf::util::CreateNormalized`][1].
    ///
    /// [1]: https://github.com/google/protobuf/blob/v3.3.2/src/google/protobuf/util/time_util.cc#L79-L100
    pub fn normalize(&mut self) {
        // Make sure nanos is in the range.
        if self.nanos <= -NANOS_PER_SECOND || self.nanos >= NANOS_PER_SECOND {
            if let Some(seconds) = self
                .seconds
                .checked_add((self.nanos / NANOS_PER_SECOND) as i64)
            {
                self.seconds = seconds;
                self.nanos %= NANOS_PER_SECOND;
            } else if self.nanos < 0 {
                // Negative overflow! Set to the least normal value.
                self.seconds = i64::MIN;
                self.nanos = -NANOS_MAX;
            } else {
                // Positive overflow! Set to the greatest normal value.
                self.seconds = i64::MAX;
                self.nanos = NANOS_MAX;
            }
        }

        // nanos should have the same sign as seconds.
        if self.seconds < 0 && self.nanos > 0 {
            if let Some(seconds) = self.seconds.checked_add(1) {
                self.seconds = seconds;
                self.nanos -= NANOS_PER_SECOND;
            } else {
                // Positive overflow! Set to the greatest normal value.
                debug_assert_eq!(self.seconds, i64::MAX);
                self.nanos = NANOS_MAX;
            }
        } else if self.seconds > 0 && self.nanos < 0 {
            if let Some(seconds) = self.seconds.checked_sub(1) {
                self.seconds = seconds;
                self.nanos += NANOS_PER_SECOND;
            } else {
                // Negative overflow! Set to the least normal value.
                debug_assert_eq!(self.seconds, i64::MIN);
                self.nanos = -NANOS_MAX;
            }
        }
        // TODO: should this be checked?
        // debug_assert!(self.seconds >= -315_576_000_000 && self.seconds <= 315_576_000_000,
        //               "invalid duration: {:?}", self);
    }
}

// impl Name for Duration {
//     const PACKAGE: &'static str = PACKAGE;
//     const NAME: &'static str = "Duration";

//     fn type_url() -> String {
//         type_url_for::<Self>()
//     }
// }

impl TryFrom<time::Duration> for Duration {
    type Error = DurationError;

    /// Converts a `std::time::Duration` to a `Duration`, failing if the duration is too large.
    fn try_from(duration: time::Duration) -> Result<Duration, DurationError> {
        let seconds = i64::try_from(duration.as_secs()).map_err(|_| DurationError::OutOfRange)?;
        let nanos = duration.subsec_nanos() as i32;

        let mut duration = Duration { seconds, nanos };
        duration.normalize();
        Ok(duration)
    }
}

impl TryFrom<Duration> for time::Duration {
    type Error = DurationError;

    /// Converts a `Duration` to a `std::time::Duration`, failing if the duration is negative.
    fn try_from(mut duration: Duration) -> Result<time::Duration, DurationError> {
        duration.normalize();
        if duration.seconds >= 0 && duration.nanos >= 0 {
            Ok(time::Duration::new(
                duration.seconds as u64,
                duration.nanos as u32,
            ))
        } else {
            Err(DurationError::NegativeDuration(time::Duration::new(
                (-duration.seconds) as u64,
                (-duration.nanos) as u32,
            )))
        }
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = *self;
        d.normalize();
        if self.seconds < 0 && self.nanos < 0 {
            write!(f, "-")?;
        }
        write!(f, "{}", d.seconds.abs())?;

        // Format subseconds to either nothing, millis, micros, or nanos.
        let nanos = d.nanos.abs();
        if nanos == 0 {
            write!(f, "s")
        } else if nanos % 1_000_000 == 0 {
            write!(f, ".{:03}s", nanos / 1_000_000)
        } else if nanos % 1_000 == 0 {
            write!(f, ".{:06}s", nanos / 1_000)
        } else {
            write!(f, ".{:09}s", nanos)
        }
    }
}

/// A duration handling error.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum DurationError {
    /// Indicates failure to parse a [`Duration`] from a string.
    ///
    /// The [`Duration`] string format is specified in the [Protobuf JSON mapping specification][1].
    ///
    /// [1]: https://developers.google.com/protocol-buffers/docs/proto3#json
    ParseFailure,

    /// Indicates failure to convert a `prost_types::Duration` to a `std::time::Duration` because
    /// the duration is negative. The included `std::time::Duration` matches the magnitude of the
    /// original negative `prost_types::Duration`.
    NegativeDuration(time::Duration),

    /// Indicates failure to convert a `std::time::Duration` to a `prost_types::Duration`.
    ///
    /// Converting a `std::time::Duration` to a `prost_types::Duration` fails if the magnitude
    /// exceeds that representable by `prost_types::Duration`.
    OutOfRange,
}

impl fmt::Display for DurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DurationError::ParseFailure => write!(f, "failed to parse duration"),
            DurationError::NegativeDuration(duration) => {
                write!(f, "failed to convert negative duration: {:?}", duration)
            }
            DurationError::OutOfRange => {
                write!(f, "failed to convert duration out of range")
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for DurationError {}

impl FromStr for Duration {
    type Err = DurationError;

    fn from_str(s: &str) -> Result<Duration, DurationError> {
        datetime::parse_duration(s).ok_or(DurationError::ParseFailure)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Chrono conversion
////////////////////////////////////////////////////////////////////////////////

/// Converts proto duration to chrono's Duration
impl From<Duration> for chrono::Duration {
    fn from(val: Duration) -> Self {
        let mut value = val;
        // A call to `normalize` should capture all out-of-bound sitations hopefully
        // ensuring a panic never happens! Ideally this implementation should be
        // deprecated in favour of TryFrom but unfortunately having `TryFrom` along with
        // `From` causes a conflict.
        value.normalize();
        let s =
            chrono::TimeDelta::try_seconds(value.seconds).expect("invalid or out-of-range seconds");
        let ns = chrono::Duration::nanoseconds(value.nanos as i64);
        s + ns
    }
}

/// Converts chrono Duration to proto duration
impl From<chrono::Duration> for Duration {
    fn from(val: chrono::Duration) -> Self {
        Duration {
            seconds: val.num_seconds(),
            nanos: (val.num_nanoseconds().unwrap() % 1_000_000_000) as i32,
        }
    }
}

impl Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let seconds: f64 = self.seconds as f64 + self.nanos as f64 / NANOS_PER_SECOND as f64;
        // Generated output always contains 0, 3, 6, or 9 fractional digits, depending on required precision, followed by the suffix "s". Accepted are any fractional digits (also none) as long as they fit into nano-seconds precision and the suffix "s" is required.
        // see: https://protobuf.dev/programming-guides/proto3/#json
        //
        // this code currently *always* serializes with 9 fractional digits.
        serializer.serialize_str(&format!("{:.9}s", seconds))
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurationVisitor;

        impl<'de> de::Visitor<'de> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A duration ending in 's'")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if !value.ends_with('s') {
                    return Err(de::Error::custom("Duration should end with 's'"));
                }

                let duration_str = &value[..value.len() - 1]; // Remove 's' from the end

                let mut parts = duration_str.split('.'); // Split seconds and fractional seconds

                let seconds: i64 = parts
                    .next()
                    .ok_or_else(|| de::Error::custom("Missing seconds"))?
                    .parse()
                    .map_err(de::Error::custom)?;

                let nanos: i32 = match parts.next() {
                    Some(fraction) => {
                        let fraction = format!("{:0<9}", fraction); // Pad fraction to nanoseconds
                        let nanos = fraction.parse().map_err(de::Error::custom)?;
                        if nanos < 0 || nanos >= NANOS_PER_SECOND as i32 {
                            return Err(de::Error::custom(format!(
                                "Fractional nanoseconds out of range: {}",
                                nanos
                            )));
                        }
                        nanos
                    }
                    None => 0,
                };

                Ok(Duration { seconds, nanos })
            }
        }

        deserializer.deserialize_str(DurationVisitor)
    }
}

#[cfg(feature = "schemars")]
mod schemars_impl {
    use super::Duration;
    use schemars::generate::SchemaGenerator;
    use schemars::{json_schema, JsonSchema, Schema};
    use std::borrow::Cow;

    impl JsonSchema for Duration {
        fn schema_name() -> Cow<'static, str> {
            Cow::Borrowed("Duration")
        }

        fn schema_id() -> Cow<'static, str> {
            Cow::Borrowed("prost_wkt_types::Duration")
        }

        fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
            json_schema!({
                "type": "string",
                "description": "A duration in seconds with up to nine fractional digits, ending with 's'",
                "examples": [
                    "1s",
                    "1.5s",
                    "1.000000001s",
                ],
                "pattern": r"^\d+(\.\d{1,9})?s$",
            })
        }
    }
}
