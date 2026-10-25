mod datetime;

mod duration;
pub use duration::DurationError;

mod timestamp;
pub use timestamp::TimestampError;

use core::convert::TryFrom;
use core::str::FromStr;
use core::time;
use core::*;
use std::convert::TryInto;

use chrono::prelude::*;

use serde::de::{self, Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};

include!(concat!(env!("OUT_DIR"), "/pbtime/google.protobuf.rs"));

////////////////////////////////////////////////////////////////////////////////
/// FROM prost-types/src/lib.rs
/// with impl Name removed until stabilized
////////////////////////////////////////////////////////////////////////////////
const NANOS_PER_SECOND: i32 = 1_000_000_000;
const NANOS_MAX: i32 = NANOS_PER_SECOND - 1;

#[cfg(test)]
mod tests {

    use crate::pbtime::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn serialize_duration_check_output() {
        // protobuf JSON specification is as follows:
        // Generated output always contains 0, 3, 6, or 9 fractional digits, depending on required precision, followed by the suffix "s". Accepted are any fractional digits (also none) as long as they fit into nano-seconds precision and the suffix "s" is required.
        // see: https://protobuf.dev/programming-guides/proto3/#json

        let duration = Duration {
            seconds: 10,
            nanos: 300,
        };
        let json = serde_json::to_string_pretty(&duration).expect("json");
        assert_eq!(json, r#""10.000000300s""#);
    }

    #[test]
    fn deserialize_duration_whole_seconds() {
        let got: Duration = serde_json::from_str(r#""10s""#).expect("json");

        let want = Duration {
            seconds: 10,
            nanos: 0,
        };
        assert_eq!(got, want);
    }

    #[test]
    fn serialize_duration() {
        let duration = Duration {
            seconds: 10,
            nanos: 100,
        };
        let json = serde_json::to_string_pretty(&duration).expect("json");
        println!("{json}");
        let back: Duration = serde_json::from_str(&json).expect("duration");
        assert_eq!(duration, back);
    }

    #[test]
    fn invalid_timestamp_test() {
        let ts = Timestamp {
            seconds: 10,
            nanos: 2000000000,
        };
        let datetime_utc: DateTime<Utc> = ts.into();

        println!("{datetime_utc:?}");
    }

    #[test]
    fn test_duration_conversion_pb_to_chrono() {
        let duration = Duration {
            seconds: 10,
            nanos: 100,
        };
        let chrono_duration: chrono::Duration = duration.into();
        assert_eq!(chrono_duration.num_seconds(), 10);
        assert_eq!(
            (chrono_duration - chrono::Duration::try_seconds(10).expect("seconds"))
                .num_nanoseconds(),
            Some(100)
        );
    }

    #[test]
    fn test_duration_conversion_chrono_to_pb() {
        let chrono_duration = chrono::Duration::try_seconds(10).expect("seconds")
            + chrono::Duration::nanoseconds(100);
        let duration: Duration = chrono_duration.into();
        assert_eq!(duration.seconds, 10);
        assert_eq!(duration.nanos, 100);
    }
}
