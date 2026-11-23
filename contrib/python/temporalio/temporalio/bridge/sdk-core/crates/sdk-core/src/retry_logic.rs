use std::{num::NonZero, time::Duration};
use temporalio_common::protos::temporal::api::{
    common::v1::RetryPolicy, failure::v1::ApplicationFailureInfo,
};

/// Represents a retry policy where all fields have valid values. Durations are stored in std type.
/// Upholds the following invariants:
/// - `maximum_interval` >= `initial_interval`
/// - `backoff_coefficient` >= 1
/// - `maximum_attempts` >= 0
#[derive(Debug, Clone)]
pub(crate) struct ValidatedRetryPolicy {
    initial_interval: Duration,
    backoff_coefficient: f64,
    maximum_interval: Duration,
    maximum_attempts: u32,
    non_retryable_error_types: Vec<String>,
}

impl ValidatedRetryPolicy {
    /// Validates and converts retry policy. If some field is invalid, it's replaced with a default value:
    /// - `initial_interval`: 1 second
    /// - `backoff_coefficient`: 2.0
    /// - `maximum_interval`: 100 * `initial_interval` if missing or inconvertible, 1 * `initial_interval` if too small
    /// - `maximum_attempts`: 0 (unlimited)
    pub(crate) fn from_proto_with_defaults(retry_policy: RetryPolicy) -> Self {
        let initial_interval = retry_policy
            .initial_interval
            .and_then(|i| i.try_into().ok())
            .unwrap_or_else(|| Duration::from_secs(1));

        let backoff_coefficient = if retry_policy.backoff_coefficient >= 1.0 {
            retry_policy.backoff_coefficient
        } else {
            2.0
        };

        let maximum_interval = if let Some(maximum_interval) = retry_policy
            .maximum_interval
            .and_then(|i| Duration::try_from(i).ok())
        {
            maximum_interval.max(initial_interval)
        } else {
            let maximum_interval = initial_interval.saturating_mul(100);
            // Verifying that serialization to proto will work. It may fail for extremely large
            // durations, so in that case we fall back to maximum_interval = initial_interval.
            if prost_types::Duration::try_from(maximum_interval).is_ok() {
                maximum_interval
            } else {
                initial_interval
            }
        };

        Self {
            initial_interval,
            backoff_coefficient,
            maximum_interval,
            maximum_attempts: retry_policy.maximum_attempts.try_into().unwrap_or(0),
            non_retryable_error_types: retry_policy.non_retryable_error_types,
        }
    }

    /// Ask this retry policy if a retry should be performed. Caller provides the current attempt
    /// number - the first attempt should start at 1.
    ///
    /// Returns `None` if it should not retry, otherwise returns a duration indicating how long to
    /// wait before performing the retry.
    pub(crate) fn should_retry(
        &self,
        attempt_number: NonZero<u32>,
        application_failure: Option<&ApplicationFailureInfo>,
    ) -> Option<Duration> {
        if self.maximum_attempts > 0 && attempt_number.get() >= self.maximum_attempts {
            #[cfg(feature = "antithesis_assertions")]
            crate::antithesis::assert_sometimes!(
                true,
                "Retry maximum_attempts limit reached",
                ::serde_json::json!({
                    "attempt": attempt_number.get(),
                    "maximum_attempts": self.maximum_attempts
                })
            );
            return None;
        }

        let non_retryable = application_failure
            .map(|f| f.non_retryable)
            .unwrap_or_default();
        if non_retryable {
            #[cfg(feature = "antithesis_assertions")]
            crate::antithesis::assert_sometimes!(
                true,
                "Non-retryable application failure encountered",
                ::serde_json::json!({
                    "attempt": attempt_number.get(),
                    "error_type": application_failure.map(|f| &f.r#type)
                })
            );
            return None;
        }

        let err_type_str = application_failure.map_or("", |f| &f.r#type);
        for pat in &self.non_retryable_error_types {
            if err_type_str.to_lowercase() == pat.to_lowercase() {
                return None;
            }
        }

        if let Some(explicit_delay) = application_failure.and_then(|af| af.next_retry_delay) {
            match explicit_delay.try_into() {
                Ok(delay) => return Some(delay),
                Err(e) => error!(
                    "Failed to convert retry delay of application failure. Normal delay calculation will be used. Conversion error: `{}`. Application failure: {:?}",
                    e, application_failure
                ),
            }
        }

        if attempt_number.get() == 1 {
            return Some(self.initial_interval);
        }

        let delay = i32::try_from(attempt_number.get())
            .ok()
            .and_then(|attempt| {
                let factor = self.backoff_coefficient.powi(attempt - 1);
                Duration::try_from_secs_f64(factor * self.initial_interval.as_secs_f64()).ok()
            })
            .map(|interval| interval.min(self.maximum_interval))
            .unwrap_or(self.maximum_interval);

        Some(delay)
    }
}

impl Default for ValidatedRetryPolicy {
    fn default() -> Self {
        Self::from_proto_with_defaults(RetryPolicy::default())
    }
}

impl From<ValidatedRetryPolicy> for RetryPolicy {
    fn from(value: ValidatedRetryPolicy) -> Self {
        // All fields were tested on struct initialization to convert successfully. Unwraps are safe.
        Self {
            initial_interval: Some(value.initial_interval.try_into().unwrap()),
            backoff_coefficient: value.backoff_coefficient,
            maximum_interval: Some(value.maximum_interval.try_into().unwrap()),
            maximum_attempts: value.maximum_attempts.try_into().unwrap(),
            non_retryable_error_types: value.non_retryable_error_types,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prost_dur;
    use std::{num::NonZero, time::Duration};

    macro_rules! nz {
        ($x:expr) => {
            NonZero::new($x).unwrap()
        };
    }

    #[test]
    fn applies_defaults_to_default_retry_policy() {
        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy::default());
        assert_eq!(rp.initial_interval, Duration::from_secs(1));
        assert_eq!(rp.backoff_coefficient, 2.0);
        assert_eq!(rp.maximum_interval, Duration::from_secs(100));
        assert_eq!(rp.maximum_attempts, 0);
        assert!(rp.non_retryable_error_types.is_empty());

        let rp = ValidatedRetryPolicy::default();
        assert_eq!(rp.initial_interval, Duration::from_secs(1));
        assert_eq!(rp.backoff_coefficient, 2.0);
        assert_eq!(rp.maximum_interval, Duration::from_secs(100));
        assert_eq!(rp.maximum_attempts, 0);
        assert!(rp.non_retryable_error_types.is_empty());
    }

    #[test]
    fn applies_defaults_to_invalid_fields_only() {
        let base_rp = RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(2))),
            backoff_coefficient: 1.5,
            maximum_interval: Some(prost_dur!(from_secs(4))),
            maximum_attempts: 2,
            non_retryable_error_types: vec!["error".into()],
        };
        let base_values = ValidatedRetryPolicy::from_proto_with_defaults(base_rp.clone());
        assert_eq!(base_values.initial_interval, Duration::from_secs(2));
        assert_eq!(base_values.backoff_coefficient, 1.5);
        assert_eq!(base_values.maximum_interval, Duration::from_secs(4));
        assert_eq!(base_values.maximum_attempts, 2);
        assert_eq!(
            base_values.non_retryable_error_types,
            vec!["error".to_owned()]
        );

        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            initial_interval: Some(prost_types::Duration {
                seconds: -5,
                nanos: 0,
            }),
            ..base_rp.clone()
        });
        assert_eq!(rp.initial_interval, Duration::from_secs(1));
        assert_eq!(rp.backoff_coefficient, base_values.backoff_coefficient);
        assert_eq!(rp.maximum_interval, base_values.maximum_interval);
        assert_eq!(rp.maximum_attempts, base_values.maximum_attempts);
        assert_eq!(
            rp.non_retryable_error_types,
            base_values.non_retryable_error_types
        );

        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            backoff_coefficient: 0.5,
            ..base_rp.clone()
        });
        assert_eq!(rp.initial_interval, base_values.initial_interval);
        assert_eq!(rp.backoff_coefficient, 2.0);
        assert_eq!(rp.maximum_interval, base_values.maximum_interval);
        assert_eq!(rp.maximum_attempts, base_values.maximum_attempts);
        assert_eq!(
            rp.non_retryable_error_types,
            base_values.non_retryable_error_types
        );

        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            maximum_interval: Some(prost_types::Duration {
                seconds: -5,
                nanos: 0,
            }),
            ..base_rp.clone()
        });
        assert_eq!(rp.initial_interval, base_values.initial_interval);
        assert_eq!(rp.backoff_coefficient, base_values.backoff_coefficient);
        assert_eq!(rp.maximum_interval, 100 * base_values.initial_interval);
        assert_eq!(rp.maximum_attempts, base_values.maximum_attempts);
        assert_eq!(
            rp.non_retryable_error_types,
            base_values.non_retryable_error_types
        );

        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            maximum_interval: Some(prost_dur!(from_secs(1))), // valid but less than initial interval
            ..base_rp.clone()
        });
        assert_eq!(rp.initial_interval, base_values.initial_interval);
        assert_eq!(rp.backoff_coefficient, base_values.backoff_coefficient);
        assert_eq!(rp.maximum_interval, base_values.initial_interval);
        assert_eq!(rp.maximum_attempts, base_values.maximum_attempts);
        assert_eq!(
            rp.non_retryable_error_types,
            base_values.non_retryable_error_types
        );

        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            maximum_attempts: -5,
            ..base_rp.clone()
        });
        assert_eq!(rp.initial_interval, base_values.initial_interval);
        assert_eq!(rp.backoff_coefficient, base_values.backoff_coefficient);
        assert_eq!(rp.maximum_interval, base_values.maximum_interval);
        assert_eq!(rp.maximum_attempts, 0);
        assert_eq!(
            rp.non_retryable_error_types,
            base_values.non_retryable_error_types
        );

        // non_retryable_error_types is always valid
    }

    #[test]
    fn calcs_backoffs_properly() {
        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_interval: Some(prost_dur!(from_secs(10))),
            maximum_attempts: 10,
            non_retryable_error_types: vec![],
        });
        assert_eq!(rp.should_retry(nz!(1), None), Some(Duration::from_secs(1)));
        assert_eq!(rp.should_retry(nz!(2), None), Some(Duration::from_secs(2)));
        assert_eq!(rp.should_retry(nz!(3), None), Some(Duration::from_secs(4)));
        assert_eq!(rp.should_retry(nz!(4), None), Some(Duration::from_secs(8)));
        assert_eq!(rp.should_retry(nz!(5), None), Some(Duration::from_secs(10)));
        assert_eq!(rp.should_retry(nz!(6), None), Some(Duration::from_secs(10)));
        // Max attempts - no retry
        assert!(rp.should_retry(nz!(10), None).is_none());
    }

    #[test]
    fn max_attempts_zero_retry_forever() {
        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 1.2,
            maximum_interval: None,
            maximum_attempts: 0,
            non_retryable_error_types: vec![],
        });
        for i in 1..50 {
            assert!(rp.should_retry(nz!(i), None).is_some());
        }
    }

    #[test]
    fn delay_calculation_does_not_overflow() {
        let maximum_interval = Duration::from_secs(1000 * 365 * 24 * 60 * 60);
        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 10.,
            maximum_interval: Some(maximum_interval.try_into().unwrap()),
            maximum_attempts: 0,
            non_retryable_error_types: vec![],
        });
        for i in 1..50 {
            assert!(rp.should_retry(nz!(i), None).unwrap() <= maximum_interval);
        }
        assert_eq!(rp.should_retry(nz!(50), None), Some(maximum_interval));
        assert_eq!(rp.should_retry(nz!(u32::MAX), None), Some(maximum_interval));
    }

    #[test]
    fn no_retry_err_str_match() {
        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_interval: Some(prost_dur!(from_secs(10))),
            maximum_attempts: 10,
            non_retryable_error_types: vec!["no retry".to_string()],
        });
        assert!(
            rp.should_retry(
                nz!(1),
                Some(&ApplicationFailureInfo {
                    r#type: "no retry".to_string(),
                    non_retryable: false,
                    ..Default::default()
                })
            )
            .is_none()
        );
    }

    #[test]
    fn no_non_retryable_application_failure() {
        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_interval: Some(prost_dur!(from_secs(10))),
            maximum_attempts: 10,
            non_retryable_error_types: vec![],
        });
        assert!(
            rp.should_retry(
                nz!(1),
                Some(&ApplicationFailureInfo {
                    r#type: "".to_string(),
                    non_retryable: true,
                    ..Default::default()
                })
            )
            .is_none()
        );
    }

    #[test]
    fn explicit_delay_is_used() {
        let rp = ValidatedRetryPolicy::from_proto_with_defaults(RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_attempts: 2,
            ..Default::default()
        });
        let afi = &ApplicationFailureInfo {
            r#type: "".to_string(),
            next_retry_delay: Some(prost_dur!(from_secs(50))),
            ..Default::default()
        };
        assert_eq!(
            rp.should_retry(nz!(1), Some(afi)),
            Some(Duration::from_secs(50))
        );
        assert!(rp.should_retry(nz!(2), Some(afi)).is_none());
    }
}
