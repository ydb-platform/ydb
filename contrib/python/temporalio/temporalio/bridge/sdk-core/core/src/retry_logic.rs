use std::time::Duration;
use temporal_sdk_core_protos::{
    temporal::api::{common::v1::RetryPolicy, failure::v1::ApplicationFailureInfo},
    utilities::TryIntoOrNone,
};

pub(crate) trait RetryPolicyExt {
    /// Ask this retry policy if a retry should be performed. Caller provides the current attempt
    /// number - the first attempt should start at 1.
    ///
    /// Returns `None` if it should not, otherwise a duration indicating how long to wait before
    /// performing the retry.
    ///
    /// Applies defaults to missing fields:
    /// `initial_interval` - 1 second
    /// `maximum_interval` - 100 x initial_interval
    /// `backoff_coefficient` - 2.0
    fn should_retry(
        &self,
        attempt_number: usize,
        application_failure: Option<&ApplicationFailureInfo>,
    ) -> Option<Duration>;
}

impl RetryPolicyExt for RetryPolicy {
    fn should_retry(
        &self,
        attempt_number: usize,
        application_failure: Option<&ApplicationFailureInfo>,
    ) -> Option<Duration> {
        let non_retryable = application_failure
            .map(|f| f.non_retryable)
            .unwrap_or_default();
        if non_retryable {
            return None;
        }
        let err_type_str = application_failure.map_or("", |f| &f.r#type);
        let realmax = self.maximum_attempts.max(0);
        if realmax > 0 && attempt_number >= realmax as usize {
            return None;
        }

        for pat in &self.non_retryable_error_types {
            if err_type_str.to_lowercase() == pat.to_lowercase() {
                return None;
            }
        }

        if let Some(explicit_delay) = application_failure.and_then(|af| af.next_retry_delay) {
            return explicit_delay.try_into().ok();
        }

        let converted_interval = self
            .initial_interval
            .try_into_or_none()
            .or(Some(Duration::from_secs(1)));
        if attempt_number == 1 {
            return converted_interval;
        }
        let coeff = if self.backoff_coefficient != 0. {
            self.backoff_coefficient
        } else {
            2.0
        };

        if let Some(interval) = converted_interval {
            let max_iv = self
                .maximum_interval
                .try_into_or_none()
                .unwrap_or_else(|| interval.saturating_mul(100));
            let mul_factor = coeff.powi(attempt_number as i32 - 1);
            let tried_mul = try_from_secs_f64(mul_factor * interval.as_secs_f64());
            Some(tried_mul.unwrap_or(max_iv).min(max_iv))
        } else {
            // No retries if initial interval is not specified
            None
        }
    }
}

const NANOS_PER_SEC: u32 = 1_000_000_000;
/// modified from rust stdlib since this feature is currently nightly only
fn try_from_secs_f64(secs: f64) -> Option<Duration> {
    const MAX_NANOS_F64: f64 = ((u64::MAX as u128 + 1) * (NANOS_PER_SEC as u128)) as f64;
    let nanos = secs * (NANOS_PER_SEC as f64);
    if !nanos.is_finite() || !(0.0..MAX_NANOS_F64).contains(&nanos) {
        None
    } else {
        Some(Duration::from_secs_f64(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prost_dur;

    #[test]
    fn calcs_backoffs_properly() {
        let rp = RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_interval: Some(prost_dur!(from_secs(10))),
            maximum_attempts: 10,
            non_retryable_error_types: vec![],
        };
        let res = rp.should_retry(1, None).unwrap();
        assert_eq!(res.as_millis(), 1_000);
        let res = rp.should_retry(2, None).unwrap();
        assert_eq!(res.as_millis(), 2_000);
        let res = rp.should_retry(3, None).unwrap();
        assert_eq!(res.as_millis(), 4_000);
        let res = rp.should_retry(4, None).unwrap();
        assert_eq!(res.as_millis(), 8_000);
        let res = rp.should_retry(5, None).unwrap();
        assert_eq!(res.as_millis(), 10_000);
        let res = rp.should_retry(6, None).unwrap();
        assert_eq!(res.as_millis(), 10_000);
        // Max attempts - no retry
        assert!(rp.should_retry(10, None).is_none());
    }

    #[test]
    fn no_interval_no_backoff() {
        let rp = RetryPolicy {
            initial_interval: None,
            backoff_coefficient: 0.,
            maximum_interval: None,
            maximum_attempts: 10,
            non_retryable_error_types: vec![],
        };
        assert!(rp.should_retry(1, None).is_some());
    }

    #[test]
    fn max_attempts_zero_retry_forever() {
        let rp = RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 1.2,
            maximum_interval: None,
            maximum_attempts: 0,
            non_retryable_error_types: vec![],
        };
        for i in 0..50 {
            assert!(rp.should_retry(i, None).is_some());
        }
    }

    #[test]
    fn no_overflows() {
        let rp = RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 10.,
            maximum_interval: None,
            maximum_attempts: 0,
            non_retryable_error_types: vec![],
        };
        for i in 0..50 {
            assert!(rp.should_retry(i, None).is_some());
        }
    }

    #[test]
    fn no_retry_err_str_match() {
        let rp = RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_interval: Some(prost_dur!(from_secs(10))),
            maximum_attempts: 10,
            non_retryable_error_types: vec!["no retry".to_string()],
        };
        assert!(
            rp.should_retry(
                1,
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
        let rp = RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_interval: Some(prost_dur!(from_secs(10))),
            maximum_attempts: 10,
            non_retryable_error_types: vec![],
        };
        assert!(
            rp.should_retry(
                1,
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
        let rp = RetryPolicy {
            initial_interval: Some(prost_dur!(from_secs(1))),
            backoff_coefficient: 2.0,
            maximum_attempts: 2,
            ..Default::default()
        };
        let afi = &ApplicationFailureInfo {
            r#type: "".to_string(),
            next_retry_delay: Some(prost_dur!(from_secs(50))),
            ..Default::default()
        };
        let res = rp.should_retry(1, Some(afi)).unwrap();
        assert_eq!(res.as_millis(), 50_000);
        assert!(rp.should_retry(2, Some(afi)).is_none());
    }
}
