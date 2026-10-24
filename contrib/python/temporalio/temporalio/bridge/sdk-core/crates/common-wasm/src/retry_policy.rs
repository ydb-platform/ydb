use std::time::Duration;

use crate::protos::temporal::api::common::v1::RetryPolicy as ProtoRetryPolicy;

const DEFAULT_INITIAL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_BACKOFF_COEFFICIENT: f64 = 2.0;
const MAX_PROTO_DURATION: prost_types::Duration = prost_types::Duration {
    seconds: 315_576_000_000,
    nanos: 999_999_999,
};

fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    duration.try_into().unwrap_or(MAX_PROTO_DURATION)
}

/// Options for retrying workflows and activities.
///
/// Durations longer than 10,000 years are clamped to the maximum valid protobuf duration.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct RetryPolicy {
    raw: ProtoRetryPolicy,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[bon::bon]
impl RetryPolicy {
    /// Create a retry policy.
    #[builder(state_mod(vis = "pub"))]
    pub fn new(
        #[builder(default = DEFAULT_INITIAL_INTERVAL)] initial_interval: Duration,
        #[builder(default = DEFAULT_BACKOFF_COEFFICIENT)] backoff_coefficient: f64,
        maximum_interval: Option<Duration>,
        #[builder(default)] maximum_attempts: u32,
        #[builder(
            with = |values: impl IntoIterator<Item = impl Into<String>>| values
                .into_iter()
                .map(Into::into)
                .collect(),
            default
        )]
        non_retryable_error_types: Vec<String>,
    ) -> Self {
        let mut policy = Self {
            raw: ProtoRetryPolicy::default(),
        };
        policy
            .set_initial_interval(initial_interval)
            .set_backoff_coefficient(backoff_coefficient)
            .set_maximum_interval(maximum_interval)
            .set_maximum_attempts(maximum_attempts)
            .set_non_retryable_error_types(non_retryable_error_types);
        policy
    }

    /// Backoff interval for the first retry.
    pub fn initial_interval(&self) -> Duration {
        self.raw
            .initial_interval
            .map(|duration| duration.try_into().ok().unwrap_or(Duration::ZERO))
            .unwrap_or(DEFAULT_INITIAL_INTERVAL)
    }

    /// Set the backoff interval for the first retry.
    pub fn set_initial_interval(&mut self, initial_interval: Duration) -> &mut Self {
        self.raw.initial_interval = Some(duration_to_proto(initial_interval));
        self
    }

    /// Coefficient used to calculate the next retry interval.
    pub fn backoff_coefficient(&self) -> f64 {
        if self.raw.backoff_coefficient == 0.0 {
            DEFAULT_BACKOFF_COEFFICIENT
        } else {
            self.raw.backoff_coefficient
        }
    }

    /// Set the coefficient used to calculate the next retry interval.
    pub fn set_backoff_coefficient(&mut self, backoff_coefficient: f64) -> &mut Self {
        self.raw.backoff_coefficient = backoff_coefficient;
        self
    }

    /// Maximum backoff interval between retries.
    pub fn maximum_interval(&self) -> Option<Duration> {
        self.raw
            .maximum_interval
            .map(|duration| duration.try_into().ok().unwrap_or(Duration::ZERO))
    }

    /// Set the maximum backoff interval between retries.
    pub fn set_maximum_interval(&mut self, maximum_interval: Option<Duration>) -> &mut Self {
        self.raw.maximum_interval = maximum_interval.map(duration_to_proto);
        self
    }

    /// Maximum number of attempts. Zero means unlimited attempts.
    pub fn maximum_attempts(&self) -> u32 {
        self.raw.maximum_attempts.try_into().unwrap_or_default()
    }

    /// Set the maximum number of attempts. Zero means unlimited attempts. Values greater than
    /// [`i32::MAX`] are clamped to [`i32::MAX`].
    pub fn set_maximum_attempts(&mut self, maximum_attempts: u32) -> &mut Self {
        self.raw.maximum_attempts = maximum_attempts.try_into().unwrap_or(i32::MAX);
        self
    }

    /// Error type names that should not be retried.
    pub fn non_retryable_error_types(&self) -> &[String] {
        &self.raw.non_retryable_error_types
    }

    /// Set the error type names that should not be retried.
    pub fn set_non_retryable_error_types(
        &mut self,
        values: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self {
        self.raw.non_retryable_error_types = values.into_iter().map(Into::into).collect();
        self
    }

    /// Access the underlying retry policy protobuf.
    pub fn raw(&self) -> &ProtoRetryPolicy {
        &self.raw
    }

    /// Consume this wrapper and return the underlying retry policy protobuf.
    pub fn into_raw(self) -> ProtoRetryPolicy {
        self.raw
    }
}

impl From<ProtoRetryPolicy> for RetryPolicy {
    fn from(value: ProtoRetryPolicy) -> Self {
        Self { raw: value }
    }
}

impl From<RetryPolicy> for ProtoRetryPolicy {
    fn from(value: RetryPolicy) -> Self {
        value.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::default(RetryPolicy::default())]
    #[case::builder(RetryPolicy::builder().build())]
    #[case::proto(RetryPolicy::from(ProtoRetryPolicy::default()))]
    fn defaults_match_temporal_retry_defaults(#[case] policy: RetryPolicy) {
        assert_eq!(policy.initial_interval(), Duration::from_secs(1));
        assert_eq!(policy.backoff_coefficient(), 2.0);
        assert_eq!(policy.maximum_attempts(), 0);
        assert_eq!(policy.maximum_interval(), None);
    }

    #[test]
    fn retry_policy_round_trips() {
        let policy = RetryPolicy::builder()
            .initial_interval(Duration::from_millis(250))
            .backoff_coefficient(1.5)
            .maximum_interval(Duration::from_secs(10))
            .maximum_attempts(5)
            .non_retryable_error_types(["InvalidInput"])
            .build();

        assert_eq!(
            RetryPolicy::from(ProtoRetryPolicy::from(policy.clone())),
            policy
        );
    }

    #[test]
    fn setters_update_raw_proto() {
        let mut policy = RetryPolicy::default();
        policy
            .set_initial_interval(Duration::from_millis(250))
            .set_backoff_coefficient(1.5)
            .set_maximum_interval(Some(Duration::from_secs(10)))
            .set_maximum_attempts(5)
            .set_non_retryable_error_types(["InvalidInput"]);

        assert_eq!(policy.initial_interval(), Duration::from_millis(250));
        assert_eq!(policy.backoff_coefficient(), 1.5);
        assert_eq!(policy.maximum_interval(), Some(Duration::from_secs(10)));
        assert_eq!(policy.maximum_attempts(), 5);
        assert_eq!(policy.non_retryable_error_types(), ["InvalidInput"]);
        assert_eq!(
            policy.raw().initial_interval,
            Duration::from_millis(250).try_into().ok()
        );
        assert_eq!(policy.raw().backoff_coefficient, 1.5);
        assert_eq!(
            policy.raw().maximum_interval,
            Duration::from_secs(10).try_into().ok()
        );
        assert_eq!(policy.raw().maximum_attempts, 5);
        assert_eq!(policy.raw().non_retryable_error_types, ["InvalidInput"]);
    }

    #[test]
    fn invalid_raw_values_are_normalized_by_getters() {
        let raw = ProtoRetryPolicy {
            initial_interval: Some(prost_types::Duration {
                seconds: i64::MIN,
                nanos: 999_999_999,
            }),
            maximum_interval: Some(prost_types::Duration {
                seconds: i64::MIN,
                nanos: 999_999_999,
            }),
            maximum_attempts: -1,
            ..Default::default()
        };
        let policy = RetryPolicy::from(raw.clone());

        assert_eq!(policy.initial_interval(), Duration::ZERO);
        assert_eq!(policy.maximum_interval(), Some(Duration::ZERO));
        assert_eq!(policy.maximum_attempts(), 0);
        assert_eq!(policy.raw(), &raw);
    }
}
