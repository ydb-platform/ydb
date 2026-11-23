use core::time::Duration;

use crate::backoff::BackoffBuilder;

/// ConstantBuilder is used to create a [`ConstantBackoff`], providing a steady delay with a fixed number of retries.
///
/// # Default
///
/// - delay: 1s
/// - max_times: 3
///
/// # Examples
///
/// ```no_run
/// use anyhow::Result;
/// use backon::ConstantBuilder;
/// use backon::Retryable;
///
/// async fn fetch() -> Result<String> {
///     Ok(reqwest::get("https://www.rust-lang.org")
///         .await?
///         .text()
///         .await?)
/// }
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() -> Result<()> {
///     let content = fetch.retry(ConstantBuilder::default()).await?;
///     println!("fetch succeeded: {}", content);
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ConstantBuilder {
    delay: Duration,
    max_times: Option<usize>,
    jitter: bool,
    seed: Option<u64>,
}

impl Default for ConstantBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstantBuilder {
    /// Create a new `ConstantBuilder` with default values.
    pub const fn new() -> Self {
        Self {
            delay: Duration::from_secs(1),
            max_times: Some(3),
            jitter: false,
            seed: None,
        }
    }

    /// Set the delay for the backoff.
    pub const fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Set the maximum number of attempts to be made.
    pub const fn with_max_times(mut self, max_times: usize) -> Self {
        self.max_times = Some(max_times);
        self
    }

    /// Enable jitter for the backoff.
    ///
    /// Jitter is a random value added to the delay to prevent a thundering herd problem.
    pub const fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }

    /// Set the seed value for the jitter random number generator. If no seed is given, a random seed is used in std and default seed is used in no_std.
    pub fn with_jitter_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Set no max times for the backoff.
    ///
    /// The backoff will not stop by itself.
    ///
    /// _The backoff could stop reaching `usize::MAX` attempts but this is **unrealistic**._
    pub const fn without_max_times(mut self) -> Self {
        self.max_times = None;
        self
    }
}

impl BackoffBuilder for ConstantBuilder {
    type Backoff = ConstantBackoff;

    fn build(self) -> Self::Backoff {
        ConstantBackoff {
            delay: self.delay,
            max_times: self.max_times,

            attempts: 0,
            jitter: self.jitter,
            rng: if let Some(seed) = self.seed {
                fastrand::Rng::with_seed(seed)
            } else {
                #[cfg(feature = "std")]
                let rng = fastrand::Rng::new();

                #[cfg(not(feature = "std"))]
                let rng = fastrand::Rng::with_seed(super::RANDOM_SEED);

                rng
            },
        }
    }
}

impl BackoffBuilder for &ConstantBuilder {
    type Backoff = ConstantBackoff;

    fn build(self) -> Self::Backoff {
        (*self).build()
    }
}

/// ConstantBackoff offers a consistent delay with a limited number of retries.
///
/// This backoff strategy is constructed by [`ConstantBuilder`].
#[doc(hidden)]
#[derive(Debug)]
pub struct ConstantBackoff {
    delay: Duration,
    max_times: Option<usize>,

    attempts: usize,
    jitter: bool,
    rng: fastrand::Rng,
}

impl Iterator for ConstantBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        let mut delay = || match self.jitter {
            true => self.delay + self.delay.mul_f32(self.rng.f32()),
            false => self.delay,
        };
        match self.max_times {
            None => Some(delay()),
            Some(max_times) => {
                if self.attempts >= max_times {
                    None
                } else {
                    self.attempts += 1;
                    Some(delay())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    use super::*;

    const TEST_BUILDER: ConstantBuilder = ConstantBuilder::new()
        .with_delay(Duration::from_secs(2))
        .with_max_times(5)
        .with_jitter();

    #[test]
    fn test_constant_default() {
        let mut it = ConstantBuilder::default().build();

        assert_eq!(Some(Duration::from_secs(1)), it.next());
        assert_eq!(Some(Duration::from_secs(1)), it.next());
        assert_eq!(Some(Duration::from_secs(1)), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn test_constant_with_delay() {
        let mut it = ConstantBuilder::default()
            .with_delay(Duration::from_secs(2))
            .build();

        assert_eq!(Some(Duration::from_secs(2)), it.next());
        assert_eq!(Some(Duration::from_secs(2)), it.next());
        assert_eq!(Some(Duration::from_secs(2)), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn test_constant_with_times() {
        let mut it = ConstantBuilder::default().with_max_times(1).build();

        assert_eq!(Some(Duration::from_secs(1)), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn test_constant_with_jitter() {
        let mut it = ConstantBuilder::default().with_jitter().build();

        let dur = it.next().unwrap();
        fastrand::seed(7);
        assert!(dur > Duration::from_secs(1));
    }

    #[test]
    fn test_constant_without_max_times() {
        let mut it = ConstantBuilder::default().without_max_times().build();

        for _ in 0..10_000 {
            assert_eq!(Some(Duration::from_secs(1)), it.next());
        }
    }

    // allow assertions on constants because they are not optimized out by unit tests
    #[allow(clippy::assertions_on_constants)]
    #[test]
    fn test_constant_const_builder() {
        assert_eq!(TEST_BUILDER.delay, Duration::from_secs(2));
        assert_eq!(TEST_BUILDER.max_times, Some(5));
        assert!(TEST_BUILDER.jitter);
    }
}
