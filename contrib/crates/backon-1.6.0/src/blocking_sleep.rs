use core::time::Duration;

/// A sleeper is used sleep for a specified duration.
pub trait BlockingSleeper: 'static {
    /// sleep for a specified duration.
    fn sleep(&self, dur: Duration);
}

/// A stub trait allowing non-[`BlockingSleeper`] types to be used as a generic parameter in [`BlockingRetry`][crate::BlockingRetry].
/// It does not provide actual functionality.
#[doc(hidden)]
pub trait MaybeBlockingSleeper: 'static {}

/// All `BlockingSleeper` will implement  `MaybeBlockingSleeper`, but not vice versa.
impl<T: BlockingSleeper + ?Sized> MaybeBlockingSleeper for T {}

/// All `Fn(Duration)` implements `Sleeper`.
impl<F: Fn(Duration) + 'static> BlockingSleeper for F {
    fn sleep(&self, dur: Duration) {
        self(dur)
    }
}

/// The default implementation of `Sleeper` when no features are enabled.
///
/// It will fail to compile if a containing [`Retry`][crate::Retry] is `.await`ed without calling [`Retry::sleep`][crate::Retry::sleep] to provide a valid sleeper.
#[cfg(not(feature = "std-blocking-sleep"))]
pub type DefaultBlockingSleeper = PleaseEnableAFeatureOrProvideACustomSleeper;
/// The default implementation of `Sleeper` while feature `std-blocking-sleep` enabled.
///
/// it uses [`std::thread::sleep`].
#[cfg(feature = "std-blocking-sleep")]
pub type DefaultBlockingSleeper = StdSleeper;

/// A placeholder type that does not implement [`Sleeper`] and will therefore fail to compile if used as one.
///
/// Users should enable a feature of this crate that provides a valid [`Sleeper`] implementation when this type appears in compilation errors. Alternatively, a custom [`Sleeper`] implementation should be provided where necessary, such as in [`crate::Retry::sleeper`].
#[doc(hidden)]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default)]
pub struct PleaseEnableAFeatureOrProvideACustomSleeper;

/// Implement `MaybeSleeper` but not `Sleeper`.
impl MaybeBlockingSleeper for PleaseEnableAFeatureOrProvideACustomSleeper {}

/// The implementation of `StdSleeper` uses [`std::thread::sleep`].
#[cfg(feature = "std-blocking-sleep")]
#[derive(Clone, Copy, Debug, Default)]
pub struct StdSleeper;

#[cfg(feature = "std-blocking-sleep")]
impl BlockingSleeper for StdSleeper {
    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur)
    }
}
