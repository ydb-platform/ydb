use crate::RetryPolicy;

/// An error handler trait.
///
/// Please note that this trait is implemented for any `FnMut` closure with a compatible signature,
/// so for some simple cases you might simply use a closure instead of creating your own type and
/// implementing this trait for it.
///
/// Here's an example of an error handler that counts *consecutive* error attempts.
///
/// ```
/// use futures_retry::{ErrorHandler, RetryPolicy};
/// use std::io;
/// use std::time::Duration;
///
/// pub struct CustomHandler {
///     max_attempts: usize,
/// }
///
/// impl CustomHandler {
///
///     pub fn new(attempts: usize) -> Self {
///         Self {
///             max_attempts: attempts,
///         }
///     }
/// }
///
/// impl ErrorHandler<io::Error> for CustomHandler {
///     type OutError = io::Error;
///
///     fn handle(&mut self, attempt: usize, e: io::Error) -> RetryPolicy<io::Error> {
///         if attempt == self.max_attempts {
///             eprintln!("No attempts left");
///             return RetryPolicy::ForwardError(e);
///         }
///         match e.kind() {
///             io::ErrorKind::ConnectionRefused => RetryPolicy::WaitRetry(Duration::from_secs(1)),
///             io::ErrorKind::TimedOut => RetryPolicy::Repeat,
///             _ => RetryPolicy::ForwardError(e),
///         }
///     }
/// }
/// ```
pub trait ErrorHandler<InError> {
    /// An error that the `handle` function will produce.
    type OutError;

    /// Handles an error.
    ///
    /// Refer to the [`RetryPolicy`](enum.RetryPolicy.html) type to understand what this method
    /// might return.
    fn handle(&mut self, attempt: usize, _: InError) -> RetryPolicy<Self::OutError>;

    /// This method is called on a successful execution (before returning an item) of the underlying
    /// future/stream.
    ///
    /// One can use this method to reset an internal state.
    ///
    /// By default the method is a no-op.
    fn ok(&mut self, _attempt: usize) {}
}

impl<InError, F, OutError> ErrorHandler<InError> for F
where
    F: Unpin + FnMut(InError) -> RetryPolicy<OutError>,
{
    type OutError = OutError;

    fn handle(&mut self, _attempt: usize, e: InError) -> RetryPolicy<OutError> {
        (self)(e)
    }
}
