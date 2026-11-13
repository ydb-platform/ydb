use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Implements something a bit like a `OnceLock`, but starts already initialized and allows you
/// to take everything out of it only once in a thread-safe way. This isn't optimized for super
/// fast-path usage.
pub(crate) struct TakeCell<T> {
    taken: AtomicBool,
    data: Mutex<Option<T>>,
}

impl<T> TakeCell<T> {
    pub(crate) fn new(val: T) -> Self {
        Self {
            taken: AtomicBool::new(false),
            data: Mutex::new(Some(val)),
        }
    }

    /// If the cell has not already been taken from, takes the value and returns it
    pub(crate) fn take_once(&self) -> Option<T> {
        if self.taken.load(Ordering::Acquire) {
            return None;
        }
        self.taken.store(true, Ordering::Release);
        self.data.lock().take()
    }
}
