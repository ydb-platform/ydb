use super::Observer;
use core::num::NonZeroUsize;

/// Trait that should be implemented by ring buffer wrappers.
///
/// Used for automatically delegating methods.
pub trait Based {
    /// Type the wrapper based on.
    type Base: ?Sized;
    /// Reference to base.
    fn base(&self) -> &Self::Base;
    /// Mutable reference to base.
    fn base_mut(&mut self) -> &mut Self::Base;
}

/// Modulus for pointers to item in ring buffer storage.
///
/// Equals to `2 * capacity`.
#[inline]
pub fn modulus<O: Observer + ?Sized>(this: &O) -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(2 * this.capacity().get()) }
}
