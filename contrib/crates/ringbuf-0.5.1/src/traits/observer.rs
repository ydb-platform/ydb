use super::{Based, utils::modulus};
use core::{mem::MaybeUninit, num::NonZeroUsize};

/// Ring buffer observer.
///
/// Can observe ring buffer state but cannot safely access its data.
pub trait Observer {
    type Item: Sized;

    /// Capacity of the ring buffer.
    ///
    /// It is constant during the whole ring buffer lifetime.
    fn capacity(&self) -> NonZeroUsize;

    /// Index of the last item in the ring buffer.
    ///
    /// Index value is in range `0..(2 * capacity)`.
    fn read_index(&self) -> usize;
    /// Index of the next empty slot in the ring buffer.
    ///
    /// Index value is in range `0..(2 * capacity)`.
    fn write_index(&self) -> usize;

    /// Get slice between `start` and `end` indices.
    ///
    /// # Safety
    ///
    /// Slice must not overlap with any mutable slice existing at the same time.
    ///
    /// Non-`Sync` items must not be accessed from multiple threads at the same time.
    unsafe fn unsafe_slices(&self, start: usize, end: usize) -> (&[MaybeUninit<Self::Item>], &[MaybeUninit<Self::Item>]);

    /// Get mutable slice between `start` and `end` indices.
    ///
    /// # Safety
    ///
    /// There must not exist overlapping slices at the same time.
    #[allow(clippy::mut_from_ref)]
    unsafe fn unsafe_slices_mut(&self, start: usize, end: usize) -> (&mut [MaybeUninit<Self::Item>], &mut [MaybeUninit<Self::Item>]);

    /// Whether read end is held by consumer.
    fn read_is_held(&self) -> bool;
    /// Whether write end is held by producer.
    fn write_is_held(&self) -> bool;

    /// The number of items stored in the buffer.
    ///
    /// *Actual number may be greater or less than returned value due to concurring activity of producer or consumer respectively.*
    fn occupied_len(&self) -> usize {
        let modulus = modulus(self);
        (modulus.get() + self.write_index() - self.read_index()) % modulus
    }

    /// The number of remaining free places in the buffer.
    ///
    /// *Actual number may be greater or less than returned value due to concurring activity of consumer or producer respectively.*
    fn vacant_len(&self) -> usize {
        let modulus = modulus(self);
        (self.capacity().get() + self.read_index() - self.write_index()) % modulus
    }

    /// Checks if the ring buffer is empty.
    ///
    /// *The result may become irrelevant at any time because of concurring producer activity.*
    #[inline]
    fn is_empty(&self) -> bool {
        self.read_index() == self.write_index()
    }

    /// Checks if the ring buffer is full.
    ///
    /// *The result may become irrelevant at any time because of concurring consumer activity.*
    #[inline]
    fn is_full(&self) -> bool {
        self.vacant_len() == 0
    }
}

/// Trait used for delegating observer methods.
pub trait DelegateObserver: Based
where
    Self::Base: Observer,
{
}

impl<D: DelegateObserver> Observer for D
where
    D::Base: Observer,
{
    type Item = <D::Base as Observer>::Item;

    #[inline]
    fn capacity(&self) -> NonZeroUsize {
        self.base().capacity()
    }

    #[inline]
    fn read_index(&self) -> usize {
        self.base().read_index()
    }
    #[inline]
    fn write_index(&self) -> usize {
        self.base().write_index()
    }

    #[inline]
    unsafe fn unsafe_slices(&self, start: usize, end: usize) -> (&[MaybeUninit<Self::Item>], &[MaybeUninit<Self::Item>]) {
        unsafe { self.base().unsafe_slices(start, end) }
    }
    #[inline]
    unsafe fn unsafe_slices_mut(&self, start: usize, end: usize) -> (&mut [MaybeUninit<Self::Item>], &mut [MaybeUninit<Self::Item>]) {
        unsafe { self.base().unsafe_slices_mut(start, end) }
    }

    #[inline]
    fn read_is_held(&self) -> bool {
        self.base().read_is_held()
    }
    #[inline]
    fn write_is_held(&self) -> bool {
        self.base().write_is_held()
    }

    #[inline]
    fn occupied_len(&self) -> usize {
        self.base().occupied_len()
    }

    #[inline]
    fn vacant_len(&self) -> usize {
        self.base().vacant_len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.base().is_empty()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.base().is_full()
    }
}
