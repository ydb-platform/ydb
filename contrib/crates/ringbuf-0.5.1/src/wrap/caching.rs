//! Caching implementation.
//!
//! Fetches changes from the ring buffer only when there is no more slots to perform requested operation.

use super::{direct::Obs, frozen::Frozen, traits::Wrap};
use crate::{
    rb::RbRef,
    traits::{
        Observer,
        consumer::{Consumer, impl_consumer_traits},
        producer::{Producer, impl_producer_traits},
    },
};
use core::{mem::MaybeUninit, num::NonZeroUsize};

/// Caching wrapper of a ring buffer.
pub struct Caching<R: RbRef, const P: bool, const C: bool> {
    frozen: Frozen<R, P, C>,
}

/// Caching producer implementation.
pub type CachingProd<R> = Caching<R, true, false>;
/// Caching consumer implementation.
pub type CachingCons<R> = Caching<R, false, true>;

impl<R: RbRef, const P: bool, const C: bool> Caching<R, P, C> {
    /// Create a new ring buffer cached wrapper.
    ///
    /// Panics if wrapper with matching rights already exists.
    pub fn new(rb: R) -> Self {
        Self { frozen: Frozen::new(rb) }
    }

    /// Get ring buffer observer.
    pub fn observe(&self) -> Obs<R> {
        self.frozen.observe()
    }

    /// Freeze current state.
    pub fn freeze(self) -> Frozen<R, P, C> {
        self.frozen
    }
}

impl<R: RbRef, const P: bool, const C: bool> Wrap for Caching<R, P, C> {
    type RbRef = R;

    fn rb_ref(&self) -> &R {
        self.frozen.rb_ref()
    }
    fn into_rb_ref(self) -> R {
        self.frozen.into_rb_ref()
    }
}

impl<R: RbRef, const P: bool, const C: bool> AsRef<Self> for Caching<R, P, C> {
    fn as_ref(&self) -> &Self {
        self
    }
}
impl<R: RbRef, const P: bool, const C: bool> AsMut<Self> for Caching<R, P, C> {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl<R: RbRef, const P: bool, const C: bool> Observer for Caching<R, P, C> {
    type Item = <R::Rb as Observer>::Item;

    #[inline]
    fn capacity(&self) -> NonZeroUsize {
        self.frozen.capacity()
    }

    #[inline]
    fn read_index(&self) -> usize {
        if P {
            self.frozen.fetch();
        }
        self.frozen.read_index()
    }
    #[inline]
    fn write_index(&self) -> usize {
        if C {
            self.frozen.fetch();
        }
        self.frozen.write_index()
    }

    unsafe fn unsafe_slices(&self, start: usize, end: usize) -> (&[MaybeUninit<Self::Item>], &[MaybeUninit<Self::Item>]) {
        unsafe { self.frozen.unsafe_slices(start, end) }
    }
    unsafe fn unsafe_slices_mut(&self, start: usize, end: usize) -> (&mut [MaybeUninit<Self::Item>], &mut [MaybeUninit<Self::Item>]) {
        unsafe { self.frozen.unsafe_slices_mut(start, end) }
    }

    #[inline]
    fn read_is_held(&self) -> bool {
        self.frozen.read_is_held()
    }
    #[inline]
    fn write_is_held(&self) -> bool {
        self.frozen.write_is_held()
    }
}

impl<R: RbRef> Producer for CachingProd<R> {
    #[inline]
    unsafe fn set_write_index(&self, value: usize) {
        unsafe { self.frozen.set_write_index(value) };
        self.frozen.commit();
    }

    fn try_push(&mut self, elem: Self::Item) -> Result<(), Self::Item> {
        if self.frozen.is_full() {
            self.frozen.fetch();
        }
        let r = self.frozen.try_push(elem);
        if r.is_ok() {
            self.frozen.commit();
        }
        r
    }
}

impl<R: RbRef> Consumer for CachingCons<R> {
    #[inline]
    unsafe fn set_read_index(&self, value: usize) {
        unsafe { self.frozen.set_read_index(value) };
        self.frozen.commit();
    }

    fn try_pop(&mut self) -> Option<<Self as Observer>::Item> {
        if self.frozen.is_empty() {
            self.frozen.fetch();
        }
        let r = self.frozen.try_pop();
        if r.is_some() {
            self.frozen.commit();
        }
        r
    }
}

impl_producer_traits!(CachingProd<R: RbRef>);
impl_consumer_traits!(CachingCons<R: RbRef>);
