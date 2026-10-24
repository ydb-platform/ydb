//! Direct implementation.
//!
//! All changes are synchronized with the ring buffer immediately.

use super::{frozen::Frozen, traits::Wrap};
use crate::{
    rb::RbRef,
    traits::{
        Observer, RingBuffer,
        consumer::{Consumer, impl_consumer_traits},
        producer::{Producer, impl_producer_traits},
    },
};
use core::{
    mem::{ManuallyDrop, MaybeUninit},
    num::NonZeroUsize,
    ptr,
};

/// Direct wrapper of a ring buffer.
pub struct Direct<R: RbRef, const P: bool, const C: bool> {
    rb: R,
}

/// Observer of a ring buffer.
pub type Obs<R> = Direct<R, false, false>;
/// Producer of a ring buffer.
pub type Prod<R> = Direct<R, true, false>;
/// Consumer of a ring buffer.
pub type Cons<R> = Direct<R, false, true>;

impl<R: RbRef> Clone for Obs<R> {
    fn clone(&self) -> Self {
        Self { rb: self.rb.clone() }
    }
}

impl<R: RbRef, const P: bool, const C: bool> Direct<R, P, C> {
    /// Create a new ring buffer direct wrapper.
    ///
    /// Panics if wrapper with matching rights already exists.
    pub fn new(rb: R) -> Self {
        if P {
            assert!(!unsafe { rb.rb().hold_write(true) });
        }
        if C {
            assert!(!unsafe { rb.rb().hold_read(true) });
        }
        Self { rb }
    }

    /// Get ring buffer observer.
    pub fn observe(&self) -> Obs<R> {
        Obs { rb: self.rb.clone() }
    }

    /// Freeze current state.
    pub fn freeze(self) -> Frozen<R, P, C> {
        let this = ManuallyDrop::new(self);
        unsafe { Frozen::new_unchecked(ptr::read(&this.rb)) }
    }

    /// # Safety
    ///
    /// Must not be used after this call.
    unsafe fn close(&mut self) {
        if P {
            unsafe { self.rb().hold_write(false) };
        }
        if C {
            unsafe { self.rb().hold_read(false) };
        }
    }
}

impl<R: RbRef, const P: bool, const C: bool> Wrap for Direct<R, P, C> {
    type RbRef = R;
    fn rb_ref(&self) -> &R {
        &self.rb
    }
    fn into_rb_ref(mut self) -> R {
        unsafe {
            self.close();
            let this = ManuallyDrop::new(self);
            ptr::read(&this.rb)
        }
    }
}

impl<R: RbRef, const P: bool, const C: bool> AsRef<Self> for Direct<R, P, C> {
    fn as_ref(&self) -> &Self {
        self
    }
}
impl<R: RbRef, const P: bool, const C: bool> AsMut<Self> for Direct<R, P, C> {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl<R: RbRef, const P: bool, const C: bool> Observer for Direct<R, P, C> {
    type Item = <R::Rb as Observer>::Item;

    #[inline]
    fn capacity(&self) -> NonZeroUsize {
        self.rb().capacity()
    }
    #[inline]
    fn read_index(&self) -> usize {
        self.rb().read_index()
    }
    #[inline]
    fn write_index(&self) -> usize {
        self.rb().write_index()
    }
    #[inline]
    unsafe fn unsafe_slices(&self, start: usize, end: usize) -> (&[MaybeUninit<Self::Item>], &[MaybeUninit<Self::Item>]) {
        unsafe { self.rb().unsafe_slices(start, end) }
    }
    #[inline]
    unsafe fn unsafe_slices_mut(&self, start: usize, end: usize) -> (&mut [MaybeUninit<Self::Item>], &mut [MaybeUninit<Self::Item>]) {
        unsafe { self.rb().unsafe_slices_mut(start, end) }
    }
    #[inline]
    fn read_is_held(&self) -> bool {
        self.rb().read_is_held()
    }
    #[inline]
    fn write_is_held(&self) -> bool {
        self.rb().write_is_held()
    }
}

impl<R: RbRef> Producer for Prod<R> {
    #[inline]
    unsafe fn set_write_index(&self, value: usize) {
        unsafe { self.rb().set_write_index(value) }
    }
}

impl<R: RbRef> Consumer for Cons<R> {
    #[inline]
    unsafe fn set_read_index(&self, value: usize) {
        unsafe { self.rb().set_read_index(value) }
    }
}

impl<R: RbRef, const P: bool, const C: bool> Drop for Direct<R, P, C> {
    fn drop(&mut self) {
        unsafe { self.close() };
    }
}

impl_producer_traits!(Prod<R: RbRef>);
impl_consumer_traits!(Cons<R: RbRef>);
