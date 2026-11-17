//! Frozen implementation.
//!
//! Changes are not synchronized with the ring buffer until its explicitly requested or when dropped.

use super::{direct::Obs, traits::Wrap};
use crate::{
    rb::RbRef,
    traits::{
        Observer, RingBuffer,
        consumer::{Consumer, impl_consumer_traits},
        producer::{Producer, impl_producer_traits},
    },
};
use core::{
    cell::Cell,
    mem::{ManuallyDrop, MaybeUninit},
    num::NonZeroUsize,
    ptr,
};

/// Frozen wrapper of the ring buffer.
pub struct Frozen<R: RbRef, const P: bool, const C: bool> {
    rb: R,
    read: Cell<usize>,
    write: Cell<usize>,
}

/// Frozen write end of some ring buffer.
///
/// Inserted items is not visible for an opposite write end until [`Self::commit`]/[`Self::sync`] is called or `Self` is dropped.
/// A free space of items removed by an opposite write end is not visible for `Self` until [`Self::sync`] is called.
pub type FrozenProd<R> = Frozen<R, true, false>;

/// Frozen read end of some ring buffer.
///
/// A free space of removed items is not visible for an opposite write end until [`Self::commit`]/[`Self::sync`] is called or `Self` is dropped.
/// Items inserted by an opposite write end is not visible for `Self` until [`Self::sync`] is called.
pub type FrozenCons<R> = Frozen<R, false, true>;

impl<R: RbRef, const P: bool, const C: bool> Frozen<R, P, C> {
    /// Create a new ring buffer frozen wrapper.
    ///
    /// Panics if wrapper with matching rights already exists.
    pub fn new(rb: R) -> Self {
        if P {
            assert!(!unsafe { rb.rb().hold_write(true) });
        }
        if C {
            assert!(!unsafe { rb.rb().hold_read(true) });
        }
        unsafe { Self::new_unchecked(rb) }
    }

    /// Create wrapper without checking that such wrapper already exists.
    ///
    /// # Safety
    ///
    /// There must be maximum one instance of matching rights.
    pub(crate) unsafe fn new_unchecked(rb: R) -> Self {
        Self {
            read: Cell::new(rb.rb().read_index()),
            write: Cell::new(rb.rb().write_index()),
            rb,
        }
    }

    /// Get ring buffer observer.
    pub fn observe(&self) -> Obs<R> {
        Obs::new(self.rb.clone())
    }

    unsafe fn close(&mut self) {
        if P {
            unsafe { self.rb().hold_write(false) };
        }
        if C {
            unsafe { self.rb().hold_read(false) };
        }
    }
}

impl<R: RbRef, const P: bool, const C: bool> Wrap for Frozen<R, P, C> {
    type RbRef = R;

    fn rb_ref(&self) -> &R {
        &self.rb
    }
    fn into_rb_ref(mut self) -> R {
        self.commit();
        unsafe {
            self.close();
            let this = ManuallyDrop::new(self);
            ptr::read(&this.rb)
        }
    }
}

impl<R: RbRef, const P: bool, const C: bool> AsRef<Self> for Frozen<R, P, C> {
    fn as_ref(&self) -> &Self {
        self
    }
}
impl<R: RbRef, const P: bool, const C: bool> AsMut<Self> for Frozen<R, P, C> {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl<R: RbRef, const P: bool, const C: bool> Frozen<R, P, C> {
    /// Commit changes to the ring buffer.
    pub fn commit(&self) {
        unsafe {
            if P {
                self.rb().set_write_index(self.write.get());
            }
            if C {
                self.rb().set_read_index(self.read.get());
            }
        }
    }

    /// Fetch changes from the ring buffer.
    pub fn fetch(&self) {
        if P {
            self.read.set(self.rb().read_index());
        }
        if C {
            self.write.set(self.rb().write_index());
        }
    }

    /// Commit changes to and fetch updates from the ring buffer.
    pub fn sync(&self) {
        self.commit();
        self.fetch();
    }
}

impl<R: RbRef> FrozenProd<R> {
    /// Discard new items pushed since last sync.
    pub fn discard(&mut self) {
        let last_tail = self.rb().write_index();
        let (first, second) = unsafe { self.rb().unsafe_slices_mut(last_tail, self.write.get()) };
        for item_mut in first.iter_mut().chain(second.iter_mut()) {
            unsafe { item_mut.assume_init_drop() };
        }
        self.write.set(last_tail);
    }
}

impl<R: RbRef, const P: bool, const C: bool> Observer for Frozen<R, P, C> {
    type Item = <R::Rb as Observer>::Item;

    #[inline]
    fn capacity(&self) -> NonZeroUsize {
        self.rb().capacity()
    }

    #[inline]
    fn read_index(&self) -> usize {
        self.read.get()
    }
    #[inline]
    fn write_index(&self) -> usize {
        self.write.get()
    }

    unsafe fn unsafe_slices(&self, start: usize, end: usize) -> (&[MaybeUninit<Self::Item>], &[MaybeUninit<Self::Item>]) {
        unsafe { self.rb().unsafe_slices(start, end) }
    }
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

impl<R: RbRef> Producer for FrozenProd<R> {
    #[inline]
    unsafe fn set_write_index(&self, value: usize) {
        self.write.set(value);
    }
}

impl<R: RbRef> Consumer for FrozenCons<R> {
    #[inline]
    unsafe fn set_read_index(&self, value: usize) {
        self.read.set(value);
    }
}

impl<R: RbRef, const P: bool, const C: bool> Drop for Frozen<R, P, C> {
    fn drop(&mut self) {
        self.commit();
        unsafe { self.close() };
    }
}

impl_producer_traits!(FrozenProd<R: RbRef>);
impl_consumer_traits!(FrozenCons<R: RbRef>);
