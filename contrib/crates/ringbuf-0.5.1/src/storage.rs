#[cfg(feature = "alloc")]
use alloc::{boxed::Box, vec::Vec};
use core::{cell::UnsafeCell, marker::PhantomData, mem::MaybeUninit, ops::Range, ptr::NonNull, slice};
#[cfg(feature = "alloc")]
use core::{mem::ManuallyDrop, ptr};

/// Abstract storage for the ring buffer.
///
/// Storage items must be stored as a contiguous array.
///
/// # Safety
///
/// Must not alias with its contents
/// (it must be safe to store mutable references to storage itself and to its data at the same time).
///
/// [`Self::as_mut_ptr`] must point to underlying data.
///
/// [`Self::len`] must always return the same value.
pub unsafe trait Storage {
    /// Stored item.
    type Item: Sized;

    /// Length of the storage.
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return pointer to the beginning of the storage items.
    fn as_ptr(&self) -> *const MaybeUninit<Self::Item> {
        self.as_mut_ptr().cast_const()
    }
    /// Return mutable pointer to the beginning of the storage items.
    fn as_mut_ptr(&self) -> *mut MaybeUninit<Self::Item>;

    /// Returns a mutable slice of storage in specified `range`.
    ///
    /// # Safety
    ///
    /// Slice must not overlab with existing mutable slices.
    ///
    /// Non-`Sync` items must not be accessed concurrently.
    unsafe fn slice(&self, range: Range<usize>) -> &[MaybeUninit<Self::Item>] {
        unsafe { slice::from_raw_parts(self.as_ptr().add(range.start), range.len()) }
    }
    /// Returns a mutable slice of storage in specified `range`.
    ///
    /// # Safety
    ///
    /// Slices must not overlap.
    #[allow(clippy::mut_from_ref)]
    unsafe fn slice_mut(&self, range: Range<usize>) -> &mut [MaybeUninit<Self::Item>] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr().add(range.start), range.len()) }
    }
}

pub struct Ref<'a, T> {
    _ghost: PhantomData<&'a mut [T]>,
    ptr: *mut MaybeUninit<T>,
    len: usize,
}
unsafe impl<T> Send for Ref<'_, T> where T: Send {}
unsafe impl<T> Sync for Ref<'_, T> where T: Send {}
unsafe impl<T> Storage for Ref<'_, T> {
    type Item = T;
    #[inline]
    fn as_mut_ptr(&self) -> *mut MaybeUninit<T> {
        self.ptr
    }
    #[inline]
    fn len(&self) -> usize {
        self.len
    }
}
impl<'a, T> From<&'a mut [MaybeUninit<T>]> for Ref<'a, T> {
    fn from(value: &'a mut [MaybeUninit<T>]) -> Self {
        Self {
            _ghost: PhantomData,
            ptr: value.as_mut_ptr(),
            len: value.len(),
        }
    }
}
impl<'a, T> From<Ref<'a, T>> for &'a mut [MaybeUninit<T>] {
    fn from(value: Ref<'a, T>) -> Self {
        unsafe { slice::from_raw_parts_mut(value.ptr, value.len) }
    }
}

pub struct Owning<T: ?Sized> {
    data: UnsafeCell<T>,
}
unsafe impl<T: ?Sized> Sync for Owning<T> where T: Send {}
impl<T> From<T> for Owning<T> {
    fn from(value: T) -> Self {
        Self {
            data: UnsafeCell::new(value),
        }
    }
}

pub type Array<T, const N: usize> = Owning<[MaybeUninit<T>; N]>;
unsafe impl<T, const N: usize> Storage for Array<T, N> {
    type Item = T;
    #[inline]
    fn as_mut_ptr(&self) -> *mut MaybeUninit<T> {
        self.data.get().cast()
    }
    #[inline]
    fn len(&self) -> usize {
        N
    }
}
impl<T, const N: usize> From<Array<T, N>> for [MaybeUninit<T>; N] {
    fn from(value: Array<T, N>) -> Self {
        value.data.into_inner()
    }
}

pub type Slice<T> = Owning<[MaybeUninit<T>]>;
unsafe impl<T> Storage for Slice<T> {
    type Item = T;
    #[inline]
    fn as_mut_ptr(&self) -> *mut MaybeUninit<T> {
        self.data.get().cast()
    }
    #[inline]
    fn len(&self) -> usize {
        unsafe { NonNull::new_unchecked(self.data.get()) }.len()
    }
}

#[cfg(feature = "alloc")]
pub struct Heap<T> {
    ptr: *mut MaybeUninit<T>,
    len: usize,
}
#[cfg(feature = "alloc")]
unsafe impl<T> Send for Heap<T> where T: Send {}
#[cfg(feature = "alloc")]
unsafe impl<T> Sync for Heap<T> where T: Send {}
#[cfg(feature = "alloc")]
unsafe impl<T> Storage for Heap<T> {
    type Item = T;
    #[inline]
    fn as_mut_ptr(&self) -> *mut MaybeUninit<T> {
        self.ptr
    }
    #[inline]
    fn len(&self) -> usize {
        self.len
    }
}
#[cfg(feature = "alloc")]
impl<T> Heap<T> {
    /// Create a new heap storage with exact capacity.
    pub fn new(capacity: usize) -> Self {
        let mut data = Vec::<MaybeUninit<T>>::with_capacity(capacity);
        // `data.capacity()` is not guaranteed to be equal to `capacity`.
        // We enforce that by `set_len` and converting to boxed slice.
        unsafe { data.set_len(capacity) };
        Self::from(data.into_boxed_slice())
    }
}
#[cfg(feature = "alloc")]
impl<T> From<Vec<MaybeUninit<T>>> for Heap<T> {
    fn from(mut value: Vec<MaybeUninit<T>>) -> Self {
        // Convert `value` to boxed slice of length equals to `value.capacity()`
        // except for zero-sized types - for them length will be `value.len()` because `Vec::capacity` for ZST is undefined
        // (see <https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees>).
        if core::mem::size_of::<T>() != 0 {
            unsafe { value.set_len(value.capacity()) };
        }
        Self::from(value.into_boxed_slice())
    }
}
#[cfg(feature = "alloc")]
impl<T> From<Box<[MaybeUninit<T>]>> for Heap<T> {
    fn from(value: Box<[MaybeUninit<T>]>) -> Self {
        Self {
            len: value.len(),
            ptr: Box::into_raw(value).cast(),
        }
    }
}
#[cfg(feature = "alloc")]
impl<T> From<Heap<T>> for Box<[MaybeUninit<T>]> {
    fn from(value: Heap<T>) -> Self {
        let value = ManuallyDrop::new(value);
        unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(value.ptr, value.len)) }
    }
}
#[cfg(feature = "alloc")]
impl<T> Drop for Heap<T> {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(self.ptr, self.len)) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::{cell::Cell, marker::PhantomData};

    struct Check<S: Storage + Send + Sync + ?Sized>(PhantomData<S>);

    #[allow(dead_code)]
    fn check_send_sync() {
        let _: Check<Ref<Cell<i32>>>;
        let _: Check<Array<Cell<i32>, 4>>;
        let _: Check<Slice<Cell<i32>>>;
        let _: Check<Heap<Cell<i32>>>;
    }
}
