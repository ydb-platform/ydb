use super::{
    observer::{DelegateObserver, Observer},
    utils::modulus,
};
#[cfg(feature = "std")]
use crate::utils::slice_assume_init_mut;
use crate::utils::write_slice;
use core::mem::MaybeUninit;
#[cfg(feature = "std")]
use std::{
    cmp,
    io::{self, Read},
};

/// Producer part of ring buffer.
pub trait Producer: Observer {
    /// Set read index.
    ///
    /// # Safety
    ///
    /// Index must go only forward, never backward. It is recommended to use [`Self::advance_write_index`] instead.
    ///
    /// All slots with index less than `value` must be initialized until write index, all slots with index equal or greater - must be uninitialized.
    unsafe fn set_write_index(&self, value: usize);

    /// Moves `write` pointer by `count` places forward.
    ///
    /// # Safety
    ///
    /// First `count` items in free space must be initialized.
    ///
    /// Must not be called concurrently.
    unsafe fn advance_write_index(&self, count: usize) {
        unsafe { self.set_write_index((self.write_index() + count) % modulus(self)) };
    }

    /// Provides a direct access to the ring buffer vacant memory.
    ///
    /// Returns a pair of slices of uninitialized memory, the second one may be empty.
    fn vacant_slices(&self) -> (&[MaybeUninit<Self::Item>], &[MaybeUninit<Self::Item>]) {
        unsafe { self.unsafe_slices(self.write_index(), self.read_index() + self.capacity().get()) }
    }

    /// Mutable version of [`Self::vacant_slices`].
    ///
    /// Vacant memory is uninitialized. Initialized items must be put starting from the beginning of first slice.
    /// When first slice is fully filled then items must be put to the beginning of the second slice.
    ///
    /// *This method must be followed by [`Self::advance_write_index`] call with the number of items being put previously as argument.*
    /// *No other mutating calls allowed before that.*
    ///
    /// *Vacant slices must not be used to store any data because their contents aren't synchronized properly.*
    fn vacant_slices_mut(&mut self) -> (&mut [MaybeUninit<Self::Item>], &mut [MaybeUninit<Self::Item>]) {
        unsafe { self.unsafe_slices_mut(self.write_index(), self.read_index() + self.capacity().get()) }
    }

    /// Appends an item to the ring buffer.
    ///
    /// If buffer is full returns an `Err` containing the item that hasn't been appended.
    fn try_push(&mut self, elem: Self::Item) -> Result<(), Self::Item> {
        if !self.is_full() {
            unsafe {
                self.vacant_slices_mut().0.get_unchecked_mut(0).write(elem);
                self.advance_write_index(1)
            };
            Ok(())
        } else {
            Err(elem)
        }
    }

    /// Appends items from an iterator to the ring buffer.
    /// Elements that haven't been added to the ring buffer remain in the iterator.
    ///
    /// Returns count of items been appended to the ring buffer.
    ///
    /// *Inserted items are committed to the ring buffer all at once in the end,*
    /// *e.g. when buffer is full or iterator has ended.*
    fn push_iter<I: Iterator<Item = Self::Item>>(&mut self, mut iter: I) -> usize {
        let (left, right) = self.vacant_slices_mut();
        let mut count = 0;
        for place in left.iter_mut().chain(right.iter_mut()) {
            match iter.next() {
                Some(elem) => unsafe { place.as_mut_ptr().write(elem) },
                None => break,
            }
            count += 1;
        }
        unsafe { self.advance_write_index(count) };
        count
    }

    /// Appends items from slice to the ring buffer.
    ///
    /// Returns count of items been appended to the ring buffer.
    fn push_slice(&mut self, elems: &[Self::Item]) -> usize
    where
        Self::Item: Copy,
    {
        let (left, right) = self.vacant_slices_mut();
        let count = if elems.len() < left.len() {
            write_slice(&mut left[..elems.len()], elems);
            elems.len()
        } else {
            let (left_elems, elems) = elems.split_at(left.len());
            write_slice(left, left_elems);
            left.len()
                + if elems.len() < right.len() {
                    write_slice(&mut right[..elems.len()], elems);
                    elems.len()
                } else {
                    write_slice(right, &elems[..right.len()]);
                    right.len()
                }
        };
        unsafe { self.advance_write_index(count) };
        count
    }

    #[cfg(feature = "std")]
    /// Reads at most `count` bytes from `Read` instance and appends them to the ring buffer.
    /// If `count` is `None` then as much as possible bytes will be read.
    ///
    /// Returns:
    /// + `None`: ring buffer is full or `count` is `0`. In this case `read` isn't called at all.
    /// + `Some(Ok(n))`: `read` succeeded. `n` is number of bytes been read. `n == 0` means that `read` also returned `0`.
    /// + `Some(Err(e))` `read` is failed and `e` is original error. In this case it is guaranteed that no items was read from the reader.
    ///   To achieve this we read only one contiguous slice at once. So this call may read less than `vacant_len` items in the buffer even if the reader is ready to provide more.
    fn read_from<S: Read>(&mut self, reader: &mut S, count: Option<usize>) -> Option<io::Result<usize>>
    where
        Self: Producer<Item = u8>,
    {
        let (left, _) = self.vacant_slices_mut();
        let count = cmp::min(count.unwrap_or(left.len()), left.len());
        if count == 0 {
            return None;
        }

        let buf = &mut left[..count];
        // Initialize memory before read. It's an overhead but there's no way to read to uninit buffer in stable Rust yet.
        // TODO: Use `reader.read_buf` when it stabilized (see https://github.com/rust-lang/rust/issues/78485).
        buf.fill(MaybeUninit::new(0));
        let left_init = unsafe { slice_assume_init_mut(buf) };

        let read_count = match reader.read(left_init) {
            Ok(n) => n,
            Err(e) => return Some(Err(e)),
        };
        assert!(read_count <= count);
        unsafe { self.advance_write_index(read_count) };
        Some(Ok(read_count))
    }
}

/// Trait used for delegating consumer methods.
pub trait DelegateProducer: DelegateObserver
where
    Self::Base: Producer,
{
}

impl<D: DelegateProducer> Producer for D
where
    D::Base: Producer,
{
    #[inline]
    unsafe fn set_write_index(&self, value: usize) {
        unsafe { self.base().set_write_index(value) }
    }
    #[inline]
    unsafe fn advance_write_index(&self, count: usize) {
        unsafe { self.base().advance_write_index(count) }
    }

    #[inline]
    fn vacant_slices(&self) -> (&[core::mem::MaybeUninit<Self::Item>], &[core::mem::MaybeUninit<Self::Item>]) {
        self.base().vacant_slices()
    }

    #[inline]
    fn vacant_slices_mut(&mut self) -> (&mut [core::mem::MaybeUninit<Self::Item>], &mut [core::mem::MaybeUninit<Self::Item>]) {
        self.base_mut().vacant_slices_mut()
    }

    #[inline]
    fn try_push(&mut self, elem: Self::Item) -> Result<(), Self::Item> {
        self.base_mut().try_push(elem)
    }

    #[inline]
    fn push_iter<I: Iterator<Item = Self::Item>>(&mut self, iter: I) -> usize {
        self.base_mut().push_iter(iter)
    }

    #[inline]
    fn push_slice(&mut self, elems: &[Self::Item]) -> usize
    where
        Self::Item: Copy,
    {
        self.base_mut().push_slice(elems)
    }
}

macro_rules! impl_producer_traits {
    ($type:ident $(< $( $param:tt $( : $first_bound:tt $(+ $next_bound:tt )* )? ),+ >)?) => {

        #[cfg(feature = "std")]
        impl $(< $( $param $( : $first_bound $(+ $next_bound )* )? ),+ >)? std::io::Write for $type $(< $( $param ),+ >)?
        where
            Self: $crate::traits::Producer<Item = u8>,
        {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let n = self.push_slice(buf);
                if n == 0 {
                    Err(std::io::ErrorKind::WouldBlock.into())
                } else {
                    Ok(n)
                }
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
         }

        impl $(< $( $param $( : $first_bound $(+ $next_bound )* )? ),+ >)? core::fmt::Write for $type $(< $( $param ),+ >)?
        where
            Self: $crate::traits::Producer<Item = u8>,
        {
            fn write_str(&mut self, s: &str) -> core::fmt::Result {
                let n = self.push_slice(s.as_bytes());
                if n != s.len() {
                    Err(core::fmt::Error::default())
                } else {
                    Ok(())
                }
            }
         }
    };
 }
pub(crate) use impl_producer_traits;
