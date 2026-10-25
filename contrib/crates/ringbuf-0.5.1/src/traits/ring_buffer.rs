use super::{
    Observer,
    consumer::{Consumer, DelegateConsumer},
    producer::{DelegateProducer, Producer},
};

/// An abstract ring buffer that exclusively owns its data.
pub trait RingBuffer: Observer + Consumer + Producer {
    /// Tell whether read end of the ring buffer is held by consumer or not.
    ///
    /// Returns old value.
    ///
    /// # Safety
    ///
    /// Must not be set to `false` while consumer exists.
    unsafe fn hold_read(&self, flag: bool) -> bool;
    /// Tell whether write end of the ring buffer is held by producer or not.
    ///
    /// Returns old value.
    ///
    /// # Safety
    ///
    /// Must not be set to `false` while producer exists.
    unsafe fn hold_write(&self, flag: bool) -> bool;

    /// Pushes an item to the ring buffer overwriting the least recent item if the buffer is full.
    ///
    /// Returns overwritten item if overwriting took place.
    fn push_overwrite(&mut self, elem: Self::Item) -> Option<Self::Item> {
        let ret = if self.is_full() { self.try_pop() } else { None };
        let _ = self.try_push(elem);
        ret
    }

    /// Appends items from an iterator to the ring buffer.
    ///
    /// *This method consumes iterator until its end.*
    /// Exactly last `min(iter.len(), capacity)` items from the iterator will be stored in the ring buffer.
    fn push_iter_overwrite<I: Iterator<Item = Self::Item>>(&mut self, iter: I) {
        for elem in iter {
            self.push_overwrite(elem);
        }
    }

    /// Appends items from slice to the ring buffer overwriting existing items in the ring buffer.
    ///
    /// If the slice length is greater than ring buffer capacity then only last `capacity` items from slice will be stored in the buffer.
    fn push_slice_overwrite(&mut self, elems: &[Self::Item])
    where
        Self::Item: Copy,
    {
        if elems.len() > self.vacant_len() {
            self.skip(usize::min(elems.len() - self.vacant_len(), self.occupied_len()));
        }
        self.push_slice(if elems.len() > self.vacant_len() {
            &elems[(elems.len() - self.vacant_len())..]
        } else {
            elems
        });
    }
}

/// Trait used for delegating owning ring buffer methods.
pub trait DelegateRingBuffer: DelegateProducer + DelegateConsumer
where
    Self::Base: RingBuffer,
{
}

impl<D: DelegateRingBuffer> RingBuffer for D
where
    D::Base: RingBuffer,
{
    unsafe fn hold_read(&self, flag: bool) -> bool {
        unsafe { self.base().hold_read(flag) }
    }
    unsafe fn hold_write(&self, flag: bool) -> bool {
        unsafe { self.base().hold_write(flag) }
    }

    #[inline]
    fn push_overwrite(&mut self, elem: Self::Item) -> Option<Self::Item> {
        self.base_mut().push_overwrite(elem)
    }

    #[inline]
    fn push_iter_overwrite<I: Iterator<Item = Self::Item>>(&mut self, iter: I) {
        self.base_mut().push_iter_overwrite(iter)
    }

    #[inline]
    fn push_slice_overwrite(&mut self, elems: &[Self::Item])
    where
        Self::Item: Copy,
    {
        self.base_mut().push_slice_overwrite(elems)
    }
}
