// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Per-object thread-local storage
//!
//! This library provides the `ThreadLocal` type which allows a separate copy of
//! an object to be used for each thread. This allows for per-object
//! thread-local storage, unlike the standard library's `thread_local!` macro
//! which only allows static thread-local storage.
//!
//! Per-thread objects are not destroyed when a thread exits. Instead, objects
//! are only destroyed when the `ThreadLocal` containing them is destroyed.
//!
//! You can also iterate over the thread-local values of all thread in a
//! `ThreadLocal` object using the `iter_mut` and `into_iter` methods. This can
//! only be done if you have mutable access to the `ThreadLocal` object, which
//! guarantees that you are the only thread currently accessing it.
//!
//! Note that since thread IDs are recycled when a thread exits, it is possible
//! for one thread to retrieve the object of another thread. Since this can only
//! occur after a thread has exited this does not lead to any race conditions.
//!
//! # Examples
//!
//! Basic usage of `ThreadLocal`:
//!
//! ```rust
//! use thread_local::ThreadLocal;
//! let tls: ThreadLocal<u32> = ThreadLocal::new();
//! assert_eq!(tls.get(), None);
//! assert_eq!(tls.get_or(|| 5), &5);
//! assert_eq!(tls.get(), Some(&5));
//! ```
//!
//! Combining thread-local values into a single result:
//!
//! ```rust
//! use thread_local::ThreadLocal;
//! use std::cell::Cell;
//! use std::thread;
//!
//! let tls = ThreadLocal::new();
//!
//! // Create a bunch of threads to do stuff
//! thread::scope(|scope| {
//!     for _ in 0..5 {
//!         scope.spawn(|| {
//!             // Increment a counter to count some event...
//!             let cell = tls.get_or(|| Cell::new(0));
//!             cell.set(cell.get() + 1);
//!         });
//!     }
//! });
//!
//! // Once all threads are done, collect the counter values and return the
//! // sum of all thread-local counter values.
//! let total = tls.into_iter().fold(0, |x, y| x + y.get());
//! assert_eq!(total, 5);
//! ```

#![warn(missing_docs)]
#![warn(clippy::undocumented_unsafe_blocks)]
#![warn(unsafe_op_in_unsafe_fn)]
#![cfg_attr(feature = "nightly", feature(thread_local))]

mod cached;
mod thread_id;

#[allow(deprecated)]
pub use cached::{CachedIntoIter, CachedIterMut, CachedThreadLocal};

use std::cell::UnsafeCell;
use std::fmt;
use std::iter::FusedIterator;
use std::mem::MaybeUninit;
use std::panic::UnwindSafe;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use thread_id::Thread;

/// The total number of buckets stored in each thread local.
/// All buckets combined can hold up to `usize::MAX - 1` entries.
const BUCKETS: usize = (usize::BITS - 1) as usize;

/// Thread-local variable wrapper
///
/// See the [module-level documentation](index.html) for more.
pub struct ThreadLocal<T: Send> {
    /// The buckets in the thread local. The nth bucket contains `2^n`
    /// elements. Each bucket is lazily allocated.
    buckets: [AtomicPtr<Entry<T>>; BUCKETS],

    /// The number of values in the thread local. This can be less than the real number of values,
    /// but is never more.
    values: AtomicUsize,
}

struct Entry<T> {
    present: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Entry<T> {
    fn get_value_cell(&self) -> Option<&UnsafeCell<MaybeUninit<T>>> {
        self.present.load(Ordering::Acquire).then_some(&self.value)
    }

    /// # Safety
    /// The caller must guarantee that there are no concurent mutable accesses into
    /// this entry's value.
    unsafe fn as_ref<'a>(&self) -> Option<&'a T> {
        self.get_value_cell()
            // SAFETY: The caller guarantees that there are no concurrent mutable
            // accesses into this value.
            .map(|cell| unsafe { (&*cell.get()).assume_init_ref() })
    }
}

impl<T> Drop for Entry<T> {
    fn drop(&mut self) {
        if *self.present.get_mut() {
            // SAFETY:
            //  * If `present` is true, then the value was properly initalized.
            //    and never dropped before.
            //  * The value is embedded within an `Entry<T>` so the produced
            //    pointer must be properly aligned and non-null, even if T is
            //    a ZST.
            unsafe {
                MaybeUninit::assume_init_drop(&mut *self.value.get());
            }
        }
    }
}

// SAFETY: ThreadLocal is always Sync, even if T isn't
unsafe impl<T: Send> Sync for ThreadLocal<T> {}

impl<T: Send> Default for ThreadLocal<T> {
    fn default() -> ThreadLocal<T> {
        ThreadLocal::new()
    }
}

impl<T: Send> Drop for ThreadLocal<T> {
    fn drop(&mut self) {
        // Free each non-null bucket
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let bucket_ptr = *bucket.get_mut();

            let this_bucket_size = 1 << i;

            if bucket_ptr.is_null() {
                continue;
            }

            // SAFETY: All buckets are allocated from `allocate_bucket`.
            unsafe { deallocate_bucket(bucket_ptr, this_bucket_size) };
        }
    }
}

impl<T: Send> ThreadLocal<T> {
    #[allow(clippy::declare_interior_mutable_const)]
    const NULL_BUCKET: AtomicPtr<Entry<T>> = AtomicPtr::new(ptr::null_mut());

    /// Creates a new empty `ThreadLocal`.
    pub const fn new() -> ThreadLocal<T> {
        Self {
            buckets: [Self::NULL_BUCKET; BUCKETS],
            values: AtomicUsize::new(0),
        }
    }

    /// Creates a new `ThreadLocal` with an initial capacity. If less than the capacity threads
    /// access the thread local it will never reallocate. The capacity may be rounded up to the
    /// nearest power of two.
    pub fn with_capacity(capacity: usize) -> ThreadLocal<T> {
        let allocated_buckets = (usize::BITS - capacity.leading_zeros()) as usize;

        let mut buckets = [Self::NULL_BUCKET; BUCKETS];
        for (i, bucket) in buckets[..allocated_buckets].iter_mut().enumerate() {
            *bucket.get_mut() = allocate_bucket::<T>(1 << i);
        }

        Self {
            buckets,
            values: AtomicUsize::new(0),
        }
    }

    /// Returns the element for the current thread, if it exists.
    pub fn get(&self) -> Option<&T> {
        thread_id::try_get().and_then(|id| self.get_inner(id))
    }

    /// Returns the element for the current thread, or creates it if it doesn't
    /// exist.
    pub fn get_or<F>(&self, create: F) -> &T
    where
        F: FnOnce() -> T,
    {
        let result = self.get_or_try(|| Ok::<T, ()>(create()));
        // SAFETY: The provided closure will never return an Err instance.
        unsafe { result.unwrap_unchecked() }
    }

    /// Returns the element for the current thread, or creates it if it doesn't
    /// exist. If `create` fails, that error is returned and no element is
    /// added.
    pub fn get_or_try<F, E>(&self, create: F) -> Result<&T, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        let thread = thread_id::get();
        if let Some(val) = self.get_inner(thread) {
            return Ok(val);
        }

        Ok(self.insert(thread, create()?))
    }

    fn get_inner(&self, thread: Thread) -> Option<&T> {
        let bucket_ptr = self.get_bucket(thread).load(Ordering::Acquire);
        if bucket_ptr.is_null() {
            return None;
        }
        // SAFETY:
        // - Any allocation larger than isize::MAX bytes would fail to
        //   allocate and thus the `bucket` pointer will be null, it thus must
        //   be safe to that offset and create a mutable borrow from it.
        // - This function has immutable access to the `ThreadLocal` and its contents.
        //   so there should not be concurrent mutable accesses into the same entry.
        // - `thread.index` is guaranteed to be in bounds within the selected bucket.
        let entry = unsafe { &*bucket_ptr.add(thread.index) };
        // SAFETY: This function has immutable access to the `ThreadLocal` and its contents.
        // so there should not be concurrent mutable accesses into the same entry.
        unsafe { entry.as_ref() }
    }

    #[cold]
    fn insert(&self, thread: Thread, data: T) -> &T {
        let bucket_atomic_ptr = self.get_bucket(thread);
        let bucket_ptr: *const _ = bucket_atomic_ptr.load(Ordering::Acquire);

        // If the bucket doesn't already exist, we need to allocate it
        let bucket_ptr = if bucket_ptr.is_null() {
            let new_bucket = allocate_bucket(thread.bucket_size());

            match bucket_atomic_ptr.compare_exchange(
                ptr::null_mut(),
                new_bucket,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => new_bucket,
                // If the bucket value changed (from null), that means
                // another thread stored a new bucket before we could,
                // and we can free our bucket and use that one instead
                Err(bucket_ptr) => {
                    // SAFETY: This bucket was just allocated from the call
                    // allocate_bucket above, and using the same bucket size.
                    unsafe { deallocate_bucket(new_bucket, thread.bucket_size()) }
                    bucket_ptr
                }
            }
        } else {
            bucket_ptr
        };

        // Insert the new element into the bucket
        // SAFETY:
        // - Any allocation larger than isize::MAX bytes would fail to
        //   allocate and thus the `bucket` pointer will be null, it thus must
        //   be safe to that offset and create a mutable borrow from it.
        // - This function has immutable access to the `ThreadLocal` and its contents.
        //   so there should not be concurrent mutable accesses into the same entry.
        // - `thread.index` is guaranteed to be in bounds within the selected bucket.
        let entry = unsafe { &*bucket_ptr.add(thread.index) };
        // If `create` reentrantly called `get_or`/`get_or_try` on this same
        // `ThreadLocal` from this thread, the slot is already initialized. Return
        // the existing value and drop the one we just created, instead of
        // overwriting it. Overwriting would leak the old value, double-count
        // `self.values` (causing out-of-bounds iteration in `RawIter::next_mut`),
        // and invalidate references handed out by the reentrant call.
        // SAFETY: This function has `&self`, so there are no concurrent mutable
        // accesses into this entry (this thread's slot is only written by this thread).
        if let Some(existing) = unsafe { entry.as_ref() } {
            return existing;
        }
        let value_ptr = entry.value.get();
        // SAFETY: No concurrent read accesses are possible as this value has not
        // been initialized until now, and no data races are possible since only
        // the local thread can access this value. The target location of the pointer
        // is valid since it came from the UnsafeCell and a valid initialized value
        // of type `T` is being written into the cell.
        let data_ref = &*unsafe { &mut *value_ptr }.write(data);
        entry.present.store(true, Ordering::Release);

        self.values.fetch_add(1, Ordering::Release);

        data_ref
    }

    #[inline]
    fn get_bucket(&self, thread: Thread) -> &AtomicPtr<Entry<T>> {
        // SAFETY: Thread::bucket can never be BUCKETS or larger, and thus must be a valid offset.
        unsafe { self.buckets.get_unchecked(thread.bucket) }
    }

    /// Returns an iterator over the local values of all threads in unspecified
    /// order.
    ///
    /// This call can be done safely, as `T` is required to implement [`Sync`].
    pub fn iter(&self) -> Iter<'_, T>
    where
        T: Sync,
    {
        Iter {
            thread_local: self,
            raw: RawIter::new(),
        }
    }

    /// Returns a mutable iterator over the local values of all threads in
    /// unspecified order.
    ///
    /// Since this call borrows the `ThreadLocal` mutably, this operation can
    /// be done safely---the mutable borrow statically guarantees no other
    /// threads are currently accessing their associated values.
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            thread_local: self,
            raw: RawIter::new(),
        }
    }

    /// Removes all thread-specific values from the `ThreadLocal`, effectively
    /// resetting it to its original state.
    ///
    /// Since this call borrows the `ThreadLocal` mutably, this operation can
    /// be done safely---the mutable borrow statically guarantees no other
    /// threads are currently accessing their associated values.
    pub fn clear(&mut self) {
        *self = ThreadLocal::new();
    }
}

impl<T: Send> IntoIterator for ThreadLocal<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            thread_local: self,
            raw: RawIter::new(),
        }
    }
}

impl<'a, T: Send + Sync> IntoIterator for &'a ThreadLocal<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: Send> IntoIterator for &'a mut ThreadLocal<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T: Send + Default> ThreadLocal<T> {
    /// Returns the element for the current thread, or creates a default one if
    /// it doesn't exist.
    pub fn get_or_default(&self) -> &T {
        self.get_or(Default::default)
    }
}

impl<T: Send + fmt::Debug> fmt::Debug for ThreadLocal<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ThreadLocal {{ local_data: {:?} }}", self.get())
    }
}

impl<T: Send + UnwindSafe> UnwindSafe for ThreadLocal<T> {}

#[derive(Debug)]
struct RawIter {
    yielded: usize,
    bucket: usize,
    bucket_size: usize,
    index: usize,
}

impl RawIter {
    #[inline]
    fn new() -> Self {
        Self {
            yielded: 0,
            bucket: 0,
            bucket_size: 1,
            index: 0,
        }
    }

    fn next<'a, T: Send + Sync>(&mut self, thread_local: &'a ThreadLocal<T>) -> Option<&'a T> {
        while let Some(bucket) = thread_local.buckets.get(self.bucket) {
            let bucket = bucket.load(Ordering::Acquire);

            if !bucket.is_null() {
                while self.index < self.bucket_size {
                    // SAFETY:
                    // - Any allocation larger than isize::MAX bytes would fail to
                    //   allocate and thus the `bucket` pointer will be null, it thus must
                    //   be safe to that offset and create a mutable borrow from it.
                    // - This function has immutable access to the `ThreadLocal` and its contents.
                    //   so there should not be concurrent mutable accesses into the same entry.
                    // - `thread.index` is guaranteed to be in bounds within the selected bucket.
                    let entry = unsafe { &*bucket.add(self.index) };
                    self.index += 1;
                    // SAFETY: As Iter has a read-only borrow on the ThreadLocal,
                    // no mutable borrows on the values stored inside can exist at
                    // the same time.
                    //
                    // The above present check is properly synchronized and ensures
                    // that the value has been properly initialized.
                    if let Some(value) = unsafe { entry.as_ref() } {
                        self.yielded += 1;
                        return Some(value);
                    }
                }
            }

            self.next_bucket();
        }
        None
    }

    fn next_mut<'a, T: Send>(
        &mut self,
        thread_local: &'a mut ThreadLocal<T>,
    ) -> Option<&'a mut Entry<T>> {
        if *thread_local.values.get_mut() == self.yielded {
            return None;
        }

        loop {
            // SAFETY: The above if check will evaluate to true before self.bucket grows
            // large enough to be bigger than BUCKETS, thus the result of get_unchecked_mut
            // must be valid for all possible values of self.bucket.
            let bucket = unsafe { thread_local.buckets.get_unchecked_mut(self.bucket) };
            let bucket = *bucket.get_mut();

            if !bucket.is_null() {
                while self.index < self.bucket_size {
                    // SAFETY: Any allocation larger than isize::MAX bytes would fail to
                    // allocate and thus the `bucket` pointer will be null, it thus must
                    // be safe to that offset and create a mutable borrow from it.
                    //
                    // IterMut and IntoIter both have exclusive access to the
                    // `ThreadLocal` and its contents, so there should not be concurrent
                    // immutable accesses into the same entry.
                    let entry = unsafe { &mut *bucket.add(self.index) };
                    self.index += 1;
                    if *entry.present.get_mut() {
                        self.yielded += 1;
                        return Some(entry);
                    }
                }
            }

            self.next_bucket();
        }
    }

    #[inline]
    fn next_bucket(&mut self) {
        self.bucket_size <<= 1;
        self.bucket += 1;
        self.index = 0;
    }

    fn size_hint<T: Send>(&self, thread_local: &ThreadLocal<T>) -> (usize, Option<usize>) {
        let total = thread_local.values.load(Ordering::Relaxed);

        // NOTE: `saturating_sub` is required here to avoid integer underflow during
        // concurrent insertion and iteration. The shortest dangerous interleaving is:
        //
        // - Thread A inserts, pausing after `present = true` but *before* `values = 1`
        // - Thread B iterates, sees `present = true`, and sets `yielded = 1`
        // - Thread B calls `size_hint` and sees `values = 0` and `yielded = 1`
        (total.saturating_sub(self.yielded), None)
    }

    fn size_hint_frozen<T: Send>(&self, thread_local: &ThreadLocal<T>) -> (usize, Option<usize>) {
        let total = thread_local.values.load(Ordering::Relaxed);

        // NOTE: this method assumes no concurrent insertion to `thread_local`,
        // so this subtraction cannot underflow as in `size_hint` above.
        let remaining = total - self.yielded;
        (remaining, Some(remaining))
    }
}

/// Iterator over the contents of a `ThreadLocal`.
#[derive(Debug)]
pub struct Iter<'a, T: Send + Sync> {
    thread_local: &'a ThreadLocal<T>,
    raw: RawIter,
}

impl<'a, T: Send + Sync> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next(self.thread_local)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw.size_hint(self.thread_local)
    }
}

impl<T: Send + Sync> FusedIterator for Iter<'_, T> {}

/// Mutable iterator over the contents of a `ThreadLocal`.
pub struct IterMut<'a, T: Send> {
    thread_local: &'a mut ThreadLocal<T>,
    raw: RawIter,
}

impl<'a, T: Send> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<&'a mut T> {
        self.raw
            .next_mut(self.thread_local)
            // SAFETY: IterMut has exclusive access to the underlying ThreadLocal
            // and if RawIter::next_mut returns an entry, it's guaranteed to have
            // been initialized.
            .map(|entry| unsafe { (&mut *entry.value.get()).assume_init_mut() })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw.size_hint_frozen(self.thread_local)
    }
}

impl<T: Send> ExactSizeIterator for IterMut<'_, T> {}
impl<T: Send> FusedIterator for IterMut<'_, T> {}

// Manual impl so we don't call Debug on the ThreadLocal, as doing so would create a reference to
// this thread's value that potentially aliases with a mutable reference we have given out.
impl<'a, T: Send + fmt::Debug> fmt::Debug for IterMut<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IterMut").field("raw", &self.raw).finish()
    }
}

/// An iterator that moves out of a `ThreadLocal`.
#[derive(Debug)]
pub struct IntoIter<T: Send> {
    thread_local: ThreadLocal<T>,
    raw: RawIter,
}

impl<T: Send> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.raw.next_mut(&mut self.thread_local).map(|entry| {
            *entry.present.get_mut() = false;
            // SAFETY: IntoIter owns the ThreadLocal and has exclusive access to it
            // and the values stored within.
            let cell = unsafe { &mut *entry.value.get() };
            let old_value = std::mem::replace(cell, MaybeUninit::uninit());
            // SAFETY: If RawIter returned a non-None result, it means this cell was
            // previously populated and thus has been initialized.
            unsafe { old_value.assume_init() }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw.size_hint_frozen(&self.thread_local)
    }
}

impl<T: Send> ExactSizeIterator for IntoIter<T> {}
impl<T: Send> FusedIterator for IntoIter<T> {}

fn allocate_bucket<T>(size: usize) -> *mut Entry<T> {
    Box::into_raw(
        (0..size)
            .map(|_| Entry::<T> {
                present: AtomicBool::new(false),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect(),
    ) as *mut _
}

/// # Safety
/// The caller must ensure that `bucket` was allocated from [allocate_bucket]
/// with the same `size` parameter.
unsafe fn deallocate_bucket<T>(bucket: *mut Entry<T>, size: usize) {
    // SAFETY: The caller ensures that the bucket pointer and size come from a
    // corresponding call to `allocate_bucket` with an identical size, and thus:
    //  * `bucket` must not be null.
    //  * `size` must match the same length as when the bucket was allocated.
    //  * `bucket` points to a slice of properly initialized `Entry<T>`.
    //  * The total size of the allocation cannot be larger than isize::MAX
    //    bytes or the allocation would have failed and panicked.
    let slice = unsafe { std::slice::from_raw_parts_mut(bucket, size) };
    // SAFETY: It's the caller's responsibliity that the bucket was created
    // from `allocate_bucket`, which ensures that it was allocated using the
    // global allocator.
    drop(unsafe { Box::from_raw(slice) });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use std::thread;

    fn make_create() -> Arc<dyn Fn() -> usize + Send + Sync> {
        let count = AtomicUsize::new(0);
        Arc::new(move || count.fetch_add(1, Relaxed))
    }

    #[test]
    fn same_thread() {
        let create = make_create();
        let mut tls = ThreadLocal::new();
        assert_eq!(None, tls.get());
        assert_eq!("ThreadLocal { local_data: None }", format!("{:?}", &tls));
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!("ThreadLocal { local_data: Some(0) }", format!("{:?}", &tls));
        tls.clear();
        assert_eq!(None, tls.get());
    }

    #[test]
    fn reentrant_get_or() {
        let tls: ThreadLocal<Box<u32>> = ThreadLocal::new();
        let outer = tls.get_or(|| {
            let inner = tls.get_or(|| Box::new(1));
            assert_eq!(**inner, 1);
            Box::new(2)
        });
        // The reentrant call already populated the slot, so the outer
        // value is discarded and the existing value is returned.
        assert_eq!(**outer, 1);
        // Pre-fix, `values` was double-counted and this iterated out of
        // bounds (UB caught by Miri). Post-fix exactly one value is present.
        let collected: Vec<u32> = tls.into_iter().map(|b| *b).collect();
        assert_eq!(collected, vec![1]);
    }

    #[test]
    fn different_thread() {
        let create = make_create();
        let tls = Arc::new(ThreadLocal::new());
        assert_eq!(None, tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());

        let tls2 = tls.clone();
        let create2 = create.clone();
        thread::spawn(move || {
            assert_eq!(None, tls2.get());
            assert_eq!(1, *tls2.get_or(|| create2()));
            assert_eq!(Some(&1), tls2.get());
        })
        .join()
        .unwrap();

        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
    }

    #[test]
    fn iter() {
        let tls = Arc::new(ThreadLocal::new());
        tls.get_or(|| Box::new(1));

        let tls2 = tls.clone();
        thread::spawn(move || {
            tls2.get_or(|| Box::new(2));
            let tls3 = tls2.clone();
            thread::spawn(move || {
                tls3.get_or(|| Box::new(3));
            })
            .join()
            .unwrap();
            drop(tls2);
        })
        .join()
        .unwrap();

        let mut tls = Arc::try_unwrap(tls).unwrap();

        let mut v = tls.iter().map(|x| **x).collect::<Vec<i32>>();
        v.sort_unstable();
        assert_eq!(vec![1, 2, 3], v);

        let mut v = tls.iter_mut().map(|x| **x).collect::<Vec<i32>>();
        v.sort_unstable();
        assert_eq!(vec![1, 2, 3], v);

        let mut v = tls.into_iter().map(|x| *x).collect::<Vec<i32>>();
        v.sort_unstable();
        assert_eq!(vec![1, 2, 3], v);
    }

    #[test]
    fn miri_iter_soundness_check() {
        let tls = Arc::new(ThreadLocal::new());
        let _local = tls.get_or(|| Box::new(1));

        let tls2 = tls.clone();
        let join_1 = thread::spawn(move || {
            let _tls = tls2.get_or(|| Box::new(2));
            let iter = tls2.iter();
            for item in iter {
                println!("{:?}", item);
            }
        });

        let iter = tls.iter();
        for item in iter {
            println!("{:?}", item);
        }

        join_1.join().ok();
    }

    #[test]
    fn test_drop() {
        let local = ThreadLocal::new();
        struct Dropped(Arc<AtomicUsize>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        local.get_or(|| Dropped(dropped.clone()));
        assert_eq!(dropped.load(Relaxed), 0);
        drop(local);
        assert_eq!(dropped.load(Relaxed), 1);
    }

    #[test]
    fn test_earlyreturn_buckets() {
        struct Dropped(Arc<AtomicUsize>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }
        let dropped = Arc::new(AtomicUsize::new(0));

        // We use a high `id` here to guarantee that a lazily allocated bucket somewhere in the middle is used.
        // Neither iteration nor `Drop` must early-return on `null` buckets that are used for lower `buckets`.
        let thread = Thread::new(1234);
        assert!(thread.bucket > 1);

        let mut local = ThreadLocal::new();
        local.insert(thread, Dropped(dropped.clone()));

        let item = local.iter().next().unwrap();
        assert_eq!(item.0.load(Relaxed), 0);
        let item = local.iter_mut().next().unwrap();
        assert_eq!(item.0.load(Relaxed), 0);
        drop(local);
        assert_eq!(dropped.load(Relaxed), 1);
    }

    #[test]
    fn is_sync() {
        fn foo<T: Sync>() {}
        foo::<ThreadLocal<String>>();
        foo::<ThreadLocal<RefCell<String>>>();
    }
}
