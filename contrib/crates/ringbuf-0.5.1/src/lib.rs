//! Lock-free SPSC FIFO ring buffer with direct access to inner data.
//!
//! # Usage
//!
//! At first you need to create the ring buffer itself. [`HeapRb`] is recommended but you may [choose another one](#types).
//!
//! After the ring buffer is created it may be splitted into pair of [`Producer`](`traits::Producer`) and [`Consumer`](`traits::Consumer`).
//! Producer is used to insert items to the ring buffer, consumer - to remove items from it.
//!
//! # Types
//!
//! There are several types of ring buffers provided:
//!
//! + [`LocalRb`]. Only for single-threaded use.
//! + [`SharedRb`]. Can be shared between threads. Its frequently used instances:
//!   + [`HeapRb`]. Contents are stored in dynamic memory. *Recommended for use in most cases.*
//!   + [`StaticRb`]. Contents can be stored in statically-allocated memory.
//!
//! You may also provide your own generic parameters.
//!
//! # Performance
//!
//! [`SharedRb`] needs to synchronize CPU cache between CPU cores. This synchronization has some overhead.
//! To avoid multiple unnecessary synchronizations you may use methods that operate many items at once
//! ([`push_slice`](`traits::Producer::push_slice`)/[`push_iter`](`traits::Producer::push_iter`), [`pop_slice`](`traits::Consumer::pop_slice`)/[`pop_iter`](`traits::Consumer::pop_iter`), etc.)
//! or you can `freeze` [producer](`Prod::freeze`) or [consumer](`Cons::freeze`) and then synchronize threads manually (see items in [`frozen`](`wrap::frozen`) module).
//!
//! For single-threaded usage [`LocalRb`] is recommended because it is slightly faster than [`SharedRb`] due to absence of CPU cache synchronization.
//!
//! # Examples
//!
#![cfg_attr(
    feature = "alloc",
    doc = r##"
## Simple

```rust
use ringbuf::{traits::*, HeapRb};

# fn main() {
let rb = HeapRb::<i32>::new(2);
let (mut prod, mut cons) = rb.split();

prod.try_push(0).unwrap();
prod.try_push(1).unwrap();
assert_eq!(prod.try_push(2), Err(2));

assert_eq!(cons.try_pop(), Some(0));

prod.try_push(2).unwrap();

assert_eq!(cons.try_pop(), Some(1));
assert_eq!(cons.try_pop(), Some(2));
assert_eq!(cons.try_pop(), None);
# }
```
"##
)]
#![doc = r##"
## No heap

```rust
use ringbuf::{traits::*, StaticRb};

# fn main() {
const RB_SIZE: usize = 1;
let mut rb = StaticRb::<i32, RB_SIZE>::default();
let (mut prod, mut cons) = rb.split_ref();

assert_eq!(prod.try_push(123), Ok(()));
assert_eq!(prod.try_push(321), Err(321));

assert_eq!(cons.try_pop(), Some(123));
assert_eq!(cons.try_pop(), None);
# }
```
"##]
#![cfg_attr(
    feature = "std",
    doc = r##"
## Overwrite

Ring buffer can be used in overwriting mode when insertion overwrites the latest element if the buffer is full.

```rust
use ringbuf::{traits::*, HeapRb};

# fn main() {
let mut rb = HeapRb::<i32>::new(2);

assert_eq!(rb.push_overwrite(0), None);
assert_eq!(rb.push_overwrite(1), None);
assert_eq!(rb.push_overwrite(2), Some(0));

assert_eq!(rb.try_pop(), Some(1));
assert_eq!(rb.try_pop(), Some(2));
assert_eq!(rb.try_pop(), None);
# }
```

Note that [`push_overwrite`](`traits::RingBuffer::push_overwrite`) requires exclusive access to the ring buffer
so to perform it concurrently you need to guard the ring buffer with mutex or some other lock.
"##
)]
//!
//! # Implementation details
//!
//! Each ring buffer here consists of the following parts:
//!
//! + Storage
//! + Indices
//! + Hold flags
//!
//! ## Storage
//!
//! [`Storage`](`storage::Storage`) is a place where ring buffer items are actually stored.
//! It must span a single contiguous memory area (e.g. we can obtain a slice or subslice of it).
//! Ring buffer can own its storage or it can hold only a mutable reference to it.
//! Storage length is refered as `capacity`.
//!
//! ## Indices
//!
//! Ring buffer also contains two indices: `read` and `write`.
//!
//! `read % capacity` points to the oldest item in the storage.
//! `write % capacity` points to empty slot next to the most recently inserted item.
//!
//! When an item is extracted from the ring buffer it is taken from the `read % capacity` slot and then `read` index is incremented.
//! New item is put into the `write % capacity` slot and `write` index is incremented after that.
//!
//! Slots with indices between (in modular arithmetic) `(read % capacity)` (including) and
//! `(write % capacity)` (excluding) contain items (are initialized).
//! All other slots do not contain items (are uninitialized).
//!
//! The actual values of `read` and `write` indices are modulo `2 * capacity` instead of just `capacity`.
//! It allows us to distinguish situations when the buffer is empty (`read == write`)
//! and when the buffer is full (`(write - read) % (2 * capacity) == capacity`)
//! without using an extra slot in container that cannot be occupied.
//!
//! But this causes the existense of invalid combinations of indices.
//! For example, we cannot store more than `capacity` items in the buffer,
//! so `(write - read) % (2 * capacity)` is not allowed to be greater than `capacity`.
//!
//! ## Hold flags
//!
//! Ring buffer can have at most one producer and at most one consumer at the same time.
//! These flags indicates whether it's safe to obtain a new producer or a consumer.
//!
#![no_std]
#![allow(clippy::type_complexity)]
#![cfg_attr(feature = "bench", feature(test))]

#[cfg(feature = "alloc")]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

/// Shortcuts for frequently used types.
mod alias;
/// Ring buffer implementations.
pub mod rb;
/// Storage types.
pub mod storage;
/// Ring buffer traits.
pub mod traits;
/// Items transfer between ring buffers.
mod transfer;
/// Internal utilities.
mod utils;
/// Producer and consumer implementations.
pub mod wrap;

#[cfg(test)]
mod tests;

pub use alias::*;
pub use rb::{LocalRb, SharedRb};
pub use traits::{consumer, producer};
pub use transfer::transfer;
pub use wrap::{CachingCons, CachingProd, Cons, Obs, Prod};

#[cfg(feature = "bench")]
extern crate test;
#[cfg(feature = "bench")]
mod benchmarks;
