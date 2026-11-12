#[cfg(feature = "alloc")]
use super::storage::Heap;
use super::{
    rb::SharedRb,
    storage::Array,
    wrap::{CachingCons, CachingProd},
};

#[cfg(all(feature = "alloc", not(feature = "portable-atomic")))]
pub use alloc::sync::Arc;
#[cfg(all(feature = "alloc", feature = "portable-atomic"))]
pub use portable_atomic_util::Arc;

/// Stack-allocated ring buffer with static capacity.
///
/// *Capacity (`N`) must be greater than zero.*
pub type StaticRb<T, const N: usize> = SharedRb<Array<T, N>>;

/// Alias for [`StaticRb`] producer.
pub type StaticProd<'a, T, const N: usize> = CachingProd<&'a StaticRb<T, N>>;

/// Alias for [`StaticRb`] consumer.
pub type StaticCons<'a, T, const N: usize> = CachingCons<&'a StaticRb<T, N>>;

/// Heap-allocated ring buffer.
#[cfg(feature = "alloc")]
pub type HeapRb<T> = SharedRb<Heap<T>>;

#[cfg(feature = "alloc")]
/// Alias for [`HeapRb`] producer.
pub type HeapProd<T> = CachingProd<Arc<HeapRb<T>>>;

#[cfg(feature = "alloc")]
/// Alias for [`HeapRb`] consumer.
pub type HeapCons<T> = CachingCons<Arc<HeapRb<T>>>;
