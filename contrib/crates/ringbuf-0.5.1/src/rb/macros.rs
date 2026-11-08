macro_rules! rb_impl_init {
    ($type:ident) => {
        impl<T, const N: usize> Default for $type<crate::storage::Array<T, N>> {
            fn default() -> Self {
                unsafe { Self::from_raw_parts(crate::utils::uninit_array().into(), usize::default(), usize::default()) }
            }
        }

        impl<T, const N: usize> From<[T; N]> for $type<crate::storage::Array<T, N>> {
            fn from(value: [T; N]) -> Self {
                let (read, write) = (0, value.len());
                unsafe { Self::from_raw_parts(crate::utils::array_to_uninit(value).into(), read, write) }
            }
        }

        #[cfg(feature = "alloc")]
        impl<T> $type<crate::storage::Heap<T>> {
            /// Creates a new instance of a ring buffer.
            ///
            /// *Panics if allocation failed or `capacity` is zero.*
            pub fn new(capacity: usize) -> Self {
                unsafe { Self::from_raw_parts(crate::storage::Heap::<T>::new(capacity), usize::default(), usize::default()) }
            }
            /// Creates a new instance of a ring buffer returning an error if allocation failed.
            ///
            /// *Panics if `capacity` is zero.*
            pub fn try_new(capacity: usize) -> Result<Self, alloc::collections::TryReserveError> {
                let mut vec = alloc::vec::Vec::<core::mem::MaybeUninit<T>>::new();
                vec.try_reserve_exact(capacity)?;
                unsafe { vec.set_len(capacity) };
                Ok(unsafe { Self::from_raw_parts(vec.into_boxed_slice().into(), usize::default(), usize::default()) })
            }
        }

        #[cfg(feature = "alloc")]
        impl<T> From<alloc::vec::Vec<T>> for $type<crate::storage::Heap<T>> {
            fn from(value: alloc::vec::Vec<T>) -> Self {
                let (read, write) = (0, value.len());
                unsafe { Self::from_raw_parts(crate::utils::vec_to_uninit(value).into(), read, write) }
            }
        }

        #[cfg(feature = "alloc")]
        impl<T> From<alloc::boxed::Box<[T]>> for $type<crate::storage::Heap<T>> {
            fn from(value: alloc::boxed::Box<[T]>) -> Self {
                let (read, write) = (0, value.len());
                unsafe { Self::from_raw_parts(crate::utils::boxed_slice_to_uninit(value).into(), read, write) }
            }
        }
    };
}

pub(crate) use rb_impl_init;
