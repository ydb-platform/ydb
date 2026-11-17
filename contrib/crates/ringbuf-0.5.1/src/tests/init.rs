use super::Rb;
#[cfg(feature = "alloc")]
use crate::storage::Heap;
use crate::traits::*;
#[cfg(feature = "alloc")]
use alloc::{boxed::Box, vec::Vec};

#[test]
fn from_array() {
    let mut rb = Rb::from([123, 321]);
    let (mut prod, mut cons) = rb.split_ref();

    assert_eq!(prod.capacity().get(), 2);
    assert_eq!(cons.occupied_len(), 2);

    assert_eq!(prod.try_push(444), Err(444));

    assert_eq!(cons.try_pop(), Some(123));
    assert_eq!(cons.try_pop(), Some(321));
    assert_eq!(cons.try_pop(), None);
}

#[cfg(feature = "alloc")]
#[test]
fn new() {
    const CAP: usize = 2;
    let rb = Rb::<Heap<i32>>::new(CAP);
    let (mut prod, mut cons) = rb.split();

    assert_eq!(prod.capacity().get(), CAP);

    assert_eq!(prod.try_push(0), Ok(()));
    assert_eq!(prod.try_push(1), Ok(()));
    assert_eq!(prod.try_push(2), Err(2));

    assert_eq!(cons.try_pop(), Some(0));
    assert_eq!(cons.try_pop(), Some(1));
    assert_eq!(cons.try_pop(), None);
}

#[cfg(feature = "alloc")]
#[test]
fn from_vec() {
    let mut vec = Vec::<i32>::with_capacity(2);
    vec.push(123);
    let rb = Rb::from(vec);
    let (mut prod, mut cons) = rb.split();

    assert_eq!(prod.capacity().get(), 2);
    assert_eq!(cons.occupied_len(), 1);

    assert_eq!(prod.try_push(321), Ok(()));
    assert_eq!(prod.try_push(444), Err(444));

    assert_eq!(cons.try_pop(), Some(123));
    assert_eq!(cons.try_pop(), Some(321));
    assert_eq!(cons.try_pop(), None);
}

#[cfg(feature = "alloc")]
#[test]
fn from_boxed_slice() {
    let boxed_slice = Box::new([123, 321]) as Box<[i32]>;
    let rb = Rb::from(boxed_slice);
    let (mut prod, mut cons) = rb.split();

    assert_eq!(prod.capacity().get(), 2);
    assert_eq!(cons.occupied_len(), 2);

    assert_eq!(prod.try_push(444), Err(444));

    assert_eq!(cons.try_pop(), Some(123));
    assert_eq!(cons.try_pop(), Some(321));
    assert_eq!(cons.try_pop(), None);
}
