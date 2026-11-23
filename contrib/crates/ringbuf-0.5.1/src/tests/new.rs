use super::Rb;
use crate::{storage::Array, traits::*};
#[cfg(feature = "alloc")]
use alloc::vec::Vec;

#[test]
fn new_static() {
    let rb = Rb::<Array<i32, 2>>::default();
    let (mut prod, mut cons) = rb.split();

    assert_eq!(cons.occupied_len(), 0);
    assert_eq!(cons.capacity().get(), 2);

    assert_eq!(prod.try_push(1), Ok(()));
    assert_eq!(prod.try_push(2), Ok(()));
    assert_eq!(cons.try_pop(), Some(1));

    assert_eq!(prod.try_push(3), Ok(()));
    assert_eq!(cons.try_pop(), Some(2));
    assert_eq!(cons.try_pop(), Some(3));
    assert_eq!(cons.try_pop(), None);
}

#[cfg(feature = "alloc")]
#[test]
fn from_vec() {
    let mut vec = Vec::<i32>::with_capacity(4);
    vec.extend_from_slice(&[1, 2, 3]);
    let rb = Rb::from(vec);
    let (mut prod, mut cons) = rb.split();

    assert_eq!(cons.occupied_len(), 3);
    assert_eq!(cons.capacity().get(), 4);

    assert_eq!(cons.try_pop(), Some(1));
    assert_eq!(cons.try_pop(), Some(2));
    assert_eq!(cons.try_pop(), Some(3));
    assert_eq!(cons.try_pop(), None);

    assert_eq!(prod.try_push(4), Ok(()));
    assert_eq!(prod.try_push(5), Ok(()));
    assert_eq!(cons.try_pop(), Some(4));
    assert_eq!(cons.try_pop(), Some(5));
    assert_eq!(cons.try_pop(), None);
}
