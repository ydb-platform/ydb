use super::Rb;
use crate::{storage::Array, traits::*};

#[test]
fn iter() {
    let mut rb = Rb::<Array<i32, 2>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    prod.try_push(10).unwrap();
    prod.try_push(20).unwrap();

    let sum: i32 = cons.iter().sum();

    let first = cons.try_pop().expect("First item is not available");
    let second = cons.try_pop().expect("Second item is not available");

    assert_eq!(sum, first + second);
}

#[test]
fn iter_mut() {
    let mut rb = Rb::<Array<i32, 2>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    prod.try_push(10).unwrap();
    prod.try_push(20).unwrap();

    for v in cons.iter_mut() {
        *v *= 2;
    }

    let sum: i32 = cons.iter().sum();

    let first = cons.try_pop().expect("First item is not available");
    let second = cons.try_pop().expect("Second item is not available");

    assert_eq!(sum, first + second);
}

#[test]
fn pop_iter() {
    let mut rb = Rb::<Array<i32, 3>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    prod.try_push(0).unwrap();
    prod.try_push(1).unwrap();
    for (i, v) in cons.pop_iter().enumerate() {
        assert_eq!(i as i32, v);
    }

    prod.try_push(2).unwrap();
    prod.try_push(3).unwrap();
    for (i, v) in cons.pop_iter().enumerate() {
        assert_eq!(i as i32 + 2, v);
    }
    assert!(prod.is_empty());
}

#[test]
fn push_pop_iter_partial() {
    let mut rb = Rb::<Array<i32, 4>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    prod.try_push(0).unwrap();
    prod.try_push(1).unwrap();
    prod.try_push(2).unwrap();
    for (i, v) in (0..2).zip(cons.pop_iter()) {
        assert_eq!(i, v);
    }

    prod.try_push(3).unwrap();
    prod.try_push(4).unwrap();
    prod.try_push(5).unwrap();
    for (i, v) in (2..5).zip(cons.pop_iter()) {
        assert_eq!(i, v);
    }
    assert_eq!(cons.try_pop().unwrap(), 5);
    assert!(prod.is_empty());
}
