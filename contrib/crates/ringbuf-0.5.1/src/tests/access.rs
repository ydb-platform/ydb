use super::Rb;
use crate::{storage::Array, traits::*};
use core::mem::MaybeUninit;

#[test]
fn try_push() {
    let mut rb = Rb::<Array<i32, 3>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    let vs_20 = (123, 456);
    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 3);
        assert_eq!(right.len(), 0);
        left[0] = MaybeUninit::new(vs_20.0);
        left[1] = MaybeUninit::new(vs_20.1);
        unsafe { prod.advance_write_index(2) };
    }
    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 0);
    }
    assert_eq!(cons.try_pop().unwrap(), vs_20.0);
    assert_eq!(cons.try_pop().unwrap(), vs_20.1);
    assert_eq!(cons.try_pop(), None);

    let vs_11 = (123, 456);
    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 2);
        left[0] = MaybeUninit::new(vs_11.0);
        right[0] = MaybeUninit::new(vs_11.1);
        unsafe { prod.advance_write_index(2) };
    }
    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 0);
    }
    assert_eq!(cons.try_pop().unwrap(), vs_11.0);
    assert_eq!(cons.try_pop().unwrap(), vs_11.1);
    assert_eq!(cons.try_pop(), None);
}

#[test]
fn pop_full() {
    const CAP: usize = 2;
    let mut rb = Rb::<Array<i32, CAP>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    for i in 0..CAP {
        prod.try_push(i as i32).unwrap();
    }
    assert_eq!(prod.try_push(0), Err(0));

    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), CAP);
        assert_eq!(right.len(), 0);
        for (i, x) in left.iter().enumerate() {
            assert_eq!(unsafe { x.assume_init() }, i as i32);
        }
        unsafe { cons.advance_read_index(CAP) };
    }

    assert_eq!(cons.occupied_len(), 0);
    assert_eq!(cons.try_pop(), None);
}

#[test]
fn pop_empty() {
    let mut rb = Rb::<Array<i32, 2>>::default();
    let (_, cons) = rb.split_ref();

    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 0);
        assert_eq!(right.len(), 0);
        unsafe { cons.advance_read_index(0) };
    }
}

#[test]
fn try_pop() {
    let mut rb = Rb::<Array<i32, 3>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    let vs_20 = (123, 456, 789);
    assert_eq!(prod.try_push(vs_20.0), Ok(()));
    assert_eq!(prod.try_push(vs_20.1), Ok(()));
    assert_eq!(prod.try_push(vs_20.2), Ok(()));
    assert_eq!(prod.try_push(0), Err(0));
    assert_eq!(prod.occupied_len(), 3);
    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 3);
        assert_eq!(right.len(), 0);
        assert_eq!(unsafe { left[0].assume_init() }, vs_20.0);
        assert_eq!(unsafe { left[1].assume_init() }, vs_20.1);
        assert_eq!(unsafe { left[2].assume_init() }, vs_20.2);
        unsafe { cons.advance_read_index(2) };
    }
    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 0);
    }
    assert_eq!(prod.occupied_len(), 1);

    let vs_11 = (654, 321);
    assert_eq!(prod.try_push(vs_11.0), Ok(()));
    assert_eq!(prod.try_push(vs_11.1), Ok(()));
    assert_eq!(prod.try_push(0), Err(0));
    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 2);
        assert_eq!(unsafe { left[0].assume_init() }, vs_20.2);
        assert_eq!(unsafe { right[0].assume_init() }, vs_11.0);
        assert_eq!(unsafe { right[1].assume_init() }, vs_11.1);
        unsafe { cons.advance_read_index(2) };
    }
    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 0);
    }
    assert_eq!(prod.occupied_len(), 1);
    assert_eq!(cons.try_pop(), Some(vs_11.1));
}

#[test]
fn push_return() {
    let mut rb = Rb::<Array<i32, 2>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 2);
        assert_eq!(right.len(), 0);
        unsafe { prod.advance_write_index(0) };
    }

    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 2);
        assert_eq!(right.len(), 0);
        left[0] = MaybeUninit::new(12);
        unsafe { prod.advance_write_index(1) };
    }

    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 0);
        left[0] = MaybeUninit::new(34);
        unsafe { prod.advance_write_index(1) };
    }

    assert_eq!(cons.try_pop().unwrap(), 12);
    assert_eq!(cons.try_pop().unwrap(), 34);
    assert_eq!(cons.try_pop(), None);
}

#[test]
fn pop_return() {
    let mut rb = Rb::<Array<i32, 2>>::default();
    let (mut prod, cons) = rb.split_ref();

    assert_eq!(prod.try_push(12), Ok(()));
    assert_eq!(prod.try_push(34), Ok(()));
    assert_eq!(prod.try_push(0), Err(0));

    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 2);
        assert_eq!(right.len(), 0);
        unsafe { cons.advance_read_index(0) };
    }

    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 2);
        assert_eq!(right.len(), 0);
        assert_eq!(unsafe { left[0].assume_init() }, 12);
        unsafe { cons.advance_read_index(1) };
    }

    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 0);
        assert_eq!(unsafe { left[0].assume_init() }, 34);
        unsafe { cons.advance_read_index(1) };
    }

    assert_eq!(prod.occupied_len(), 0);
}

#[test]
fn push_pop() {
    let mut rb = Rb::<Array<i32, 3>>::default();
    let (mut prod, cons) = rb.split_ref();

    let vs_20 = (123, 456);
    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 3);
        assert_eq!(right.len(), 0);
        left[0] = MaybeUninit::new(vs_20.0);
        left[1] = MaybeUninit::new(vs_20.1);
        unsafe { prod.advance_write_index(2) };
    }
    assert_eq!(prod.occupied_len(), 2);
    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 2);
        assert_eq!(right.len(), 0);
        assert_eq!(unsafe { left[0].assume_init() }, vs_20.0);
        assert_eq!(unsafe { left[1].assume_init() }, vs_20.1);
        unsafe { cons.advance_read_index(2) };
    }
    assert_eq!(prod.occupied_len(), 0);

    let vs_11 = (123, 456);
    {
        let (left, right) = prod.vacant_slices_mut();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 2);
        left[0] = MaybeUninit::new(vs_11.0);
        right[0] = MaybeUninit::new(vs_11.1);
        unsafe { prod.advance_write_index(2) };
    }
    assert_eq!(prod.occupied_len(), 2);
    {
        let (left, right) = cons.occupied_slices();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        assert_eq!(unsafe { left[0].assume_init() }, vs_11.0);
        assert_eq!(unsafe { right[0].assume_init() }, vs_11.1);
        unsafe { cons.advance_read_index(2) };
    }
    assert_eq!(prod.occupied_len(), 0);
}
