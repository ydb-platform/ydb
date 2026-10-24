use super::Rb;
use crate::{storage::Array, traits::*};
use alloc::rc::Rc;

#[test]
fn skip() {
    // Initialize ringbuffer, prod and cons
    let mut rb = Rb::<Array<i8, 10>>::default();
    let (mut prod, mut cons) = rb.split_ref();
    let mut i = 0;

    // Fill the buffer
    for _ in 0..10 {
        prod.try_push(i).unwrap();
        i += 1;
    }

    // Pop in the middle of the buffer
    assert_eq!(cons.skip(5), 5);

    // Make sure changes are taken into account
    assert_eq!(cons.try_pop().unwrap(), 5);

    // Fill the buffer again
    for _ in 0..5 {
        prod.try_push(i).unwrap();
        i += 1;
    }

    assert_eq!(cons.skip(6), 6);
    assert_eq!(cons.try_pop().unwrap(), 12);

    // Fill the buffer again
    for _ in 0..7 {
        prod.try_push(i).unwrap();
        i += 1;
    }

    // Ask too much, delete the max number of items
    assert_eq!(cons.skip(10), 9);

    // Try to remove more than possible
    assert_eq!(cons.skip(1), 0);

    // Make sure it is still usable
    assert_eq!(cons.try_pop(), None);
    assert_eq!(prod.try_push(0), Ok(()));
    assert_eq!(cons.try_pop(), Some(0));
}

#[test]
fn skip_drop() {
    let rc = Rc::<()>::new(());

    const CAP: usize = 10;
    let mut rb = Rb::<Array<Rc<()>, CAP>>::default();
    let (mut prod, mut cons) = rb.split_ref();

    for _ in 0..CAP {
        prod.try_push(rc.clone()).unwrap();
    }

    assert_eq!(cons.occupied_len(), CAP);
    assert_eq!(Rc::strong_count(&rc), CAP + 1);

    assert_eq!(cons.skip(CAP), CAP);

    // Check ring buffer is empty
    assert_eq!(cons.occupied_len(), 0);
    // Check that items are dropped
    assert_eq!(Rc::strong_count(&rc), 1);
}
