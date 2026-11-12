use crate::{storage::Heap, traits::*, SharedRb};
use std::{cell::Cell, thread, thread::sleep, time::Duration, vec::Vec};

fn yield_() {
    sleep(Duration::from_millis(1));
}

#[test]
fn concurrent() {
    const MSG: &[u8] = b"The quick brown fox jumps over the lazy dog\0";
    let rb = SharedRb::<Heap<u8>>::new(4);
    let (mut prod, mut cons) = rb.split();

    let pjh = thread::spawn({
        let mut msg = MSG;
        move || {
            while !msg.is_empty() {
                prod.read_from(&mut msg, None).transpose().unwrap();
                yield_();
            }
        }
    });

    let cjh = thread::spawn(move || {
        let mut msg = Vec::new();
        while msg.last().copied() != Some(0) {
            cons.write_into(&mut msg, None).transpose().unwrap();
            yield_();
        }
        msg
    });

    pjh.join().unwrap();
    assert_eq!(cjh.join().unwrap(), MSG);
}

#[test]
fn non_sync() {
    const N: i32 = 256;

    let rb = SharedRb::<Heap<Cell<i32>>>::new(4);
    let (mut prod, mut cons) = rb.split();

    let pjh = thread::spawn({
        move || {
            for i in 0..N {
                while prod.try_push(Cell::new(i)).is_err() {
                    yield_();
                }
            }
        }
    });

    let cjh = thread::spawn(move || {
        for i in 0..N {
            assert_eq!(
                i,
                loop {
                    match cons.try_pop() {
                        Some(i) => break i.get(),
                        None => yield_(),
                    }
                }
            );
        }
    });

    pjh.join().unwrap();
    cjh.join().unwrap();
}
