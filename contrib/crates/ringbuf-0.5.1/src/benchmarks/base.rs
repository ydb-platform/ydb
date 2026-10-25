use crate::{storage::Array, traits::*, LocalRb, SharedRb};
use test::{black_box, Bencher};

const RB_SIZE: usize = 256;
const BATCH_SIZE: usize = 100;

#[bench]
fn push_pop_shared(b: &mut Bencher) {
    let buf = SharedRb::<Array<u64, RB_SIZE>>::default();
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[1; RB_SIZE / 2]);
    b.iter(|| {
        prod.try_push(1).unwrap();
        black_box(cons.try_pop().unwrap());
    });
}

#[bench]
fn push_pop_local(b: &mut Bencher) {
    let buf = LocalRb::<Array<u64, RB_SIZE>>::default();
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[1; RB_SIZE / 2]);
    b.iter(|| {
        prod.try_push(1).unwrap();
        black_box(cons.try_pop().unwrap());
    });
}

#[bench]
fn push_pop_x100(b: &mut Bencher) {
    let buf = SharedRb::<Array<u64, RB_SIZE>>::default();
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[1; RB_SIZE / 2]);
    b.iter(|| {
        for _ in 0..BATCH_SIZE {
            prod.try_push(1).unwrap();
        }
        for _ in 0..BATCH_SIZE {
            black_box(cons.try_pop().unwrap());
        }
    });
}
