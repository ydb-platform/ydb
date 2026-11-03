use crate::{storage::Array, traits::*, SharedRb};
use test::{black_box, Bencher};

const RB_SIZE: usize = 256;

#[bench]
fn advance(b: &mut Bencher) {
    let buf = SharedRb::<Array<u64, RB_SIZE>>::default();
    let (mut prod, cons) = buf.split();
    prod.push_slice(&[1; RB_SIZE / 2]);
    b.iter(|| {
        unsafe { prod.advance_write_index(1) };
        unsafe { cons.advance_read_index(1) };
    });
}

#[bench]
fn get_occupied_slices(b: &mut Bencher) {
    let buf = SharedRb::<Array<u64, RB_SIZE>>::default();
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[0; 3 * RB_SIZE / 4]);
    cons.skip(RB_SIZE);
    prod.push_slice(&[1; RB_SIZE / 2]);
    b.iter(|| {
        black_box(unsafe { cons.occupied_slices_mut() });
        black_box(&mut cons);
    });
}

#[bench]
fn get_vacant_slices(b: &mut Bencher) {
    let buf = SharedRb::<Array<u64, RB_SIZE>>::default();
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[0; 1 * RB_SIZE / 4]);
    cons.skip(RB_SIZE);
    prod.push_slice(&[1; RB_SIZE / 2]);
    b.iter(|| {
        black_box(prod.vacant_slices_mut());
        black_box(&mut prod);
    });
}
