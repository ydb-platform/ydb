use crate::{traits::*, HeapRb};
use test::{black_box, Bencher};

const RB_SIZE: usize = 1024;

#[bench]
fn slice_x10(b: &mut Bencher) {
    let buf = HeapRb::<u64>::new(RB_SIZE);
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[1; RB_SIZE / 2]);
    let mut data = [1; 10];
    b.iter(|| {
        prod.push_slice(&data);
        cons.pop_slice(&mut data);
        black_box(data);
    });
}

#[bench]
fn slice_x100(b: &mut Bencher) {
    let buf = HeapRb::<u64>::new(RB_SIZE);
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[1; RB_SIZE / 2]);
    let mut data = [1; 100];
    b.iter(|| {
        prod.push_slice(&data);
        cons.pop_slice(&mut data);
        black_box(data);
    });
}

#[bench]
fn slice_x1000(b: &mut Bencher) {
    let buf = HeapRb::<u64>::new(RB_SIZE);
    let (mut prod, mut cons) = buf.split();
    prod.push_slice(&[1; 12]);
    let mut data = [1; 1000];
    b.iter(|| {
        prod.push_slice(&data);
        cons.pop_slice(&mut data);
    });
    black_box(data);
}
