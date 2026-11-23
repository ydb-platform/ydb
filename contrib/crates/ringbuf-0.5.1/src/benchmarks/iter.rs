use crate::{traits::*, HeapRb};
use test::{black_box, Bencher};

const RB_SIZE: usize = 1024;

#[bench]
fn push_iter_x1000(b: &mut Bencher) {
    let buf = HeapRb::<i32>::new(RB_SIZE);
    let (mut prod, mut cons) = buf.split();

    prod.push_slice(&[0; RB_SIZE / 2]);
    cons.skip(RB_SIZE / 2);

    b.iter(|| {
        prod.push_iter(0..1000);
        black_box(cons.as_slices());
        unsafe { cons.advance_read_index(1000) };
    });
}

#[bench]
fn pop_iter_x1000(b: &mut Bencher) {
    let buf = HeapRb::<i32>::new(RB_SIZE);
    let (mut prod, mut cons) = buf.split();

    prod.push_slice(&[0; RB_SIZE / 2]);
    cons.skip(RB_SIZE / 2);
    prod.push_slice(&[1; 1000]);

    b.iter(|| {
        for x in cons.pop_iter() {
            black_box(x);
        }
        unsafe { prod.advance_write_index(1000) };
    });
}
