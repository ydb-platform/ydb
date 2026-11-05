use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use rsvg::bench_only::{Pixel, PixelOps};

const OTHER: Pixel = Pixel {
    r: 0x10,
    g: 0x20,
    b: 0x30,
    a: 0x40,
};
const N: usize = 1024;

fn make_pixels(n: usize) -> Vec<Pixel> {
    (0..n)
        .map(|i| Pixel {
            r: (i / 2) as u8,
            g: (i / 3) as u8,
            b: (i / 4) as u8,
            a: i as u8,
        })
        .collect()
}

fn bench_op<F>(pixels: &[Pixel], op: F)
where
    F: Fn(&Pixel) -> Pixel,
{
    let result: Vec<Pixel> = pixels.iter().map(op).collect();
    black_box(result);
}

fn bench_pixel_ops(c: &mut Criterion) {
    c.bench_function("pixel_diff", |b| {
        let pixels = black_box(make_pixels(N));
        let other = black_box(OTHER);
        b.iter(|| bench_op(&pixels, |pixel| pixel.diff(&other)))
    });

    c.bench_function("pixel_to_luminance_mask", |b| {
        let pixels = black_box(make_pixels(N));
        b.iter(|| bench_op(&pixels, |pixel| pixel.to_luminance_mask()))
    });

    c.bench_function("pixel_premultiply", |b| {
        let pixels = black_box(make_pixels(N));
        b.iter(|| bench_op(&pixels, |pixel| pixel.premultiply()))
    });

    c.bench_function("pixel_unpremultiply", |b| {
        let pixels = black_box(make_pixels(N));
        b.iter(|| bench_op(&pixels, |pixel| pixel.unpremultiply()))
    });
}

criterion_group!(benches, bench_pixel_ops);
criterion_main!(benches);
