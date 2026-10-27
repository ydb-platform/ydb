use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use rsvg::bench_only::{ExclusiveImageSurface, IRect, Pixels, SharedImageSurface, SurfaceType};

const SURFACE_SIDE: i32 = 512;
const BOUNDS: IRect = IRect {
    x0: 64,
    y0: 32,
    x1: 448,
    y1: 480,
};

fn bench_pixel_iterators(c: &mut Criterion) {
    c.bench_function("pixel_iterators straightforward", |b| {
        let mut surface =
            ExclusiveImageSurface::new(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();
        let stride = surface.stride() as i32;
        let data = surface.data();

        let bounds = black_box(BOUNDS);

        b.iter(|| {
            let mut r = 0usize;
            let mut g = 0usize;
            let mut b = 0usize;
            let mut a = 0usize;

            for y in bounds.y_range() {
                for x in bounds.x_range() {
                    let base = (y * stride + x * 4) as usize;

                    r += data[base] as usize;
                    g += data[base + 1] as usize;
                    b += data[base + 2] as usize;
                    a += data[base + 3] as usize;
                }
            }

            (r, g, b, a)
        })
    });

    c.bench_function("pixel_iterators get_pixel", |b| {
        let surface =
            SharedImageSurface::empty(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();

        let bounds = black_box(BOUNDS);

        b.iter(|| {
            let mut r = 0usize;
            let mut g = 0usize;
            let mut b = 0usize;
            let mut a = 0usize;

            for y in bounds.y_range() {
                for x in bounds.x_range() {
                    let pixel = surface.get_pixel(x as u32, y as u32);

                    r += pixel.r as usize;
                    g += pixel.g as usize;
                    b += pixel.b as usize;
                    a += pixel.a as usize;
                }
            }

            (r, g, b, a)
        })
    });

    c.bench_function("pixel_iterators pixels", |b| {
        let surface =
            SharedImageSurface::empty(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();

        let bounds = black_box(BOUNDS);

        b.iter(|| {
            let mut r = 0usize;
            let mut g = 0usize;
            let mut b = 0usize;
            let mut a = 0usize;

            for (_x, _y, pixel) in Pixels::within(&surface, bounds) {
                r += pixel.r as usize;
                g += pixel.g as usize;
                b += pixel.b as usize;
                a += pixel.a as usize;
            }

            (r, g, b, a)
        })
    });
}

criterion_group!(benches, bench_pixel_iterators);
criterion_main!(benches);
