use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use rsvg::bench_only::{
    ExclusiveImageSurface, IRect, SharedImageSurface, SurfaceType, composite_arithmetic,
};

const SURFACE_SIDE: i32 = 512;
const BOUNDS: IRect = IRect {
    x0: 64,
    y0: 64,
    x1: 64 + 64,
    y1: 64 + 64,
};

fn bench_composite(c: &mut Criterion) {
    c.bench_function("composite arithmetic", |b| {
        let input_surface =
            SharedImageSurface::empty(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();
        let input_2_surface =
            SharedImageSurface::empty(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();

        let mut output_surface =
            ExclusiveImageSurface::new(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();

        let bounds = black_box(BOUNDS);

        b.iter(|| {
            composite_arithmetic(
                &input_surface,
                &input_2_surface,
                &mut output_surface,
                bounds,
                0.5,
                0.5,
                0.5,
                0.5,
            );
        })
    });
}

criterion_group!(benches, bench_composite);
criterion_main!(benches);
