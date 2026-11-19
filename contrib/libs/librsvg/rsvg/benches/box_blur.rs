use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use rsvg::bench_only::{
    AlphaOnly, Horizontal, IRect, NotAlphaOnly, SharedImageSurface, SurfaceType, Vertical,
};

const SURFACE_SIDE: i32 = 512;
const BOUNDS: IRect = IRect {
    x0: 64,
    y0: 64,
    x1: 64 + 64,
    y1: 64 + 64,
};

fn bench_box_blur(c: &mut Criterion) {
    let mut group = c.benchmark_group("box_blur 9");

    for input in [(false, false), (false, true), (true, false), (true, true)].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", input)),
            &input,
            |b, &(vertical, alpha_only)| {
                let surface_type = if *alpha_only {
                    SurfaceType::AlphaOnly
                } else {
                    SurfaceType::SRgb
                };
                let input_surface =
                    SharedImageSurface::empty(SURFACE_SIDE, SURFACE_SIDE, surface_type).unwrap();

                let mut output_surface =
                    cairo::ImageSurface::create(cairo::Format::ARgb32, SURFACE_SIDE, SURFACE_SIDE)
                        .unwrap();
                const KERNEL_SIZE: usize = 9;

                let f = match (vertical, alpha_only) {
                    (true, true) => SharedImageSurface::box_blur_loop::<Vertical, AlphaOnly>,
                    (true, false) => SharedImageSurface::box_blur_loop::<Vertical, NotAlphaOnly>,
                    (false, true) => SharedImageSurface::box_blur_loop::<Horizontal, AlphaOnly>,
                    (false, false) => SharedImageSurface::box_blur_loop::<Horizontal, NotAlphaOnly>,
                };

                b.iter(|| {
                    f(
                        &input_surface,
                        &mut output_surface,
                        BOUNDS,
                        KERNEL_SIZE,
                        KERNEL_SIZE / 2,
                    )
                })
            },
        );
    }
}

criterion_group!(benches, bench_box_blur);
criterion_main!(benches);
