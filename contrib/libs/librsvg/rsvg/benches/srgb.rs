use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use rsvg::bench_only::{
    ExclusiveImageSurface, IRect, ImageSurfaceDataExt, Pixel, SurfaceType, linearize,
    map_unpremultiplied_components_loop,
};

const SURFACE_SIDE: i32 = 512;
const BOUNDS: IRect = IRect {
    x0: 64,
    y0: 32,
    x1: 448,
    y1: 480,
};

fn bench_srgb_linearization(c: &mut Criterion) {
    c.bench_function("srgb map_unpremultiplied_components", |b| {
        let mut surface =
            ExclusiveImageSurface::new(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::LinearRgb).unwrap();

        // Fill the surface with non-zero alpha (otherwise linearization is a no-op).
        surface.modify(&mut |data, stride| {
            for y in BOUNDS.y_range() {
                for x in BOUNDS.x_range() {
                    let pixel = Pixel {
                        r: 0,
                        g: 0,
                        b: 0,
                        a: 127,
                    };

                    data.set_pixel(stride, pixel, x as u32, y as u32);
                }
            }
        });

        let surface = surface.share().unwrap();
        let mut output_surface =
            ExclusiveImageSurface::new(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();
        let bounds = black_box(BOUNDS);

        b.iter(|| {
            map_unpremultiplied_components_loop(&surface, &mut output_surface, bounds, linearize);
        })
    });
}

criterion_group!(benches, bench_srgb_linearization);
criterion_main!(benches);
