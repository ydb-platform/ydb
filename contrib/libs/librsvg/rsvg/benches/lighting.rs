use criterion::{Criterion, criterion_group, criterion_main};
use nalgebra::{Matrix3, Vector2};

use rsvg::bench_only::Normal;

use rsvg::bench_only::{EdgeMode, IRect, PixelRectangle, Pixels, SharedImageSurface, SurfaceType};

/// Computes and returns the normal vector for the light filters.
fn normal(surface: &SharedImageSurface, bounds: IRect, x: u32, y: u32) -> Normal {
    assert!(x as i32 >= bounds.x0);
    assert!(y as i32 >= bounds.y0);
    assert!((x as i32) < bounds.x1);
    assert!((y as i32) < bounds.y1);

    // Get the correct sobel kernel and factor for the pixel position.
    // Performance note: it's possible to replace the matrices with normal arrays.
    #[rustfmt::skip]
    let (factor_x, kx, factor_y, ky) = match (x as i32, y as i32) {
        (x, y) if (x, y) == (bounds.x0, bounds.y0) => (
            2. / 3.,
            Matrix3::new(
                0,  0, 0,
                0, -2, 2,
                0, -1, 1,
            ),
            2. / 3.,
            Matrix3::new(
                0,  0,  0,
                0, -2, -1,
                0,  2,  1,
            ),
        ),
        (x, y) if (x + 1, y) == (bounds.x1, bounds.y0) => (
            2. / 3.,
            Matrix3::new(
                0,  0, 0,
                -2, 2, 0,
                -1, 1, 0,
            ),
            2. / 3.,
            Matrix3::new(
                 0,  0, 0,
                -1, -2, 0,
                 1,  2, 0,
            ),
        ),
        (x, y) if (x, y + 1) == (bounds.x0, bounds.y1) => (
            2. / 3.,
            Matrix3::new(
                0, -1, 1,
                0, -2, 2,
                0,  0, 0,
            ),
            2. / 3.,
            Matrix3::new(
                0, -2, -1,
                0,  2,  1,
                0,  0,  0,
            ),
        ),
        (x, y) if (x + 1, y + 1) == (bounds.x1, bounds.y1) => (
            2. / 3.,
            Matrix3::new(
                -1, 1, 0,
                -2, 2, 0,
                 0, 0, 0,
            ),
            2. / 3.,
            Matrix3::new(
                -1, -2, 0,
                 1,  2, 0,
                 0,  0, 0,
            ),
        ),
        (_, y) if y == bounds.y0 => (
            1. / 3.,
            Matrix3::new(
                 0, 0, 0,
                -2, 0, 2,
                -1, 0, 1,
            ),
            1. / 2.,
            Matrix3::new(
                 0,  0,  0,
                -1, -2, -1,
                 1,  2,  1,
            ),
        ),
        (x, _) if x == bounds.x0 => (
            1. / 2.,
            Matrix3::new(
                0, -1, 1,
                0, -2, 2,
                0, -1, 1,
            ),
            1. / 3.,
            Matrix3::new(
                0, -2, -1,
                0,  0,  0,
                0,  2,  1,
            ),
        ),
        (x, _) if x + 1 == bounds.x1 => (
            1. / 2.,
            Matrix3::new(
                -1, 1, 0,
                -2, 2, 0,
                -1, 1, 0,
            ),
            1. / 3.,
            Matrix3::new(
                -1, -2, 0,
                 0,  0, 0,
                 1,  2, 0,
            ),
        ),
        (_, y) if y + 1 == bounds.y1 => (
            1. / 3.,
            Matrix3::new(
                -1, 0, 1,
                -2, 0, 2,
                 0, 0, 0,
            ),
            1. / 2.,
            Matrix3::new(
                -1, -2, -1,
                 1,  2,  1,
                 0,  0,  0,
            ),
        ),
        _ => (
            1. / 4.,
            Matrix3::new(
                -1, 0, 1,
                -2, 0, 2,
                -1, 0, 1,
            ),
            1. / 4.,
            Matrix3::new(
                -1, -2, -1,
                 0,  0,  0,
                 1,  2,  1,
            ),
        ),
    };

    let kernel_bounds = IRect::new(x as i32 - 1, y as i32 - 1, x as i32 + 2, y as i32 + 2);

    let mut nx = 0;
    let mut ny = 0;
    for (x, y, pixel) in PixelRectangle::within(surface, bounds, kernel_bounds, EdgeMode::None) {
        let kernel_x = (x - kernel_bounds.x0) as usize;
        let kernel_y = (y - kernel_bounds.y0) as usize;

        nx += i16::from(pixel.a) * kx[(kernel_y, kernel_x)];
        ny += i16::from(pixel.a) * ky[(kernel_y, kernel_x)];
    }

    // Negative nx and ny to account for the different coordinate system.
    Normal {
        factor: Vector2::new(factor_x, factor_y),
        normal: Vector2::new(-nx, -ny),
    }
}

const SURFACE_SIDE: i32 = 512;
const BOUNDS: IRect = IRect {
    x0: 64,
    y0: 64,
    x1: 64 + 64,
    y1: 64 + 64,
};

fn bench_normal(c: &mut Criterion) {
    c.bench_function("normal", |b| {
        let surface =
            SharedImageSurface::empty(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();

        b.iter(|| {
            let mut z = 0;
            for (x, y, _pixel) in Pixels::within(&surface, BOUNDS) {
                let n = normal(&surface, BOUNDS, x, y);
                z += n.normal.x;
            }
            z
        })
    });

    c.bench_function("normal unrolled", |b| {
        let surface =
            SharedImageSurface::empty(SURFACE_SIDE, SURFACE_SIDE, SurfaceType::SRgb).unwrap();

        b.iter(|| {
            let mut z = 0;

            // Top left.
            {
                let n = Normal::top_left(&surface, BOUNDS);
                z += n.normal.x;
            }

            // Top right.
            {
                let n = Normal::top_right(&surface, BOUNDS);
                z += n.normal.x;
            }

            // Bottom left.
            {
                let n = Normal::bottom_left(&surface, BOUNDS);
                z += n.normal.x;
            }

            // Bottom right.
            {
                let n = Normal::bottom_right(&surface, BOUNDS);
                z += n.normal.x;
            }

            // Top row.
            for x in BOUNDS.x0 as u32 + 1..BOUNDS.x1 as u32 - 1 {
                let n = Normal::top_row(&surface, BOUNDS, x);
                z += n.normal.x;
            }

            // Bottom row.
            for x in BOUNDS.x0 as u32 + 1..BOUNDS.x1 as u32 - 1 {
                let n = Normal::bottom_row(&surface, BOUNDS, x);
                z += n.normal.x;
            }

            // Left column.
            for y in BOUNDS.y0 as u32 + 1..BOUNDS.y1 as u32 - 1 {
                let n = Normal::left_column(&surface, BOUNDS, y);
                z += n.normal.x;
            }

            // Right column.
            for y in BOUNDS.y0 as u32 + 1..BOUNDS.y1 as u32 - 1 {
                let n = Normal::right_column(&surface, BOUNDS, y);
                z += n.normal.x;
            }

            // Interior pixels.
            for y in BOUNDS.y0 as u32 + 1..BOUNDS.y1 as u32 - 1 {
                for x in BOUNDS.x0 as u32 + 1..BOUNDS.x1 as u32 - 1 {
                    let n = Normal::interior(&surface, BOUNDS, x, y);
                    z += n.normal.x;
                }
            }

            z
        })
    });
}

criterion_group!(benches, bench_normal);
criterion_main!(benches);
