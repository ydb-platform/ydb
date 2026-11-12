use std::fmt;

use crate::surface_utils::{
    ImageSurfaceDataExt, Pixel, PixelOps,
    iterators::Pixels,
    shared_surface::{SharedImageSurface, SurfaceType},
};

use rgb::{ColorComponentMap, ComponentMap, RGB};

pub enum BufferDiff {
    DifferentSizes,
    Diff(Diff),
}

pub struct Diff {
    pub num_pixels_changed: usize,
    pub max_diff: u8,
    pub surface: SharedImageSurface,
}

impl fmt::Display for BufferDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BufferDiff::DifferentSizes => write!(f, "different sizes"),
            BufferDiff::Diff(diff) => diff.fmt(f),
        }
    }
}

impl fmt::Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} pixels are different, with a maximum difference of {}",
            self.num_pixels_changed, self.max_diff
        )
    }
}

#[inline]
fn emphasize(p: &Pixel) -> Pixel {
    let emphasize_component = |c| {
        // emphasize
        let mut c = c as u32 * 4;
        // make sure it's visible
        if c > 0 {
            c += 128;
        }
        c.min(255) as u8
    };
    p.map(emphasize_component)
}

pub fn compare_surfaces(
    surf_a: &SharedImageSurface,
    surf_b: &SharedImageSurface,
) -> Result<BufferDiff, cairo::Error> {
    let a_width = surf_a.width();
    let a_height = surf_a.height();

    let b_width = surf_b.width();
    let b_height = surf_b.height();

    if a_width != b_width || a_height != b_height {
        return Ok(BufferDiff::DifferentSizes);
    }

    let mut surf_diff = cairo::ImageSurface::create(cairo::Format::ARgb32, a_width, a_height)?;
    let diff_stride = surf_diff.stride() as usize;

    let mut num_pixels_changed = 0;
    let mut max_diff = 0;

    let black = Pixel::default().with_alpha(255);

    {
        let mut diff_data = surf_diff.data().unwrap();

        for ((xa, ya, pixel_a), (_, _, pixel_b)) in Pixels::new(surf_a).zip(Pixels::new(surf_b)) {
            let dest = if pixel_a != pixel_b {
                num_pixels_changed += 1;

                let pixel_diff = pixel_a.diff(&pixel_b);

                max_diff = pixel_diff.iter().fold(max_diff, |acc, c| acc.max(c));

                let pixel_diff = emphasize(&pixel_diff);

                if pixel_diff.rgb() == RGB::default() {
                    // alpha only difference; convert alpha to gray
                    let a = pixel_diff.a;
                    pixel_diff.map_colors(|_| a)
                } else {
                    pixel_diff.with_alpha(255)
                }
            } else {
                black
            };

            diff_data.set_pixel(diff_stride, dest, xa, ya);
        }
    }

    let surface = SharedImageSurface::wrap(surf_diff, SurfaceType::SRgb)?;

    Ok(BufferDiff::Diff(Diff {
        num_pixels_changed,
        max_diff,
        surface,
    }))
}
