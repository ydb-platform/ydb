//! Various utilities for working with Cairo image surfaces.

use std::alloc;
use std::slice;

pub mod iterators;
pub mod shared_surface;
pub mod srgb;

// These two are for Cairo's platform-endian 0xaarrggbb pixels

#[cfg(target_endian = "little")]
use rgb::alt::BGRA8;
#[cfg(target_endian = "little")]
#[allow(clippy::upper_case_acronyms)]
pub type CairoARGB = BGRA8;

#[cfg(target_endian = "big")]
use rgb::alt::ARGB8;
#[cfg(target_endian = "big")]
#[allow(clippy::upper_case_acronyms)]
pub type CairoARGB = ARGB8;

use rgb::ColorComponentMap;

/// Analogous to `rgb::FromSlice`, to convert from `[T]` to `[CairoARGB]`
#[allow(clippy::upper_case_acronyms)]
pub trait AsCairoARGB {
    /// Reinterpret slice as `CairoARGB` pixels.
    fn as_cairo_argb(&self) -> &[CairoARGB];

    /// Reinterpret mutable slice as `CairoARGB` pixels.
    fn as_cairo_argb_mut(&mut self) -> &mut [CairoARGB];
}

// SAFETY: transmuting from u32 to CairoRGB is based on the following assumptions:
//  * there are no invalid bit representations for ARGB
//  * u32 and ARGB are the same size
//  * u32 is sufficiently aligned
impl AsCairoARGB for [u32] {
    fn as_cairo_argb(&self) -> &[CairoARGB] {
        const LAYOUT_U32: alloc::Layout = alloc::Layout::new::<u32>();
        const LAYOUT_ARGB: alloc::Layout = alloc::Layout::new::<CairoARGB>();
        let _: [(); LAYOUT_U32.size()] = [(); LAYOUT_ARGB.size()];
        let _: [(); 0] = [(); LAYOUT_U32.align() % LAYOUT_ARGB.align()];
        unsafe { slice::from_raw_parts(self.as_ptr() as *const _, self.len()) }
    }

    fn as_cairo_argb_mut(&mut self) -> &mut [CairoARGB] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr() as *mut _, self.len()) }
    }
}

/// Modes which specify how the values of out of bounds pixels are computed.
///
/// <https://www.w3.org/TR/filter-effects/#element-attrdef-fegaussianblur-edgemode>
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EdgeMode {
    /// The nearest inbounds pixel value is returned.
    Duplicate,
    /// The image is extended by taking the color values from the opposite of the image.
    ///
    /// Imagine the image being tiled infinitely, with the original image at the origin.
    Wrap,
    /// Zero RGBA values are returned.
    None,
}

/// Trait to convert pixels in various formats to our own Pixel layout.
pub trait ToPixel {
    fn to_pixel(&self) -> Pixel;
}

/// Trait to convert pixels in various formats to Cairo's endian-dependent 0xaarrggbb.
pub trait ToCairoARGB {
    fn to_cairo_argb(&self) -> CairoARGB;
}

impl ToPixel for CairoARGB {
    #[inline]
    fn to_pixel(&self) -> Pixel {
        Pixel {
            r: self.r,
            g: self.g,
            b: self.b,
            a: self.a,
        }
    }
}

impl ToPixel for image::Rgba<u8> {
    #[inline]
    fn to_pixel(&self) -> Pixel {
        Pixel {
            r: self.0[0],
            g: self.0[1],
            b: self.0[2],
            a: self.0[3],
        }
    }
}

impl ToCairoARGB for Pixel {
    #[inline]
    fn to_cairo_argb(&self) -> CairoARGB {
        CairoARGB {
            r: self.r,
            g: self.g,
            b: self.b,
            a: self.a,
        }
    }
}

/// Extension methods for `cairo::ImageSurfaceData`.
pub trait ImageSurfaceDataExt {
    /// Sets the pixel at the given coordinates. Assumes the `ARgb32` format.
    fn set_pixel(&mut self, stride: usize, pixel: Pixel, x: u32, y: u32);
}

/// A pixel consisting of R, G, B and A values.
pub type Pixel = rgb::RGBA8;

pub trait PixelOps {
    fn premultiply(self) -> Self;
    fn unpremultiply(self) -> Self;
    fn diff(&self, other: &Self) -> Self;
    fn to_luminance_mask(&self) -> Self;
    fn to_u32(&self) -> u32;
    fn from_u32(x: u32) -> Self;
}

impl PixelOps for Pixel {
    /// Returns an unpremultiplied value of this pixel.
    ///
    /// For a fully transparent pixel, a transparent black pixel will be returned.
    #[inline]
    fn unpremultiply(self) -> Self {
        if self.a == 0 {
            Self {
                r: 0,
                g: 0,
                b: 0,
                a: 0,
            }
        } else {
            let alpha = f32::from(self.a) / 255.0;
            self.map_colors(|x| ((f32::from(x) / alpha) + 0.5) as u8)
        }
    }

    /// Returns a premultiplied value of this pixel.
    #[inline]
    fn premultiply(self) -> Self {
        let a = self.a as u32;
        self.map_colors(|x| (((x as u32) * a + 127) / 255) as u8)
    }

    #[inline]
    fn diff(&self, other: &Pixel) -> Pixel {
        self.iter()
            .zip(other.iter())
            .map(|(l, r)| (l as i32 - r as i32).unsigned_abs() as u8)
            .collect()
    }

    /// Returns a 'mask' pixel with only the alpha channel
    ///
    /// Assuming, the pixel is linear RGB (not sRGB)
    /// y = luminance
    /// Y = 0.2126 R + 0.7152 G + 0.0722 B
    /// 1.0 opacity = 255
    ///
    /// When Y = 1.0, pixel for mask should be 0xFFFFFFFF
    /// (you get 1.0 luminance from 255 from R, G and B)
    ///
    /// r_mult = 0xFFFFFFFF / (255.0 * 255.0) * .2126 = 14042.45  ~= 14042
    /// g_mult = 0xFFFFFFFF / (255.0 * 255.0) * .7152 = 47239.69  ~= 47240
    /// b_mult = 0xFFFFFFFF / (255.0 * 255.0) * .0722 =  4768.88  ~= 4769
    ///
    /// This allows for the following expected behaviour:
    ///    (we only care about the most significant byte)
    /// if pixel = 0x00FFFFFF, pixel' = 0xFF......
    /// if pixel = 0x00020202, pixel' = 0x02......
    ///
    /// if pixel = 0x00000000, pixel' = 0x00......
    #[inline]
    fn to_luminance_mask(&self) -> Self {
        let r = u32::from(self.r);
        let g = u32::from(self.g);
        let b = u32::from(self.b);

        Self {
            r: 0,
            g: 0,
            b: 0,
            a: (((r * 14042 + g * 47240 + b * 4769) * 255) >> 24) as u8,
        }
    }

    /// Returns the pixel value as a `u32`, in the same format as `cairo::Format::ARgb32`.
    #[inline]
    fn to_u32(&self) -> u32 {
        (u32::from(self.a) << 24)
            | (u32::from(self.r) << 16)
            | (u32::from(self.g) << 8)
            | u32::from(self.b)
    }

    /// Converts a `u32` in the same format as `cairo::Format::ARgb32` into a `Pixel`.
    #[inline]
    fn from_u32(x: u32) -> Self {
        Self {
            r: ((x >> 16) & 0xFF) as u8,
            g: ((x >> 8) & 0xFF) as u8,
            b: (x & 0xFF) as u8,
            a: ((x >> 24) & 0xFF) as u8,
        }
    }
}

impl<'a> ImageSurfaceDataExt for cairo::ImageSurfaceData<'a> {
    #[inline]
    fn set_pixel(&mut self, stride: usize, pixel: Pixel, x: u32, y: u32) {
        let this: &mut [u8] = &mut *self;
        // SAFETY: this code assumes that cairo image surface data is correctly
        // aligned for u32. This assumption is justified by the Cairo docs,
        // which say this:
        //
        // https://cairographics.org/manual/cairo-Image-Surfaces.html#cairo-image-surface-create-for-data
        //
        // > This pointer must be suitably aligned for any kind of variable,
        // > (for example, a pointer returned by malloc).
        #[allow(clippy::cast_ptr_alignment)]
        let this: &mut [u32] =
            unsafe { slice::from_raw_parts_mut(this.as_mut_ptr() as *mut u32, this.len() / 4) };
        this.set_pixel(stride, pixel, x, y);
    }
}
impl ImageSurfaceDataExt for [u8] {
    #[inline]
    fn set_pixel(&mut self, stride: usize, pixel: Pixel, x: u32, y: u32) {
        let this = &mut self[y as usize * stride + x as usize * 4..];
        this[..4].copy_from_slice(&pixel.to_u32().to_ne_bytes());
    }
}
impl ImageSurfaceDataExt for [u32] {
    #[inline]
    fn set_pixel(&mut self, stride: usize, pixel: Pixel, x: u32, y: u32) {
        self[(y as usize * stride + x as usize * 4) / 4] = pixel.to_u32();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn pixel_diff() {
        let a = Pixel::new(0x10, 0x20, 0xf0, 0x40);
        assert_eq!(a, a.diff(&Pixel::default()));
        let b = Pixel::new(0x50, 0xff, 0x20, 0x10);
        assert_eq!(a.diff(&b), Pixel::new(0x40, 0xdf, 0xd0, 0x30));
    }

    // Floating-point reference implementation
    fn premultiply_float(pixel: Pixel) -> Pixel {
        let alpha = f64::from(pixel.a) / 255.0;
        pixel.map_colors(|x| ((f64::from(x) * alpha) + 0.5) as u8)
    }

    prop_compose! {
        fn arbitrary_pixel()(a: u8, r: u8, g: u8, b: u8) -> Pixel {
            Pixel { r, g, b, a }
        }
    }

    proptest! {
        #[test]
        fn pixel_premultiply(pixel in arbitrary_pixel()) {
            prop_assert_eq!(pixel.premultiply(), premultiply_float(pixel));
        }

        #[test]
        fn pixel_unpremultiply(pixel in arbitrary_pixel()) {
            let roundtrip = pixel.premultiply().unpremultiply();
            if pixel.a == 0 {
                prop_assert_eq!(roundtrip, Pixel::default());
            } else {
                // roundtrip can't be perfect, the accepted error depends on alpha
                let tolerance = 0xff / pixel.a;
                let diff = roundtrip.diff(&pixel);
                prop_assert!(diff.r <= tolerance, "red component value differs by more than {}: {:?}", tolerance, roundtrip);
                prop_assert!(diff.g <= tolerance, "green component value differs by more than {}: {:?}", tolerance, roundtrip);
                prop_assert!(diff.b <= tolerance, "blue component value differs by more than {}: {:?}", tolerance, roundtrip);

                prop_assert_eq!(pixel.a, roundtrip.a);
            }
       }
    }
}
