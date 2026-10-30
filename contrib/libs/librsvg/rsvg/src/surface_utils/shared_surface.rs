//! Shared access to Cairo image surfaces.
use std::cmp::min;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::slice;

use cast::i32;
use nalgebra::{Dim, Matrix, storage::Storage};

use crate::color::{Color, color_to_rgba};
use crate::drawing_ctx::set_source_color_on_cairo;
use crate::error::*;
use crate::rect::{IRect, Rect};
use crate::surface_utils::srgb;
use crate::util::clamp;

use super::{
    AsCairoARGB, CairoARGB, EdgeMode, ImageSurfaceDataExt, Pixel, PixelOps, ToCairoARGB, ToPixel,
    iterators::{PixelRectangle, Pixels},
};

/// Interpolation when scaling images.
///
/// This is meant to be translated from the `ImageRendering` property.  We don't use
/// `ImageRendering` directly here, because this module is supposed to be lower-level
/// than the main part of librsvg.  Here, we take `Interpolation` and translate it
/// to Cairo's own values for pattern filtering.
///
/// This enum can be expanded to use more of Cairo's filtering modes.
pub enum Interpolation {
    Nearest,
    Smooth,
}

impl From<Interpolation> for cairo::Filter {
    fn from(i: Interpolation) -> cairo::Filter {
        // Cairo's default for interpolation is CAIRO_FILTER_GOOD.  This happens in Cairo's internals, as
        // CAIRO_FILTER_DEFAULT is an internal macro that expands to CAIRO_FILTER_GOOD.
        match i {
            Interpolation::Nearest => cairo::Filter::Nearest,
            Interpolation::Smooth => cairo::Filter::Good,
        }
    }
}

/// Types of pixel data in a `ImageSurface`.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SurfaceType {
    /// The pixel data is in the sRGB color space.
    SRgb,
    /// The pixel data is in the linear sRGB color space.
    LinearRgb,
    /// The pixel data is alpha-only (contains meaningful data only in the alpha channel).
    ///
    /// A number of methods are optimized for alpha-only surfaces. For example, linearization and
    /// unlinearization have no effect for alpha-only surfaces.
    AlphaOnly,
}

impl SurfaceType {
    /// Combines surface types
    ///
    /// If combining two alpha-only surfaces, the result is alpha-only.
    /// If one is alpha-only, the result is the other.
    /// If none is alpha-only, the types should be the same.
    ///
    /// # Panics
    /// Panics if the surface types are not alpha-only and differ.
    pub fn combine(self, other: SurfaceType) -> SurfaceType {
        match (self, other) {
            (SurfaceType::AlphaOnly, t) => t,
            (t, SurfaceType::AlphaOnly) => t,
            (t1, t2) if t1 == t2 => t1,
            _ => panic!(),
        }
    }
}

/// Operators supported by `ImageSurface<Shared>::compose`.
pub enum Operator {
    Over,
    In,
    Out,
    Atop,
    Xor,
    Multiply,
    Screen,
    Darken,
    Lighten,
    Overlay,
    ColorDodge,
    ColorBurn,
    HardLight,
    SoftLight,
    Difference,
    Exclusion,
    HslHue,
    HslSaturation,
    HslColor,
    HslLuminosity,
}

/// Wrapper for a Cairo image surface that enforces exclusive access when modifying it.
///
/// Shared access to `cairo::ImageSurface` is tricky since a read-only borrowed reference
/// can still be cloned and then modified. We can't simply use `cairo::ImageSurface::data()`
/// because in the filter code we have surfaces referenced from multiple places and it would
/// probably add more complexity to remove that and start passing around references.
///
/// This wrapper asserts the uniqueness of its image surface.
///
/// It uses the typestate pattern to ensure that the surface can be modified only when
/// it is in the `Exclusive` state, while in the `Shared` state it only allows read-only access.
#[derive(Debug, Clone)]
pub struct ImageSurface<T> {
    surface: cairo::ImageSurface,

    data_ptr: NonNull<u8>, // *const.
    width: i32,
    height: i32,
    stride: isize,

    surface_type: SurfaceType,

    _state: PhantomData<T>,
}

#[derive(Debug, Clone)]
pub struct Shared;

/// Shared state of `ImageSurface`
pub type SharedImageSurface = ImageSurface<Shared>;

#[derive(Debug, Clone)]
pub struct Exclusive;

/// Exclusive state of `ImageSurface`
pub type ExclusiveImageSurface = ImageSurface<Exclusive>;

// The access is read-only, the ref-counting on an `cairo::ImageSurface` is atomic.
unsafe impl Sync for SharedImageSurface {}

/// A compile-time blur direction variable.
pub trait BlurDirection {
    const IS_VERTICAL: bool;
}

/// Vertical blur direction.
pub enum Vertical {}
/// Horizontal blur direction.
pub enum Horizontal {}

impl BlurDirection for Vertical {
    const IS_VERTICAL: bool = true;
}

impl BlurDirection for Horizontal {
    const IS_VERTICAL: bool = false;
}

/// A compile-time alpha-only marker variable.
pub trait IsAlphaOnly {
    const IS_ALPHA_ONLY: bool;
}

/// Alpha-only.
pub enum AlphaOnly {}
/// Not alpha-only.
pub enum NotAlphaOnly {}

/// Iterator over the rows of a `SharedImageSurface`.
pub struct Rows<'a> {
    surface: &'a SharedImageSurface,
    next_row: i32,
}

/// Iterator over the mutable rows of an `ExclusiveImageSurface`.
pub struct RowsMut<'a> {
    // Keep an ImageSurfaceData here instead of a raw mutable pointer to the bytes,
    // so that the ImageSurfaceData will mark the surface as dirty when it is dropped.
    data: cairo::ImageSurfaceData<'a>,

    width: i32,
    height: i32,
    stride: i32,

    next_row: i32,
}

impl IsAlphaOnly for AlphaOnly {
    const IS_ALPHA_ONLY: bool = true;
}

impl IsAlphaOnly for NotAlphaOnly {
    const IS_ALPHA_ONLY: bool = false;
}

impl<T> ImageSurface<T> {
    /// Returns the surface width.
    #[inline]
    pub fn width(&self) -> i32 {
        self.width
    }

    /// Returns the surface height.
    #[inline]
    pub fn height(&self) -> i32 {
        self.height
    }

    /// Returns the surface stride.
    #[inline]
    pub fn stride(&self) -> isize {
        self.stride
    }
}

impl ImageSurface<Shared> {
    /// Creates a `SharedImageSurface` from a unique `cairo::ImageSurface`.
    ///
    /// # Panics
    /// Panics if the surface format isn't `ARgb32` and if the surface is not unique, that is, its
    /// reference count isn't 1.
    #[inline]
    pub fn wrap(
        surface: cairo::ImageSurface,
        surface_type: SurfaceType,
    ) -> Result<SharedImageSurface, cairo::Error> {
        // get_pixel() assumes ARgb32.
        assert_eq!(surface.format(), cairo::Format::ARgb32);

        let reference_count =
            unsafe { cairo::ffi::cairo_surface_get_reference_count(surface.to_raw_none()) };
        assert_eq!(reference_count, 1);

        let (width, height) = (surface.width(), surface.height());

        // Cairo allows zero-sized surfaces, but it does malloc(0), whose result
        // is implementation-defined.  So, we can't assume NonNull below.  This is
        // why we disallow zero-sized surfaces here.
        if !(width > 0 && height > 0) {
            return Err(cairo::Error::InvalidSize);
        }

        surface.flush();

        let data_ptr = NonNull::new(unsafe {
            cairo::ffi::cairo_image_surface_get_data(surface.to_raw_none())
        })
        .unwrap();

        let stride = surface.stride() as isize;

        Ok(SharedImageSurface {
            surface,
            data_ptr,
            width,
            height,
            stride,
            surface_type,
            _state: PhantomData,
        })
    }

    /// Creates a `SharedImageSurface` copying from a `cairo::ImageSurface`, even if it
    /// does not have a reference count of 1.
    #[inline]
    pub fn copy_from_surface(surface: &cairo::ImageSurface) -> Result<Self, cairo::Error> {
        let copy =
            cairo::ImageSurface::create(cairo::Format::ARgb32, surface.width(), surface.height())?;

        {
            let cr = cairo::Context::new(&copy)?;
            cr.set_source_surface(surface, 0f64, 0f64)?;
            cr.paint()?;
        }

        SharedImageSurface::wrap(copy, SurfaceType::SRgb)
    }

    /// Creates an empty `SharedImageSurface` of the given size and `type`.
    #[inline]
    pub fn empty(width: i32, height: i32, surface_type: SurfaceType) -> Result<Self, cairo::Error> {
        let s = cairo::ImageSurface::create(cairo::Format::ARgb32, width, height)?;

        SharedImageSurface::wrap(s, surface_type)
    }

    /// Converts this `SharedImageSurface` back into a Cairo image surface.
    #[inline]
    pub fn into_image_surface(self) -> Result<cairo::ImageSurface, cairo::Error> {
        let reference_count =
            unsafe { cairo::ffi::cairo_surface_get_reference_count(self.surface.to_raw_none()) };

        if reference_count == 1 {
            Ok(self.surface)
        } else {
            // If there are any other references, copy the underlying surface.
            self.copy_surface(IRect::from_size(self.width, self.height))
        }
    }

    pub fn from_image(
        image: &image::DynamicImage,
        content_type: Option<&str>,
        mime_data: Option<Vec<u8>>,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let rgba_image = image.to_rgba8();

        let width = i32(rgba_image.width()).map_err(|_| cairo::Error::InvalidSize)?;
        let height = i32(rgba_image.height()).map_err(|_| cairo::Error::InvalidSize)?;

        let mut surf = ExclusiveImageSurface::new(width, height, SurfaceType::SRgb)?;

        rgba_image
            .rows()
            .zip(surf.rows_mut())
            .flat_map(|(src_row, dest_row)| src_row.zip(dest_row.iter_mut()))
            .for_each(|(src, dest)| *dest = src.to_pixel().premultiply().to_cairo_argb());

        if let (Some(content_type), Some(bytes)) = (content_type, mime_data) {
            surf.surface.set_mime_data(content_type, bytes)?;
        }

        surf.share()
    }

    /// Returns `true` if the surface contains meaningful data only in the alpha channel.
    #[inline]
    fn is_alpha_only(&self) -> bool {
        self.surface_type == SurfaceType::AlphaOnly
    }

    /// Returns the type of this surface.
    #[inline]
    pub fn surface_type(&self) -> SurfaceType {
        self.surface_type
    }

    /// Retrieves the pixel value at the given coordinates.
    #[inline]
    pub fn get_pixel(&self, x: u32, y: u32) -> Pixel {
        assert!(x < self.width as u32);
        assert!(y < self.height as u32);

        #[allow(clippy::cast_ptr_alignment)]
        let value = unsafe {
            *(self
                .data_ptr
                .as_ptr()
                .offset(y as isize * self.stride + x as isize * 4) as *const u32)
        };

        Pixel::from_u32(value)
    }

    /// Retrieves the pixel value by offset into the pixel data array.
    #[inline]
    pub fn get_pixel_by_offset(&self, offset: isize) -> Pixel {
        assert!(offset < self.stride * self.height as isize);

        #[allow(clippy::cast_ptr_alignment)]
        let value = unsafe { *(self.data_ptr.as_ptr().offset(offset) as *const u32) };
        Pixel::from_u32(value)
    }

    /// Calls `set_source_surface()` on the given Cairo context.
    #[inline]
    pub fn set_as_source_surface(
        &self,
        cr: &cairo::Context,
        x: f64,
        y: f64,
    ) -> Result<(), cairo::Error> {
        cr.set_source_surface(&self.surface, x, y)
    }

    /// Creates a Cairo surface pattern from the surface
    pub fn to_cairo_pattern(&self) -> cairo::SurfacePattern {
        cairo::SurfacePattern::create(&self.surface)
    }

    /// Returns a new `cairo::ImageSurface` with the same contents as the one stored in this
    /// `SharedImageSurface` within the given bounds.
    fn copy_surface(&self, bounds: IRect) -> Result<cairo::ImageSurface, cairo::Error> {
        let output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        let cr = cairo::Context::new(&output_surface)?;
        let r = cairo::Rectangle::from(bounds);
        cr.rectangle(r.x(), r.y(), r.width(), r.height());
        cr.clip();

        cr.set_source_surface(&self.surface, 0f64, 0f64)?;
        cr.paint()?;

        Ok(output_surface)
    }

    /// Scales the given surface by `x` and `y` into a surface `width`×`height` in size, clipped by
    /// `bounds`.
    pub fn scale_to(
        &self,
        width: i32,
        height: i32,
        bounds: IRect,
        x: f64,
        y: f64,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let output_surface = cairo::ImageSurface::create(cairo::Format::ARgb32, width, height)?;

        {
            let cr = cairo::Context::new(&output_surface)?;
            let r = cairo::Rectangle::from(bounds);
            cr.rectangle(r.x(), r.y(), r.width(), r.height());
            cr.clip();

            cr.scale(x, y);
            self.set_as_source_surface(&cr, 0.0, 0.0)?;
            cr.paint()?;
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Returns a scaled version of a surface and bounds.
    #[inline]
    pub fn scale(
        &self,
        bounds: IRect,
        x: f64,
        y: f64,
    ) -> Result<(SharedImageSurface, IRect), cairo::Error> {
        let new_width = (f64::from(self.width) * x).ceil() as i32;
        let new_height = (f64::from(self.height) * y).ceil() as i32;
        let new_bounds = bounds.scale(x, y);

        Ok((
            self.scale_to(new_width, new_height, new_bounds, x, y)?,
            new_bounds,
        ))
    }

    /// Returns a surface with black background and alpha channel matching this surface.
    pub fn extract_alpha(&self, bounds: IRect) -> Result<SharedImageSurface, cairo::Error> {
        let mut output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        let output_stride = output_surface.stride() as usize;
        {
            let mut output_data = output_surface.data().unwrap();

            for (x, y, Pixel { a, .. }) in Pixels::within(self, bounds) {
                let output_pixel = Pixel {
                    r: 0,
                    g: 0,
                    b: 0,
                    a,
                };
                output_data.set_pixel(output_stride, output_pixel, x, y);
            }
        }

        SharedImageSurface::wrap(output_surface, SurfaceType::AlphaOnly)
    }

    /// Returns a surface whose alpha channel for each pixel is equal to the
    /// luminance of that pixel's unpremultiplied RGB values.  The resulting
    /// surface's RGB values are not meanignful; only the alpha channel has
    /// useful luminance data.
    ///
    /// This is to get a mask suitable for use with cairo_mask_surface().
    pub fn to_luminance_mask(&self) -> Result<SharedImageSurface, cairo::Error> {
        let bounds = IRect::from_size(self.width, self.height);

        let mut output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        let stride = output_surface.stride() as usize;
        {
            let mut data = output_surface.data().unwrap();

            for (x, y, pixel) in Pixels::within(self, bounds) {
                data.set_pixel(stride, pixel.to_luminance_mask(), x, y);
            }
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Returns a surface with pre-multiplication of color values undone.
    ///
    /// HACK: this is storing unpremultiplied pixels in an ARGB32 image surface (which is supposed
    /// to be premultiplied pixels).
    pub fn unpremultiply(&self, bounds: IRect) -> Result<SharedImageSurface, cairo::Error> {
        // Unpremultiplication doesn't affect the alpha channel.
        if self.is_alpha_only() {
            return Ok(self.clone());
        }

        let mut output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        let stride = output_surface.stride() as usize;
        {
            let mut data = output_surface.data().unwrap();

            for (x, y, pixel) in Pixels::within(self, bounds) {
                data.set_pixel(stride, pixel.unpremultiply(), x, y);
            }
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Converts the surface to the linear sRGB color space.
    #[inline]
    pub fn to_linear_rgb(&self, bounds: IRect) -> Result<SharedImageSurface, cairo::Error> {
        match self.surface_type {
            SurfaceType::LinearRgb | SurfaceType::AlphaOnly => Ok(self.clone()),
            _ => srgb::linearize_surface(self, bounds),
        }
    }

    /// Converts the surface to the sRGB color space.
    #[inline]
    pub fn to_srgb(&self, bounds: IRect) -> Result<SharedImageSurface, cairo::Error> {
        match self.surface_type {
            SurfaceType::SRgb | SurfaceType::AlphaOnly => Ok(self.clone()),
            _ => srgb::unlinearize_surface(self, bounds),
        }
    }

    /// Performs a convolution.
    ///
    /// Note that `kernel` is rotated 180 degrees.
    ///
    /// The `target` parameter determines the position of the kernel relative to each pixel of the
    /// image. The value of `(0, 0)` indicates that the top left pixel of the (180-degrees-rotated)
    /// kernel corresponds to the current pixel, and the rest of the kernel is to the right and
    /// bottom of the pixel. The value of `(cols / 2, rows / 2)` centers a kernel with an odd
    /// number of rows and columns.
    ///
    /// # Panics
    /// Panics if `kernel` has zero rows or columns.
    pub fn convolve<R: Dim, C: Dim, S: Storage<f64, R, C>>(
        &self,
        bounds: IRect,
        target: (i32, i32),
        kernel: &Matrix<f64, R, C, S>,
        edge_mode: EdgeMode,
    ) -> Result<SharedImageSurface, cairo::Error> {
        assert!(kernel.nrows() >= 1);
        assert!(kernel.ncols() >= 1);

        let mut output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        let output_stride = output_surface.stride() as usize;
        {
            let mut output_data = output_surface.data().unwrap();

            if self.is_alpha_only() {
                for (x, y, _pixel) in Pixels::within(self, bounds) {
                    let kernel_bounds = IRect::new(
                        x as i32 - target.0,
                        y as i32 - target.1,
                        x as i32 - target.0 + kernel.ncols() as i32,
                        y as i32 - target.1 + kernel.nrows() as i32,
                    );

                    let mut a = 0.0;

                    for (x, y, pixel) in
                        PixelRectangle::within(self, bounds, kernel_bounds, edge_mode)
                    {
                        let kernel_x = (kernel_bounds.x1 - x - 1) as usize;
                        let kernel_y = (kernel_bounds.y1 - y - 1) as usize;
                        let factor = kernel[(kernel_y, kernel_x)];

                        a += f64::from(pixel.a) * factor;
                    }

                    let convert = |x: f64| (clamp(x, 0.0, 255.0) + 0.5) as u8;

                    let output_pixel = Pixel {
                        r: 0,
                        g: 0,
                        b: 0,
                        a: convert(a),
                    };

                    output_data.set_pixel(output_stride, output_pixel, x, y);
                }
            } else {
                for (x, y, _pixel) in Pixels::within(self, bounds) {
                    let kernel_bounds = IRect::new(
                        x as i32 - target.0,
                        y as i32 - target.1,
                        x as i32 - target.0 + kernel.ncols() as i32,
                        y as i32 - target.1 + kernel.nrows() as i32,
                    );

                    let mut r = 0.0;
                    let mut g = 0.0;
                    let mut b = 0.0;
                    let mut a = 0.0;

                    for (x, y, pixel) in
                        PixelRectangle::within(self, bounds, kernel_bounds, edge_mode)
                    {
                        let kernel_x = (kernel_bounds.x1 - x - 1) as usize;
                        let kernel_y = (kernel_bounds.y1 - y - 1) as usize;
                        let factor = kernel[(kernel_y, kernel_x)];

                        r += f64::from(pixel.r) * factor;
                        g += f64::from(pixel.g) * factor;
                        b += f64::from(pixel.b) * factor;
                        a += f64::from(pixel.a) * factor;
                    }

                    let convert = |x: f64| (clamp(x, 0.0, 255.0) + 0.5) as u8;

                    let output_pixel = Pixel {
                        r: convert(r),
                        g: convert(g),
                        b: convert(b),
                        a: convert(a),
                    };

                    output_data.set_pixel(output_stride, output_pixel, x, y);
                }
            }
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Performs a horizontal or vertical box blur.
    ///
    /// The `target` parameter determines the position of the kernel relative to each pixel of the
    /// image. The value of `0` indicates that the first pixel of the kernel corresponds to the
    /// current pixel, and the rest of the kernel is to the right or bottom of the pixel. The value
    /// of `kernel_size / 2` centers a kernel with an odd size.
    ///
    /// # Panics
    /// Panics if `kernel_size` is `0` or if `target >= kernel_size`.
    // This is public (and not inlined into box_blur()) for the purpose of accessing it from the
    // benchmarks.
    pub fn box_blur_loop<B: BlurDirection, A: IsAlphaOnly>(
        &self,
        output_surface: &mut cairo::ImageSurface,
        bounds: IRect,
        kernel_size: usize,
        target: usize,
    ) {
        assert_ne!(kernel_size, 0);
        assert!(target < kernel_size);
        assert_eq!(self.is_alpha_only(), A::IS_ALPHA_ONLY);

        {
            // The following code is needed for a parallel implementation of the blur loop. The
            // blurring is done either for each row or for each column of pixels, depending on the
            // value of `vertical`, independently of the others. Naturally, we want to run the
            // outer loop on a thread pool.
            //
            // The case of `vertical == false` is simple since the input image slice can be
            // partitioned into chunks for each row of pixels and processed in parallel with rayon.
            // The case of `vertical == true`, however, is more involved because we can't just make
            // mutable slices for all pixel columns (they would be overlapping which is forbidden
            // by the aliasing rules).
            //
            // This is where the following struct comes into play: it stores a sub-slice of the
            // pixel data and can be split at any row or column into two parts (similar to
            // slice::split_at_mut()).
            struct UnsafeSendPixelData<'a> {
                width: u32,
                height: u32,
                stride: isize,
                ptr: NonNull<u8>,
                _marker: PhantomData<&'a mut ()>,
            }

            unsafe impl<'a> Send for UnsafeSendPixelData<'a> {}

            impl<'a> UnsafeSendPixelData<'a> {
                /// Creates a new `UnsafeSendPixelData`.
                ///
                /// # Safety
                /// You must call `cairo_surface_mark_dirty()` on the surface once all instances of
                /// `UnsafeSendPixelData` are dropped to make sure the pixel changes are committed
                /// to Cairo.
                #[inline]
                unsafe fn new(surface: &mut cairo::ImageSurface) -> Self {
                    assert_eq!(surface.format(), cairo::Format::ARgb32);
                    let ptr = surface.data().unwrap().as_mut_ptr();

                    Self {
                        width: surface.width() as u32,
                        height: surface.height() as u32,
                        stride: surface.stride() as isize,
                        ptr: NonNull::new(ptr).unwrap(),
                        _marker: PhantomData,
                    }
                }

                /// Sets a pixel value at the given coordinates.
                #[inline]
                fn set_pixel(&mut self, pixel: Pixel, x: u32, y: u32) {
                    assert!(x < self.width);
                    assert!(y < self.height);

                    let value = pixel.to_u32();

                    #[allow(clippy::cast_ptr_alignment)]
                    unsafe {
                        let ptr = self
                            .ptr
                            .as_ptr()
                            .offset(y as isize * self.stride + x as isize * 4)
                            as *mut u32;
                        *ptr = value;
                    }
                }

                /// Splits this `UnsafeSendPixelData` into two at the given row.
                ///
                /// The first one contains rows `0..index` (`index` not included) and the second one
                /// contains rows `index..height`.
                #[inline]
                fn split_at_row(self, index: u32) -> (Self, Self) {
                    assert!(index <= self.height);

                    (
                        UnsafeSendPixelData {
                            width: self.width,
                            height: index,
                            stride: self.stride,
                            ptr: self.ptr,
                            _marker: PhantomData,
                        },
                        UnsafeSendPixelData {
                            width: self.width,
                            height: self.height - index,
                            stride: self.stride,
                            ptr: NonNull::new(unsafe {
                                self.ptr.as_ptr().offset(index as isize * self.stride)
                            })
                            .unwrap(),
                            _marker: PhantomData,
                        },
                    )
                }

                /// Splits this `UnsafeSendPixelData` into two at the given column.
                ///
                /// The first one contains columns `0..index` (`index` not included) and the second
                /// one contains columns `index..width`.
                #[inline]
                fn split_at_column(self, index: u32) -> (Self, Self) {
                    assert!(index <= self.width);

                    (
                        UnsafeSendPixelData {
                            width: index,
                            height: self.height,
                            stride: self.stride,
                            ptr: self.ptr,
                            _marker: PhantomData,
                        },
                        UnsafeSendPixelData {
                            width: self.width - index,
                            height: self.height,
                            stride: self.stride,
                            ptr: NonNull::new(unsafe {
                                self.ptr.as_ptr().offset(index as isize * 4)
                            })
                            .unwrap(),
                            _marker: PhantomData,
                        },
                    )
                }
            }

            let output_data = unsafe { UnsafeSendPixelData::new(output_surface) };

            // Shift is target into the opposite direction.
            let shift = (kernel_size - target) as i32;
            let target = target as i32;

            // Convert to f64 once since we divide by it.
            let kernel_size_f64 = kernel_size as f64;
            let compute = |x: u32| (f64::from(x) / kernel_size_f64 + 0.5) as u8;

            // Depending on `vertical`, we're blurring either horizontally line-by-line, or
            // vertically column-by-column. In the code below, the main axis is the axis along
            // which the blurring happens (so if `vertical` is false, the main axis is the
            // horizontal axis). The other axis is the outer loop axis. The code uses `i` and `j`
            // for the other axis and main axis coordinates, respectively.
            let (main_axis_min, main_axis_max, other_axis_min, other_axis_max) = if B::IS_VERTICAL {
                (bounds.y0, bounds.y1, bounds.x0, bounds.x1)
            } else {
                (bounds.x0, bounds.x1, bounds.y0, bounds.y1)
            };

            // Helper function for getting the pixels.
            let pixel = |i, j| {
                let (x, y) = if B::IS_VERTICAL { (i, j) } else { (j, i) };

                self.get_pixel(x as u32, y as u32)
            };

            // The following loop assumes the first row or column of `output_data` is the first row
            // or column inside `bounds`.
            let mut output_data = if B::IS_VERTICAL {
                output_data.split_at_column(bounds.x0 as u32).1
            } else {
                output_data.split_at_row(bounds.y0 as u32).1
            };

            rayon::scope(|s| {
                for i in other_axis_min..other_axis_max {
                    // Split off one row or column and launch its processing on another thread.
                    // Thanks to the initial split before the loop, there's no special case for the
                    // very first split.
                    let (mut current, remaining) = if B::IS_VERTICAL {
                        output_data.split_at_column(1)
                    } else {
                        output_data.split_at_row(1)
                    };

                    output_data = remaining;

                    s.spawn(move |_| {
                        // Helper function for setting the pixels.
                        let mut set_pixel = |j, pixel| {
                            // We're processing rows or columns one-by-one, so the other coordinate
                            // is always 0.
                            let (x, y) = if B::IS_VERTICAL { (0, j) } else { (j, 0) };
                            current.set_pixel(pixel, x, y);
                        };

                        // The idea is that since all weights of the box blur kernel are equal, for
                        // each step along the main axis, instead of recomputing the full sum, we
                        // can take the previous sum, subtract the "oldest" pixel value and add the
                        // "newest" pixel value.
                        //
                        // The sum is u32 so that it can fit MAXIMUM_KERNEL_SIZE * 255.
                        let mut sum_r = 0;
                        let mut sum_g = 0;
                        let mut sum_b = 0;
                        let mut sum_a = 0;

                        // The whole sum needs to be computed for the first pixel. However, we know
                        // that values outside of bounds are transparent, so the loop starts on the
                        // first pixel in bounds.
                        for j in main_axis_min..min(main_axis_max, main_axis_min + shift) {
                            let Pixel { r, g, b, a } = pixel(i, j);

                            if !A::IS_ALPHA_ONLY {
                                sum_r += u32::from(r);
                                sum_g += u32::from(g);
                                sum_b += u32::from(b);
                            }

                            sum_a += u32::from(a);
                        }

                        set_pixel(
                            main_axis_min as u32,
                            Pixel {
                                r: compute(sum_r),
                                g: compute(sum_g),
                                b: compute(sum_b),
                                a: compute(sum_a),
                            },
                        );

                        // Now, go through all the other pixels.
                        //
                        // j - target - 1 >= main_axis_min
                        // j >= main_axis_min + target + 1
                        let start_subtracting_at = main_axis_min + target + 1;

                        // j + shift - 1 < main_axis_max
                        // j < main_axis_max - shift + 1
                        let stop_adding_at = main_axis_max - shift + 1;

                        for j in main_axis_min + 1..main_axis_max {
                            if j >= start_subtracting_at {
                                let old_pixel = pixel(i, j - target - 1);

                                if !A::IS_ALPHA_ONLY {
                                    sum_r -= u32::from(old_pixel.r);
                                    sum_g -= u32::from(old_pixel.g);
                                    sum_b -= u32::from(old_pixel.b);
                                }

                                sum_a -= u32::from(old_pixel.a);
                            }

                            if j < stop_adding_at {
                                let new_pixel = pixel(i, j + shift - 1);

                                if !A::IS_ALPHA_ONLY {
                                    sum_r += u32::from(new_pixel.r);
                                    sum_g += u32::from(new_pixel.g);
                                    sum_b += u32::from(new_pixel.b);
                                }

                                sum_a += u32::from(new_pixel.a);
                            }

                            set_pixel(
                                j as u32,
                                Pixel {
                                    r: compute(sum_r),
                                    g: compute(sum_g),
                                    b: compute(sum_b),
                                    a: compute(sum_a),
                                },
                            );
                        }
                    });
                }
            });
        }

        // Don't forget to manually mark the surface as dirty (due to usage of
        // `UnsafeSendPixelData`).
        unsafe { cairo::ffi::cairo_surface_mark_dirty(output_surface.to_raw_none()) }
    }

    /// Performs a horizontal or vertical box blur.
    ///
    /// The `target` parameter determines the position of the kernel relative to each pixel of the
    /// image. The value of `0` indicates that the first pixel of the kernel corresponds to the
    /// current pixel, and the rest of the kernel is to the right or bottom of the pixel. The value
    /// of `kernel_size / 2` centers a kernel with an odd size.
    ///
    /// # Panics
    /// Panics if `kernel_size` is `0` or if `target >= kernel_size`.
    #[inline]
    pub fn box_blur<B: BlurDirection>(
        &self,
        bounds: IRect,
        kernel_size: usize,
        target: usize,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let mut output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        if self.is_alpha_only() {
            self.box_blur_loop::<B, AlphaOnly>(&mut output_surface, bounds, kernel_size, target);
        } else {
            self.box_blur_loop::<B, NotAlphaOnly>(&mut output_surface, bounds, kernel_size, target);
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Fills the with a specified color.
    #[inline]
    pub fn flood(&self, bounds: IRect, color: Color) -> Result<SharedImageSurface, cairo::Error> {
        let output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        let rgba = color_to_rgba(&color);

        if rgba.alpha > 0.0 {
            let cr = cairo::Context::new(&output_surface)?;
            let r = cairo::Rectangle::from(bounds);
            cr.rectangle(r.x(), r.y(), r.width(), r.height());
            cr.clip();

            set_source_color_on_cairo(&cr, &color);
            cr.paint()?;
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Offsets the image of the specified amount.
    #[inline]
    pub fn offset(
        &self,
        bounds: Rect,
        dx: f64,
        dy: f64,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        // output_bounds contains all pixels within bounds,
        // for which (x - ox) and (y - oy) also lie within bounds.
        if let Some(output_bounds) = bounds.translate((dx, dy)).intersection(&bounds) {
            let cr = cairo::Context::new(&output_surface)?;
            let r = cairo::Rectangle::from(output_bounds);
            cr.rectangle(r.x(), r.y(), r.width(), r.height());
            cr.clip();

            self.set_as_source_surface(&cr, dx, dy)?;
            cr.paint()?;
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Returns a new surface of the same size, with the contents of the
    /// specified image, optionally transformed to match a given box
    #[inline]
    pub fn paint_image(
        &self,
        bounds: Rect,
        image: &SharedImageSurface,
        rect: Option<Rect>,
        interpolation: Interpolation,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        if rect.is_none() || !rect.unwrap().is_empty() {
            let cr = cairo::Context::new(&output_surface)?;
            let r = cairo::Rectangle::from(bounds);
            cr.rectangle(r.x(), r.y(), r.width(), r.height());
            cr.clip();

            image.set_as_source_surface(&cr, 0f64, 0f64)?;

            if let Some(rect) = rect {
                let mut matrix = cairo::Matrix::new(
                    rect.width() / f64::from(image.width()),
                    0.0,
                    0.0,
                    rect.height() / f64::from(image.height()),
                    rect.x0,
                    rect.y0,
                );
                matrix.invert();

                cr.source().set_matrix(matrix);
                cr.source().set_filter(cairo::Filter::from(interpolation));
            }

            cr.paint()?;
        }

        SharedImageSurface::wrap(output_surface, image.surface_type)
    }

    /// Creates a new surface with the size and content specified in `bounds`
    ///
    /// # Panics
    /// Panics if `bounds` is an empty rectangle, since `SharedImageSurface` cannot
    /// represent zero-sized images.
    #[inline]
    pub fn tile(&self, bounds: IRect) -> Result<SharedImageSurface, cairo::Error> {
        // Cairo lets us create zero-sized surfaces, but the call to SharedImageSurface::wrap()
        // below will panic in that case.  So, disallow requesting a zero-sized subregion.
        assert!(!bounds.is_empty());

        let output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, bounds.width(), bounds.height())?;

        {
            let cr = cairo::Context::new(&output_surface)?;
            self.set_as_source_surface(&cr, f64::from(-bounds.x0), f64::from(-bounds.y0))?;
            cr.paint()?;
        }

        SharedImageSurface::wrap(output_surface, self.surface_type)
    }

    /// Returns a new surface of the same size, with the contents of the specified
    /// image repeated to fill the bounds and starting from the given position.
    #[inline]
    pub fn paint_image_tiled(
        &self,
        bounds: IRect,
        image: &SharedImageSurface,
        x: i32,
        y: i32,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let output_surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, self.width, self.height)?;

        {
            let cr = cairo::Context::new(&output_surface)?;

            let ptn = image.to_cairo_pattern();
            ptn.set_extend(cairo::Extend::Repeat);
            let mut mat = cairo::Matrix::identity();
            mat.translate(f64::from(-x), f64::from(-y));
            ptn.set_matrix(mat);

            let r = cairo::Rectangle::from(bounds);
            cr.rectangle(r.x(), r.y(), r.width(), r.height());
            cr.clip();

            cr.set_source(&ptn)?;
            cr.paint()?;
        }

        SharedImageSurface::wrap(output_surface, image.surface_type)
    }

    /// Performs the combination of two input surfaces using Porter-Duff
    /// compositing operators.
    ///
    /// # Panics
    /// Panics if the two surface types are not compatible.
    #[inline]
    pub fn compose(
        &self,
        other: &SharedImageSurface,
        bounds: IRect,
        operator: Operator,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let output_surface = other.copy_surface(bounds)?;

        {
            let cr = cairo::Context::new(&output_surface)?;
            let r = cairo::Rectangle::from(bounds);
            cr.rectangle(r.x(), r.y(), r.width(), r.height());
            cr.clip();

            self.set_as_source_surface(&cr, 0.0, 0.0)?;
            cr.set_operator(operator.into());
            cr.paint()?;
        }

        SharedImageSurface::wrap(
            output_surface,
            self.surface_type.combine(other.surface_type),
        )
    }

    /// Performs the combination of two input surfaces.
    ///
    /// Each pixel of the resulting image is computed using the following formula:
    /// `res = k1*i1*i2 + k2*i1 + k3*i2 + k4`
    ///
    /// # Panics
    /// Panics if the two surface types are not compatible.
    #[inline]
    pub fn compose_arithmetic(
        &self,
        other: &SharedImageSurface,
        bounds: IRect,
        k1: f64,
        k2: f64,
        k3: f64,
        k4: f64,
    ) -> Result<SharedImageSurface, cairo::Error> {
        let mut output_surface = ExclusiveImageSurface::new(
            self.width,
            self.height,
            self.surface_type.combine(other.surface_type),
        )?;

        composite_arithmetic(self, other, &mut output_surface, bounds, k1, k2, k3, k4);

        output_surface.share()
    }

    pub fn rows(&self) -> Rows<'_> {
        Rows {
            surface: self,
            next_row: 0,
        }
    }
}

impl<'a> Iterator for Rows<'a> {
    type Item = &'a [CairoARGB];

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_row == self.surface.height {
            return None;
        }

        let row = self.next_row;

        self.next_row += 1;

        // SAFETY: this code assumes that cairo image surface data is correctly
        // aligned for u32. This assumption is justified by the Cairo docs,
        // which say this:
        //
        // https://cairographics.org/manual/cairo-Image-Surfaces.html#cairo-image-surface-create-for-data
        //
        // > This pointer must be suitably aligned for any kind of variable,
        // > (for example, a pointer returned by malloc).
        unsafe {
            let row_ptr: *const u8 = self
                .surface
                .data_ptr
                .as_ptr()
                .offset(row as isize * self.surface.stride);
            let row_of_u32: &[u32] =
                slice::from_raw_parts(row_ptr as *const u32, self.surface.width as usize);
            let pixels = row_of_u32.as_cairo_argb();
            assert!(pixels.len() == self.surface.width as usize);
            Some(pixels)
        }
    }
}

impl<'a> Iterator for RowsMut<'a> {
    type Item = &'a mut [CairoARGB];

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_row == self.height {
            return None;
        }

        let row = self.next_row as usize;

        self.next_row += 1;

        // SAFETY: this code assumes that cairo image surface data is correctly
        // aligned for u32. This assumption is justified by the Cairo docs,
        // which say this:
        //
        // https://cairographics.org/manual/cairo-Image-Surfaces.html#cairo-image-surface-create-for-data
        //
        // > This pointer must be suitably aligned for any kind of variable,
        // > (for example, a pointer returned by malloc).
        unsafe {
            // We do this with raw pointers, instead of re-slicing the &mut self.data[....],
            // because with the latter we can't synthesize an appropriate lifetime for
            // the return value.

            let data_ptr = self.data.as_mut_ptr();
            let row_ptr: *mut u8 = data_ptr.offset(row as isize * self.stride as isize);
            let row_of_u32: &mut [u32] =
                slice::from_raw_parts_mut(row_ptr as *mut u32, self.width as usize);
            let pixels = row_of_u32.as_cairo_argb_mut();
            assert!(pixels.len() == self.width as usize);
            Some(pixels)
        }
    }
}

/// Performs the arithmetic composite operation. Public for benchmarking.
#[inline]
pub fn composite_arithmetic(
    surface1: &SharedImageSurface,
    surface2: &SharedImageSurface,
    output_surface: &mut ExclusiveImageSurface,
    bounds: IRect,
    k1: f64,
    k2: f64,
    k3: f64,
    k4: f64,
) {
    output_surface.modify(&mut |data, stride| {
        for (x, y, pixel, pixel_2) in
            Pixels::within(surface1, bounds).map(|(x, y, p)| (x, y, p, surface2.get_pixel(x, y)))
        {
            let i1a = f64::from(pixel.a) / 255f64;
            let i2a = f64::from(pixel_2.a) / 255f64;
            let oa = k1 * i1a * i2a + k2 * i1a + k3 * i2a + k4;
            let oa = clamp(oa, 0f64, 1f64);

            // Contents of image surfaces are transparent by default, so if the resulting pixel is
            // transparent there's no need to do anything.
            if oa > 0f64 {
                let compute = |i1, i2| {
                    let i1 = f64::from(i1) / 255f64;
                    let i2 = f64::from(i2) / 255f64;

                    let o = k1 * i1 * i2 + k2 * i1 + k3 * i2 + k4;
                    let o = clamp(o, 0f64, oa);

                    ((o * 255f64) + 0.5) as u8
                };

                let output_pixel = Pixel {
                    r: compute(pixel.r, pixel_2.r),
                    g: compute(pixel.g, pixel_2.g),
                    b: compute(pixel.b, pixel_2.b),
                    a: ((oa * 255f64) + 0.5) as u8,
                };

                data.set_pixel(stride, output_pixel, x, y);
            }
        }
    });
}

impl ImageSurface<Exclusive> {
    #[inline]
    pub fn new(
        width: i32,
        height: i32,
        surface_type: SurfaceType,
    ) -> Result<ExclusiveImageSurface, cairo::Error> {
        let surface = cairo::ImageSurface::create(cairo::Format::ARgb32, width, height)?;

        let (width, height) = (surface.width(), surface.height());

        // Cairo allows zero-sized surfaces, but it does malloc(0), whose result
        // is implementation-defined.  So, we can't assume NonNull below.  This is
        // why we disallow zero-sized surfaces here.
        if !(width > 0 && height > 0) {
            return Err(cairo::Error::InvalidSize);
        }

        let data_ptr = NonNull::new(unsafe {
            cairo::ffi::cairo_image_surface_get_data(surface.to_raw_none())
        })
        .unwrap();

        let stride = surface.stride() as isize;

        Ok(ExclusiveImageSurface {
            surface,
            data_ptr,
            width,
            height,
            stride,
            surface_type,
            _state: PhantomData,
        })
    }

    #[inline]
    pub fn share(self) -> Result<SharedImageSurface, cairo::Error> {
        SharedImageSurface::wrap(self.surface, self.surface_type)
    }

    /// Raw access to the image data as a slice
    #[inline]
    pub fn data(&mut self) -> cairo::ImageSurfaceData<'_> {
        self.surface.data().unwrap()
    }

    /// Modify the image data
    #[inline]
    pub fn modify(&mut self, draw_fn: &mut dyn FnMut(&mut cairo::ImageSurfaceData<'_>, usize)) {
        let stride = self.stride() as usize;
        let mut data = self.data();

        draw_fn(&mut data, stride)
    }

    /// Draw on the surface using cairo
    #[inline]
    pub fn draw(
        &mut self,
        draw_fn: &mut dyn FnMut(cairo::Context) -> Result<(), Box<InternalRenderingError>>,
    ) -> Result<(), Box<InternalRenderingError>> {
        let cr = cairo::Context::new(&self.surface)?;
        draw_fn(cr)
    }

    pub fn rows_mut(&mut self) -> RowsMut<'_> {
        let width = self.surface.width();
        let height = self.surface.height();
        let stride = self.surface.stride();

        let data = self.surface.data().unwrap();

        RowsMut {
            width,
            height,
            stride,
            data,
            next_row: 0,
        }
    }
}

impl From<Operator> for cairo::Operator {
    fn from(op: Operator) -> cairo::Operator {
        use Operator::*;
        use cairo::Operator as Cairo;

        match op {
            Over => Cairo::Over,
            In => Cairo::In,
            Out => Cairo::Out,
            Atop => Cairo::Atop,
            Xor => Cairo::Xor,
            Multiply => Cairo::Multiply,
            Screen => Cairo::Screen,
            Darken => Cairo::Darken,
            Lighten => Cairo::Lighten,
            Overlay => Cairo::Overlay,
            ColorDodge => Cairo::ColorDodge,
            ColorBurn => Cairo::ColorBurn,
            HardLight => Cairo::HardLight,
            SoftLight => Cairo::SoftLight,
            Difference => Cairo::Difference,
            Exclusion => Cairo::Exclusion,
            HslHue => Cairo::HslHue,
            HslSaturation => Cairo::HslSaturation,
            HslColor => Cairo::HslColor,
            HslLuminosity => Cairo::HslLuminosity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::surface_utils::iterators::Pixels;

    #[test]
    fn test_extract_alpha() {
        const WIDTH: i32 = 32;
        const HEIGHT: i32 = 64;

        let bounds = IRect::new(8, 24, 16, 48);
        let full_bounds = IRect::from_size(WIDTH, HEIGHT);

        let mut surface = ExclusiveImageSurface::new(WIDTH, HEIGHT, SurfaceType::SRgb).unwrap();

        // Fill the surface with some data.
        {
            let mut data = surface.data();

            let mut counter = 0u16;
            for x in data.iter_mut() {
                *x = counter as u8;
                counter = (counter + 1) % 256;
            }
        }

        let surface = surface.share().unwrap();
        let alpha = surface.extract_alpha(bounds).unwrap();

        for (x, y, p, pa) in
            Pixels::within(&surface, full_bounds).map(|(x, y, p)| (x, y, p, alpha.get_pixel(x, y)))
        {
            assert_eq!(pa.r, 0);
            assert_eq!(pa.g, 0);
            assert_eq!(pa.b, 0);

            if !bounds.contains(x as i32, y as i32) {
                assert_eq!(pa.a, 0);
            } else {
                assert_eq!(pa.a, p.a);
            }
        }
    }
}
