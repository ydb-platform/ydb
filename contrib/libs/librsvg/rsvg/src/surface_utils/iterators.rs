//! Pixel iterators for `SharedImageSurface`.
use crate::rect::IRect;
use crate::util::clamp;

use super::shared_surface::SharedImageSurface;
use super::{EdgeMode, Pixel};

/// Iterator over pixels of a `SharedImageSurface`.
pub struct Pixels<'a> {
    surface: &'a SharedImageSurface,
    bounds: IRect,
    x: u32,
    y: u32,
    offset: isize,
}

/// Iterator over a (potentially out of bounds) rectangle of pixels of a `SharedImageSurface`.
pub struct PixelRectangle<'a> {
    surface: &'a SharedImageSurface,
    bounds: IRect,
    rectangle: IRect,
    edge_mode: EdgeMode,
    x: i32,
    y: i32,
}

impl<'a> Pixels<'a> {
    /// Creates an iterator over the image surface pixels
    #[inline]
    pub fn new(surface: &'a SharedImageSurface) -> Self {
        let bounds = IRect::from_size(surface.width(), surface.height());

        Self::within(surface, bounds)
    }

    /// Creates an iterator over the image surface pixels, constrained within the given bounds.
    #[inline]
    pub fn within(surface: &'a SharedImageSurface, bounds: IRect) -> Self {
        // Sanity checks.
        assert!(bounds.x0 >= 0);
        assert!(bounds.x0 <= surface.width());
        assert!(bounds.x1 >= bounds.x0);
        assert!(bounds.x1 <= surface.width());
        assert!(bounds.y0 >= 0);
        assert!(bounds.y0 <= surface.height());
        assert!(bounds.y1 >= bounds.y0);
        assert!(bounds.y1 <= surface.height());

        Self {
            surface,
            bounds,
            x: bounds.x0 as u32,
            y: bounds.y0 as u32,
            offset: bounds.y0 as isize * surface.stride() + bounds.x0 as isize * 4,
        }
    }
}

impl<'a> PixelRectangle<'a> {
    /// Creates an iterator over the image surface pixels
    #[inline]
    pub fn new(surface: &'a SharedImageSurface, rectangle: IRect, edge_mode: EdgeMode) -> Self {
        let bounds = IRect::from_size(surface.width(), surface.height());

        Self::within(surface, bounds, rectangle, edge_mode)
    }

    /// Creates an iterator over the image surface pixels, constrained within the given bounds.
    #[inline]
    pub fn within(
        surface: &'a SharedImageSurface,
        bounds: IRect,
        rectangle: IRect,
        edge_mode: EdgeMode,
    ) -> Self {
        // Sanity checks.
        assert!(bounds.x0 >= 0);
        assert!(bounds.x0 <= surface.width());
        assert!(bounds.x1 >= bounds.x0);
        assert!(bounds.x1 <= surface.width());
        assert!(bounds.y0 >= 0);
        assert!(bounds.y0 <= surface.height());
        assert!(bounds.y1 >= bounds.y0);
        assert!(bounds.y1 <= surface.height());

        // Non-None EdgeMode values need at least one pixel available.
        if edge_mode != EdgeMode::None {
            assert!(bounds.x1 > bounds.x0);
            assert!(bounds.y1 > bounds.y0);
        }

        assert!(rectangle.x1 >= rectangle.x0);
        assert!(rectangle.y1 >= rectangle.y0);

        Self {
            surface,
            bounds,
            rectangle,
            edge_mode,
            x: rectangle.x0,
            y: rectangle.y0,
        }
    }
}

impl<'a> Iterator for Pixels<'a> {
    type Item = (u32, u32, Pixel);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // This means we hit the end on the last iteration.
        if self.x == self.bounds.x1 as u32 || self.y == self.bounds.y1 as u32 {
            return None;
        }

        let rv = Some((
            self.x,
            self.y,
            self.surface.get_pixel_by_offset(self.offset),
        ));

        if self.x + 1 == self.bounds.x1 as u32 {
            self.x = self.bounds.x0 as u32;
            self.y += 1;
            self.offset += self.surface.stride() - (self.bounds.width() - 1) as isize * 4;
        } else {
            self.x += 1;
            self.offset += 4;
        }

        rv
    }
}

impl<'a> Iterator for PixelRectangle<'a> {
    type Item = (i32, i32, Pixel);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        // This means we hit the end on the last iteration.
        if self.x == self.rectangle.x1 || self.y == self.rectangle.y1 {
            return None;
        }

        let rv = {
            let get_pixel = |x, y| {
                if !self.bounds.contains(x, y) {
                    match self.edge_mode {
                        EdgeMode::None => Pixel {
                            r: 0,
                            g: 0,
                            b: 0,
                            a: 0,
                        },
                        EdgeMode::Duplicate => {
                            let x = clamp(x, self.bounds.x0, self.bounds.x1 - 1);
                            let y = clamp(y, self.bounds.y0, self.bounds.y1 - 1);
                            self.surface.get_pixel(x as u32, y as u32)
                        }
                        EdgeMode::Wrap => {
                            let wrap = |mut x, v| {
                                while x < 0 {
                                    x += v;
                                }
                                x % v
                            };

                            let x = self.bounds.x0 + wrap(x - self.bounds.x0, self.bounds.width());
                            let y = self.bounds.y0 + wrap(y - self.bounds.y0, self.bounds.height());
                            self.surface.get_pixel(x as u32, y as u32)
                        }
                    }
                } else {
                    self.surface.get_pixel(x as u32, y as u32)
                }
            };

            Some((self.x, self.y, get_pixel(self.x, self.y)))
        };

        if self.x + 1 == self.rectangle.x1 {
            self.x = self.rectangle.x0;
            self.y += 1;
        } else {
            self.x += 1;
        }

        rv
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::surface_utils::shared_surface::SurfaceType;

    #[test]
    fn pixels_count() {
        const WIDTH: i32 = 32;
        const HEIGHT: i32 = 64;

        let surface = SharedImageSurface::empty(WIDTH, HEIGHT, SurfaceType::SRgb).unwrap();

        // Full image.
        assert_eq!(Pixels::new(&surface).count(), (WIDTH * HEIGHT) as usize);

        // 1-wide column.
        let bounds = IRect::from_size(1, HEIGHT);
        assert_eq!(Pixels::within(&surface, bounds).count(), HEIGHT as usize);

        // 1-tall row.
        let bounds = IRect::from_size(WIDTH, 1);
        assert_eq!(Pixels::within(&surface, bounds).count(), WIDTH as usize);

        // 1×1.
        let bounds = IRect::from_size(1, 1);
        assert_eq!(Pixels::within(&surface, bounds).count(), 1);

        // Nothing (x0 == x1).
        let bounds = IRect::from_size(0, HEIGHT);
        assert_eq!(Pixels::within(&surface, bounds).count(), 0);

        // Nothing (y0 == y1).
        let bounds = IRect::from_size(WIDTH, 0);
        assert_eq!(Pixels::within(&surface, bounds).count(), 0);

        // Nothing (x0 == x1, y0 == y1).
        let bounds = IRect::new(0, 0, 0, 0);
        assert_eq!(Pixels::within(&surface, bounds).count(), 0);
    }

    #[test]
    fn pixel_rectangle() {
        const WIDTH: i32 = 32;
        const HEIGHT: i32 = 64;

        let surface = SharedImageSurface::empty(WIDTH, HEIGHT, SurfaceType::SRgb).unwrap();

        let rect_bounds = IRect::new(-8, -8, 8, 8);
        assert_eq!(
            PixelRectangle::new(&surface, rect_bounds, EdgeMode::None).count(),
            (16 * 16) as usize
        );
    }
}
