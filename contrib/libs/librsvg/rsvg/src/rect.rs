//! Types for rectangles.

use crate::coord_units::CoordUnits;
use crate::transform::Transform;

#[allow(clippy::module_inception)]
mod rect {
    use crate::float_eq_cairo::ApproxEqCairo;
    use core::ops::{Add, Range, Sub};
    use float_cmp::approx_eq;
    use num_traits::Zero;

    // Use our own min() and max() that are acceptable for floating point

    fn min<T: PartialOrd>(x: T, y: T) -> T {
        if x <= y { x } else { y }
    }

    fn max<T: PartialOrd>(x: T, y: T) -> T {
        if x >= y { x } else { y }
    }

    #[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Rect<T> {
        pub x0: T,
        pub y0: T,
        pub x1: T,
        pub y1: T,
    }

    impl<T> Rect<T> {
        #[inline]
        pub fn new(x0: T, y0: T, x1: T, y1: T) -> Self {
            Self { x0, y0, x1, y1 }
        }
    }

    impl<T> Rect<T>
    where
        T: Copy + PartialOrd + PartialEq + Add<T, Output = T> + Sub<T, Output = T> + Zero,
    {
        #[inline]
        pub fn from_size(w: T, h: T) -> Self {
            Self {
                x0: Zero::zero(),
                y0: Zero::zero(),
                x1: w,
                y1: h,
            }
        }

        #[inline]
        pub fn width(&self) -> T {
            self.x1 - self.x0
        }

        #[inline]
        pub fn height(&self) -> T {
            self.y1 - self.y0
        }

        #[inline]
        pub fn size(&self) -> (T, T) {
            (self.width(), self.height())
        }

        #[inline]
        pub fn x_range(&self) -> Range<T> {
            self.x0..self.x1
        }

        #[inline]
        pub fn y_range(&self) -> Range<T> {
            self.y0..self.y1
        }

        #[inline]
        pub fn contains(self, x: T, y: T) -> bool {
            x >= self.x0 && x < self.x1 && y >= self.y0 && y < self.y1
        }

        #[inline]
        pub fn translate(&self, by: (T, T)) -> Self {
            Self {
                x0: self.x0 + by.0,
                y0: self.y0 + by.1,
                x1: self.x1 + by.0,
                y1: self.y1 + by.1,
            }
        }

        #[inline]
        pub fn intersection(&self, rect: &Self) -> Option<Self> {
            let (x0, y0, x1, y1) = (
                max(self.x0, rect.x0),
                max(self.y0, rect.y0),
                min(self.x1, rect.x1),
                min(self.y1, rect.y1),
            );

            if x1 > x0 && y1 > y0 {
                Some(Self { x0, y0, x1, y1 })
            } else {
                None
            }
        }

        #[inline]
        pub fn union(&self, rect: &Self) -> Self {
            Self {
                x0: min(self.x0, rect.x0),
                y0: min(self.y0, rect.y0),
                x1: max(self.x1, rect.x1),
                y1: max(self.y1, rect.y1),
            }
        }
    }

    impl Rect<i32> {
        #[inline]
        pub fn is_empty(&self) -> bool {
            // Give an explicit type to the right hand side of the ==, since sometimes
            // type inference fails to figure it out.  I have no idea why.
            self.width() == <i32 as Zero>::zero() || self.height() == <i32 as Zero>::zero()
        }

        #[inline]
        pub fn scale(self, x: f64, y: f64) -> Self {
            Self {
                x0: (f64::from(self.x0) * x).floor() as i32,
                y0: (f64::from(self.y0) * y).floor() as i32,
                x1: (f64::from(self.x1) * x).ceil() as i32,
                y1: (f64::from(self.y1) * y).ceil() as i32,
            }
        }
    }

    impl Rect<f64> {
        #[inline]
        pub fn is_empty(&self) -> bool {
            self.width().approx_eq_cairo(0.0) || self.height().approx_eq_cairo(0.0)
        }

        pub fn approx_eq(&self, other: &Self) -> bool {
            // FIXME: this is super fishy; shouldn't we be using 2x the epsilon against the width/height
            // instead of the raw coordinates?
            approx_eq!(f64, self.x0, other.x0, epsilon = 0.0001)
                && approx_eq!(f64, self.y0, other.y0, epsilon = 0.0001)
                && approx_eq!(f64, self.x1, other.x1, epsilon = 0.0001)
                && approx_eq!(f64, self.y1, other.y1, epsilon = 0.00012)
        }
    }
}

pub type Rect = rect::Rect<f64>;

impl From<Rect> for IRect {
    #[inline]
    fn from(r: Rect) -> Self {
        Self {
            x0: r.x0.floor() as i32,
            y0: r.y0.floor() as i32,
            x1: r.x1.ceil() as i32,
            y1: r.y1.ceil() as i32,
        }
    }
}

impl From<cairo::Rectangle> for Rect {
    #[inline]
    fn from(r: cairo::Rectangle) -> Self {
        Self {
            x0: r.x(),
            y0: r.y(),
            x1: r.x() + r.width(),
            y1: r.y() + r.height(),
        }
    }
}

impl From<Rect> for cairo::Rectangle {
    #[inline]
    fn from(r: Rect) -> Self {
        Self::new(r.x0, r.y0, r.x1 - r.x0, r.y1 - r.y0)
    }
}

/// Creates a transform to map to a rectangle.
///
/// The rectangle is an `Option<Rect>` to indicate the possibility that there is no
/// bounding box from where the rectangle could be obtained.
///
/// This depends on a `CoordUnits` parameter.  When this is
/// `CoordUnits::ObjectBoundingBox`, the bounding box must not be empty, since the calling
/// code would then not have a usable size to work with.  In that case, if the bbox is
/// empty, this function returns `Err(())`.
///
/// Usually calling code can simply ignore the action it was about to take if this
/// function returns an error.
pub fn rect_to_transform(rect: &Option<Rect>, units: CoordUnits) -> Result<Transform, ()> {
    match units {
        CoordUnits::UserSpaceOnUse => Ok(Transform::identity()),
        CoordUnits::ObjectBoundingBox => {
            if rect.as_ref().is_none_or(|r| r.is_empty()) {
                Err(())
            } else {
                let r = rect.as_ref().unwrap();
                let t = Transform::new_unchecked(r.width(), 0.0, 0.0, r.height(), r.x0, r.y0);

                if t.is_invertible() { Ok(t) } else { Err(()) }
            }
        }
    }
}

pub type IRect = rect::Rect<i32>;

impl From<IRect> for Rect {
    #[inline]
    fn from(r: IRect) -> Self {
        Self {
            x0: f64::from(r.x0),
            y0: f64::from(r.y0),
            x1: f64::from(r.x1),
            y1: f64::from(r.y1),
        }
    }
}

impl From<IRect> for cairo::Rectangle {
    #[inline]
    fn from(r: IRect) -> Self {
        Self::new(
            f64::from(r.x0),
            f64::from(r.y0),
            f64::from(r.x1 - r.x0),
            f64::from(r.y1 - r.y0),
        )
    }
}
