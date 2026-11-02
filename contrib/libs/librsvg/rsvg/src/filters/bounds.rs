//! Filter primitive subregion computation.
use crate::rect::Rect;
use crate::transform::Transform;

use super::context::{FilterContext, FilterInput};

/// A helper type for filter primitive subregion computation.
pub struct BoundsBuilder {
    /// Filter primitive attributes.
    x: Option<f64>,
    y: Option<f64>,
    width: Option<f64>,
    height: Option<f64>,

    /// The transform to use when generating the rect
    transform: Transform,

    /// The inverse transform used when adding rects
    inverse: Transform,

    /// Whether one of the input nodes is standard input.
    standard_input_was_referenced: bool,

    /// The current bounding rectangle.
    rect: Option<Rect>,
}

/// A filter primitive's subregion.
#[derive(Debug)]
pub struct Bounds {
    /// Filter primitive attributes.
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub width: Option<f64>,
    pub height: Option<f64>,

    /// Primitive's subregion, clipped to the filter effects region.
    pub clipped: Rect,

    /// Primitive's subregion, unclipped.
    pub unclipped: Rect,
}

impl BoundsBuilder {
    /// Constructs a new `BoundsBuilder`.
    #[inline]
    pub fn new(
        x: Option<f64>,
        y: Option<f64>,
        width: Option<f64>,
        height: Option<f64>,
        transform: Transform,
    ) -> Self {
        // We panic if transform is not invertible. This is checked in the caller.
        Self {
            x,
            y,
            width,
            height,
            transform,
            inverse: transform.invert().unwrap(),
            standard_input_was_referenced: false,
            rect: None,
        }
    }

    /// Adds a filter primitive input to the bounding box.
    #[inline]
    pub fn add_input(mut self, input: &FilterInput) -> Self {
        // If a standard input was referenced, the default value is the filter effects region
        // regardless of other referenced inputs. This means we can skip computing the bounds.
        if self.standard_input_was_referenced {
            return self;
        }

        match *input {
            FilterInput::StandardInput(_) => {
                self.standard_input_was_referenced = true;
            }
            FilterInput::PrimitiveOutput(ref output) => {
                let input_rect = self.inverse.transform_rect(&Rect::from(output.bounds));
                self.rect = Some(self.rect.map_or(input_rect, |r| input_rect.union(&r)));
            }
        }

        self
    }

    /// Returns the final exact bounds, both with and without clipping to the effects region.
    pub fn compute(self, ctx: &FilterContext) -> Bounds {
        let effects_region = ctx.effects_region();

        // The default value is the filter effects region converted into
        // the ptimitive coordinate system.
        let mut rect = match self.rect {
            Some(r) if !self.standard_input_was_referenced => r,
            _ => self.inverse.transform_rect(&effects_region),
        };

        // If any of the properties were specified, we need to respect them.
        // These replacements are possible because of the primitive coordinate system.
        if self.x.is_some() || self.y.is_some() || self.width.is_some() || self.height.is_some() {
            if let Some(x) = self.x {
                let w = rect.width();
                rect.x0 = x;
                rect.x1 = rect.x0 + w;
            }
            if let Some(y) = self.y {
                let h = rect.height();
                rect.y0 = y;
                rect.y1 = rect.y0 + h;
            }
            if let Some(width) = self.width {
                rect.x1 = rect.x0 + width;
            }
            if let Some(height) = self.height {
                rect.y1 = rect.y0 + height;
            }
        }

        // Convert into the surface coordinate system.
        let unclipped = self.transform.transform_rect(&rect);

        let clipped = unclipped.intersection(&effects_region).unwrap_or_default();

        Bounds {
            x: self.x,
            y: self.y,
            width: self.width,
            height: self.height,
            clipped,
            unclipped,
        }
    }
}
