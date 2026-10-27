//! Handling of `preserveAspectRatio` values.
//!
//! This module handles `preserveAspectRatio` values [per the SVG specification][spec].
//! We have an [`AspectRatio`] struct which encapsulates such a value.
//!
//! ```
//! # use rsvg::doctest_only::AspectRatio;
//! # use rsvg::doctest_only::Parse;
//! assert_eq!(
//!     AspectRatio::parse_str("xMidYMid").unwrap(),
//!     AspectRatio::default()
//! );
//! ```
//!
//! [spec]: https://www.w3.org/TR/SVG/coords.html#PreserveAspectRatioAttribute

use cssparser::{BasicParseError, Parser};
use std::ops::Deref;

use crate::error::*;
use crate::parse_identifiers;
use crate::parsers::Parse;
use crate::rect::Rect;
use crate::transform::{Transform, ValidTransform};
use crate::viewbox::ViewBox;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
enum FitMode {
    #[default]
    Meet,
    Slice,
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
enum Align1D {
    Min,
    #[default]
    Mid,
    Max,
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
struct X(Align1D);
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
struct Y(Align1D);

impl Deref for X {
    type Target = Align1D;

    fn deref(&self) -> &Align1D {
        &self.0
    }
}

impl Deref for Y {
    type Target = Align1D;

    fn deref(&self) -> &Align1D {
        &self.0
    }
}

impl Align1D {
    fn compute(self, dest_pos: f64, dest_size: f64, obj_size: f64) -> f64 {
        match self {
            Align1D::Min => dest_pos,
            Align1D::Mid => dest_pos + (dest_size - obj_size) / 2.0,
            Align1D::Max => dest_pos + dest_size - obj_size,
        }
    }
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
struct Align {
    x: X,
    y: Y,
    fit: FitMode,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AspectRatio {
    defer: bool,
    align: Option<Align>,
}

impl Default for AspectRatio {
    fn default() -> AspectRatio {
        AspectRatio {
            defer: false,
            align: Some(Align::default()),
        }
    }
}

impl AspectRatio {
    /// Produces the equivalent of `preserveAspectRatio="none"`.
    pub fn none() -> AspectRatio {
        AspectRatio {
            defer: false,
            align: None,
        }
    }

    pub fn is_slice(&self) -> bool {
        matches!(
            self.align,
            Some(Align {
                fit: FitMode::Slice,
                ..
            })
        )
    }

    pub fn compute(&self, vbox: &ViewBox, viewport: &Rect) -> Rect {
        match self.align {
            None => *viewport,

            Some(Align { x, y, fit }) => {
                let (vb_width, vb_height) = vbox.size();
                let (vp_width, vp_height) = viewport.size();

                let w_factor = vp_width / vb_width;
                let h_factor = vp_height / vb_height;

                let factor = match fit {
                    FitMode::Meet => w_factor.min(h_factor),
                    FitMode::Slice => w_factor.max(h_factor),
                };

                let w = vb_width * factor;
                let h = vb_height * factor;

                let xpos = x.compute(viewport.x0, vp_width, w);
                let ypos = y.compute(viewport.y0, vp_height, h);

                Rect::new(xpos, ypos, xpos + w, ypos + h)
            }
        }
    }

    /// Computes the viewport to viewbox transformation.
    ///
    /// Given a viewport, returns a transformation that will create a coordinate
    /// space inside it.  The `(vbox.x0, vbox.y0)` will be mapped to the viewport's
    /// upper-left corner, and the `(vbox.x1, vbox.y1)` will be mapped to the viewport's
    /// lower-right corner.
    ///
    /// If the vbox or viewport are empty, returns `Ok(None)`.  Per the SVG spec, either
    /// of those mean that the corresponding element should not be rendered.
    ///
    /// If the vbox would create an invalid transform (say, a vbox with huge numbers that
    /// leads to a near-zero scaling transform), returns an `Err(())`.
    pub fn viewport_to_viewbox_transform(
        &self,
        vbox: Option<ViewBox>,
        viewport: &Rect,
    ) -> Result<Option<ValidTransform>, InvalidTransform> {
        // width or height set to 0 disables rendering of the element
        // https://www.w3.org/TR/SVG/struct.html#SVGElementWidthAttribute
        // https://www.w3.org/TR/SVG/struct.html#UseElementWidthAttribute
        // https://www.w3.org/TR/SVG/struct.html#ImageElementWidthAttribute
        // https://www.w3.org/TR/SVG/painting.html#MarkerWidthAttribute

        if viewport.is_empty() {
            return Ok(None);
        }

        // the preserveAspectRatio attribute is only used if viewBox is specified
        // https://www.w3.org/TR/SVG/coords.html#PreserveAspectRatioAttribute
        let transform = if let Some(vbox) = vbox {
            if vbox.is_empty() {
                // Width or height of 0 for the viewBox disables rendering of the element
                // https://www.w3.org/TR/SVG/coords.html#ViewBoxAttribute
                return Ok(None);
            } else {
                let r = self.compute(&vbox, viewport);
                Transform::new_translate(r.x0, r.y0)
                    .pre_scale(r.width() / vbox.width(), r.height() / vbox.height())
                    .pre_translate(-vbox.x0, -vbox.y0)
            }
        } else {
            Transform::new_translate(viewport.x0, viewport.y0)
        };

        ValidTransform::try_from(transform).map(Some)
    }
}

fn parse_align_xy<'i>(parser: &mut Parser<'i, '_>) -> Result<Option<(X, Y)>, BasicParseError<'i>> {
    use self::Align1D::*;

    parse_identifiers!(
        parser,

        "none" => None,

        "xMinYMin" => Some((X(Min), Y(Min))),
        "xMidYMin" => Some((X(Mid), Y(Min))),
        "xMaxYMin" => Some((X(Max), Y(Min))),

        "xMinYMid" => Some((X(Min), Y(Mid))),
        "xMidYMid" => Some((X(Mid), Y(Mid))),
        "xMaxYMid" => Some((X(Max), Y(Mid))),

        "xMinYMax" => Some((X(Min), Y(Max))),
        "xMidYMax" => Some((X(Mid), Y(Max))),
        "xMaxYMax" => Some((X(Max), Y(Max))),
    )
}

fn parse_fit_mode<'i>(parser: &mut Parser<'i, '_>) -> Result<FitMode, BasicParseError<'i>> {
    parse_identifiers!(
        parser,
        "meet" => FitMode::Meet,
        "slice" => FitMode::Slice,
    )
}

impl Parse for AspectRatio {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<AspectRatio, ParseError<'i>> {
        let defer = parser
            .try_parse(|p| p.expect_ident_matching("defer"))
            .is_ok();

        let align_xy = parser.try_parse(parse_align_xy)?;
        let fit = parser.try_parse(parse_fit_mode).unwrap_or_default();
        let align = align_xy.map(|(x, y)| Align { x, y, fit });

        Ok(AspectRatio { defer, align })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{assert_approx_eq_cairo, float_eq_cairo::ApproxEqCairo};

    #[test]
    fn aspect_ratio_none() {
        assert_eq!(AspectRatio::none(), AspectRatio::parse_str("none").unwrap());
    }

    #[test]
    fn parsing_invalid_strings_yields_error() {
        assert!(AspectRatio::parse_str("").is_err());
        assert!(AspectRatio::parse_str("defer").is_err());
        assert!(AspectRatio::parse_str("defer foo").is_err());
        assert!(AspectRatio::parse_str("defer xMidYMid foo").is_err());
        assert!(AspectRatio::parse_str("xMidYMid foo").is_err());
        assert!(AspectRatio::parse_str("defer xMidYMid meet foo").is_err());
    }

    #[test]
    fn parses_valid_strings() {
        assert_eq!(
            AspectRatio::parse_str("defer none").unwrap(),
            AspectRatio {
                defer: true,
                align: None,
            }
        );

        assert_eq!(
            AspectRatio::parse_str("xMidYMid").unwrap(),
            AspectRatio {
                defer: false,
                align: Some(Align {
                    x: X(Align1D::Mid),
                    y: Y(Align1D::Mid),
                    fit: FitMode::Meet,
                },),
            }
        );

        assert_eq!(
            AspectRatio::parse_str("defer xMidYMid").unwrap(),
            AspectRatio {
                defer: true,
                align: Some(Align {
                    x: X(Align1D::Mid),
                    y: Y(Align1D::Mid),
                    fit: FitMode::Meet,
                },),
            }
        );

        assert_eq!(
            AspectRatio::parse_str("defer xMinYMax").unwrap(),
            AspectRatio {
                defer: true,
                align: Some(Align {
                    x: X(Align1D::Min),
                    y: Y(Align1D::Max),
                    fit: FitMode::Meet,
                },),
            }
        );

        assert_eq!(
            AspectRatio::parse_str("defer xMaxYMid meet").unwrap(),
            AspectRatio {
                defer: true,
                align: Some(Align {
                    x: X(Align1D::Max),
                    y: Y(Align1D::Mid),
                    fit: FitMode::Meet,
                },),
            }
        );

        assert_eq!(
            AspectRatio::parse_str("defer xMinYMax slice").unwrap(),
            AspectRatio {
                defer: true,
                align: Some(Align {
                    x: X(Align1D::Min),
                    y: Y(Align1D::Max),
                    fit: FitMode::Slice,
                },),
            }
        );
    }

    fn assert_rect_equal(r1: &Rect, r2: &Rect) {
        assert_approx_eq_cairo!(r1.x0, r2.x0);
        assert_approx_eq_cairo!(r1.y0, r2.y0);
        assert_approx_eq_cairo!(r1.x1, r2.x1);
        assert_approx_eq_cairo!(r1.y1, r2.y1);
    }

    #[test]
    fn aligns() {
        let viewbox = ViewBox::from(Rect::from_size(1.0, 10.0));

        let foo = AspectRatio::parse_str("xMinYMin meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::from_size(0.1, 1.0));

        let foo = AspectRatio::parse_str("xMinYMin slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::from_size(10.0, 100.0));

        let foo = AspectRatio::parse_str("xMinYMid meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::from_size(0.1, 1.0));

        let foo = AspectRatio::parse_str("xMinYMid slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(0.0, -49.5, 10.0, 100.0 - 49.5));

        let foo = AspectRatio::parse_str("xMinYMax meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::from_size(0.1, 1.0));

        let foo = AspectRatio::parse_str("xMinYMax slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(0.0, -99.0, 10.0, 1.0));

        let foo = AspectRatio::parse_str("xMidYMin meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(4.95, 0.0, 4.95 + 0.1, 1.0));

        let foo = AspectRatio::parse_str("xMidYMin slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::from_size(10.0, 100.0));

        let foo = AspectRatio::parse_str("xMidYMid meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(4.95, 0.0, 4.95 + 0.1, 1.0));

        let foo = AspectRatio::parse_str("xMidYMid slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(0.0, -49.5, 10.0, 100.0 - 49.5));

        let foo = AspectRatio::parse_str("xMidYMax meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(4.95, 0.0, 4.95 + 0.1, 1.0));

        let foo = AspectRatio::parse_str("xMidYMax slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(0.0, -99.0, 10.0, 1.0));

        let foo = AspectRatio::parse_str("xMaxYMin meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(9.9, 0.0, 10.0, 1.0));

        let foo = AspectRatio::parse_str("xMaxYMin slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::from_size(10.0, 100.0));

        let foo = AspectRatio::parse_str("xMaxYMid meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(9.9, 0.0, 10.0, 1.0));

        let foo = AspectRatio::parse_str("xMaxYMid slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(0.0, -49.5, 10.0, 100.0 - 49.5));

        let foo = AspectRatio::parse_str("xMaxYMax meet").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(9.9, 0.0, 10.0, 1.0));

        let foo = AspectRatio::parse_str("xMaxYMax slice").unwrap();
        let foo = foo.compute(&viewbox, &Rect::from_size(10.0, 1.0));
        assert_rect_equal(&foo, &Rect::new(0.0, -99.0, 10.0, 1.0));
    }

    #[test]
    fn empty_viewport() {
        let a = AspectRatio::default();
        let t = a
            .viewport_to_viewbox_transform(
                Some(ViewBox::parse_str("10 10 40 40").unwrap()),
                &Rect::from_size(0.0, 0.0),
            )
            .unwrap();

        assert_eq!(t, None);
    }

    #[test]
    fn empty_viewbox() {
        let a = AspectRatio::default();
        let t = a
            .viewport_to_viewbox_transform(
                Some(ViewBox::parse_str("10 10 0 0").unwrap()),
                &Rect::from_size(10.0, 10.0),
            )
            .unwrap();

        assert_eq!(t, None);
    }

    #[test]
    fn valid_viewport_and_viewbox() {
        let a = AspectRatio::default();
        let t = a
            .viewport_to_viewbox_transform(
                Some(ViewBox::parse_str("10 10 40 40").unwrap()),
                &Rect::new(1.0, 1.0, 2.0, 2.0),
            )
            .unwrap();

        assert_eq!(
            t,
            Some(
                ValidTransform::try_from(
                    Transform::identity()
                        .pre_translate(1.0, 1.0)
                        .pre_scale(0.025, 0.025)
                        .pre_translate(-10.0, -10.0)
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn invalid_viewbox() {
        let a = AspectRatio::default();
        let t = a.viewport_to_viewbox_transform(
            Some(ViewBox::parse_str("0 0 6E20 540").unwrap()),
            &Rect::new(1.0, 1.0, 2.0, 2.0),
        );

        assert_eq!(t, Err(InvalidTransform));
    }
}
