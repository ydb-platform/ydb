//! Parser for the `viewBox` attribute.

use cssparser::Parser;
use std::ops::Deref;

use crate::error::*;
use crate::parsers::{CommaSeparatedList, Parse};
use crate::rect::Rect;

/// Newtype around a [`Rect`], used to represent the `viewBox` attribute.
///
/// A `ViewBox` is a new user-space coordinate system mapped onto the rectangle defined by
/// the current viewport.  See <https://www.w3.org/TR/SVG2/coords.html#ViewBoxAttribute>
///
/// `ViewBox` derefs to [`Rect`], so you can use [`Rect`]'s methods and fields directly like
/// `vbox.x0` or `vbox.width()`.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct ViewBox(Rect);

impl Deref for ViewBox {
    type Target = Rect;

    fn deref(&self) -> &Rect {
        &self.0
    }
}

impl From<Rect> for ViewBox {
    fn from(r: Rect) -> ViewBox {
        ViewBox(r)
    }
}

impl Parse for ViewBox {
    // Parse a viewBox attribute
    // https://www.w3.org/TR/SVG/coords.html#ViewBoxAttribute
    //
    // viewBox: double [,] double [,] double [,] double [,]
    //
    // x, y, w, h
    //
    // Where w and h must be nonnegative.
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<ViewBox, ParseError<'i>> {
        let loc = parser.current_source_location();

        let CommaSeparatedList::<f64, 4, 4>(v) = CommaSeparatedList::parse(parser)?;
        let (x, y, width, height) = (v[0], v[1], v[2], v[3]);

        if width >= 0.0 && height >= 0.0 {
            Ok(ViewBox(Rect::new(x, y, x + width, y + height)))
        } else {
            Err(loc.new_custom_error(ValueErrorKind::value_error(
                "width and height must not be negative",
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_viewboxes() {
        assert_eq!(
            ViewBox::parse_str("  1 2 3 4").unwrap(),
            ViewBox(Rect::new(1.0, 2.0, 4.0, 6.0))
        );

        assert_eq!(
            ViewBox::parse_str(" -1.5 -2.5e1,34,56e2  ").unwrap(),
            ViewBox(Rect::new(-1.5, -25.0, 32.5, 5575.0))
        );
    }

    #[test]
    fn parsing_invalid_viewboxes_yields_error() {
        assert!(ViewBox::parse_str("").is_err());
        assert!(ViewBox::parse_str(" 1,2,-3,-4 ").is_err());
        assert!(ViewBox::parse_str("qwerasdfzxcv").is_err());
        assert!(ViewBox::parse_str(" 1 2 3 4   5").is_err());
        assert!(ViewBox::parse_str(" 1 2 foo 3 4").is_err());

        // https://gitlab.gnome.org/GNOME/librsvg/issues/344
        assert!(ViewBox::parse_str("0 0 9E80.7").is_err());
    }
}
