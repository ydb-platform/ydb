// Take a look at the license at the top of the repository in the LICENSE file.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub use glib;
pub use pango_sys as ffi;

#[allow(clippy::too_many_arguments)]
#[allow(clippy::derived_hash_with_manual_eq)]
#[allow(clippy::missing_safety_doc)]
mod auto;
pub use crate::{auto::*, functions::*};

#[doc(alias = "PANGO_SCALE")]
pub const SCALE: i32 = ffi::PANGO_SCALE;
#[doc(alias = "PANGO_ANALYSIS_FLAG_CENTERED_BASELINE")]
pub const ANALYSIS_FLAG_CENTERED_BASELINE: i32 = ffi::PANGO_ANALYSIS_FLAG_CENTERED_BASELINE;
#[doc(alias = "PANGO_ANALYSIS_FLAG_IS_ELLIPSIS")]
pub const ANALYSIS_FLAG_IS_ELLIPSIS: i32 = ffi::PANGO_ANALYSIS_FLAG_IS_ELLIPSIS;
#[doc(alias = "PANGO_ANALYSIS_FLAG_NEED_HYPHEN")]
pub const ANALYSIS_FLAG_NEED_HYPHEN: i32 = ffi::PANGO_ANALYSIS_FLAG_NEED_HYPHEN;
#[doc(alias = "PANGO_ATTR_INDEX_FROM_TEXT_BEGINNING")]
pub const ATTR_INDEX_FROM_TEXT_BEGINNING: u32 = ffi::PANGO_ATTR_INDEX_FROM_TEXT_BEGINNING;
#[doc(alias = "PANGO_ATTR_INDEX_TO_TEXT_END")]
pub const ATTR_INDEX_TO_TEXT_END: u32 = ffi::PANGO_ATTR_INDEX_TO_TEXT_END;
#[doc(alias = "PANGO_GLYPH_EMPTY")]
pub const GLYPH_EMPTY: Glyph = ffi::PANGO_GLYPH_EMPTY;
#[doc(alias = "PANGO_GLYPH_INVALID_INPUT")]
pub const GLYPH_INVALID_INPUT: Glyph = ffi::PANGO_GLYPH_INVALID_INPUT;
#[doc(alias = "PANGO_GLYPH_UNKNOWN_FLAG")]
pub const GLYPH_UNKNOWN_FLAG: Glyph = ffi::PANGO_GLYPH_UNKNOWN_FLAG;

// rustdoc-stripper-ignore-next
/// The scale factor for three shrinking steps (1 / (1.2 * 1.2 * 1.2)).
pub const SCALE_XX_SMALL: f64 = 0.5787037037037;

// rustdoc-stripper-ignore-next
/// The scale factor for two shrinking steps (1 / (1.2 * 1.2)).
pub const SCALE_X_SMALL: f64 = 0.6944444444444;

// rustdoc-stripper-ignore-next
/// The scale factor for one shrinking step (1 / 1.2).
pub const SCALE_SMALL: f64 = 0.8333333333333;

// rustdoc-stripper-ignore-next
/// The scale factor for normal size (1.0).
pub const SCALE_MEDIUM: f64 = 1.0;

// rustdoc-stripper-ignore-next
/// The scale factor for one magnification step (1.2).
pub const SCALE_LARGE: f64 = 1.2;

// rustdoc-stripper-ignore-next
/// The scale factor for two magnification steps (1.2 * 1.2).
pub const SCALE_X_LARGE: f64 = 1.44;

// rustdoc-stripper-ignore-next
/// The scale factor for three magnification steps (1.2 * 1.2 * 1.2).
pub const SCALE_XX_LARGE: f64 = 1.728;

pub mod prelude;

#[macro_use]
mod attribute;
pub use attribute::IsAttribute;

mod analysis;
pub use analysis::Analysis;
mod attr_class;
pub use attr_class::AttrClass;
mod attr_color;
pub use attr_color::AttrColor;
mod attr_float;
pub use attr_float::AttrFloat;
mod attr_font_desc;
pub use attr_font_desc::AttrFontDesc;
mod attr_font_features;
pub use attr_font_features::AttrFontFeatures;
mod attr_int;
pub use attr_int::AttrInt;
mod attr_iterator;
pub use attr_iterator::{AttrIntoIter, AttrIterator};
mod attr_language;
pub use attr_language::AttrLanguage;
mod attr_list;
mod attr_shape;
pub use attr_shape::AttrShape;
mod attr_size;
pub use attr_size::AttrSize;
mod attr_string;
pub use attr_string::AttrString;
mod color;
mod coverage;
pub use coverage::Coverage;
mod enums;
mod functions;
mod glyph_geometry;
pub use glyph_geometry::GlyphGeometry;
mod glyph_info;
pub use glyph_info::GlyphInfo;
mod glyph_item;
mod glyph_item_iter;
pub use glyph_item_iter::{GlyphItemIntoIter, GlyphItemIter};
mod glyph_string;
mod item;
mod language;
mod layout;
pub use layout::HitPosition;
mod matrix;
mod rectangle;
pub use rectangle::Rectangle;
mod script_iter;
pub use script_iter::{ScriptIntoIter, ScriptIter};
mod tab_array;
