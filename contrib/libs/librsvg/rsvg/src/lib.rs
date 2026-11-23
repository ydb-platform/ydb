//! Load and render SVG images into Cairo surfaces.
//!
//! This crate can load SVG images and render them to Cairo surfaces,
//! using a mixture of SVG's [static mode] and [secure static mode].
//! Librsvg does not do animation nor scripting, and can load
//! references to external data only in some situations; see below.
//!
//! Librsvg supports reading [SVG 1.1] data, and is gradually adding
//! support for features in [SVG 2].  Librsvg also supports SVGZ
//! files, which are just an SVG stream compressed with the GZIP
//! algorithm.
//!
//! # Basic usage
//!
//! * Create a [`Loader`] struct.
//! * Get an [`SvgHandle`] from the [`Loader`].
//! * Create a [`CairoRenderer`] for the [`SvgHandle`] and render to a Cairo context.
//!
//! You can put the following in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! librsvg = "2.62.0"
//! cairo-rs = "0.22.0"
//! gio = "0.22.0"   # only if you need streams
//! ```
//!
//! # Example
//!
//! ```
//! const WIDTH: i32 = 640;
//! const HEIGHT: i32 = 480;
//!
//! fn main() {
//!     // Loading from a file
//!
//!     let handle = rsvg::Loader::new().read_path("example.svg").unwrap();
//!
//!     let surface = cairo::ImageSurface::create(cairo::Format::ARgb32, WIDTH, HEIGHT).unwrap();
//!     let cr = cairo::Context::new(&surface).expect("Failed to create a cairo context");
//!
//!     let renderer = rsvg::CairoRenderer::new(&handle);
//!     renderer.render_document(
//!         &cr,
//!         &cairo::Rectangle::new(0.0, 0.0, f64::from(WIDTH), f64::from(HEIGHT))
//!     ).unwrap();
//!
//!     // Loading from a static SVG asset
//!
//!     let bytes = glib::Bytes::from_static(
//!         br#"<?xml version="1.0" encoding="UTF-8"?>
//!             <svg xmlns="http://www.w3.org/2000/svg" width="50" height="50">
//!                 <rect id="foo" x="10" y="10" width="30" height="30"/>
//!             </svg>
//!         "#
//!     );
//!     let stream = gio::MemoryInputStream::from_bytes(&bytes);
//!
//!     let handle = rsvg::Loader::new().read_stream(
//!         &stream,
//!         None::<&gio::File>,          // no base file as this document has no references
//!         None::<&gio::Cancellable>,   // no cancellable
//!     ).unwrap();
//! }
//! ```
//!
//! # The "base file" and resolving references to external files
//!
//! When you load an SVG, librsvg needs to know the location of the "base file"
//! for it.  This is so that librsvg can determine the location of referenced
//! entities.  For example, say you have an SVG in <filename>/foo/bar/foo.svg</filename>
//! and that it has an image element like this:
//!
//! ```xml
//! <image href="resources/foo.png" .../>
//! ```
//!
//! In this case, librsvg needs to know the location of the toplevel
//! `/foo/bar/foo.svg` so that it can generate the appropriate
//! reference to `/foo/bar/resources/foo.png`.
//!
//! ## Security and locations of referenced files
//!
//! When processing an SVG, librsvg will only load referenced files if
//! they are in the same directory as the base file, or in a
//! subdirectory of it.  That is, if the base file is
//! `/foo/bar/baz.svg`, then librsvg will only try to load referenced
//! files (from SVG's `<image>` element, for example, or from content
//! included through XML entities) if those files are in `/foo/bar/*`
//! or in `/foo/bar/*/.../*`.  This is so that malicious SVG documents
//! cannot include files that are in a directory above.
//!
//! The full set of rules for deciding which URLs may be loaded is as follows;
//! they are applied in order.  A referenced URL will not be loaded as soon as
//! one of these rules fails:
//!
//! 1. All `data:` URLs may be loaded.  These are sometimes used to
//!    include raster image data, encoded as base-64, directly in an SVG
//!    file.
//!
//! 2. URLs with queries ("?") or fragment identifiers ("#") are not allowed.
//!
//! 3. All URL schemes other than data: in references require a base URL.  For
//!    example, this means that if you load an SVG with [`Loader::read_stream`]
//!    without providing a `base_file`, then any referenced files will not
//!    be allowed (e.g. raster images to be loaded from other files will
//!    not work).
//!
//! 4. If referenced URLs are absolute, rather than relative, then
//!    they must have the same scheme as the base URL.  For example, if
//!    the base URL has a "`file`" scheme, then all URL references inside
//!    the SVG must also have the "`file`" scheme, or be relative
//!    references which will be resolved against the base URL.
//!
//! 5. If referenced URLs have a "`resource`" scheme, that is, if they
//!    are included into your binary program with GLib's resource
//!    mechanism, they are allowed to be loaded (provided that the base
//!    URL is also a "`resource`", per the previous rule).
//!
//! 6. Otherwise, non-`file` schemes are not allowed.  For example,
//!    librsvg will not load `http` resources, to keep malicious SVG data
//!    from "phoning home".
//!
//! 7. URLs with a `file` scheme are rejected if they contain a hostname, as in
//!    `file://hostname/some/directory/foo.svg`.  Windows UNC paths with a hostname are
//!    also rejected.  This is to prevent documents from trying to access resources on
//!    other machines.
//!
//! 8. A relative URL must resolve to the same directory as the base
//!    URL, or to one of its subdirectories.  Librsvg will canonicalize
//!    filenames, by removing "`..`" path components and resolving symbolic
//!    links, to decide whether files meet these conditions.
//!
//! [static mode]: https://www.w3.org/TR/SVG2/conform.html#static-mode
//! [secure static mode]: https://www.w3.org/TR/SVG2/conform.html#secure-static-mode
//! [SVG 1.1]: https://www.w3.org/TR/SVG11/
//! [SVG 2]: https://www.w3.org/TR/SVG2/

#![doc(html_logo_url = "https://gnome.pages.gitlab.gnome.org/librsvg/Rsvg-2.0/librsvg-r.svg")]
#![allow(rustdoc::private_intra_doc_links)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![warn(nonstandard_style, rust_2018_idioms, unused)]
// Some lints no longer exist
#![warn(renamed_and_removed_lints)]
// Standalone lints
#![warn(trivial_casts, trivial_numeric_casts)]
// The public API is exported here
pub use crate::api::*;

mod accept_language;
mod angle;
mod api;
mod aspect_ratio;
mod bbox;
mod cairo_path;
mod color;
mod cond;
mod coord_units;
mod css;
mod dasharray;
mod document;
mod dpi;
mod drawing_ctx;
mod element;
mod error;
mod filter;
mod filter_func;
mod filters;
mod float_eq_cairo;
mod font_props;
mod gradient;
mod href;
mod image;
mod io;
mod iri;
mod layout;
mod length;
mod limits;
mod log;
mod marker;
mod node;
mod paint_server;
mod parsers;
mod path_builder;
mod path_parser;
mod pattern;
mod properties;
mod property_defs;
mod property_macros;
mod rect;
mod session;
mod shapes;
mod space;
mod structure;
mod style;
mod surface_utils;
mod text;
mod text2;
mod transform;
mod unit_interval;
mod url_resolver;
mod util;
mod viewbox;
mod xml;

#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub mod test_utils;

#[doc(hidden)]
pub mod bench_only {
    pub use crate::filters::lighting::Normal;
    pub use crate::path_builder::PathBuilder;
    pub use crate::path_parser::Lexer;
    pub use crate::rect::IRect;
    pub use crate::surface_utils::{
        EdgeMode, ImageSurfaceDataExt, Pixel, PixelOps,
        iterators::{PixelRectangle, Pixels},
        shared_surface::{
            AlphaOnly, ExclusiveImageSurface, Horizontal, NotAlphaOnly, SharedImageSurface,
            SurfaceType, Vertical, composite_arithmetic,
        },
        srgb::{linearize, map_unpremultiplied_components_loop},
    };
}

#[doc(hidden)]
#[cfg(feature = "capi")]
pub mod c_api_only {
    pub use crate::dpi::Dpi;
    pub use crate::rsvg_log;
    pub use crate::session::Session;
    pub use crate::surface_utils::shared_surface::{SharedImageSurface, SurfaceType};
    pub use crate::surface_utils::{Pixel, PixelOps, ToPixel};
}

#[doc(hidden)]
pub mod doctest_only {
    pub use crate::aspect_ratio::AspectRatio;
    pub use crate::error::AttributeResultExt;
    pub use crate::error::ElementError;
    pub use crate::error::ValueErrorKind;
    pub use crate::href::is_href;
    pub use crate::href::set_href;
    pub use crate::length::{Both, CssLength, Horizontal, Length, LengthUnit, ULength, Vertical};
    pub use crate::parsers::{Parse, ParseValue};
}

#[doc(hidden)]
pub mod rsvg_convert_only {
    pub use crate::aspect_ratio::AspectRatio;
    pub use crate::color::Color;
    pub use crate::dpi::Dpi;
    pub use crate::drawing_ctx::set_source_color_on_cairo;
    pub use crate::error::ParseError;
    pub use crate::length::{
        CssLength, Horizontal, Length, Normalize, NormalizeParams, Signed, ULength, Unsigned,
        Validate, Vertical,
    };
    pub use crate::parsers::{Parse, ParseValue};
    pub use crate::rect::Rect;
    pub use crate::surface_utils::shared_surface::{SharedImageSurface, SurfaceType};
    pub use crate::viewbox::ViewBox;
}

#[doc(hidden)]
pub mod tests_only {
    pub use crate::rect::Rect;
    pub use crate::surface_utils::shared_surface::{SharedImageSurface, SurfaceType};
}
