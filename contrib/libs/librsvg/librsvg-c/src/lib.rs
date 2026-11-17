//! C API for librsvg, based on GObject.
//!
//! The main API is in the [`handle`] module.  The other modules have utility functions
//! and the legacy pixbuf-based API in the `pixbuf_utils` module.

#![allow(clippy::missing_safety_doc)]

#[rustfmt::skip]
pub use handle::{
    rsvg_error_get_type,
    rsvg_handle_close,
    rsvg_handle_flags_get_type,
    rsvg_handle_get_base_uri,
    rsvg_handle_get_dimensions,
    rsvg_handle_get_dimensions_sub,
    rsvg_handle_get_geometry_for_element,
    rsvg_handle_get_geometry_for_layer,
    rsvg_handle_get_intrinsic_dimensions,
    rsvg_handle_get_intrinsic_size_in_pixels,
    rsvg_handle_get_position_sub,
    rsvg_handle_has_sub,
    rsvg_handle_internal_set_testing,
    rsvg_handle_new_from_data,
    rsvg_handle_new_from_file,
    rsvg_handle_new_from_gfile_sync,
    rsvg_handle_new_from_stream_sync,
    rsvg_handle_new_with_flags,
    rsvg_handle_read_stream_sync,
    rsvg_handle_render_cairo_sub,
    rsvg_handle_render_element,
    rsvg_handle_render_document,
    rsvg_handle_render_layer,
    rsvg_handle_set_base_gfile,
    rsvg_handle_set_base_uri,
    rsvg_handle_set_dpi_x_y,
    rsvg_handle_set_size_callback,
    rsvg_handle_write,
};

#[cfg(feature = "pixbuf")]
pub use handle::rsvg_handle_get_pixbuf_sub;

pub use dpi::{rsvg_set_default_dpi, rsvg_set_default_dpi_x_y};

#[rustfmt::skip]
#[cfg(feature = "pixbuf")]
pub use pixbuf_utils::{
    rsvg_pixbuf_from_file,
    rsvg_pixbuf_from_file_at_max_size,
    rsvg_pixbuf_from_file_at_size,
    rsvg_pixbuf_from_file_at_zoom,
    rsvg_pixbuf_from_file_at_zoom_with_max,
};

#[macro_use]
mod messages;

mod dpi;
pub mod handle;

#[cfg(feature = "pixbuf")]
pub mod pixbuf_utils;
pub mod sizing;
