// Take a look at the license at the top of the repository in the LICENSE file.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub use cairo;
pub use glib;
pub use pango;
pub use pango_cairo_sys as ffi;

mod auto;

pub use crate::auto::*;
pub mod prelude;
pub mod functions {
    pub use super::auto::functions::*;
}

mod font_map;
