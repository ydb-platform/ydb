// Take a look at the license at the top of the repository in the LICENSE file.

#![allow(clippy::manual_c_str_literals)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub use gdk_pixbuf_sys as ffi;
pub use gio;
pub use glib;

#[allow(clippy::too_many_arguments)]
#[allow(unused_imports)]
#[allow(clippy::empty_line_after_outer_attr)]
mod auto;

pub mod subclass;

mod pixbuf;
mod pixbuf_animation;
mod pixbuf_animation_iter;
pub mod prelude;

pub use self::pixbuf_animation_iter::PixbufAnimationIter;
pub use crate::auto::*;
