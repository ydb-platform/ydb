// Take a look at the license at the top of the repository in the LICENSE file.

// rustdoc-stripper-ignore-next
//! Traits intended for blanket imports.

#[doc(hidden)]
pub use glib::prelude::*;

#[doc(hidden)]
pub use gio::prelude::*;

pub use crate::{auto::traits::*, pixbuf_animation::PixbufAnimationExtManual};
