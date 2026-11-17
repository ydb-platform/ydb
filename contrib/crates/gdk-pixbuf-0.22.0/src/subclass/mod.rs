// Take a look at the license at the top of the repository in the LICENSE file.

// rustdoc-stripper-ignore-next
//! Traits intended for creating custom types.

pub mod pixbuf_animation;
pub mod pixbuf_animation_iter;
pub mod pixbuf_loader;

pub mod prelude {
    pub use glib::subclass::prelude::*;

    pub use gio::subclass::prelude::*;

    pub use super::{
        pixbuf_animation::{PixbufAnimationImpl, PixbufAnimationImplExt},
        pixbuf_animation_iter::{PixbufAnimationIterImpl, PixbufAnimationIterImplExt},
        pixbuf_loader::{PixbufLoaderImpl, PixbufLoaderImplExt},
    };
}
