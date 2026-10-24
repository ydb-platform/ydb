// Take a look at the license at the top of the repository in the LICENSE file.

use std::{ptr, time::SystemTime};

use glib::{prelude::*, translate::*};

use crate::{PixbufAnimation, PixbufAnimationIter, ffi};

pub trait PixbufAnimationExtManual: IsA<PixbufAnimation> + 'static {
    #[doc(alias = "gdk_pixbuf_animation_get_iter")]
    #[doc(alias = "get_iter")]
    fn iter(&self, start_time: Option<SystemTime>) -> PixbufAnimationIter {
        let start_time = start_time.map(|s| {
            let diff = s
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("failed to convert time");
            glib::ffi::GTimeVal {
                tv_sec: diff.as_secs() as _,
                tv_usec: diff.subsec_micros() as _,
            }
        });

        unsafe {
            from_glib_full(ffi::gdk_pixbuf_animation_get_iter(
                self.as_ref().to_glib_none().0,
                start_time
                    .as_ref()
                    .map(|t| t as *const glib::ffi::GTimeVal)
                    .unwrap_or(ptr::null()),
            ))
        }
    }
}

impl<O: IsA<PixbufAnimation>> PixbufAnimationExtManual for O {}
