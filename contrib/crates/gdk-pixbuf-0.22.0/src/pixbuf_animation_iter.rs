// Take a look at the license at the top of the repository in the LICENSE file.

use std::time::{Duration, SystemTime};

use glib::translate::*;

use super::{Pixbuf, ffi};

glib::wrapper! {
    #[doc(alias = "GdkPixbufAnimationIter")]
    pub struct PixbufAnimationIter(Object<ffi::GdkPixbufAnimationIter, ffi::GdkPixbufAnimationIterClass>);

    match fn {
        type_ => || ffi::gdk_pixbuf_animation_iter_get_type(),
    }
}

impl PixbufAnimationIter {
    #[doc(alias = "gdk_pixbuf_animation_iter_advance")]
    pub fn advance(&self, current_time: SystemTime) -> bool {
        let diff = current_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to convert time");

        unsafe {
            from_glib(ffi::gdk_pixbuf_animation_iter_advance(
                self.to_glib_none().0,
                &glib::ffi::GTimeVal {
                    tv_sec: diff.as_secs() as _,
                    tv_usec: diff.subsec_micros() as _,
                },
            ))
        }
    }

    #[doc(alias = "gdk_pixbuf_animation_iter_get_pixbuf")]
    #[doc(alias = "get_pixbuf")]
    pub fn pixbuf(&self) -> Pixbuf {
        unsafe {
            from_glib_none(ffi::gdk_pixbuf_animation_iter_get_pixbuf(
                self.to_glib_none().0,
            ))
        }
    }

    #[doc(alias = "gdk_pixbuf_animation_iter_get_delay_time")]
    #[doc(alias = "get_delay_time")]
    pub fn delay_time(&self) -> Option<Duration> {
        unsafe {
            let res = ffi::gdk_pixbuf_animation_iter_get_delay_time(self.to_glib_none().0);

            if res < 0 {
                None
            } else {
                Some(Duration::from_millis(res as u64))
            }
        }
    }

    #[doc(alias = "gdk_pixbuf_animation_iter_on_currently_loading_frame")]
    pub fn on_currently_loading_frame(&self) -> bool {
        unsafe {
            from_glib(ffi::gdk_pixbuf_animation_iter_on_currently_loading_frame(
                self.to_glib_none().0,
            ))
        }
    }
}
