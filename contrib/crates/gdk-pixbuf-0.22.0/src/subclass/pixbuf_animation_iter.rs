// Take a look at the license at the top of the repository in the LICENSE file.

// rustdoc-stripper-ignore-next
//! Traits intended for subclassing [`PixbufAnimationIter`].

use std::{
    sync::OnceLock,
    time::{Duration, SystemTime},
};

use glib::{prelude::*, subclass::prelude::*, translate::*};

use crate::{Pixbuf, PixbufAnimationIter, ffi};

pub trait PixbufAnimationIterImpl:
    ObjectImpl + ObjectSubclass<Type: IsA<PixbufAnimationIter>>
{
    // rustdoc-stripper-ignore-next
    /// Time in milliseconds, returning `None` implies showing the same pixbuf forever.
    fn delay_time(&self) -> Option<Duration> {
        self.parent_delay_time()
    }

    fn pixbuf(&self) -> Pixbuf {
        self.parent_pixbuf()
    }

    fn on_currently_loading_frame(&self) -> bool {
        self.parent_on_currently_loading_frame()
    }

    fn advance(&self, current_time: SystemTime) -> bool {
        self.parent_advance(current_time)
    }
}

pub trait PixbufAnimationIterImplExt: PixbufAnimationIterImpl {
    fn parent_delay_time(&self) -> Option<Duration> {
        unsafe {
            let data = Self::type_data();
            let parent_class =
                data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationIterClass;
            let f = (*parent_class)
                .get_delay_time
                .expect("No parent class implementation for \"get_delay_time\"");

            let time = f(self
                .obj()
                .unsafe_cast_ref::<PixbufAnimationIter>()
                .to_glib_none()
                .0);
            if time < 0 {
                None
            } else {
                Some(Duration::from_millis(time as u64))
            }
        }
    }

    fn parent_pixbuf(&self) -> Pixbuf {
        unsafe {
            let data = Self::type_data();
            let parent_class =
                data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationIterClass;
            let f = (*parent_class)
                .get_pixbuf
                .expect("No parent class implementation for \"get_pixbuf\"");

            from_glib_none(f(self
                .obj()
                .unsafe_cast_ref::<PixbufAnimationIter>()
                .to_glib_none()
                .0))
        }
    }

    fn parent_on_currently_loading_frame(&self) -> bool {
        unsafe {
            let data = Self::type_data();
            let parent_class =
                data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationIterClass;
            let f = (*parent_class)
                .on_currently_loading_frame
                .expect("No parent class implementation for \"on_currently_loading_frame\"");

            from_glib(f(self
                .obj()
                .unsafe_cast_ref::<PixbufAnimationIter>()
                .to_glib_none()
                .0))
        }
    }

    fn parent_advance(&self, current_time: SystemTime) -> bool {
        unsafe {
            let data = Self::type_data();
            let parent_class =
                data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationIterClass;
            let f = (*parent_class)
                .advance
                .expect("No parent class implementation for \"advance\"");

            let diff = current_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("failed to convert time");
            let time = glib::ffi::GTimeVal {
                tv_sec: diff.as_secs() as _,
                tv_usec: diff.subsec_micros() as _,
            };
            from_glib(f(
                self.obj()
                    .unsafe_cast_ref::<PixbufAnimationIter>()
                    .to_glib_none()
                    .0,
                &time,
            ))
        }
    }
}

impl<T: PixbufAnimationIterImpl> PixbufAnimationIterImplExt for T {}

unsafe impl<T: PixbufAnimationIterImpl> IsSubclassable<T> for PixbufAnimationIter {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.get_delay_time = Some(animation_iter_get_delay_time::<T>);
        klass.get_pixbuf = Some(animation_iter_get_pixbuf::<T>);
        klass.on_currently_loading_frame = Some(animation_iter_on_currently_loading_frame::<T>);
        klass.advance = Some(animation_iter_advance::<T>);
    }
}

unsafe extern "C" fn animation_iter_get_delay_time<T: PixbufAnimationIterImpl>(
    ptr: *mut ffi::GdkPixbufAnimationIter,
) -> i32 {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.delay_time().map(|t| t.as_millis() as i32).unwrap_or(-1)
    }
}

unsafe extern "C" fn animation_iter_get_pixbuf<T: PixbufAnimationIterImpl>(
    ptr: *mut ffi::GdkPixbufAnimationIter,
) -> *mut ffi::GdkPixbuf {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let pixbuf = imp.pixbuf();
        // Ensure that the pixbuf stays alive until the next call
        let pixbuf_quark = {
            static QUARK: OnceLock<glib::Quark> = OnceLock::new();
            *QUARK.get_or_init(|| glib::Quark::from_str("gtk-rs-subclass-pixbuf"))
        };
        imp.obj().set_qdata(pixbuf_quark, pixbuf.clone());
        pixbuf.to_glib_none().0
    }
}

unsafe extern "C" fn animation_iter_on_currently_loading_frame<T: PixbufAnimationIterImpl>(
    ptr: *mut ffi::GdkPixbufAnimationIter,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.on_currently_loading_frame().into_glib()
    }
}

unsafe extern "C" fn animation_iter_advance<T: PixbufAnimationIterImpl>(
    ptr: *mut ffi::GdkPixbufAnimationIter,
    current_time_ptr: *const glib::ffi::GTimeVal,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let current_time = SystemTime::UNIX_EPOCH
            + Duration::from_secs((*current_time_ptr).tv_sec.try_into().unwrap())
            + Duration::from_micros((*current_time_ptr).tv_usec.try_into().unwrap());

        imp.advance(current_time).into_glib()
    }
}
