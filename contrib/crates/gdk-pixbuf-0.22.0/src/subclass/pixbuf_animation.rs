// Take a look at the license at the top of the repository in the LICENSE file.

// rustdoc-stripper-ignore-next
//! Traits intended for subclassing [`PixbufAnimation`].

use std::{
    mem::MaybeUninit,
    sync::OnceLock,
    time::{Duration, SystemTime},
};

use glib::{prelude::*, subclass::prelude::*, translate::*};

use crate::{Pixbuf, PixbufAnimation, PixbufAnimationIter, ffi};

pub trait PixbufAnimationImpl: ObjectImpl + ObjectSubclass<Type: IsA<PixbufAnimation>> {
    fn is_static_image(&self) -> bool {
        self.parent_is_static_image()
    }

    fn static_image(&self) -> Option<Pixbuf> {
        self.parent_static_image()
    }

    fn size(&self) -> (i32, i32) {
        self.parent_size()
    }

    fn iter(&self, start_time: SystemTime) -> PixbufAnimationIter {
        self.parent_iter(start_time)
    }
}

pub trait PixbufAnimationImplExt: PixbufAnimationImpl {
    fn parent_is_static_image(&self) -> bool {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationClass;
            let f = (*parent_class)
                .is_static_image
                .expect("No parent class implementation for \"is_static_image\"");

            from_glib(f(self
                .obj()
                .unsafe_cast_ref::<PixbufAnimation>()
                .to_glib_none()
                .0))
        }
    }

    fn parent_static_image(&self) -> Option<Pixbuf> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationClass;
            let f = (*parent_class)
                .get_static_image
                .expect("No parent class implementation for \"get_static_image\"");

            from_glib_none(f(self
                .obj()
                .unsafe_cast_ref::<PixbufAnimation>()
                .to_glib_none()
                .0))
        }
    }

    fn parent_size(&self) -> (i32, i32) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationClass;
            let f = (*parent_class)
                .get_size
                .expect("No parent class implementation for \"get_size\"");
            let mut width = MaybeUninit::uninit();
            let mut height = MaybeUninit::uninit();
            f(
                self.obj()
                    .unsafe_cast_ref::<PixbufAnimation>()
                    .to_glib_none()
                    .0,
                width.as_mut_ptr(),
                height.as_mut_ptr(),
            );
            (width.assume_init(), height.assume_init())
        }
    }

    fn parent_iter(&self, start_time: SystemTime) -> PixbufAnimationIter {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufAnimationClass;
            let f = (*parent_class)
                .get_iter
                .expect("No parent class implementation for \"get_iter\"");

            let diff = start_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("failed to convert time");
            let time = glib::ffi::GTimeVal {
                tv_sec: diff.as_secs() as _,
                tv_usec: diff.subsec_micros() as _,
            };
            from_glib_full(f(
                self.obj()
                    .unsafe_cast_ref::<PixbufAnimation>()
                    .to_glib_none()
                    .0,
                &time,
            ))
        }
    }
}

impl<T: PixbufAnimationImpl> PixbufAnimationImplExt for T {}

unsafe impl<T: PixbufAnimationImpl> IsSubclassable<T> for PixbufAnimation {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.get_static_image = Some(animation_get_static_image::<T>);
        klass.get_size = Some(animation_get_size::<T>);
        klass.get_iter = Some(animation_get_iter::<T>);
        klass.is_static_image = Some(animation_is_static_image::<T>);
    }
}

unsafe extern "C" fn animation_is_static_image<T: PixbufAnimationImpl>(
    ptr: *mut ffi::GdkPixbufAnimation,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.is_static_image().into_glib()
    }
}

unsafe extern "C" fn animation_get_size<T: PixbufAnimationImpl>(
    ptr: *mut ffi::GdkPixbufAnimation,
    width_ptr: *mut libc::c_int,
    height_ptr: *mut libc::c_int,
) {
    unsafe {
        if width_ptr.is_null() && height_ptr.is_null() {
            return;
        }

        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let (width, height) = imp.size();
        if !width_ptr.is_null() {
            *width_ptr = width;
        }
        if !height_ptr.is_null() {
            *height_ptr = height;
        }
    }
}

unsafe extern "C" fn animation_get_static_image<T: PixbufAnimationImpl>(
    ptr: *mut ffi::GdkPixbufAnimation,
) -> *mut ffi::GdkPixbuf {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let instance = imp.obj();
        let static_image = imp.static_image();
        // Ensure that a) the static image stays alive as long as the animation instance and b) that
        // the same static image is returned every time. This is a requirement by the gdk-pixbuf API.
        let static_image_quark = {
            static QUARK: OnceLock<glib::Quark> = OnceLock::new();
            *QUARK.get_or_init(|| glib::Quark::from_str("gtk-rs-subclass-static-image"))
        };
        if let Some(old_image) = instance.qdata::<Option<Pixbuf>>(static_image_quark) {
            let old_image = old_image.as_ref();

            if let Some(old_image) = old_image {
                assert_eq!(
                    Some(old_image),
                    static_image.as_ref(),
                    "Did not return same static image again"
                );
            }
        }
        instance.set_qdata(static_image_quark, static_image.clone());
        static_image.to_glib_none().0
    }
}

unsafe extern "C" fn animation_get_iter<T: PixbufAnimationImpl>(
    ptr: *mut ffi::GdkPixbufAnimation,
    start_time_ptr: *const glib::ffi::GTimeVal,
) -> *mut ffi::GdkPixbufAnimationIter {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let start_time = SystemTime::UNIX_EPOCH
            + Duration::from_secs((*start_time_ptr).tv_sec.try_into().unwrap())
            + Duration::from_micros((*start_time_ptr).tv_usec.try_into().unwrap());

        imp.iter(start_time).into_glib_ptr()
    }
}
