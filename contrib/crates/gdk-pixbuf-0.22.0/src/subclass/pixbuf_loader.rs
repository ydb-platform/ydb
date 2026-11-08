// Take a look at the license at the top of the repository in the LICENSE file.

// rustdoc-stripper-ignore-next
//! Traits intended for subclassing [`PixbufLoader`].

use glib::{prelude::*, subclass::prelude::*, translate::*};

use crate::{PixbufLoader, ffi};

pub trait PixbufLoaderImpl: ObjectImpl + ObjectSubclass<Type: IsA<PixbufLoader>> {
    fn size_prepared(&self, width: i32, height: i32) {
        self.parent_size_prepared(width, height)
    }

    fn area_prepared(&self) {
        self.parent_area_prepared()
    }

    fn area_updated(&self, x: i32, y: i32, width: i32, height: i32) {
        self.parent_area_updated(x, y, width, height)
    }

    fn closed(&self) {
        self.parent_closed()
    }
}

pub trait PixbufLoaderImplExt: PixbufLoaderImpl {
    fn parent_size_prepared(&self, width: i32, height: i32) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufLoaderClass;
            if let Some(f) = (*parent_class).size_prepared {
                f(
                    self.obj()
                        .unsafe_cast_ref::<PixbufLoader>()
                        .to_glib_none()
                        .0,
                    width,
                    height,
                )
            }
        }
    }

    fn parent_area_prepared(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufLoaderClass;
            if let Some(f) = (*parent_class).area_prepared {
                f(self
                    .obj()
                    .unsafe_cast_ref::<PixbufLoader>()
                    .to_glib_none()
                    .0)
            }
        }
    }

    fn parent_area_updated(&self, x: i32, y: i32, width: i32, height: i32) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufLoaderClass;
            if let Some(f) = (*parent_class).area_updated {
                f(
                    self.obj()
                        .unsafe_cast_ref::<PixbufLoader>()
                        .to_glib_none()
                        .0,
                    x,
                    y,
                    width,
                    height,
                )
            }
        }
    }

    fn parent_closed(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GdkPixbufLoaderClass;
            if let Some(f) = (*parent_class).closed {
                f(self
                    .obj()
                    .unsafe_cast_ref::<PixbufLoader>()
                    .to_glib_none()
                    .0)
            }
        }
    }
}

impl<T: PixbufLoaderImpl> PixbufLoaderImplExt for T {}

unsafe impl<T: PixbufLoaderImpl> IsSubclassable<T> for PixbufLoader {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.size_prepared = Some(loader_size_prepared::<T>);
        klass.area_prepared = Some(loader_area_prepared::<T>);
        klass.area_updated = Some(loader_area_updated::<T>);
        klass.closed = Some(loader_closed::<T>);
    }
}

unsafe extern "C" fn loader_size_prepared<T: PixbufLoaderImpl>(
    ptr: *mut ffi::GdkPixbufLoader,
    width: i32,
    height: i32,
) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.size_prepared(width, height)
    }
}

unsafe extern "C" fn loader_area_prepared<T: PixbufLoaderImpl>(ptr: *mut ffi::GdkPixbufLoader) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.area_prepared();
    }
}

unsafe extern "C" fn loader_area_updated<T: PixbufLoaderImpl>(
    ptr: *mut ffi::GdkPixbufLoader,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.area_updated(x, y, width, height)
    }
}

unsafe extern "C" fn loader_closed<T: PixbufLoaderImpl>(ptr: *mut ffi::GdkPixbufLoader) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.closed()
    }
}
