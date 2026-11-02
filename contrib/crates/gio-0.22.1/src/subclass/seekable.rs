// Take a look at the license at the top of the repository in the LICENSE file.

use std::ptr;

use glib::{Error, SeekType, prelude::*, subclass::prelude::*, translate::*};

use crate::{Cancellable, Seekable, ffi};

pub trait SeekableImpl: Send + ObjectImpl + ObjectSubclass<Type: IsA<Seekable>> {
    fn tell(&self) -> i64;
    fn can_seek(&self) -> bool;
    fn seek(
        &self,
        offset: i64,
        type_: SeekType,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error>;
    fn can_truncate(&self) -> bool;
    fn truncate(&self, offset: i64, cancellable: Option<&Cancellable>) -> Result<(), Error>;
}

pub trait SeekableImplExt: SeekableImpl {
    fn parent_tell(&self) -> i64 {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<Seekable>() as *const ffi::GSeekableIface;

            let func = (*parent_iface)
                .tell
                .expect("no parent \"tell\" implementation");
            func(self.obj().unsafe_cast_ref::<Seekable>().to_glib_none().0)
        }
    }

    fn parent_can_seek(&self) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<Seekable>() as *const ffi::GSeekableIface;

            let func = (*parent_iface)
                .can_seek
                .expect("no parent \"can_seek\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<Seekable>().to_glib_none().0);
            from_glib(ret)
        }
    }

    fn parent_seek(
        &self,
        offset: i64,
        type_: SeekType,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<Seekable>() as *const ffi::GSeekableIface;

            let func = (*parent_iface)
                .seek
                .expect("no parent \"seek\" implementation");

            let mut err = ptr::null_mut();
            func(
                self.obj().unsafe_cast_ref::<Seekable>().to_glib_none().0,
                offset,
                type_.into_glib(),
                cancellable.to_glib_none().0,
                &mut err,
            );

            if err.is_null() {
                Ok(())
            } else {
                Err(from_glib_full(err))
            }
        }
    }

    fn parent_can_truncate(&self) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<Seekable>() as *const ffi::GSeekableIface;

            let func = (*parent_iface)
                .can_truncate
                .expect("no parent \"can_truncate\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<Seekable>().to_glib_none().0);
            from_glib(ret)
        }
    }

    fn parent_truncate(&self, offset: i64, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<Seekable>() as *const ffi::GSeekableIface;

            let func = (*parent_iface)
                .truncate_fn
                .expect("no parent \"truncate\" implementation");

            let mut err = ptr::null_mut();
            func(
                self.obj().unsafe_cast_ref::<Seekable>().to_glib_none().0,
                offset,
                cancellable.to_glib_none().0,
                &mut err,
            );

            if err.is_null() {
                Ok(())
            } else {
                Err(from_glib_full(err))
            }
        }
    }
}

impl<T: SeekableImpl> SeekableImplExt for T {}

unsafe impl<T: SeekableImpl> IsImplementable<T> for Seekable {
    fn interface_init(iface: &mut glib::Interface<Self>) {
        let iface = iface.as_mut();

        iface.tell = Some(seekable_tell::<T>);
        iface.can_seek = Some(seekable_can_seek::<T>);
        iface.seek = Some(seekable_seek::<T>);
        iface.can_truncate = Some(seekable_can_truncate::<T>);
        iface.truncate_fn = Some(seekable_truncate::<T>);
    }
}

unsafe extern "C" fn seekable_tell<T: SeekableImpl>(seekable: *mut ffi::GSeekable) -> i64 {
    unsafe {
        let instance = &*(seekable as *mut T::Instance);
        let imp = instance.imp();

        imp.tell()
    }
}

unsafe extern "C" fn seekable_can_seek<T: SeekableImpl>(
    seekable: *mut ffi::GSeekable,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(seekable as *mut T::Instance);
        let imp = instance.imp();

        imp.can_seek().into_glib()
    }
}

unsafe extern "C" fn seekable_seek<T: SeekableImpl>(
    seekable: *mut ffi::GSeekable,
    offset: i64,
    type_: glib::ffi::GSeekType,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(seekable as *mut T::Instance);
        let imp = instance.imp();

        match imp.seek(
            offset,
            from_glib(type_),
            Option::<Cancellable>::from_glib_borrow(cancellable)
                .as_ref()
                .as_ref(),
        ) {
            Ok(()) => glib::ffi::GTRUE,
            Err(e) => {
                if !err.is_null() {
                    *err = e.into_glib_ptr();
                }
                glib::ffi::GFALSE
            }
        }
    }
}

unsafe extern "C" fn seekable_can_truncate<T: SeekableImpl>(
    seekable: *mut ffi::GSeekable,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(seekable as *mut T::Instance);
        let imp = instance.imp();

        imp.can_truncate().into_glib()
    }
}

unsafe extern "C" fn seekable_truncate<T: SeekableImpl>(
    seekable: *mut ffi::GSeekable,
    offset: i64,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(seekable as *mut T::Instance);
        let imp = instance.imp();

        match imp.truncate(
            offset,
            Option::<Cancellable>::from_glib_borrow(cancellable)
                .as_ref()
                .as_ref(),
        ) {
            Ok(()) => glib::ffi::GTRUE,
            Err(e) => {
                if !err.is_null() {
                    *err = e.into_glib_ptr();
                }
                glib::ffi::GFALSE
            }
        }
    }
}
