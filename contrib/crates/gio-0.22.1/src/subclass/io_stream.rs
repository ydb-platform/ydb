// Take a look at the license at the top of the repository in the LICENSE file.

use std::{ptr, sync::OnceLock};

use glib::{Error, prelude::*, subclass::prelude::*, translate::*};

use crate::{Cancellable, IOStream, InputStream, OutputStream, ffi};

pub trait IOStreamImpl: Send + ObjectImpl + ObjectSubclass<Type: IsA<IOStream>> {
    fn input_stream(&self) -> InputStream {
        self.parent_input_stream()
    }

    fn output_stream(&self) -> OutputStream {
        self.parent_output_stream()
    }

    fn close(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_close(cancellable)
    }
}

pub trait IOStreamImplExt: IOStreamImpl {
    fn parent_input_stream(&self) -> InputStream {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GIOStreamClass;
            let f = (*parent_class)
                .get_input_stream
                .expect("No parent class implementation for \"input_stream\"");
            from_glib_none(f(self.obj().unsafe_cast_ref::<IOStream>().to_glib_none().0))
        }
    }

    fn parent_output_stream(&self) -> OutputStream {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GIOStreamClass;
            let f = (*parent_class)
                .get_output_stream
                .expect("No parent class implementation for \"output_stream\"");
            from_glib_none(f(self.obj().unsafe_cast_ref::<IOStream>().to_glib_none().0))
        }
    }

    fn parent_close(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GIOStreamClass;
            let mut err = ptr::null_mut();
            if let Some(f) = (*parent_class).close_fn {
                if from_glib(f(
                    self.obj().unsafe_cast_ref::<IOStream>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut err,
                )) {
                    Ok(())
                } else {
                    Err(from_glib_full(err))
                }
            } else {
                Ok(())
            }
        }
    }
}

impl<T: IOStreamImpl> IOStreamImplExt for T {}

unsafe impl<T: IOStreamImpl> IsSubclassable<T> for IOStream {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.get_input_stream = Some(stream_get_input_stream::<T>);
        klass.get_output_stream = Some(stream_get_output_stream::<T>);
        klass.close_fn = Some(stream_close::<T>);
    }
}

unsafe extern "C" fn stream_get_input_stream<T: IOStreamImpl>(
    ptr: *mut ffi::GIOStream,
) -> *mut ffi::GInputStream {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let ret = imp.input_stream();

        let instance = imp.obj();
        // Ensure that a) the stream stays alive as long as the IO stream instance and
        // b) that the same stream is returned every time. This is a requirement by the
        // IO stream API.
        let input_stream_quark = {
            static QUARK: OnceLock<glib::Quark> = OnceLock::new();
            *QUARK.get_or_init(|| glib::Quark::from_str("gtk-rs-subclass-input-stream"))
        };
        if let Some(old_stream) = instance.qdata::<InputStream>(input_stream_quark) {
            assert_eq!(
                old_stream.as_ref(),
                &ret,
                "Did not return same input stream again"
            );
        }
        instance.set_qdata(input_stream_quark, ret.clone());
        ret.to_glib_none().0
    }
}

unsafe extern "C" fn stream_get_output_stream<T: IOStreamImpl>(
    ptr: *mut ffi::GIOStream,
) -> *mut ffi::GOutputStream {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let ret = imp.output_stream();

        let instance = imp.obj();
        // Ensure that a) the stream stays alive as long as the IO stream instance and
        // b) that the same stream is returned every time. This is a requirement by the
        // IO stream API.
        let output_stream_quark = {
            static QUARK: OnceLock<glib::Quark> = OnceLock::new();
            *QUARK.get_or_init(|| glib::Quark::from_str("gtk-rs-subclass-output-stream"))
        };
        if let Some(old_stream) = instance.qdata::<OutputStream>(output_stream_quark) {
            assert_eq!(
                old_stream.as_ref(),
                &ret,
                "Did not return same output stream again"
            );
        }
        instance.set_qdata(output_stream_quark, ret.clone());
        ret.to_glib_none().0
    }
}

unsafe extern "C" fn stream_close<T: IOStreamImpl>(
    ptr: *mut ffi::GIOStream,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        match imp.close(
            Option::<Cancellable>::from_glib_borrow(cancellable)
                .as_ref()
                .as_ref(),
        ) {
            Ok(_) => glib::ffi::GTRUE,
            Err(e) => {
                if !err.is_null() {
                    *err = e.into_glib_ptr();
                }
                glib::ffi::GFALSE
            }
        }
    }
}
