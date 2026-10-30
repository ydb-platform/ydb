// Take a look at the license at the top of the repository in the LICENSE file.

use std::ptr;

use glib::{Error, prelude::*, subclass::prelude::*, translate::*};

use crate::{Cancellable, InputStream, OutputStream, OutputStreamSpliceFlags, ffi};

pub trait OutputStreamImpl: Send + ObjectImpl + ObjectSubclass<Type: IsA<OutputStream>> {
    fn write(&self, buffer: &[u8], cancellable: Option<&Cancellable>) -> Result<usize, Error> {
        self.parent_write(buffer, cancellable)
    }

    fn close(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_close(cancellable)
    }

    fn flush(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_flush(cancellable)
    }

    fn splice(
        &self,
        input_stream: &InputStream,
        flags: OutputStreamSpliceFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<usize, Error> {
        self.parent_splice(input_stream, flags, cancellable)
    }
}

pub trait OutputStreamImplExt: OutputStreamImpl {
    fn parent_write(
        &self,
        buffer: &[u8],
        cancellable: Option<&Cancellable>,
    ) -> Result<usize, Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GOutputStreamClass;
            let f = (*parent_class)
                .write_fn
                .expect("No parent class implementation for \"write\"");
            let mut err = ptr::null_mut();
            let res = f(
                self.obj()
                    .unsafe_cast_ref::<OutputStream>()
                    .to_glib_none()
                    .0,
                mut_override(buffer.as_ptr()),
                buffer.len(),
                cancellable.to_glib_none().0,
                &mut err,
            );
            if res == -1 {
                Err(from_glib_full(err))
            } else {
                debug_assert!(res >= 0);
                let res = res as usize;
                debug_assert!(res <= buffer.len());
                Ok(res)
            }
        }
    }

    fn parent_close(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GOutputStreamClass;
            let mut err = ptr::null_mut();
            if let Some(f) = (*parent_class).close_fn {
                if from_glib(f(
                    self.obj()
                        .unsafe_cast_ref::<OutputStream>()
                        .to_glib_none()
                        .0,
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

    fn parent_flush(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GOutputStreamClass;
            let mut err = ptr::null_mut();
            if let Some(f) = (*parent_class).flush {
                if from_glib(f(
                    self.obj()
                        .unsafe_cast_ref::<OutputStream>()
                        .to_glib_none()
                        .0,
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

    fn parent_splice(
        &self,
        input_stream: &InputStream,
        flags: OutputStreamSpliceFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<usize, Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GOutputStreamClass;
            let mut err = ptr::null_mut();
            let f = (*parent_class)
                .splice
                .expect("No parent class implementation for \"splice\"");
            let res = f(
                self.obj()
                    .unsafe_cast_ref::<OutputStream>()
                    .to_glib_none()
                    .0,
                input_stream.to_glib_none().0,
                flags.into_glib(),
                cancellable.to_glib_none().0,
                &mut err,
            );
            if res == -1 {
                Err(from_glib_full(err))
            } else {
                debug_assert!(res >= 0);
                let res = res as usize;
                Ok(res)
            }
        }
    }
}

impl<T: OutputStreamImpl> OutputStreamImplExt for T {}

unsafe impl<T: OutputStreamImpl> IsSubclassable<T> for OutputStream {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.write_fn = Some(stream_write::<T>);
        klass.close_fn = Some(stream_close::<T>);
        klass.flush = Some(stream_flush::<T>);
        klass.splice = Some(stream_splice::<T>);
    }
}

unsafe extern "C" fn stream_write<T: OutputStreamImpl>(
    ptr: *mut ffi::GOutputStream,
    buffer: *mut u8,
    count: usize,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> isize {
    unsafe {
        debug_assert!(count <= isize::MAX as usize);

        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        match imp.write(
            if count == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(buffer as *const u8, count)
            },
            Option::<Cancellable>::from_glib_borrow(cancellable)
                .as_ref()
                .as_ref(),
        ) {
            Ok(res) => {
                assert!(res <= isize::MAX as usize);
                assert!(res <= count);
                res as isize
            }
            Err(e) => {
                if !err.is_null() {
                    *err = e.into_glib_ptr();
                }
                -1
            }
        }
    }
}

unsafe extern "C" fn stream_close<T: OutputStreamImpl>(
    ptr: *mut ffi::GOutputStream,
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

unsafe extern "C" fn stream_flush<T: OutputStreamImpl>(
    ptr: *mut ffi::GOutputStream,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        match imp.flush(
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

unsafe extern "C" fn stream_splice<T: OutputStreamImpl>(
    ptr: *mut ffi::GOutputStream,
    input_stream: *mut ffi::GInputStream,
    flags: ffi::GOutputStreamSpliceFlags,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> isize {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        match imp.splice(
            &from_glib_borrow(input_stream),
            from_glib(flags),
            Option::<Cancellable>::from_glib_borrow(cancellable)
                .as_ref()
                .as_ref(),
        ) {
            Ok(res) => {
                assert!(res <= isize::MAX as usize);
                res as isize
            }
            Err(e) => {
                if !err.is_null() {
                    *err = e.into_glib_ptr();
                }
                -1
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::prelude::*;

    mod imp {
        use super::*;

        #[derive(Default)]
        pub struct SimpleOutputStream {
            pub sum: RefCell<usize>,
        }

        #[glib::object_subclass]
        impl ObjectSubclass for SimpleOutputStream {
            const NAME: &'static str = "SimpleOutputStream";
            type Type = super::SimpleOutputStream;
            type ParentType = OutputStream;
        }

        impl ObjectImpl for SimpleOutputStream {}

        impl OutputStreamImpl for SimpleOutputStream {
            fn write(
                &self,
                buffer: &[u8],
                _cancellable: Option<&Cancellable>,
            ) -> Result<usize, Error> {
                let mut sum = self.sum.borrow_mut();
                for b in buffer {
                    *sum += *b as usize;
                }

                Ok(buffer.len())
            }
        }
    }

    glib::wrapper! {
        pub struct SimpleOutputStream(ObjectSubclass<imp::SimpleOutputStream>)
            @extends OutputStream;
    }

    #[test]
    fn test_simple_stream() {
        let stream = glib::Object::new::<SimpleOutputStream>();

        assert_eq!(*stream.imp().sum.borrow(), 0);
        assert_eq!(
            stream.write(&[1, 2, 3, 4, 5], crate::Cancellable::NONE),
            Ok(5)
        );
        assert_eq!(*stream.imp().sum.borrow(), 15);
    }
}
