// Take a look at the license at the top of the repository in the LICENSE file.

use std::ptr;

use glib::{Error, prelude::*, subclass::prelude::*, translate::*};

use crate::{Cancellable, InputStream, ffi};

pub trait InputStreamImpl: Send + ObjectImpl + ObjectSubclass<Type: IsA<InputStream>> {
    fn read(&self, buffer: &mut [u8], cancellable: Option<&Cancellable>) -> Result<usize, Error> {
        self.parent_read(buffer, cancellable)
    }

    fn close(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_close(cancellable)
    }

    fn skip(&self, count: usize, cancellable: Option<&Cancellable>) -> Result<usize, Error> {
        self.parent_skip(count, cancellable)
    }
}

pub trait InputStreamImplExt: InputStreamImpl {
    fn parent_read(
        &self,
        buffer: &mut [u8],
        cancellable: Option<&Cancellable>,
    ) -> Result<usize, Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GInputStreamClass;
            let f = (*parent_class)
                .read_fn
                .expect("No parent class implementation for \"read\"");
            let mut err = ptr::null_mut();
            let res = f(
                self.obj().unsafe_cast_ref::<InputStream>().to_glib_none().0,
                buffer.as_mut_ptr() as glib::ffi::gpointer,
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
            let parent_class = data.as_ref().parent_class() as *mut ffi::GInputStreamClass;
            let mut err = ptr::null_mut();
            if let Some(f) = (*parent_class).close_fn {
                if from_glib(f(
                    self.obj().unsafe_cast_ref::<InputStream>().to_glib_none().0,
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

    fn parent_skip(&self, count: usize, cancellable: Option<&Cancellable>) -> Result<usize, Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GInputStreamClass;
            let mut err = ptr::null_mut();
            let f = (*parent_class)
                .skip
                .expect("No parent class implementation for \"skip\"");
            let res = f(
                self.obj().unsafe_cast_ref::<InputStream>().to_glib_none().0,
                count,
                cancellable.to_glib_none().0,
                &mut err,
            );
            if res == -1 {
                Err(from_glib_full(err))
            } else {
                debug_assert!(res >= 0);
                let res = res as usize;
                debug_assert!(res <= count);
                Ok(res)
            }
        }
    }
}

impl<T: InputStreamImpl> InputStreamImplExt for T {}

unsafe impl<T: InputStreamImpl> IsSubclassable<T> for InputStream {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.read_fn = Some(stream_read::<T>);
        klass.close_fn = Some(stream_close::<T>);
        klass.skip = Some(stream_skip::<T>);
    }
}

unsafe extern "C" fn stream_read<T: InputStreamImpl>(
    ptr: *mut ffi::GInputStream,
    buffer: glib::ffi::gpointer,
    count: usize,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> isize {
    unsafe {
        debug_assert!(count <= isize::MAX as usize);

        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        match imp.read(
            if count == 0 {
                &mut []
            } else {
                std::slice::from_raw_parts_mut(buffer as *mut u8, count)
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

unsafe extern "C" fn stream_close<T: InputStreamImpl>(
    ptr: *mut ffi::GInputStream,
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

unsafe extern "C" fn stream_skip<T: InputStreamImpl>(
    ptr: *mut ffi::GInputStream,
    count: usize,
    cancellable: *mut ffi::GCancellable,
    err: *mut *mut glib::ffi::GError,
) -> isize {
    unsafe {
        debug_assert!(count <= isize::MAX as usize);

        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        match imp.skip(
            count,
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::{prelude::*, subclass::prelude::*};

    mod imp {
        use super::*;

        #[derive(Default)]
        pub struct SimpleInputStream {
            pub pos: RefCell<usize>,
        }

        #[glib::object_subclass]
        impl ObjectSubclass for SimpleInputStream {
            const NAME: &'static str = "SimpleInputStream";
            type Type = super::SimpleInputStream;
            type ParentType = InputStream;
            type Interfaces = (crate::Seekable,);
        }

        impl ObjectImpl for SimpleInputStream {}

        impl InputStreamImpl for SimpleInputStream {
            fn read(
                &self,
                buffer: &mut [u8],
                _cancellable: Option<&Cancellable>,
            ) -> Result<usize, Error> {
                let mut pos = self.pos.borrow_mut();
                for b in buffer.iter_mut() {
                    *b = ((*pos) % 255) as u8;
                    *pos += 1;
                }
                Ok(buffer.len())
            }
        }

        impl SeekableImpl for SimpleInputStream {
            fn tell(&self) -> i64 {
                *self.pos.borrow() as i64
            }

            fn can_seek(&self) -> bool {
                true
            }

            fn seek(
                &self,
                offset: i64,
                type_: glib::SeekType,
                _cancellable: Option<&Cancellable>,
            ) -> Result<(), glib::Error> {
                let mut pos = self.pos.borrow_mut();
                match type_ {
                    glib::SeekType::Set => {
                        *pos = offset as usize;
                        Ok(())
                    }
                    glib::SeekType::Cur => {
                        if offset < 0 {
                            *pos -= (-offset) as usize;
                        } else {
                            *pos += offset as usize;
                        }

                        Ok(())
                    }
                    glib::SeekType::End => Err(glib::Error::new(
                        crate::IOErrorEnum::NotSupported,
                        "Can't seek relative to end",
                    )),
                    _ => unreachable!(),
                }
            }

            fn can_truncate(&self) -> bool {
                false
            }
            fn truncate(
                &self,
                _offset: i64,
                _cancellable: Option<&Cancellable>,
            ) -> Result<(), Error> {
                unimplemented!()
            }
        }
    }

    glib::wrapper! {
        pub struct SimpleInputStream(ObjectSubclass<imp::SimpleInputStream>)
            @extends InputStream,
            @implements crate::Seekable;
    }

    #[test]
    fn test_simple_stream() {
        let stream = glib::Object::new::<SimpleInputStream>();

        let mut buf = [0; 16];
        assert_eq!(stream.read(&mut buf, crate::Cancellable::NONE), Ok(16));
        assert_eq!(
            &buf,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );

        assert_eq!(stream.skip(2, crate::Cancellable::NONE), Ok(2));

        assert_eq!(stream.read(&mut buf, crate::Cancellable::NONE), Ok(16));
        assert_eq!(
            &buf,
            &[
                18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33
            ]
        );

        let seekable = stream.dynamic_cast_ref::<crate::Seekable>().unwrap();
        assert_eq!(seekable.tell(), 34);
        assert!(seekable.can_seek());

        assert_eq!(
            seekable.seek(0, glib::SeekType::Set, crate::Cancellable::NONE),
            Ok(())
        );

        assert_eq!(seekable.tell(), 0);
        assert_eq!(stream.read(&mut buf, crate::Cancellable::NONE), Ok(16));
        assert_eq!(
            &buf,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );

        assert_eq!(stream.close(crate::Cancellable::NONE), Ok(()));
    }
}
