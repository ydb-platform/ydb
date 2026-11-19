// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    any::Any,
    io::{Read, Seek},
};

use crate::{InputStream, prelude::*, subclass::prelude::*};

mod imp {
    use std::cell::RefCell;

    use super::*;

    pub(super) enum Reader {
        Read(AnyReader),
        ReadSeek(AnyReader),
    }

    #[derive(Default)]
    pub struct ReadInputStream {
        pub(super) read: RefCell<Option<Reader>>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for ReadInputStream {
        const NAME: &'static str = "ReadInputStream";
        const ALLOW_NAME_CONFLICT: bool = true;
        type Type = super::ReadInputStream;
        type ParentType = InputStream;
        type Interfaces = (crate::Seekable,);
    }

    impl ObjectImpl for ReadInputStream {}

    impl InputStreamImpl for ReadInputStream {
        fn read(
            &self,
            buffer: &mut [u8],
            _cancellable: Option<&crate::Cancellable>,
        ) -> Result<usize, glib::Error> {
            let mut read = self.read.borrow_mut();
            let read = match *read {
                None => {
                    return Err(glib::Error::new(
                        crate::IOErrorEnum::Closed,
                        "Already closed",
                    ));
                }
                Some(Reader::Read(ref mut read)) => read,
                Some(Reader::ReadSeek(ref mut read)) => read,
            };

            loop {
                match std_error_to_gio_error(read.read(buffer)) {
                    None => continue,
                    Some(res) => return res,
                }
            }
        }

        fn close(&self, _cancellable: Option<&crate::Cancellable>) -> Result<(), glib::Error> {
            let _ = self.read.take();
            Ok(())
        }
    }

    impl SeekableImpl for ReadInputStream {
        fn tell(&self) -> i64 {
            // XXX: stream_position is not stable yet
            // let mut read = self.read.borrow_mut();
            // match *read {
            //     Some(Reader::ReadSeek(ref mut read)) => {
            //         read.stream_position().map(|pos| pos as i64).unwrap_or(-1)
            //     },
            //     _ => -1,
            // };
            -1
        }

        fn can_seek(&self) -> bool {
            let read = self.read.borrow();
            matches!(*read, Some(Reader::ReadSeek(_)))
        }

        fn seek(
            &self,
            offset: i64,
            type_: glib::SeekType,
            _cancellable: Option<&crate::Cancellable>,
        ) -> Result<(), glib::Error> {
            use std::io::SeekFrom;

            let mut read = self.read.borrow_mut();
            match *read {
                Some(Reader::ReadSeek(ref mut read)) => {
                    let pos = match type_ {
                        glib::SeekType::Cur => SeekFrom::Current(offset),
                        glib::SeekType::Set => {
                            if offset < 0 {
                                return Err(glib::Error::new(
                                    crate::IOErrorEnum::InvalidArgument,
                                    "Invalid Argument",
                                ));
                            } else {
                                SeekFrom::Start(offset as u64)
                            }
                        }
                        glib::SeekType::End => SeekFrom::End(offset),
                        _ => unimplemented!(),
                    };

                    loop {
                        match std_error_to_gio_error(read.seek(pos)) {
                            None => continue,
                            Some(res) => return res.map(|_| ()),
                        }
                    }
                }
                _ => Err(glib::Error::new(
                    crate::IOErrorEnum::NotSupported,
                    "Truncating not supported",
                )),
            }
        }

        fn can_truncate(&self) -> bool {
            false
        }

        fn truncate(
            &self,
            _offset: i64,
            _cancellable: Option<&crate::Cancellable>,
        ) -> Result<(), glib::Error> {
            Err(glib::Error::new(
                crate::IOErrorEnum::NotSupported,
                "Truncating not supported",
            ))
        }
    }
}

glib::wrapper! {
    pub struct ReadInputStream(ObjectSubclass<imp::ReadInputStream>) @extends crate::InputStream, @implements crate::Seekable;
}

impl ReadInputStream {
    pub fn new<R: Read + Send + 'static>(read: R) -> ReadInputStream {
        let obj: Self = glib::Object::new();

        *obj.imp().read.borrow_mut() = Some(imp::Reader::Read(AnyReader::new(read)));

        obj
    }

    pub fn new_seekable<R: Read + Seek + Send + 'static>(read: R) -> ReadInputStream {
        let obj: Self = glib::Object::new();

        *obj.imp().read.borrow_mut() = Some(imp::Reader::ReadSeek(AnyReader::new_seekable(read)));

        obj
    }

    pub fn close_and_take(&self) -> Box<dyn Any + Send + 'static> {
        let inner = self.imp().read.take();

        let ret = match inner {
            None => {
                panic!("Stream already closed or inner taken");
            }
            Some(imp::Reader::Read(read)) => read.reader,
            Some(imp::Reader::ReadSeek(read)) => read.reader,
        };

        let _ = self.close(crate::Cancellable::NONE);

        match ret {
            AnyOrPanic::Any(r) => r,
            AnyOrPanic::Panic(p) => std::panic::resume_unwind(p),
        }
    }
}

enum AnyOrPanic {
    Any(Box<dyn Any + Send + 'static>),
    Panic(Box<dyn Any + Send + 'static>),
}

// Helper struct for dynamically dispatching to any kind of Reader and
// catching panics along the way
struct AnyReader {
    reader: AnyOrPanic,
    read_fn: fn(s: &mut AnyReader, buffer: &mut [u8]) -> std::io::Result<usize>,
    seek_fn: Option<fn(s: &mut AnyReader, pos: std::io::SeekFrom) -> std::io::Result<u64>>,
}

impl AnyReader {
    fn new<R: Read + Any + Send + 'static>(r: R) -> Self {
        Self {
            reader: AnyOrPanic::Any(Box::new(r)),
            read_fn: Self::read_fn::<R>,
            seek_fn: None,
        }
    }

    fn new_seekable<R: Read + Seek + Any + Send + 'static>(r: R) -> Self {
        Self {
            reader: AnyOrPanic::Any(Box::new(r)),
            read_fn: Self::read_fn::<R>,
            seek_fn: Some(Self::seek_fn::<R>),
        }
    }

    fn read_fn<R: Read + 'static>(s: &mut AnyReader, buffer: &mut [u8]) -> std::io::Result<usize> {
        s.with_inner(|r: &mut R| r.read(buffer))
    }

    fn seek_fn<R: Seek + 'static>(
        s: &mut AnyReader,
        pos: std::io::SeekFrom,
    ) -> std::io::Result<u64> {
        s.with_inner(|r: &mut R| r.seek(pos))
    }

    fn with_inner<R: 'static, T, F: FnOnce(&mut R) -> std::io::Result<T>>(
        &mut self,
        func: F,
    ) -> std::io::Result<T> {
        match self.reader {
            AnyOrPanic::Any(ref mut reader) => {
                let r = reader.downcast_mut::<R>().unwrap();
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| func(r))) {
                    Ok(res) => res,
                    Err(panic) => {
                        self.reader = AnyOrPanic::Panic(panic);
                        Err(std::io::Error::other("Panicked"))
                    }
                }
            }
            AnyOrPanic::Panic(_) => Err(std::io::Error::other("Panicked before")),
        }
    }

    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        (self.read_fn)(self, buffer)
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        if let Some(ref seek_fn) = self.seek_fn {
            seek_fn(self, pos)
        } else {
            unreachable!()
        }
    }
}

pub(crate) fn std_error_to_gio_error<T>(
    res: Result<T, std::io::Error>,
) -> Option<Result<T, glib::Error>> {
    match res {
        Ok(res) => Some(Ok(res)),
        Err(err) => {
            use std::io::ErrorKind;

            #[allow(clippy::wildcard_in_or_patterns)]
            match err.kind() {
                ErrorKind::NotFound => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::NotFound,
                    "Not Found",
                ))),
                ErrorKind::PermissionDenied => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::PermissionDenied,
                    "Permission Denied",
                ))),
                ErrorKind::ConnectionRefused => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::ConnectionRefused,
                    "Connection Refused",
                ))),
                ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::NotConnected => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::NotConnected,
                    "Connection Reset",
                ))),
                ErrorKind::AddrInUse | ErrorKind::AddrNotAvailable => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::AddressInUse,
                    "Address In Use",
                ))),
                ErrorKind::BrokenPipe => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::BrokenPipe,
                    "Broken Pipe",
                ))),
                ErrorKind::AlreadyExists => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::Exists,
                    "Already Exists",
                ))),
                ErrorKind::WouldBlock => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::WouldBlock,
                    "Would Block",
                ))),
                ErrorKind::InvalidInput | ErrorKind::InvalidData => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::InvalidData,
                    "Invalid Input",
                ))),
                ErrorKind::TimedOut => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::TimedOut,
                    "Timed Out",
                ))),
                ErrorKind::Interrupted => None,
                ErrorKind::UnexpectedEof => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::Closed,
                    "Unexpected Eof",
                ))),
                ErrorKind::WriteZero | _ => Some(Err(glib::Error::new(
                    crate::IOErrorEnum::Failed,
                    format!("Unknown error: {err:?}").as_str(),
                ))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read() {
        let cursor = Cursor::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let stream = ReadInputStream::new(cursor);

        let mut buf = [0u8; 1024];
        assert_eq!(stream.read(&mut buf[..], crate::Cancellable::NONE), Ok(10));
        assert_eq!(&buf[..10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10][..]);

        assert_eq!(stream.read(&mut buf[..], crate::Cancellable::NONE), Ok(0));

        let inner = stream.close_and_take();
        assert!(inner.is::<Cursor<Vec<u8>>>());
        let inner = inner.downcast_ref::<Cursor<Vec<u8>>>().unwrap();
        assert_eq!(inner.get_ref(), &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_read_seek() {
        let cursor = Cursor::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let stream = ReadInputStream::new_seekable(cursor);

        let mut buf = [0u8; 1024];
        assert_eq!(stream.read(&mut buf[..], crate::Cancellable::NONE), Ok(10));
        assert_eq!(&buf[..10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10][..]);

        assert_eq!(stream.read(&mut buf[..], crate::Cancellable::NONE), Ok(0));

        assert!(stream.can_seek());
        assert_eq!(
            stream.seek(0, glib::SeekType::Set, crate::Cancellable::NONE),
            Ok(())
        );
        assert_eq!(stream.read(&mut buf[..], crate::Cancellable::NONE), Ok(10));
        assert_eq!(&buf[..10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10][..]);

        let inner = stream.close_and_take();
        assert!(inner.is::<Cursor<Vec<u8>>>());
        let inner = inner.downcast_ref::<Cursor<Vec<u8>>>().unwrap();
        assert_eq!(inner.get_ref(), &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }
}
