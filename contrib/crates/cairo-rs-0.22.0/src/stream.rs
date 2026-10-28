// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    any::Any,
    cell::{Cell, RefCell},
    io,
    panic::AssertUnwindSafe,
    ptr,
    rc::Rc,
};

use crate::ffi;
use libc::{c_double, c_uchar, c_uint, c_void};

use crate::{Error, Surface, UserDataKey};

macro_rules! for_stream_constructors {
    ($constructor_ffi: ident) => {
        /// Takes full ownership of the output stream,
        /// which is not allowed to borrow any lifetime shorter than `'static`.
        ///
        /// Because the underlying `cairo_surface_t` is reference-counted,
        /// a lifetime parameter in a Rust wrapper type would not be enough to track
        /// how long it can keep writing to the stream.
        pub fn for_stream<W: io::Write + 'static>(
            width: f64,
            height: f64,
            stream: W,
        ) -> Result<Self, crate::error::Error> {
            Ok(Self(Surface::_for_stream(
                ffi::$constructor_ffi,
                width,
                height,
                stream,
            )?))
        }

        /// Allows writing to a borrowed stream. The lifetime of the borrow is not tracked.
        ///
        /// # Safety
        ///
        /// The value that `stream` points to must live at least until the underlying `cairo_surface_t`
        /// (which maybe be longer then the Rust `PdfSurface` wrapper, because of reference-counting),
        /// or until the output stream is removed from the surface with [`Surface::finish_output_stream`].
        ///
        /// Since the former is hard to track for sure, the latter is strongly recommended.
        /// The concrete type behind the `Box<dyn Any>` value returned by `finish_output_stream`
        /// is private, so you won’t be able to downcast it.
        /// But removing it anyway ensures that later writes do not go through a dangling pointer.
        pub unsafe fn for_raw_stream<W: io::Write + 'static>(
            width: f64,
            height: f64,
            stream: *mut W,
        ) -> Result<Self, crate::error::Error> {
            unsafe {
                Ok(Self(Surface::_for_raw_stream(
                    ffi::$constructor_ffi,
                    width,
                    height,
                    stream,
                )?))
            }
        }
    };
}

impl Surface {
    pub(crate) fn _for_stream<W: io::Write + 'static>(
        constructor: Constructor,
        width: f64,
        height: f64,
        stream: W,
    ) -> Result<Self, Error> {
        let env_rc = Rc::new(CallbackEnvironment {
            mutable: RefCell::new(MutableCallbackEnvironment {
                stream: Some((Box::new(stream), None)),
                unwind_payload: None,
            }),
            saw_already_borrowed: Cell::new(false),
        });
        let env: *const CallbackEnvironment = &*env_rc;
        unsafe {
            let ptr = constructor(Some(write_callback::<W>), env as *mut c_void, width, height);
            let surface = Surface::from_raw_full(ptr)?;
            surface.set_user_data(&STREAM_CALLBACK_ENVIRONMENT, env_rc)?;
            Ok(surface)
        }
    }

    pub(crate) unsafe fn _for_raw_stream<W: io::Write + 'static>(
        constructor: Constructor,
        width: f64,
        height: f64,
        stream: *mut W,
    ) -> Result<Self, Error> {
        Self::_for_stream(
            constructor,
            width,
            height,
            RawStream(ptr::NonNull::new(stream).expect("NULL stream passed")),
        )
    }

    /// Finish the surface, then remove and return the output stream if any.
    ///
    /// This calls [`Surface::finish`], to make sure pending writes are done.
    ///
    /// This is relevant for surfaces created for example with [`crate::PdfSurface::for_stream`].
    ///
    /// Use [`Box::downcast`] to recover the concrete stream type.
    ///
    /// # Panics
    ///
    /// This method panics if:
    ///
    /// * This method was already called for this surface, or
    /// * This surface was not created with an output stream in the first place, or
    /// * A previous write to this surface panicked, or
    /// * A previous write happened while another write was ongoing, or
    /// * A write is ongoing now.
    ///
    /// The latter two cases can only occur with a pathological output stream type
    /// that accesses the same surface again from `Write::write_all`.
    pub fn finish_output_stream(&self) -> Result<Box<dyn Any>, StreamWithError> {
        self.finish();

        let env = self
            .user_data_ptr(&STREAM_CALLBACK_ENVIRONMENT)
            .expect("surface without an output stream");

        // Safety: since `STREAM_CALLBACK_ENVIRONMENT` is private and we never
        // call `set_user_data` again or `remove_user_data` with it,
        // the contract of `get_user_data_ptr` says that the user data entry
        // lives as long as the underlying `cairo_surface_t`
        // which is at least as long as `self`.
        let env = unsafe { &*env.as_ptr() };

        if env.saw_already_borrowed.get() {
            panic!("The output stream’s RefCell was already borrowed when cairo attempted a write")
        }

        let mut mutable = env.mutable.borrow_mut();
        if let Some(payload) = mutable.unwind_payload.take() {
            std::panic::resume_unwind(payload)
        }

        let (stream, io_error) = mutable
            .stream
            .take()
            .expect("output stream was already taken");
        if let Some(error) = io_error {
            Err(StreamWithError { stream, error })
        } else {
            Ok(stream)
        }
    }
}

pub struct StreamWithError {
    pub stream: Box<dyn Any>,
    pub error: io::Error,
}

impl std::fmt::Debug for StreamWithError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::fmt::Display for StreamWithError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl From<StreamWithError> for io::Error {
    fn from(e: StreamWithError) -> Self {
        e.error
    }
}

pub(crate) type Constructor = unsafe extern "C" fn(
    ffi::cairo_write_func_t,
    *mut c_void,
    c_double,
    c_double,
) -> *mut ffi::cairo_surface_t;

static STREAM_CALLBACK_ENVIRONMENT: UserDataKey<CallbackEnvironment> = UserDataKey::new();

struct CallbackEnvironment {
    mutable: RefCell<MutableCallbackEnvironment>,
    saw_already_borrowed: Cell<bool>,
}

struct MutableCallbackEnvironment {
    stream: Option<(Box<dyn Any>, Option<io::Error>)>,
    unwind_payload: Option<Box<dyn Any + Send + 'static>>,
}

// Safety: unwinding into C is undefined behavior (https://github.com/rust-lang/rust/issues/58794)
// so code outside of the `catch_unwind` call must never panic.
extern "C" fn write_callback<W: io::Write + 'static>(
    env: *mut c_void,
    data: *const c_uchar,
    length: c_uint,
) -> ffi::cairo_status_t {
    // This is consistent with the type of `env` in `Surface::_for_stream`.
    let env: *const CallbackEnvironment = env as _;

    // Safety: the user data entry keeps `Rc<CallbackEnvironment>` alive
    // until the surface is destroyed.
    // If this is called by cairo, the surface is still alive.
    let env: &CallbackEnvironment = unsafe { &*env };

    match env.mutable.try_borrow_mut() {
        Ok(mut mutable) => {
            if let MutableCallbackEnvironment {
                stream:
                    Some((
                        stream,
                        // Don’t attempt another write, if a previous one errored or panicked:
                        io_error @ None,
                    )),
                unwind_payload: unwind_payload @ None,
            } = &mut *mutable
            {
                // Safety: `write_callback<W>` was instantiated in `Surface::_for_stream`
                // with a W parameter consistent with the box that was unsized to `Box<dyn Any>`.
                let stream = unsafe { AnyExt::downcast_mut_unchecked::<W>(&mut **stream) };
                // Safety: this is the callback contract from cairo’s API
                let data = unsafe {
                    if data.is_null() || length == 0 {
                        &[]
                    } else {
                        std::slice::from_raw_parts(data, length as usize)
                    }
                };
                // Because `<W as Write>::write_all` is a generic,
                // we must conservatively assume that it can panic.
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| stream.write_all(data)));
                match result {
                    Ok(Ok(())) => return ffi::STATUS_SUCCESS,
                    Ok(Err(error)) => {
                        *io_error = Some(error);
                    }
                    Err(payload) => {
                        *unwind_payload = Some(payload);
                    }
                }
            }
        }
        _ => env.saw_already_borrowed.set(true),
    }
    Error::WriteError.into()
}

struct RawStream<W>(ptr::NonNull<W>);

impl<W: io::Write> io::Write for RawStream<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        unsafe { (*self.0.as_ptr()).write(buf) }
    }
    fn flush(&mut self) -> io::Result<()> {
        unsafe { (*self.0.as_ptr()).flush() }
    }
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        unsafe { (*self.0.as_ptr()).write_all(buf) }
    }
}

trait AnyExt {
    /// Any::downcast_mut, but YOLO
    unsafe fn downcast_mut_unchecked<T>(&mut self) -> &mut T {
        unsafe {
            let ptr = self as *mut Self as *mut T;
            &mut *ptr
        }
    }
}
impl AnyExt for dyn Any {}
