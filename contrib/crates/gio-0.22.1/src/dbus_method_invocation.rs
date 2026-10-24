// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{VariantTy, prelude::*, translate::*};

use crate::{DBusMethodInvocation, ffi};

impl DBusMethodInvocation {
    #[doc(alias = "g_dbus_method_invocation_return_error_literal")]
    pub fn return_error<T: ErrorDomain>(&self, error: T, message: &str) {
        unsafe {
            ffi::g_dbus_method_invocation_return_error_literal(
                self.to_glib_full(),
                T::domain().into_glib(),
                error.code(),
                message.to_glib_none().0,
            );
        }
    }

    #[doc(alias = "g_dbus_method_invocation_return_gerror")]
    pub fn return_gerror(&self, error: glib::Error) {
        unsafe {
            ffi::g_dbus_method_invocation_return_gerror(
                self.to_glib_full(),
                error.to_glib_none().0,
            );
        }
    }

    // rustdoc-stripper-ignore-next
    /// Return a result for this invocation.
    ///
    /// If `Ok` return the contained value with [`Self::return_value`].  If the return
    /// value is not a tuple, automatically convert it to a one-element tuple, as
    /// DBus return values must be tuples.
    ///
    /// If `Err` return the contained error with [`Self::return_gerror`].
    pub fn return_result(self, result: Result<Option<glib::Variant>, glib::Error>) {
        match result {
            Ok(Some(value)) if !value.is_type(VariantTy::TUPLE) => {
                let tupled = glib::Variant::tuple_from_iter(std::iter::once(value));
                self.return_value(Some(&tupled));
            }
            Ok(value) => self.return_value(value.as_ref()),
            Err(error) => self.return_gerror(error),
        }
    }

    // rustdoc-stripper-ignore-next
    /// Return an async result for this invocation.
    ///
    /// Spawn the given future on the thread-default main context, and return the
    /// the result with [`Self::return_result`].  Specifically, if a variant is returned
    /// that is not a tuple it is automatically wrapped into a tuple.
    ///
    /// The given `Future` does not have to be `Send`.
    ///
    /// This can be called only from the thread where the main context is running, e.g.
    /// from any other `Future` that is executed on this main context, or after calling
    /// `with_thread_default` or `acquire` on the main context.
    pub fn return_future_local<F>(self, f: F) -> glib::JoinHandle<()>
    where
        F: std::future::Future<Output = Result<Option<glib::Variant>, glib::Error>> + 'static,
    {
        glib::spawn_future_local(async move {
            self.return_result(f.await);
        })
    }
}
