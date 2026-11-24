// Take a look at the license at the top of the repository in the LICENSE file.

use std::{boxed::Box as Box_, future::Future, marker::PhantomData, num::NonZeroU32};

use crate::{
    ActionGroup, DBusConnection, DBusInterfaceInfo, DBusMessage, DBusMethodInvocation,
    DBusSignalFlags, MenuModel, ffi,
};
use futures_channel::mpsc;
use futures_core::{FusedStream, Stream};
use glib::{WeakRef, prelude::*, translate::*, variant::VariantTypeMismatchError};
use pin_project_lite::pin_project;

pub trait DBusMethodCall: Sized {
    fn parse_call(
        obj_path: &str,
        interface: Option<&str>,
        method: &str,
        params: glib::Variant,
    ) -> Result<Self, glib::Error>;
}

// rustdoc-stripper-ignore-next
/// Handle method invocations.
pub struct MethodCallBuilder<'a, T> {
    registration: RegistrationBuilder<'a>,
    capture_type: PhantomData<T>,
}

impl<'a, T: DBusMethodCall> MethodCallBuilder<'a, T> {
    // rustdoc-stripper-ignore-next
    /// Handle invocation of a parsed method call.
    ///
    /// For each DBus method call parse the call, and then invoke the given closure
    /// with
    ///
    /// 1. the DBus connection object,
    /// 2. the name of the sender of the method call,
    /// 3. the parsed call, and
    /// 4. the method invocation object.
    ///
    /// The closure **must** return a value through the invocation object in all
    /// code paths, using any of its `return_` functions, such as
    /// [`DBusMethodInvocation::return_result`] or
    /// [`DBusMethodInvocation::return_future_local`], to finish the call.
    ///
    /// If direct access to the invocation object is not needed,
    /// [`invoke_and_return`] and [`invoke_and_return_future_local`] provide a
    /// safer interface where the callback returns a result directly.
    pub fn invoke<F>(self, f: F) -> RegistrationBuilder<'a>
    where
        F: Fn(DBusConnection, Option<&str>, T, DBusMethodInvocation) + 'static,
    {
        self.registration.method_call(
            move |connection, sender, obj_path, interface, method, params, invocation| {
                match T::parse_call(obj_path, interface, method, params) {
                    Ok(call) => f(connection, sender, call, invocation),
                    Err(error) => invocation.return_gerror(error),
                }
            },
        )
    }

    // rustdoc-stripper-ignore-next
    /// Handle invocation of a parsed method call.
    ///
    /// For each DBus method call parse the call, and then invoke the given closure
    /// with
    ///
    /// 1. the DBus connection object,
    /// 2. the name of the sender of the method call, and
    /// 3. the parsed call.
    ///
    /// The return value of the closure is then returned on the method call.
    /// If the returned variant value is not a tuple, it is automatically wrapped
    /// in a single element tuple, as DBus methods must always return tuples.
    /// See [`DBusMethodInvocation::return_result`] for details.
    pub fn invoke_and_return<F>(self, f: F) -> RegistrationBuilder<'a>
    where
        F: Fn(DBusConnection, Option<&str>, T) -> Result<Option<glib::Variant>, glib::Error>
            + 'static,
    {
        self.invoke(move |connection, sender, call, invocation| {
            invocation.return_result(f(connection, sender, call))
        })
    }

    // rustdoc-stripper-ignore-next
    /// Handle an async invocation of a parsed method call.
    ///
    /// For each DBus method call parse the call, and then invoke the given closure
    /// with
    ///
    /// 1. the DBus connection object,
    /// 2. the name of the sender of the method call, and
    /// 3. the parsed call.
    ///
    /// The output of the future is then returned on the method call.
    /// If the returned variant value is not a tuple, it is automatically wrapped
    /// in a single element tuple, as DBus methods must always return tuples.
    /// See [`DBusMethodInvocation::return_future_local`] for details.
    pub fn invoke_and_return_future_local<F, Fut>(self, f: F) -> RegistrationBuilder<'a>
    where
        F: Fn(DBusConnection, Option<&str>, T) -> Fut + 'static,
        Fut: Future<Output = Result<Option<glib::Variant>, glib::Error>> + 'static,
    {
        self.invoke(move |connection, sender, call, invocation| {
            invocation.return_future_local(f(connection, sender, call));
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct RegistrationId(NonZeroU32);
#[derive(Debug, Eq, PartialEq)]
pub struct WatcherId(NonZeroU32);
#[derive(Debug, Eq, PartialEq)]
pub struct ActionGroupExportId(NonZeroU32);
#[derive(Debug, Eq, PartialEq)]
pub struct MenuModelExportId(NonZeroU32);
#[derive(Debug, Eq, PartialEq)]
pub struct FilterId(NonZeroU32);

#[derive(Debug, Eq, PartialEq)]
pub struct SignalSubscriptionId(NonZeroU32);

// rustdoc-stripper-ignore-next
/// A strong subscription to a D-Bus signal.
///
/// Keep a reference to a D-Bus connection to maintain a subscription on a
/// D-Bus signal even if the connection has no other strong reference.
///
/// When dropped, unsubscribes from signal on the connection, and then drop the
/// reference on the connection.  If no other strong reference on the connection
/// exists the connection is closed and destroyed.
#[derive(Debug)]
pub struct SignalSubscription(DBusConnection, Option<SignalSubscriptionId>);

impl SignalSubscription {
    // rustdoc-stripper-ignore-next
    /// Downgrade this signal subscription to a weak one.
    #[must_use]
    pub fn downgrade(mut self) -> WeakSignalSubscription {
        WeakSignalSubscription(self.0.downgrade(), self.1.take())
    }
}

impl Drop for SignalSubscription {
    fn drop(&mut self) {
        if let Some(id) = self.1.take() {
            #[allow(deprecated)]
            self.0.signal_unsubscribe(id);
        }
    }
}

// rustdoc-stripper-ignore-next
/// A weak subscription to a D-Bus signal.
///
/// Like [`SignalSubscription`] but hold only a weak reference to the D-Bus
/// connection the signal is subscribed on, i.e. maintain the subscription on
/// the D-Bus signal only as long as some strong reference exists on the
/// corresponding D-Bus connection.
///
/// When dropped, unsubscribes from signal on the connection if it still exists,
/// and then drop the reference on the connection.  If no other strong reference
/// on the connection exists the connection is closed and destroyed.
#[derive(Debug)]
pub struct WeakSignalSubscription(WeakRef<DBusConnection>, Option<SignalSubscriptionId>);

impl WeakSignalSubscription {
    // rustdoc-stripper-ignore-next
    /// Upgrade this signal subscription to a strong one.
    #[must_use]
    pub fn upgrade(mut self) -> Option<SignalSubscription> {
        self.0
            .upgrade()
            .map(|c| SignalSubscription(c, self.1.take()))
    }
}

impl Drop for WeakSignalSubscription {
    fn drop(&mut self) {
        if let Some(id) = self.1.take()
            && let Some(connection) = self.0.upgrade()
        {
            #[allow(deprecated)]
            connection.signal_unsubscribe(id);
        }
    }
}

// rustdoc-stripper-ignore-next
/// An emitted D-Bus signal.
#[derive(Debug, Copy, Clone)]
pub struct DBusSignalRef<'a> {
    // rustdoc-stripper-ignore-next
    /// The connection the signal was emitted on.
    pub connection: &'a DBusConnection,
    // rustdoc-stripper-ignore-next
    /// The bus name of the sender which emitted the signal.
    pub sender_name: &'a str,
    // rustdoc-stripper-ignore-next
    /// The path of the object on `sender` the signal was emitted from.
    pub object_path: &'a str,
    // rustdoc-stripper-ignore-next
    /// The interface the signal belongs to.
    pub interface_name: &'a str,
    // rustdoc-stripper-ignore-next
    /// The name of the emitted signal.
    pub signal_name: &'a str,
    // rustdoc-stripper-ignore-next
    /// Parameters the signal was emitted with.
    pub parameters: &'a glib::Variant,
}

pin_project! {
    // rustdoc-stripper-ignore-next
    /// A subscribed stream.
    ///
    /// A stream which wraps an inner stream of type `S` while holding on to a
    /// subscription handle `H` to keep a subscription alive.
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct SubscribedSignalStream<H, S> {
        #[pin]
        stream: S,
        subscription: H,
    }
}

impl<S> SubscribedSignalStream<SignalSubscription, S> {
    // rustdoc-stripper-ignore-next
    /// Downgrade the inner signal subscription to a weak one.
    ///
    /// See [`SignalSubscription::downgrade`] and [`WeakSignalSubscription`].
    pub fn downgrade(self) -> SubscribedSignalStream<WeakSignalSubscription, S> {
        SubscribedSignalStream {
            subscription: self.subscription.downgrade(),
            stream: self.stream,
        }
    }
}

impl<S> SubscribedSignalStream<WeakSignalSubscription, S> {
    // rustdoc-stripper-ignore-next
    /// Upgrade the inner signal subscription to a strong one.
    ///
    /// See [`WeakSignalSubscription::upgrade`] and [`SignalSubscription`].
    pub fn downgrade(self) -> Option<SubscribedSignalStream<SignalSubscription, S>> {
        self.subscription
            .upgrade()
            .map(|subscription| SubscribedSignalStream {
                subscription,
                stream: self.stream,
            })
    }
}

impl<H, S> Stream for SubscribedSignalStream<H, S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<H, S> FusedStream for SubscribedSignalStream<H, S>
where
    S: FusedStream,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

// rustdoc-stripper-ignore-next
/// Build a registered DBus object, by handling different parts of DBus.
#[must_use = "The builder must be built to be used"]
pub struct RegistrationBuilder<'a> {
    connection: &'a DBusConnection,
    object_path: &'a str,
    interface_info: &'a DBusInterfaceInfo,
    #[allow(clippy::type_complexity)]
    method_call: Option<
        Box_<
            dyn Fn(
                DBusConnection,
                Option<&str>,
                &str,
                Option<&str>,
                &str,
                glib::Variant,
                DBusMethodInvocation,
            ),
        >,
    >,
    #[allow(clippy::type_complexity)]
    get_property:
        Option<Box_<dyn Fn(DBusConnection, Option<&str>, &str, &str, &str) -> glib::Variant>>,
    #[allow(clippy::type_complexity)]
    set_property:
        Option<Box_<dyn Fn(DBusConnection, Option<&str>, &str, &str, &str, glib::Variant) -> bool>>,
}

impl<'a> RegistrationBuilder<'a> {
    pub fn method_call<
        F: Fn(
                DBusConnection,
                Option<&str>,
                &str,
                Option<&str>,
                &str,
                glib::Variant,
                DBusMethodInvocation,
            ) + 'static,
    >(
        mut self,
        f: F,
    ) -> Self {
        self.method_call = Some(Box_::new(f));
        self
    }

    // rustdoc-stripper-ignore-next
    /// Handle method calls on this object.
    ///
    /// Return a builder for method calls which parses method names and
    /// parameters with the given [`DBusMethodCall`] and then allows to dispatch
    /// the parsed call either synchronously or asynchronously.
    pub fn typed_method_call<T: DBusMethodCall>(self) -> MethodCallBuilder<'a, T> {
        MethodCallBuilder {
            registration: self,
            capture_type: Default::default(),
        }
    }

    #[doc(alias = "get_property")]
    pub fn property<
        F: Fn(DBusConnection, Option<&str>, &str, &str, &str) -> glib::Variant + 'static,
    >(
        mut self,
        f: F,
    ) -> Self {
        self.get_property = Some(Box_::new(f));
        self
    }

    pub fn set_property<
        F: Fn(DBusConnection, Option<&str>, &str, &str, &str, glib::Variant) -> bool + 'static,
    >(
        mut self,
        f: F,
    ) -> Self {
        self.set_property = Some(Box_::new(f));
        self
    }

    pub fn build(self) -> Result<RegistrationId, glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let id = ffi::g_dbus_connection_register_object_with_closures(
                self.connection.to_glib_none().0,
                self.object_path.to_glib_none().0,
                self.interface_info.to_glib_none().0,
                self.method_call
                    .map(|f| {
                        glib::Closure::new_local(move |args| {
                            let conn = args[0].get::<DBusConnection>().unwrap();
                            let sender = args[1].get::<Option<&str>>().unwrap();
                            let object_path = args[2].get::<&str>().unwrap();
                            let interface_name = args[3].get::<Option<&str>>().unwrap();
                            let method_name = args[4].get::<&str>().unwrap();
                            let parameters = args[5].get::<glib::Variant>().unwrap();

                            // Work around GLib memory leak: Assume that the invocation is passed
                            // as `transfer full` into the closure.
                            //
                            // This workaround is not going to break with future versions of
                            // GLib as fixing the bug was considered a breaking API change.
                            //
                            // See https://gitlab.gnome.org/GNOME/glib/-/merge_requests/4427
                            let invocation = from_glib_full(glib::gobject_ffi::g_value_get_object(
                                args[6].as_ptr(),
                            )
                                as *mut ffi::GDBusMethodInvocation);

                            f(
                                conn,
                                sender,
                                object_path,
                                interface_name,
                                method_name,
                                parameters,
                                invocation,
                            );
                            None
                        })
                    })
                    .to_glib_none()
                    .0,
                self.get_property
                    .map(|f| {
                        glib::Closure::new_local(move |args| {
                            let conn = args[0].get::<DBusConnection>().unwrap();
                            let sender = args[1].get::<Option<&str>>().unwrap();
                            let object_path = args[2].get::<&str>().unwrap();
                            let interface_name = args[3].get::<&str>().unwrap();
                            let property_name = args[4].get::<&str>().unwrap();
                            let result =
                                f(conn, sender, object_path, interface_name, property_name);
                            Some(result.to_value())
                        })
                    })
                    .to_glib_none()
                    .0,
                self.set_property
                    .map(|f| {
                        glib::Closure::new_local(move |args| {
                            let conn = args[0].get::<DBusConnection>().unwrap();
                            let sender = args[1].get::<Option<&str>>().unwrap();
                            let object_path = args[2].get::<&str>().unwrap();
                            let interface_name = args[3].get::<&str>().unwrap();
                            let property_name = args[4].get::<&str>().unwrap();
                            let value = args[5].get::<glib::Variant>().unwrap();
                            let result = f(
                                conn,
                                sender,
                                object_path,
                                interface_name,
                                property_name,
                                value,
                            );
                            Some(result.to_value())
                        })
                    })
                    .to_glib_none()
                    .0,
                &mut error,
            );

            if error.is_null() {
                Ok(RegistrationId(NonZeroU32::new_unchecked(id)))
            } else {
                Err(from_glib_full(error))
            }
        }
    }
}

impl DBusConnection {
    #[doc(alias = "g_dbus_connection_register_object")]
    #[doc(alias = "g_dbus_connection_register_object_with_closures")]
    pub fn register_object<'a>(
        &'a self,
        object_path: &'a str,
        interface_info: &'a DBusInterfaceInfo,
    ) -> RegistrationBuilder<'a> {
        RegistrationBuilder {
            connection: self,
            object_path,
            interface_info,
            method_call: None,
            get_property: None,
            set_property: None,
        }
    }

    #[doc(alias = "g_dbus_connection_unregister_object")]
    pub fn unregister_object(
        &self,
        registration_id: RegistrationId,
    ) -> Result<(), glib::error::BoolError> {
        unsafe {
            glib::result_from_gboolean!(
                ffi::g_dbus_connection_unregister_object(
                    self.to_glib_none().0,
                    registration_id.0.into()
                ),
                "Failed to unregister D-Bus object"
            )
        }
    }

    #[doc(alias = "g_dbus_connection_export_action_group")]
    pub fn export_action_group<P: IsA<ActionGroup>>(
        &self,
        object_path: &str,
        action_group: &P,
    ) -> Result<ActionGroupExportId, glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let id = ffi::g_dbus_connection_export_action_group(
                self.to_glib_none().0,
                object_path.to_glib_none().0,
                action_group.as_ref().to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(ActionGroupExportId(NonZeroU32::new_unchecked(id)))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_dbus_connection_unexport_action_group")]
    pub fn unexport_action_group(&self, export_id: ActionGroupExportId) {
        unsafe {
            ffi::g_dbus_connection_unexport_action_group(self.to_glib_none().0, export_id.0.into());
        }
    }

    #[doc(alias = "g_dbus_connection_export_menu_model")]
    pub fn export_menu_model<P: IsA<MenuModel>>(
        &self,
        object_path: &str,
        menu: &P,
    ) -> Result<MenuModelExportId, glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let id = ffi::g_dbus_connection_export_menu_model(
                self.to_glib_none().0,
                object_path.to_glib_none().0,
                menu.as_ref().to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(MenuModelExportId(NonZeroU32::new_unchecked(id)))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_dbus_connection_unexport_menu_model")]
    pub fn unexport_menu_model(&self, export_id: MenuModelExportId) {
        unsafe {
            ffi::g_dbus_connection_unexport_menu_model(self.to_glib_none().0, export_id.0.into());
        }
    }

    #[doc(alias = "g_dbus_connection_add_filter")]
    pub fn add_filter<
        P: Fn(&DBusConnection, &DBusMessage, bool) -> Option<DBusMessage> + 'static,
    >(
        &self,
        filter_function: P,
    ) -> FilterId {
        let filter_function_data: Box_<P> = Box_::new(filter_function);
        unsafe extern "C" fn filter_function_func<
            P: Fn(&DBusConnection, &DBusMessage, bool) -> Option<DBusMessage> + 'static,
        >(
            connection: *mut ffi::GDBusConnection,
            message: *mut ffi::GDBusMessage,
            incoming: glib::ffi::gboolean,
            user_data: glib::ffi::gpointer,
        ) -> *mut ffi::GDBusMessage {
            unsafe {
                let connection = from_glib_borrow(connection);
                let message = from_glib_full(message);
                let incoming = from_glib(incoming);
                let callback: &P = &*(user_data as *mut _);
                let res = (*callback)(&connection, &message, incoming);
                res.into_glib_ptr()
            }
        }
        let filter_function = Some(filter_function_func::<P> as _);
        unsafe extern "C" fn user_data_free_func_func<
            P: Fn(&DBusConnection, &DBusMessage, bool) -> Option<DBusMessage> + 'static,
        >(
            data: glib::ffi::gpointer,
        ) {
            unsafe {
                let _callback: Box_<P> = Box_::from_raw(data as *mut _);
            }
        }
        let destroy_call3 = Some(user_data_free_func_func::<P> as _);
        let super_callback0: Box_<P> = filter_function_data;
        unsafe {
            let id = ffi::g_dbus_connection_add_filter(
                self.to_glib_none().0,
                filter_function,
                Box_::into_raw(super_callback0) as *mut _,
                destroy_call3,
            );
            FilterId(NonZeroU32::new_unchecked(id))
        }
    }

    #[doc(alias = "g_dbus_connection_remove_filter")]
    pub fn remove_filter(&self, filter_id: FilterId) {
        unsafe {
            ffi::g_dbus_connection_remove_filter(self.to_glib_none().0, filter_id.0.into());
        }
    }

    // rustdoc-stripper-ignore-next
    /// Subscribe to a D-Bus signal.
    ///
    /// See [`Self::signal_subscribe`] for arguments.
    ///
    /// Return a signal subscription which keeps a reference to this D-Bus
    /// connection and unsubscribes from the signal when dropped.
    ///
    /// To avoid reference cycles you may wish to downgrade the returned
    /// subscription to a weak one with [`SignalSubscription::downgrade`].
    #[must_use]
    pub fn subscribe_to_signal<P: Fn(DBusSignalRef) + 'static>(
        &self,
        sender: Option<&str>,
        interface_name: Option<&str>,
        member: Option<&str>,
        object_path: Option<&str>,
        arg0: Option<&str>,
        flags: DBusSignalFlags,
        callback: P,
    ) -> SignalSubscription {
        #[allow(deprecated)]
        let id = self.signal_subscribe(
            sender,
            interface_name,
            member,
            object_path,
            arg0,
            flags,
            move |connection, sender_name, object_path, interface_name, signal_name, parameters| {
                callback(DBusSignalRef {
                    connection,
                    sender_name,
                    object_path,
                    interface_name,
                    signal_name,
                    parameters,
                });
            },
        );
        SignalSubscription(self.clone(), Some(id))
    }

    #[doc(alias = "g_dbus_connection_signal_subscribe")]
    #[allow(clippy::too_many_arguments)]
    #[deprecated(note = "Prefer subscribe_to_signal")]
    pub fn signal_subscribe<
        P: Fn(&DBusConnection, &str, &str, &str, &str, &glib::Variant) + 'static,
    >(
        &self,
        sender: Option<&str>,
        interface_name: Option<&str>,
        member: Option<&str>,
        object_path: Option<&str>,
        arg0: Option<&str>,
        flags: DBusSignalFlags,
        callback: P,
    ) -> SignalSubscriptionId {
        let callback_data: Box_<P> = Box_::new(callback);
        unsafe extern "C" fn callback_func<
            P: Fn(&DBusConnection, &str, &str, &str, &str, &glib::Variant) + 'static,
        >(
            connection: *mut ffi::GDBusConnection,
            sender_name: *const libc::c_char,
            object_path: *const libc::c_char,
            interface_name: *const libc::c_char,
            signal_name: *const libc::c_char,
            parameters: *mut glib::ffi::GVariant,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let connection = from_glib_borrow(connection);
                let sender_name: Borrowed<glib::GString> = from_glib_borrow(sender_name);
                let object_path: Borrowed<glib::GString> = from_glib_borrow(object_path);
                let interface_name: Borrowed<glib::GString> = from_glib_borrow(interface_name);
                let signal_name: Borrowed<glib::GString> = from_glib_borrow(signal_name);
                let parameters = from_glib_borrow(parameters);
                let callback: &P = &*(user_data as *mut _);
                (*callback)(
                    &connection,
                    sender_name.as_str(),
                    object_path.as_str(),
                    interface_name.as_str(),
                    signal_name.as_str(),
                    &parameters,
                );
            }
        }
        let callback = Some(callback_func::<P> as _);
        unsafe extern "C" fn user_data_free_func_func<
            P: Fn(&DBusConnection, &str, &str, &str, &str, &glib::Variant) + 'static,
        >(
            data: glib::ffi::gpointer,
        ) {
            unsafe {
                let _callback: Box_<P> = Box_::from_raw(data as *mut _);
            }
        }
        let destroy_call9 = Some(user_data_free_func_func::<P> as _);
        let super_callback0: Box_<P> = callback_data;
        unsafe {
            let id = ffi::g_dbus_connection_signal_subscribe(
                self.to_glib_none().0,
                sender.to_glib_none().0,
                interface_name.to_glib_none().0,
                member.to_glib_none().0,
                object_path.to_glib_none().0,
                arg0.to_glib_none().0,
                flags.into_glib(),
                callback,
                Box_::into_raw(super_callback0) as *mut _,
                destroy_call9,
            );
            SignalSubscriptionId(NonZeroU32::new_unchecked(id))
        }
    }

    #[doc(alias = "g_dbus_connection_signal_unsubscribe")]
    #[deprecated(note = "Prefer subscribe_to_signal")]
    pub fn signal_unsubscribe(&self, subscription_id: SignalSubscriptionId) {
        unsafe {
            ffi::g_dbus_connection_signal_unsubscribe(
                self.to_glib_none().0,
                subscription_id.0.into(),
            );
        }
    }

    // rustdoc-stripper-ignore-next
    /// Subscribe to a D-Bus signal and receive signal emissions as a stream.
    ///
    /// See [`Self::signal_subscribe`] for arguments.  `map_signal` maps the
    /// received signal to the stream's element.
    ///
    /// The returned stream holds a strong reference to this D-Bus connection,
    /// and unsubscribes from the signal when dropped. To avoid reference cycles
    /// you may wish to downgrade the returned stream to hold only weak
    /// reference to the connection using [`SubscribedSignalStream::downgrade`].
    ///
    /// After invoking `map_signal` the stream threads incoming signals through
    /// an unbounded channel.  Hence, memory consumption will keep increasing
    /// as long as the stream consumer does not keep up with signal emissions.
    /// If you need to perform expensive processing in response to signals it's
    /// therefore recommended to insert an extra buffering and if the buffer
    /// overruns, either fail drop the entire stream, or drop individual signal
    /// emissions until the buffer has space again.
    pub fn receive_signal<T: 'static, F: Fn(DBusSignalRef) -> T + 'static>(
        &self,
        sender: Option<&str>,
        interface_name: Option<&str>,
        member: Option<&str>,
        object_path: Option<&str>,
        arg0: Option<&str>,
        flags: DBusSignalFlags,
        map_signal: F,
    ) -> SubscribedSignalStream<SignalSubscription, impl Stream<Item = T> + use<T, F>> {
        let (tx, rx) = mpsc::unbounded();
        let subscription = self.subscribe_to_signal(
            sender,
            interface_name,
            member,
            object_path,
            arg0,
            flags,
            move |signal| {
                // Just ignore send errors: if the receiver is dropped, the
                // signal subscription is dropped too, so the callback won't
                // be invoked anymore.
                let _ = tx.unbounded_send(map_signal(signal));
            },
        );
        SubscribedSignalStream {
            subscription,
            stream: rx,
        }
    }

    // rustdoc-stripper-ignore-next
    /// Subscribe to a D-Bus signal and receive signal parameters as a stream.
    ///
    /// Like [`Self::receive_signal`] (which see for more information), but
    /// automatically decodes the emitted signal parameters to type `T`.
    /// If decoding fails the corresponding variant type error is sent
    /// downstream.
    pub fn receive_signal_parameters<T>(
        &self,
        sender: Option<&str>,
        interface_name: Option<&str>,
        member: Option<&str>,
        object_path: Option<&str>,
        arg0: Option<&str>,
        flags: DBusSignalFlags,
    ) -> SubscribedSignalStream<
        SignalSubscription,
        impl Stream<Item = Result<T, VariantTypeMismatchError>> + use<T>,
    >
    where
        T: FromVariant + 'static,
    {
        self.receive_signal(
            sender,
            interface_name,
            member,
            object_path,
            arg0,
            flags,
            |signal| signal.parameters.try_get(),
        )
    }
}
