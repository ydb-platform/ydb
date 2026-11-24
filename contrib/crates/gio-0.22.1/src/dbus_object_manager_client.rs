// Take a look at the license at the top of the repository in the LICENSE file.

use crate::{
    BusType, Cancellable, DBusConnection, DBusObjectManagerClient, DBusObjectManagerClientFlags,
    DBusObjectProxy, DBusProxy, GioFuture, ffi,
};
use glib::object::{Cast as _, IsA};
use glib::signal::connect_raw;
use glib::translate::{
    Borrowed, FromGlibPtrBorrow as _, IntoGlib as _, ToGlibPtr as _, from_glib_borrow,
    from_glib_full,
};
use glib::{SignalHandlerId, StrVRef};
use std::future::Future;
use std::pin::Pin;

type DBusProxyTypeFn = Box<
    dyn Fn(&DBusObjectManagerClient, &str, Option<&str>) -> glib::types::Type
        + Send
        + Sync
        + 'static,
>;

impl DBusObjectManagerClient {
    #[doc(alias = "g_dbus_object_manager_client_new_sync")]
    pub fn new_sync(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: Option<&str>,
        object_path: &str,
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<DBusObjectManagerClient, glib::Error> {
        Self::new_sync_impl(connection, flags, name, object_path, None, cancellable)
    }

    #[doc(alias = "g_dbus_object_manager_client_new_sync")]
    pub fn new_sync_with_fn<
        F: Fn(&DBusObjectManagerClient, &str, Option<&str>) -> glib::types::Type
            + Send
            + Sync
            + 'static,
    >(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: Option<&str>,
        object_path: &str,
        get_proxy_type_func: F,
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<DBusObjectManagerClient, glib::Error> {
        Self::new_sync_impl(
            connection,
            flags,
            name,
            object_path,
            Some(Box::new(get_proxy_type_func)),
            cancellable,
        )
    }

    #[doc(alias = "g_dbus_object_manager_client_new_for_bus_sync")]
    pub fn for_bus_sync(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<DBusObjectManagerClient, glib::Error> {
        Self::for_bus_sync_impl(bus_type, flags, name, object_path, None, cancellable)
    }

    #[doc(alias = "g_dbus_object_manager_client_new_for_bus_sync")]
    pub fn for_bus_sync_with_fn<
        F: Fn(&DBusObjectManagerClient, &str, Option<&str>) -> glib::types::Type
            + Send
            + Sync
            + 'static,
    >(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: F,
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<DBusObjectManagerClient, glib::Error> {
        Self::for_bus_sync_impl(
            bus_type,
            flags,
            name,
            object_path,
            Some(Box::new(get_proxy_type_func)),
            cancellable,
        )
    }

    #[allow(clippy::new_ret_no_self)]
    #[doc(alias = "g_dbus_object_manager_client_new")]
    pub fn new<P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static>(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
    ) {
        Self::new_impl(
            connection,
            flags,
            name,
            object_path,
            None,
            cancellable,
            callback,
        )
    }

    #[allow(clippy::new_ret_no_self)]
    #[doc(alias = "g_dbus_object_manager_client_new")]
    pub fn new_with_fn<
        P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static,
        F: Fn(&DBusObjectManagerClient, &str, Option<&str>) -> glib::types::Type
            + Send
            + Sync
            + 'static,
    >(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: F,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
    ) {
        Self::new_impl(
            connection,
            flags,
            name,
            object_path,
            Some(Box::new(get_proxy_type_func)),
            cancellable,
            callback,
        )
    }

    #[allow(clippy::new_ret_no_self)]
    fn new_impl<P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static>(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: Option<DBusProxyTypeFn>,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
    ) {
        let main_context = glib::MainContext::ref_thread_default();
        let is_main_context_owner = main_context.is_owner();
        let has_acquired_main_context = (!is_main_context_owner)
            .then(|| main_context.acquire().ok())
            .flatten();
        assert!(
            is_main_context_owner || has_acquired_main_context.is_some(),
            "Async operations only allowed if the thread is owning the MainContext"
        );

        unsafe extern "C" fn get_proxy_type_func_func(
            manager: *mut ffi::GDBusObjectManagerClient,
            object_path: *const std::ffi::c_char,
            interface_name: *const std::ffi::c_char,
            data: glib::ffi::gpointer,
        ) -> glib::ffi::GType {
            unsafe {
                let manager = from_glib_borrow(manager);
                let object_path: Borrowed<glib::GString> = from_glib_borrow(object_path);
                let interface_name: Borrowed<Option<glib::GString>> =
                    from_glib_borrow(interface_name);
                let callback = &*(data as *mut Option<DBusProxyTypeFn>);
                if let Some(ref callback) = *callback {
                    callback(
                        &manager,
                        object_path.as_str(),
                        (*interface_name).as_ref().map(|s| s.as_str()),
                    )
                } else {
                    panic!("cannot get closure...")
                }
                .into_glib()
            }
        }

        unsafe extern "C" fn get_proxy_type_destroy_notify_func(data: glib::ffi::gpointer) {
            unsafe {
                let _callback = Box::from_raw(data as *mut Option<DBusProxyTypeFn>);
            }
        }

        unsafe extern "C" fn new_trampoline<
            P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = std::ptr::null_mut();
                let ret = ffi::g_dbus_object_manager_client_new_finish(res, &mut error);
                let result = if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<glib::thread_guard::ThreadGuard<P>> =
                    Box::from_raw(user_data as *mut _);
                let callback: P = callback.into_inner();
                callback(result);
            }
        }

        let get_proxy_type_user_data = Box::new(get_proxy_type_func);
        let get_proxy_type_func = if get_proxy_type_user_data.is_some() {
            Some(get_proxy_type_func_func as _)
        } else {
            None
        };
        let get_proxy_type_destroy_notify = if get_proxy_type_user_data.is_some() {
            Some(get_proxy_type_destroy_notify_func as _)
        } else {
            None
        };

        let user_data: Box<glib::thread_guard::ThreadGuard<P>> =
            Box::new(glib::thread_guard::ThreadGuard::new(callback));
        let callback = new_trampoline::<P>;

        unsafe {
            ffi::g_dbus_object_manager_client_new(
                connection.to_glib_none().0,
                flags.into_glib(),
                name.to_glib_none().0,
                object_path.to_glib_none().0,
                get_proxy_type_func,
                Box::into_raw(get_proxy_type_user_data) as *mut _,
                get_proxy_type_destroy_notify,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    pub fn new_future(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
    ) -> Pin<Box<dyn Future<Output = Result<DBusObjectManagerClient, glib::Error>> + 'static>> {
        Self::new_future_impl(connection, flags, name, object_path, None)
    }

    pub fn new_future_with_fn<
        F: Fn(&DBusObjectManagerClient, &str, Option<&str>) -> glib::types::Type
            + Send
            + Sync
            + 'static,
    >(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: F,
    ) -> Pin<Box<dyn Future<Output = Result<DBusObjectManagerClient, glib::Error>> + 'static>> {
        Self::new_future_impl(
            connection,
            flags,
            name,
            object_path,
            Some(Box::new(get_proxy_type_func)),
        )
    }

    fn new_future_impl(
        connection: &DBusConnection,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: Option<DBusProxyTypeFn>,
    ) -> Pin<Box<dyn Future<Output = Result<DBusObjectManagerClient, glib::Error>> + 'static>> {
        let connection = connection.clone();
        let name = String::from(name);
        let object_path = String::from(object_path);
        Box::pin(GioFuture::new(&(), move |_obj, cancellable, send| {
            Self::new_impl(
                &connection,
                flags,
                &name,
                &object_path,
                get_proxy_type_func,
                Some(cancellable),
                move |res| {
                    send.resolve(res);
                },
            );
        }))
    }

    #[doc(alias = "g_dbus_object_manager_client_new_for_bus")]
    #[allow(clippy::new_ret_no_self)]
    pub fn new_for_bus<
        P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static,
    >(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
    ) {
        Self::new_for_bus_impl(
            bus_type,
            flags,
            name,
            object_path,
            None,
            cancellable,
            callback,
        );
    }

    #[doc(alias = "g_dbus_object_manager_client_new_for_bus")]
    #[allow(clippy::new_ret_no_self)]
    pub fn new_for_bus_with_fn<
        P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static,
        F: Fn(&DBusObjectManagerClient, &str, Option<&str>) -> glib::types::Type
            + Send
            + Sync
            + 'static,
    >(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: F,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
    ) {
        Self::new_for_bus_impl(
            bus_type,
            flags,
            name,
            object_path,
            Some(Box::new(get_proxy_type_func)),
            cancellable,
            callback,
        );
    }

    #[allow(clippy::new_ret_no_self)]
    fn new_for_bus_impl<
        P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static,
    >(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: Option<DBusProxyTypeFn>,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
    ) {
        let main_context = glib::MainContext::ref_thread_default();
        let is_main_context_owner = main_context.is_owner();
        let has_acquired_main_context = (!is_main_context_owner)
            .then(|| main_context.acquire().ok())
            .flatten();
        assert!(
            is_main_context_owner || has_acquired_main_context.is_some(),
            "Async operations only allowed if the thread is owning the MainContext"
        );

        unsafe extern "C" fn get_proxy_type_func_func(
            manager: *mut ffi::GDBusObjectManagerClient,
            object_path: *const std::ffi::c_char,
            interface_name: *const std::ffi::c_char,
            data: glib::ffi::gpointer,
        ) -> glib::ffi::GType {
            unsafe {
                let manager = from_glib_borrow(manager);
                let object_path: Borrowed<glib::GString> = from_glib_borrow(object_path);
                let interface_name: Borrowed<Option<glib::GString>> =
                    from_glib_borrow(interface_name);
                let callback = &*(data as *mut Option<DBusProxyTypeFn>);
                if let Some(ref callback) = *callback {
                    callback(
                        &manager,
                        object_path.as_str(),
                        (*interface_name).as_ref().map(|s| s.as_str()),
                    )
                } else {
                    panic!("cannot get closure...")
                }
                .into_glib()
            }
        }

        unsafe extern "C" fn get_proxy_type_destroy_notify_func(data: glib::ffi::gpointer) {
            unsafe {
                let _callback = Box::from_raw(data as *mut Option<DBusProxyTypeFn>);
            }
        }

        unsafe extern "C" fn new_for_bus_trampoline<
            P: FnOnce(Result<DBusObjectManagerClient, glib::Error>) + Send + Sync + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = std::ptr::null_mut();
                let ret = ffi::g_dbus_object_manager_client_new_finish(res, &mut error);
                let result = if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<glib::thread_guard::ThreadGuard<P>> =
                    Box::from_raw(user_data as *mut _);
                let callback: P = callback.into_inner();
                callback(result);
            }
        }

        let get_proxy_type_user_data = Box::new(get_proxy_type_func);
        let get_proxy_type_func = if get_proxy_type_user_data.is_some() {
            Some(get_proxy_type_func_func as _)
        } else {
            None
        };
        let get_proxy_type_destroy_notify = if get_proxy_type_user_data.is_some() {
            Some(get_proxy_type_destroy_notify_func as _)
        } else {
            None
        };

        let user_data: Box<glib::thread_guard::ThreadGuard<P>> =
            Box::new(glib::thread_guard::ThreadGuard::new(callback));
        let callback = new_for_bus_trampoline::<P>;

        unsafe {
            ffi::g_dbus_object_manager_client_new_for_bus(
                bus_type.into_glib(),
                flags.into_glib(),
                name.to_glib_none().0,
                object_path.to_glib_none().0,
                get_proxy_type_func,
                Box::into_raw(get_proxy_type_user_data) as *mut _,
                get_proxy_type_destroy_notify,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    pub fn new_for_bus_future(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
    ) -> Pin<Box<dyn Future<Output = Result<DBusObjectManagerClient, glib::Error>> + 'static>> {
        Self::new_for_bus_future_impl(bus_type, flags, name, object_path, None)
    }

    pub fn new_for_bus_future_with_fn<
        F: Fn(&DBusObjectManagerClient, &str, Option<&str>) -> glib::types::Type
            + Send
            + Sync
            + 'static,
    >(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: F,
    ) -> Pin<Box<dyn Future<Output = Result<DBusObjectManagerClient, glib::Error>> + 'static>> {
        Self::new_for_bus_future_impl(
            bus_type,
            flags,
            name,
            object_path,
            Some(Box::new(get_proxy_type_func)),
        )
    }

    fn new_for_bus_future_impl(
        bus_type: BusType,
        flags: DBusObjectManagerClientFlags,
        name: &str,
        object_path: &str,
        get_proxy_type_func: Option<DBusProxyTypeFn>,
    ) -> Pin<Box<dyn Future<Output = Result<DBusObjectManagerClient, glib::Error>> + 'static>> {
        let name = String::from(name);
        let object_path = String::from(object_path);
        Box::pin(GioFuture::new(&(), move |_obj, cancellable, send| {
            Self::new_for_bus_impl(
                bus_type,
                flags,
                &name,
                &object_path,
                get_proxy_type_func,
                Some(cancellable),
                move |res| {
                    send.resolve(res);
                },
            );
        }))
    }
}

pub trait DBusObjectManagerClientExtManual: IsA<DBusObjectManagerClient> + 'static {
    #[doc(alias = "interface-proxy-properties-changed")]
    fn connect_interface_proxy_properties_changed<
        F: Fn(&Self, &DBusObjectProxy, &DBusProxy, &glib::Variant, &StrVRef) + Send + Sync + 'static,
    >(
        &self,
        f: F,
    ) -> SignalHandlerId {
        unsafe extern "C" fn interface_proxy_properties_changed_trampoline<
            P: IsA<DBusObjectManagerClient>,
            F: Fn(&P, &DBusObjectProxy, &DBusProxy, &glib::Variant, &StrVRef) + Send + Sync + 'static,
        >(
            this: *mut ffi::GDBusObjectManagerClient,
            object_proxy: *mut ffi::GDBusObjectProxy,
            interface_proxy: *mut ffi::GDBusProxy,
            changed_properties: *mut glib::ffi::GVariant,
            invalidated_properties: *const *const std::ffi::c_char,
            f: glib::ffi::gpointer,
        ) {
            unsafe {
                let f: &F = &*(f as *const F);
                f(
                    DBusObjectManagerClient::from_glib_borrow(this).unsafe_cast_ref(),
                    &from_glib_borrow(object_proxy),
                    &from_glib_borrow(interface_proxy),
                    &from_glib_borrow(changed_properties),
                    StrVRef::from_glib_borrow(invalidated_properties),
                )
            }
        }
        unsafe {
            let f: Box<F> = Box::new(f);
            connect_raw(
                self.as_ptr() as *mut _,
                c"interface-proxy-properties-changed".as_ptr() as *const _,
                Some(std::mem::transmute::<*const (), unsafe extern "C" fn()>(
                    interface_proxy_properties_changed_trampoline::<Self, F> as *const (),
                )),
                Box::into_raw(f),
            )
        }
    }
}

impl<O: IsA<DBusObjectManagerClient>> DBusObjectManagerClientExtManual for O {}
