// Take a look at the license at the top of the repository in the LICENSE file.

#![deny(unsafe_op_in_unsafe_fn)]

use glib::{prelude::*, subclass::prelude::*, translate::*};

use crate::{
    DBusProxy, ffi,
    subclass::prelude::{AsyncInitableImpl, DBusInterfaceImpl, InitableImpl},
};

pub trait DBusProxyImpl:
    ObjectImpl
    + AsyncInitableImpl
    + DBusInterfaceImpl
    + InitableImpl
    + ObjectSubclass<Type: IsA<DBusProxy>>
{
    fn g_properties_changed(
        &self,
        changed_properties: &glib::Variant,
        invalidated_properties: &glib::StrVRef,
    ) {
        self.parent_g_properties_changed(changed_properties, invalidated_properties);
    }

    fn g_signal(
        &self,
        sender_name: Option<&glib::GStr>,
        signal_name: &glib::GStr,
        parameters: &glib::Variant,
    ) {
        self.parent_g_signal(sender_name, signal_name, parameters);
    }
}

pub trait DBusProxyImplExt: DBusProxyImpl {
    fn parent_g_properties_changed(
        &self,
        changed_properties: &glib::Variant,
        invalidated_properties: &glib::StrVRef,
    ) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GDBusProxyClass;

            if let Some(f) = (*parent_class).g_properties_changed {
                f(
                    self.obj().unsafe_cast_ref::<DBusProxy>().to_glib_none().0,
                    changed_properties.to_glib_none().0,
                    invalidated_properties.to_glib_none().0,
                );
            }
        }
    }

    fn parent_g_signal(
        &self,
        sender_name: Option<&glib::GStr>,
        signal_name: &glib::GStr,
        parameters: &glib::Variant,
    ) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GDBusProxyClass;

            if let Some(f) = (*parent_class).g_signal {
                f(
                    self.obj().unsafe_cast_ref::<DBusProxy>().to_glib_none().0,
                    sender_name.to_glib_none().0,
                    signal_name.to_glib_none().0,
                    parameters.to_glib_none().0,
                );
            }
        }
    }
}

impl<T: DBusProxyImpl> DBusProxyImplExt for T {}

unsafe impl<T: DBusProxyImpl> IsSubclassable<T> for DBusProxy {
    fn class_init(class: &mut glib::Class<Self>) {
        Self::parent_class_init::<T>(class);
        let class = class.as_mut();
        class.g_properties_changed = Some(g_properties_changed::<T>);
        class.g_signal = Some(g_signal::<T>);
    }
}

unsafe extern "C" fn g_properties_changed<T: DBusProxyImpl>(
    proxy: *mut ffi::GDBusProxy,
    changed_properties: *mut glib::ffi::GVariant,
    invalidated_properties: *const *const libc::c_char,
) {
    let instance = unsafe { &*(proxy as *mut T::Instance) };
    let imp = instance.imp();

    let changed_properties = unsafe { from_glib_borrow(changed_properties) };
    let invalidated_properties = unsafe { glib::StrVRef::from_glib_borrow(invalidated_properties) };
    imp.g_properties_changed(&changed_properties, invalidated_properties);
}

unsafe extern "C" fn g_signal<T: DBusProxyImpl>(
    proxy: *mut ffi::GDBusProxy,
    sender_name: *const libc::c_char,
    signal_name: *const libc::c_char,
    parameters: *mut glib::ffi::GVariant,
) {
    let instance = unsafe { &*(proxy as *mut T::Instance) };
    let imp = instance.imp();

    let sender_name = unsafe { Option::<&glib::GStr>::from_glib_none(sender_name) };
    let signal_name = unsafe { from_glib_none(signal_name) };
    let parameters = unsafe { from_glib_borrow(parameters) };
    imp.g_signal(sender_name, signal_name, &parameters);
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;

    use crate::{
        AsyncInitable, Cancellable, DBusConnection, DBusConnectionFlags, DBusInterface, Initable,
        MemoryInputStream, MemoryOutputStream, SimpleIOStream,
    };

    mod imp {
        use super::*;

        #[derive(Default)]
        pub struct CustomDBusProxyImpl {
            pub(super) g_properties_changed_called: RefCell<bool>,
            pub(super) g_signal_called: RefCell<bool>,
        }

        #[glib::object_subclass]
        impl ObjectSubclass for CustomDBusProxyImpl {
            const NAME: &'static str = "CustomDBusProxyImpl";
            type Type = super::CustomDBusProxyImpl;
            type ParentType = DBusProxy;
            type Interfaces = (DBusInterface, Initable, AsyncInitable);
        }

        impl ObjectImpl for CustomDBusProxyImpl {}

        impl InitableImpl for CustomDBusProxyImpl {}
        impl AsyncInitableImpl for CustomDBusProxyImpl {}
        impl DBusInterfaceImpl for CustomDBusProxyImpl {}

        impl DBusProxyImpl for CustomDBusProxyImpl {
            fn g_signal(
                &self,
                sender_name: Option<&glib::GStr>,
                signal_name: &glib::GStr,
                parameters: &glib::Variant,
            ) {
                *self.g_signal_called.borrow_mut() = true;
                self.parent_g_signal(sender_name, signal_name, parameters);
            }

            fn g_properties_changed(
                &self,
                changed_properties: &glib::Variant,
                invalidated_properties: &glib::StrVRef,
            ) {
                *self.g_properties_changed_called.borrow_mut() = true;
                self.parent_g_properties_changed(changed_properties, invalidated_properties);
            }
        }
    }

    glib::wrapper! {
        pub struct CustomDBusProxyImpl(ObjectSubclass<imp::CustomDBusProxyImpl>)
            @extends DBusProxy,
            @implements DBusInterface, Initable, AsyncInitable;
    }

    #[test]
    fn g_signal_is_called() {
        let proxy = create_custom_proxy_impl();
        let sender_name = "org.example.Sender";
        let signal_name = "example";
        let parameters = glib::Variant::array_from_iter::<glib::Variant>([]);
        proxy
            .upcast_ref::<DBusProxy>()
            .emit_by_name::<()>("g-signal", &[&sender_name, &signal_name, &parameters]);
        assert!(*proxy.imp().g_signal_called.borrow());
    }

    #[test]
    fn g_properties_changed_is_called() {
        let proxy = create_custom_proxy_impl();
        let changed_properties = glib::Variant::array_from_iter::<String>([]);
        let invalidated_properties = glib::StrV::new();
        proxy.upcast_ref::<DBusProxy>().emit_by_name::<()>(
            "g-properties-changed",
            &[&changed_properties, &invalidated_properties],
        );
        assert!(*proxy.imp().g_properties_changed_called.borrow());
    }

    fn create_custom_proxy_impl() -> CustomDBusProxyImpl {
        // By providing a connection, we prevent the proxy
        // from trying to establish a real DBus connection.
        let connection = create_no_op_dbus_connection();
        Initable::builder()
            .property("g-connection", connection)
            .property("g-object-path", "/org/example/test")
            .property("g-interface-name", "org.example.Test")
            .build(Option::<&Cancellable>::None)
            .expect("failed to create CustomDBusProxyImpl")
    }

    fn create_no_op_dbus_connection() -> DBusConnection {
        let input = MemoryInputStream::new();
        let output = MemoryOutputStream::new_resizable();
        let stream = SimpleIOStream::new(&input, &output);
        DBusConnection::new_sync(
            &stream,
            None,
            DBusConnectionFlags::NONE,
            None,
            Option::<&Cancellable>::None,
        )
        .expect("failed to create DBusConnection")
    }
}
