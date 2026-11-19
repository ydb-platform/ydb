// Take a look at the license at the top of the repository in the LICENSE file.

use std::{boxed::Box as Box_, mem::transmute};

use glib::{SignalHandlerId, prelude::*, signal::connect_raw, translate::*};

use crate::{DBusProxy, ffi};

pub trait DBusProxyExtManual: IsA<DBusProxy> + 'static {
    #[doc(alias = "g-properties-changed")]
    fn connect_g_properties_changed<
        F: Fn(&Self, &glib::Variant, &glib::StrVRef) + Send + Sync + 'static,
    >(
        &self,
        f: F,
    ) -> SignalHandlerId {
        unsafe extern "C" fn g_properties_changed_trampoline<
            P: IsA<DBusProxy>,
            F: Fn(&P, &glib::Variant, &glib::StrVRef) + Send + Sync + 'static,
        >(
            this: *mut ffi::GDBusProxy,
            changed_properties: *mut glib::ffi::GVariant,
            invalidated_properties: *const *const libc::c_char,
            f: glib::ffi::gpointer,
        ) {
            unsafe {
                let f: &F = &*(f as *const F);
                f(
                    DBusProxy::from_glib_borrow(this).unsafe_cast_ref(),
                    &from_glib_borrow(changed_properties),
                    glib::StrVRef::from_glib_borrow(invalidated_properties),
                )
            }
        }
        unsafe {
            let f: Box_<F> = Box_::new(f);
            connect_raw(
                self.as_ptr() as *mut _,
                c"g-properties-changed".as_ptr() as *const _,
                Some(std::mem::transmute::<*const (), unsafe extern "C" fn()>(
                    g_properties_changed_trampoline::<Self, F> as *const (),
                )),
                Box_::into_raw(f),
            )
        }
    }

    #[cfg(feature = "v2_72")]
    #[doc(alias = "g-signal")]
    fn connect_g_signal<
        F: Fn(&Self, Option<&str>, &str, &glib::Variant) + Send + Sync + 'static,
    >(
        &self,
        detail: Option<&str>,
        f: F,
    ) -> SignalHandlerId {
        unsafe extern "C" fn g_signal_trampoline<
            P: IsA<DBusProxy>,
            F: Fn(&P, Option<&str>, &str, &glib::Variant) + Send + Sync + 'static,
        >(
            this: *mut ffi::GDBusProxy,
            sender_name: *mut libc::c_char,
            signal_name: *mut libc::c_char,
            parameters: *mut glib::ffi::GVariant,
            f: glib::ffi::gpointer,
        ) {
            unsafe {
                let f: &F = &*(f as *const F);
                f(
                    DBusProxy::from_glib_borrow(this).unsafe_cast_ref(),
                    Option::<glib::GString>::from_glib_borrow(sender_name)
                        .as_ref()
                        .as_deref(),
                    &glib::GString::from_glib_borrow(signal_name),
                    &from_glib_borrow(parameters),
                )
            }
        }
        unsafe {
            let f: Box_<F> = Box_::new(f);
            let detailed_signal_name = detail.map(|name| format!("g-signal::{name}\0"));
            let signal_name: &[u8] = detailed_signal_name
                .as_ref()
                .map_or(&b"g-signal\0"[..], |n| n.as_bytes());
            connect_raw(
                self.as_ptr() as *mut _,
                signal_name.as_ptr() as *const _,
                Some(transmute::<*const (), unsafe extern "C" fn()>(
                    g_signal_trampoline::<Self, F> as *const (),
                )),
                Box_::into_raw(f),
            )
        }
    }

    #[cfg(not(feature = "v2_72"))]
    #[doc(alias = "g-signal")]
    fn connect_g_signal<
        F: Fn(&Self, Option<&str>, &str, &glib::Variant) + Send + Sync + 'static,
    >(
        &self,
        f: F,
    ) -> SignalHandlerId {
        unsafe extern "C" fn g_signal_trampoline<
            P: IsA<DBusProxy>,
            F: Fn(&P, Option<&str>, &str, &glib::Variant) + Send + Sync + 'static,
        >(
            this: *mut ffi::GDBusProxy,
            sender_name: *mut libc::c_char,
            signal_name: *mut libc::c_char,
            parameters: *mut glib::ffi::GVariant,
            f: glib::ffi::gpointer,
        ) {
            unsafe {
                let f: &F = &*(f as *const F);
                f(
                    DBusProxy::from_glib_borrow(this).unsafe_cast_ref(),
                    Option::<glib::GString>::from_glib_borrow(sender_name)
                        .as_ref()
                        .as_deref(),
                    &glib::GString::from_glib_borrow(signal_name),
                    &from_glib_borrow(parameters),
                )
            }
        }
        unsafe {
            let f: Box_<F> = Box_::new(f);
            connect_raw(
                self.as_ptr() as *mut _,
                b"g-signal\0".as_ptr() as *const _,
                Some(transmute::<*const (), unsafe extern "C" fn()>(
                    g_signal_trampoline::<Self, F> as *const (),
                )),
                Box_::into_raw(f),
            )
        }
    }
}

impl<O: IsA<DBusProxy>> DBusProxyExtManual for O {}
