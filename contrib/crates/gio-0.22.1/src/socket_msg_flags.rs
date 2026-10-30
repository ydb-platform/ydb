// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;
use glib::{bitflags, prelude::*, translate::*, Type};

bitflags::bitflags! {
    #[doc(alias = "GSocketMsgFlags")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct SocketMsgFlags: ffi::GSocketMsgFlags {
        #[doc(alias = "G_SOCKET_MSG_NONE")]
        const NONE = ffi::G_SOCKET_MSG_NONE as _;
        #[doc(alias = "G_SOCKET_MSG_OOB")]
        const OOB = ffi::G_SOCKET_MSG_OOB as _;
        #[doc(alias = "G_SOCKET_MSG_PEEK")]
        const PEEK = ffi::G_SOCKET_MSG_PEEK as _;
        #[doc(alias = "G_SOCKET_MSG_DONTROUTE")]
        const DONTROUTE = ffi::G_SOCKET_MSG_DONTROUTE as _;
    }
}

#[doc(hidden)]
impl IntoGlib for SocketMsgFlags {
    type GlibType = ffi::GSocketMsgFlags;

    #[inline]
    fn into_glib(self) -> ffi::GSocketMsgFlags {
        self.bits()
    }
}

#[doc(hidden)]
impl FromGlib<ffi::GSocketMsgFlags> for SocketMsgFlags {
    #[inline]
    unsafe fn from_glib(value: ffi::GSocketMsgFlags) -> Self {
        Self::from_bits_truncate(value)
    }
}

impl StaticType for SocketMsgFlags {
    #[inline]
    fn static_type() -> Type {
        unsafe { from_glib(ffi::g_socket_msg_flags_get_type()) }
    }
}

impl glib::value::ValueType for SocketMsgFlags {
    type Type = Self;
}

unsafe impl<'a> glib::value::FromValue<'a> for SocketMsgFlags {
    type Checker = glib::value::GenericValueTypeChecker<Self>;

    unsafe fn from_value(value: &'a glib::Value) -> Self {
        from_glib(glib::gobject_ffi::g_value_get_flags(value.to_glib_none().0) as i32)
    }
}

impl ToValue for SocketMsgFlags {
    fn to_value(&self) -> glib::Value {
        let mut value = glib::Value::for_value_type::<Self>();
        unsafe {
            glib::gobject_ffi::g_value_set_flags(
                value.to_glib_none_mut().0,
                self.into_glib() as u32,
            );
        }
        value
    }

    fn value_type(&self) -> glib::Type {
        Self::static_type()
    }
}
