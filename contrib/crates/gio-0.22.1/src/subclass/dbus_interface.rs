// Take a look at the license at the top of the repository in the LICENSE file.

#![deny(unsafe_op_in_unsafe_fn)]

use glib::{prelude::*, subclass::prelude::*};

use crate::DBusInterface;

pub trait DBusInterfaceImpl: ObjectImpl + ObjectSubclass<Type: IsA<DBusInterface>> {}

pub trait DBusInterfaceImplExt: DBusInterfaceImpl {}

impl<T: DBusInterfaceImpl> DBusInterfaceImplExt for T {}

unsafe impl<T: DBusInterfaceImpl> IsImplementable<T> for DBusInterface {
    fn interface_init(_iface: &mut glib::Interface<Self>) {}
}
