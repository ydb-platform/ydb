// Take a look at the license at the top of the repository in the LICENSE file.

use glib::prelude::*;

use crate::{DBusConnection, DebugControllerDBus};

pub trait DebugControllerDBusExtManual: IsA<DebugControllerDBus> + Sized {
    fn connection(&self) -> DBusConnection {
        ObjectExt::property(self.as_ref(), "connection")
    }
}

impl<O: IsA<DebugControllerDBus>> DebugControllerDBusExtManual for O {}
