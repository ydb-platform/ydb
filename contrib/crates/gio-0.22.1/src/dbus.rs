// Take a look at the license at the top of the repository in the LICENSE file.

use std::num::NonZeroU32;

use glib::translate::*;

use crate::{BusNameOwnerFlags, BusNameWatcherFlags, BusType, DBusConnection, ffi};

#[derive(Debug, Eq, PartialEq)]
pub struct OwnerId(NonZeroU32);
#[derive(Debug, Eq, PartialEq)]
pub struct WatcherId(NonZeroU32);

fn own_closure<F>(f: F) -> glib::Closure
where
    F: Fn(DBusConnection, &str) + 'static,
{
    glib::Closure::new_local(move |args| {
        let conn = args[0].get::<DBusConnection>().unwrap();
        let name = args[1].get::<&str>().unwrap();
        f(conn, name);
        None
    })
}

fn appeared_closure<F>(f: F) -> glib::Closure
where
    F: Fn(DBusConnection, &str, &str) + 'static,
{
    glib::Closure::new_local(move |args| {
        let conn = args[0].get::<DBusConnection>().unwrap();
        let name = args[1].get::<&str>().unwrap();
        let name_owner = args[2].get::<&str>().unwrap();
        f(conn, name, name_owner);
        None
    })
}

fn vanished_closure<F>(f: F) -> glib::Closure
where
    F: Fn(DBusConnection, &str) + 'static,
{
    glib::Closure::new_local(move |args| {
        let conn = args[0].get::<DBusConnection>().unwrap();
        let name = args[1].get::<&str>().unwrap();
        f(conn, name);
        None
    })
}

#[doc(alias = "g_bus_own_name_on_connection_with_closures")]
pub fn bus_own_name_on_connection<NameAcquired, NameLost>(
    connection: &DBusConnection,
    name: &str,
    flags: BusNameOwnerFlags,
    name_acquired: NameAcquired,
    name_lost: NameLost,
) -> OwnerId
where
    NameAcquired: Fn(DBusConnection, &str) + 'static,
    NameLost: Fn(DBusConnection, &str) + 'static,
{
    unsafe {
        let id = ffi::g_bus_own_name_on_connection_with_closures(
            connection.to_glib_none().0,
            name.to_glib_none().0,
            flags.into_glib(),
            own_closure(name_acquired).to_glib_none().0,
            own_closure(name_lost).to_glib_none().0,
        );
        OwnerId(NonZeroU32::new_unchecked(id))
    }
}

#[doc(alias = "g_bus_own_name_with_closures")]
pub fn bus_own_name<BusAcquired, NameAcquired, NameLost>(
    bus_type: BusType,
    name: &str,
    flags: BusNameOwnerFlags,
    bus_acquired: BusAcquired,
    name_acquired: NameAcquired,
    name_lost: NameLost,
) -> OwnerId
where
    BusAcquired: Fn(DBusConnection, &str) + 'static,
    NameAcquired: Fn(DBusConnection, &str) + 'static,
    NameLost: Fn(Option<DBusConnection>, &str) + 'static,
{
    unsafe {
        let id = ffi::g_bus_own_name_with_closures(
            bus_type.into_glib(),
            name.to_glib_none().0,
            flags.into_glib(),
            own_closure(bus_acquired).to_glib_none().0,
            own_closure(name_acquired).to_glib_none().0,
            glib::Closure::new_local(move |args| {
                let conn = args[0].get::<Option<DBusConnection>>().unwrap();
                let name = args[1].get::<&str>().unwrap();
                name_lost(conn, name);
                None
            })
            .to_glib_none()
            .0,
        );
        OwnerId(NonZeroU32::new_unchecked(id))
    }
}

#[doc(alias = "g_bus_unown_name")]
pub fn bus_unown_name(owner_id: OwnerId) {
    unsafe {
        ffi::g_bus_unown_name(owner_id.0.into());
    }
}

#[doc(alias = "g_bus_watch_name_on_connection_with_closures")]
pub fn bus_watch_name_on_connection<NameAppeared, NameVanished>(
    connection: &DBusConnection,
    name: &str,
    flags: BusNameWatcherFlags,
    name_appeared: NameAppeared,
    name_vanished: NameVanished,
) -> WatcherId
where
    NameAppeared: Fn(DBusConnection, &str, &str) + 'static,
    NameVanished: Fn(DBusConnection, &str) + 'static,
{
    unsafe {
        let id = ffi::g_bus_watch_name_on_connection_with_closures(
            connection.to_glib_none().0,
            name.to_glib_none().0,
            flags.into_glib(),
            appeared_closure(name_appeared).to_glib_none().0,
            vanished_closure(name_vanished).to_glib_none().0,
        );
        WatcherId(NonZeroU32::new_unchecked(id))
    }
}

#[doc(alias = "g_bus_watch_name_with_closures")]
pub fn bus_watch_name<NameAppeared, NameVanished>(
    bus_type: BusType,
    name: &str,
    flags: BusNameWatcherFlags,
    name_appeared: NameAppeared,
    name_vanished: NameVanished,
) -> WatcherId
where
    NameAppeared: Fn(DBusConnection, &str, &str) + 'static,
    NameVanished: Fn(DBusConnection, &str) + 'static,
{
    unsafe {
        let id = ffi::g_bus_watch_name_with_closures(
            bus_type.into_glib(),
            name.to_glib_none().0,
            flags.into_glib(),
            appeared_closure(name_appeared).to_glib_none().0,
            vanished_closure(name_vanished).to_glib_none().0,
        );
        WatcherId(NonZeroU32::new_unchecked(id))
    }
}

#[doc(alias = "g_bus_unwatch_name")]
pub fn bus_unwatch_name(watcher_id: WatcherId) {
    unsafe {
        ffi::g_bus_unwatch_name(watcher_id.0.into());
    }
}
