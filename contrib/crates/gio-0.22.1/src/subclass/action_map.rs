// Take a look at the license at the top of the repository in the LICENSE file.

use std::{collections::HashMap, sync::OnceLock};

use glib::{GString, Quark, prelude::*, subclass::prelude::*, translate::*};

use crate::{Action, ActionMap, ffi};

pub trait ActionMapImpl: ObjectImpl + ObjectSubclass<Type: IsA<ActionMap>> {
    fn lookup_action(&self, action_name: &str) -> Option<Action>;
    fn add_action(&self, action: &Action);
    fn remove_action(&self, action_name: &str);
}

pub trait ActionMapImplExt: ActionMapImpl {
    fn parent_lookup_action(&self, name: &str) -> Option<Action> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionMap>()
                as *const ffi::GActionMapInterface;

            let func = (*parent_iface)
                .lookup_action
                .expect("no parent \"lookup_action\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<ActionMap>().to_glib_none().0,
                name.to_glib_none().0,
            );
            from_glib_none(ret)
        }
    }

    fn parent_add_action(&self, action: &Action) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionMap>()
                as *const ffi::GActionMapInterface;

            let func = (*parent_iface)
                .add_action
                .expect("no parent \"add_action\" implementation");
            func(
                self.obj().unsafe_cast_ref::<ActionMap>().to_glib_none().0,
                action.to_glib_none().0,
            );
        }
    }

    fn parent_remove_action(&self, action_name: &str) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionMap>()
                as *const ffi::GActionMapInterface;

            let func = (*parent_iface)
                .remove_action
                .expect("no parent \"remove_action\" implementation");
            func(
                self.obj().unsafe_cast_ref::<ActionMap>().to_glib_none().0,
                action_name.to_glib_none().0,
            );
        }
    }
}

impl<T: ActionMapImpl> ActionMapImplExt for T {}

unsafe impl<T: ActionMapImpl> IsImplementable<T> for ActionMap {
    fn interface_init(iface: &mut glib::Interface<Self>) {
        let iface = iface.as_mut();

        iface.lookup_action = Some(action_map_lookup_action::<T>);
        iface.add_action = Some(action_map_add_action::<T>);
        iface.remove_action = Some(action_map_remove_action::<T>);
    }
}

unsafe extern "C" fn action_map_lookup_action<T: ActionMapImpl>(
    action_map: *mut ffi::GActionMap,
    action_nameptr: *const libc::c_char,
) -> *mut ffi::GAction {
    unsafe {
        let instance = &*(action_map as *mut T::Instance);
        let action_name = GString::from_glib_borrow(action_nameptr);
        let imp = instance.imp();

        let ret = imp.lookup_action(&action_name);
        if let Some(action) = ret {
            let instance = imp.obj();
            let actionptr = action.to_glib_none().0;

            let action_map_quark = {
                static QUARK: OnceLock<Quark> = OnceLock::new();
                *QUARK.get_or_init(|| Quark::from_str("gtk-rs-subclass-action-map-lookup-action"))
            };

            let mut map = instance
                .steal_qdata::<HashMap<String, Action>>(action_map_quark)
                .unwrap_or_default();
            map.insert(action_name.to_string(), action);
            instance.set_qdata(action_map_quark, map);

            actionptr
        } else {
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn action_map_add_action<T: ActionMapImpl>(
    action_map: *mut ffi::GActionMap,
    actionptr: *mut ffi::GAction,
) {
    unsafe {
        let instance = &*(action_map as *mut T::Instance);
        let imp = instance.imp();
        let action: Borrowed<Action> = from_glib_borrow(actionptr);

        imp.add_action(&action);
    }
}

unsafe extern "C" fn action_map_remove_action<T: ActionMapImpl>(
    action_map: *mut ffi::GActionMap,
    action_nameptr: *const libc::c_char,
) {
    unsafe {
        let instance = &*(action_map as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        imp.remove_action(&action_name);
    }
}
