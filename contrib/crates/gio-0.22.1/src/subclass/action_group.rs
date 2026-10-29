// Take a look at the license at the top of the repository in the LICENSE file.

use std::{mem, ptr, sync::OnceLock};

use glib::{GString, Quark, Variant, VariantType, prelude::*, subclass::prelude::*, translate::*};

use crate::{ActionGroup, ffi};

pub trait ActionGroupImpl: ObjectImpl + ObjectSubclass<Type: IsA<ActionGroup>> {
    fn action_added(&self, action_name: &str) {
        self.parent_action_added(action_name);
    }

    fn action_enabled_changed(&self, action_name: &str, enabled: bool) {
        self.parent_action_enabled_changed(action_name, enabled);
    }

    fn action_removed(&self, action_name: &str) {
        self.parent_action_removed(action_name);
    }

    fn action_state_changed(&self, action_name: &str, state: &Variant) {
        self.parent_action_state_changed(action_name, state);
    }

    fn activate_action(&self, action_name: &str, parameter: Option<&Variant>) {
        self.parent_activate_action(action_name, parameter);
    }

    fn change_action_state(&self, action_name: &str, value: &Variant) {
        self.parent_change_action_state(action_name, value)
    }

    #[doc(alias = "get_action_enabled")]
    fn action_is_enabled(&self, action_name: &str) -> bool {
        self.parent_action_is_enabled(action_name)
    }

    #[doc(alias = "get_action_parameter_type")]
    fn action_parameter_type(&self, action_name: &str) -> Option<VariantType> {
        self.parent_action_parameter_type(action_name)
    }

    #[doc(alias = "get_action_state")]
    fn action_state(&self, action_name: &str) -> Option<Variant> {
        self.parent_action_state(action_name)
    }

    #[doc(alias = "get_action_state_hint")]
    fn action_state_hint(&self, action_name: &str) -> Option<Variant> {
        self.parent_action_state_hint(action_name)
    }

    #[doc(alias = "get_action_state_type")]
    fn action_state_type(&self, action_name: &str) -> Option<VariantType> {
        self.parent_action_state_type(action_name)
    }

    fn has_action(&self, action_name: &str) -> bool {
        self.parent_has_action(action_name)
    }

    fn list_actions(&self) -> Vec<String>;
    fn query_action(
        &self,
        action_name: &str,
    ) -> Option<(
        bool,
        Option<VariantType>,
        Option<VariantType>,
        Option<Variant>,
        Option<Variant>,
    )>;
}

pub trait ActionGroupImplExt: ActionGroupImpl {
    fn parent_action_added(&self, action_name: &str) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            if let Some(func) = (*parent_iface).action_added {
                func(
                    self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                    action_name.to_glib_none().0,
                );
            }
        }
    }

    fn parent_action_enabled_changed(&self, action_name: &str, enabled: bool) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            if let Some(func) = (*parent_iface).action_enabled_changed {
                func(
                    self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                    action_name.to_glib_none().0,
                    enabled.into_glib(),
                );
            }
        }
    }

    fn parent_action_removed(&self, action_name: &str) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            if let Some(func) = (*parent_iface).action_removed {
                func(
                    self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                    action_name.to_glib_none().0,
                );
            }
        }
    }

    fn parent_action_state_changed(&self, action_name: &str, state: &Variant) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            if let Some(func) = (*parent_iface).action_state_changed {
                func(
                    self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                    action_name.to_glib_none().0,
                    state.to_glib_none().0,
                );
            }
        }
    }

    fn parent_activate_action(&self, action_name: &str, parameter: Option<&Variant>) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .activate_action
                .expect("no parent \"activate_action\" implementation");
            func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
                parameter.to_glib_none().0,
            );
        }
    }

    fn parent_change_action_state(&self, action_name: &str, value: &Variant) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .change_action_state
                .expect("no parent \"change_action_state\" implementation");
            func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
                value.to_glib_none().0,
            );
        }
    }

    fn parent_action_is_enabled(&self, action_name: &str) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .get_action_enabled
                .expect("no parent \"action_is_enabled\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
            );
            from_glib(ret)
        }
    }

    fn parent_action_parameter_type(&self, action_name: &str) -> Option<VariantType> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .get_action_parameter_type
                .expect("no parent \"get_action_parameter_type\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
            );
            from_glib_none(ret)
        }
    }

    fn parent_action_state(&self, action_name: &str) -> Option<Variant> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .get_action_state
                .expect("no parent \"get_action_state\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
            );
            from_glib_none(ret)
        }
    }

    fn parent_action_state_hint(&self, action_name: &str) -> Option<Variant> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .get_action_state_hint
                .expect("no parent \"get_action_state_hint\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
            );
            from_glib_none(ret)
        }
    }

    fn parent_action_state_type(&self, action_name: &str) -> Option<VariantType> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .get_action_state_type
                .expect("no parent \"get_action_state_type\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
            );
            from_glib_none(ret)
        }
    }

    fn parent_has_action(&self, action_name: &str) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .has_action
                .expect("no parent \"has_action\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
            );
            from_glib(ret)
        }
    }

    fn parent_list_actions(&self) -> Vec<String> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .list_actions
                .expect("no parent \"list_actions\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0);
            FromGlibPtrContainer::from_glib_none(ret)
        }
    }

    fn parent_query_action(
        &self,
        action_name: &str,
    ) -> Option<(
        bool,
        Option<VariantType>,
        Option<VariantType>,
        Option<Variant>,
        Option<Variant>,
    )> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<ActionGroup>()
                as *const ffi::GActionGroupInterface;

            let func = (*parent_iface)
                .query_action
                .expect("no parent \"query_action\" implementation");

            let mut enabled = mem::MaybeUninit::uninit();
            let mut parameter_type = ptr::null();
            let mut state_type = ptr::null();
            let mut state_hint = ptr::null_mut();
            let mut state = ptr::null_mut();

            let ret: bool = from_glib(func(
                self.obj().unsafe_cast_ref::<ActionGroup>().to_glib_none().0,
                action_name.to_glib_none().0,
                enabled.as_mut_ptr(),
                &mut parameter_type,
                &mut state_type,
                &mut state_hint,
                &mut state,
            ));

            if !ret {
                None
            } else {
                Some((
                    from_glib(enabled.assume_init()),
                    from_glib_none(parameter_type),
                    from_glib_none(state_type),
                    from_glib_none(state_hint),
                    from_glib_none(state),
                ))
            }
        }
    }
}

impl<T: ActionGroupImpl> ActionGroupImplExt for T {}

unsafe impl<T: ActionGroupImpl> IsImplementable<T> for ActionGroup {
    fn interface_init(iface: &mut glib::Interface<Self>) {
        let iface = iface.as_mut();

        iface.action_added = Some(action_group_action_added::<T>);
        iface.action_enabled_changed = Some(action_group_action_enabled_changed::<T>);
        iface.action_removed = Some(action_group_action_removed::<T>);
        iface.action_state_changed = Some(action_group_action_state_changed::<T>);
        iface.activate_action = Some(action_group_activate_action::<T>);
        iface.change_action_state = Some(action_group_change_action_state::<T>);
        iface.get_action_enabled = Some(action_group_get_action_enabled::<T>);
        iface.get_action_parameter_type = Some(action_group_get_action_parameter_type::<T>);
        iface.get_action_state = Some(action_group_get_action_state::<T>);
        iface.get_action_state_hint = Some(action_group_get_action_state_hint::<T>);
        iface.get_action_state_type = Some(action_group_get_action_state_type::<T>);
        iface.has_action = Some(action_group_has_action::<T>);
        iface.list_actions = Some(action_group_list_actions::<T>);
        iface.query_action = Some(action_group_query_action::<T>);
    }
}

unsafe extern "C" fn action_group_has_action<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let action_name = GString::from_glib_borrow(action_nameptr);
        let imp = instance.imp();

        imp.has_action(&action_name).into_glib()
    }
}

unsafe extern "C" fn action_group_get_action_enabled<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        imp.action_is_enabled(&action_name).into_glib()
    }
}

// rustdoc-stripper-ignore-next
/// Struct to hold a pointer and free it on `Drop::drop`
struct PtrHolder<T, F: Fn(*mut T) + 'static>(*mut T, F);

impl<T, F: Fn(*mut T) + 'static> Drop for PtrHolder<T, F> {
    fn drop(&mut self) {
        (self.1)(self.0)
    }
}

unsafe extern "C" fn action_group_get_action_parameter_type<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) -> *const glib::ffi::GVariantType {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);
        let wrap = from_glib_borrow::<_, ActionGroup>(action_group);

        let ret = imp.action_parameter_type(&action_name);

        if let Some(param_type) = ret {
            let parameter_type_quark = {
                static QUARK: OnceLock<Quark> = OnceLock::new();
                *QUARK.get_or_init(|| {
                    Quark::from_str("gtk-rs-subclass-action-group-get-action-parameter")
                })
            };
            let param_type = param_type.into_glib_ptr();
            wrap.set_qdata(
                parameter_type_quark,
                PtrHolder(param_type, |ptr| glib::ffi::g_free(ptr as *mut _)),
            );
            param_type
        } else {
            ptr::null()
        }
    }
}

unsafe extern "C" fn action_group_get_action_state_type<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) -> *const glib::ffi::GVariantType {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        let ret = imp.action_state_type(&action_name);

        if let Some(state_type) = ret {
            let instance = imp.obj();
            let state_type_quark = {
                static QUARK: OnceLock<Quark> = OnceLock::new();
                *QUARK.get_or_init(|| {
                    Quark::from_str("gtk-rs-subclass-action-group-get-action-state-type")
                })
            };
            let state_type = state_type.into_glib_ptr();
            instance.set_qdata(
                state_type_quark,
                PtrHolder(state_type, |ptr| glib::ffi::g_free(ptr as *mut _)),
            );
            state_type
        } else {
            ptr::null()
        }
    }
}

unsafe extern "C" fn action_group_get_action_state_hint<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) -> *mut glib::ffi::GVariant {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        let ret = imp.action_state_hint(&action_name);
        if let Some(state_hint) = ret {
            let instance = imp.obj();
            let state_hint_quark = {
                static QUARK: OnceLock<Quark> = OnceLock::new();
                *QUARK.get_or_init(|| {
                    Quark::from_str("gtk-rs-subclass-action-group-get-action-state-hint")
                })
            };
            let state_hint_ptr = state_hint.into_glib_ptr();
            instance.set_qdata(
                state_hint_quark,
                PtrHolder(state_hint_ptr, |ptr| glib::ffi::g_variant_unref(ptr)),
            );
            state_hint_ptr
        } else {
            ptr::null_mut()
        }
    }
}

unsafe extern "C" fn action_group_get_action_state<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) -> *mut glib::ffi::GVariant {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        let ret = imp.action_state(&action_name);
        if let Some(state) = ret {
            let instance = imp.obj();
            let state_quark = {
                static QUARK: OnceLock<Quark> = OnceLock::new();
                *QUARK.get_or_init(|| {
                    Quark::from_str("gtk-rs-subclass-action-group-get-action-state")
                })
            };
            let state_ptr = state.into_glib_ptr();
            instance.set_qdata(
                state_quark,
                PtrHolder(state_ptr, |ptr| glib::ffi::g_variant_unref(ptr)),
            );
            state_ptr
        } else {
            ptr::null_mut()
        }
    }
}

unsafe extern "C" fn action_group_change_action_state<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
    stateptr: *mut glib::ffi::GVariant,
) {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);
        let state = Variant::from_glib_borrow(stateptr);

        imp.change_action_state(&action_name, &state)
    }
}

unsafe extern "C" fn action_group_activate_action<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
    parameterptr: *mut glib::ffi::GVariant,
) {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);
        let param: Borrowed<Option<Variant>> = from_glib_borrow(parameterptr);

        imp.activate_action(&action_name, param.as_ref().as_ref())
    }
}

unsafe extern "C" fn action_group_action_added<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        imp.action_added(&action_name)
    }
}

unsafe extern "C" fn action_group_action_removed<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
) {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        imp.action_removed(&action_name)
    }
}

unsafe extern "C" fn action_group_action_enabled_changed<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
    enabled: glib::ffi::gboolean,
) {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        imp.action_enabled_changed(&action_name, from_glib(enabled))
    }
}

unsafe extern "C" fn action_group_action_state_changed<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
    stateptr: *mut glib::ffi::GVariant,
) {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);
        let state = Variant::from_glib_borrow(stateptr);

        imp.action_state_changed(&action_name, &state)
    }
}

unsafe extern "C" fn action_group_list_actions<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
) -> *mut *mut libc::c_char {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();

        let actions = imp.list_actions();

        {
            let instance = imp.obj();
            let actions_quark = {
                static QUARK: OnceLock<Quark> = OnceLock::new();
                *QUARK.get_or_init(|| Quark::from_str("gtk-rs-subclass-action-group-list-actions"))
            };
            let actionsptr = actions.to_glib_full();
            instance.set_qdata(actions_quark, actionsptr);
            actionsptr
        }
    }
}

unsafe extern "C" fn action_group_query_action<T: ActionGroupImpl>(
    action_group: *mut ffi::GActionGroup,
    action_nameptr: *const libc::c_char,
    enabled: *mut glib::ffi::gboolean,
    parameter_type: *mut *const glib::ffi::GVariantType,
    state_type: *mut *const glib::ffi::GVariantType,
    state_hint: *mut *mut glib::ffi::GVariant,
    state: *mut *mut glib::ffi::GVariant,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(action_group as *mut T::Instance);
        let imp = instance.imp();
        let action_name = GString::from_glib_borrow(action_nameptr);

        let ret = imp.query_action(&action_name);
        if let Some((rs_enabled, rs_parameter_type, rs_state_type, rs_state_hint, rs_state)) = ret {
            let instance = imp.obj();

            if !enabled.is_null() {
                *enabled = rs_enabled.into_glib();
            }
            if !parameter_type.is_null() {
                if let Some(rs_parameter_type) = rs_parameter_type {
                    let param_type_quark = {
                        static QUARK: OnceLock<Quark> = OnceLock::new();
                        *QUARK.get_or_init(|| {
                            Quark::from_str(
                                "gtk-rs-subclass-action-group-query-action-parameter-type",
                            )
                        })
                    };
                    let ret = rs_parameter_type.into_glib_ptr();
                    instance.set_qdata(
                        param_type_quark,
                        PtrHolder(ret, |ptr| glib::ffi::g_free(ptr as *mut _)),
                    );
                    *parameter_type = ret;
                } else {
                    *parameter_type = ptr::null_mut();
                }
            }
            if !state_type.is_null() {
                if let Some(rs_state_type) = rs_state_type {
                    let state_type_quark = {
                        static QUARK: OnceLock<Quark> = OnceLock::new();
                        *QUARK.get_or_init(|| {
                            Quark::from_str("gtk-rs-subclass-action-group-query-action-state-type")
                        })
                    };
                    let ret = rs_state_type.into_glib_ptr();
                    instance.set_qdata(
                        state_type_quark,
                        PtrHolder(ret, |ptr| glib::ffi::g_free(ptr as *mut _)),
                    );
                    *state_type = ret;
                } else {
                    *state_type = ptr::null_mut();
                }
            }
            if !state_hint.is_null() {
                if let Some(rs_state_hint) = rs_state_hint {
                    let state_hint_quark = {
                        static QUARK: OnceLock<Quark> = OnceLock::new();
                        *QUARK.get_or_init(|| {
                            Quark::from_str("gtk-rs-subclass-action-group-query-action-state-hint")
                        })
                    };
                    let ret = rs_state_hint.into_glib_ptr();
                    instance.set_qdata(
                        state_hint_quark,
                        PtrHolder(ret, |ptr| glib::ffi::g_variant_unref(ptr)),
                    );
                    *state_hint = ret;
                } else {
                    *state_hint = ptr::null_mut();
                }
            }
            if !state.is_null() {
                if let Some(rs_state) = rs_state {
                    let state_quark = {
                        static QUARK: OnceLock<Quark> = OnceLock::new();
                        *QUARK.get_or_init(|| {
                            Quark::from_str("gtk-rs-subclass-action-group-query-action-state")
                        })
                    };
                    let ret = rs_state.into_glib_ptr();
                    instance.set_qdata(
                        state_quark,
                        PtrHolder(ret, |ptr| glib::ffi::g_variant_unref(ptr)),
                    );
                    *state = ret;
                } else {
                    *state = ptr::null_mut();
                }
            }
            true
        } else {
            false
        }
        .into_glib()
    }
}
