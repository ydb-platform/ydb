// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{BoolError, StrV, Variant, prelude::*, translate::*};

use crate::{Settings, SettingsBindFlags, ffi, prelude::*};

#[must_use = "The builder must be built to be used"]
pub struct BindingBuilder<'a> {
    settings: &'a Settings,
    key: &'a str,
    object: &'a glib::Object,
    property: &'a str,
    flags: SettingsBindFlags,
    #[allow(clippy::type_complexity)]
    get_mapping: Option<Box<dyn Fn(&glib::Variant, glib::Type) -> Option<glib::Value>>>,
    #[allow(clippy::type_complexity)]
    set_mapping: Option<Box<dyn Fn(&glib::Value, glib::VariantType) -> Option<glib::Variant>>>,
}

impl BindingBuilder<'_> {
    pub fn flags(mut self, flags: SettingsBindFlags) -> Self {
        self.flags = flags;
        self
    }

    // rustdoc-stripper-ignore-next
    /// Set the binding flags to [`GET`][crate::SettingsBindFlags::GET].
    pub fn get(mut self) -> Self {
        self.flags |= SettingsBindFlags::GET;
        self
    }

    // rustdoc-stripper-ignore-next
    /// Set the binding flags to [`SET`][crate::SettingsBindFlags::SET].
    pub fn set(mut self) -> Self {
        self.flags |= SettingsBindFlags::SET;
        self
    }

    // rustdoc-stripper-ignore-next
    /// Unsets the default [`GET`][crate::SettingsBindFlags::GET] flag.
    pub fn set_only(mut self) -> Self {
        self.flags = (self.flags - SettingsBindFlags::GET) | SettingsBindFlags::SET;
        self
    }

    // rustdoc-stripper-ignore-next
    /// Unsets the default [`SET`][crate::SettingsBindFlags::SET] flag.
    pub fn get_only(mut self) -> Self {
        self.flags = (self.flags - SettingsBindFlags::SET) | SettingsBindFlags::GET;
        self
    }

    // rustdoc-stripper-ignore-next
    /// Set the binding flags to [`NO_SENSITIVITY`][crate::SettingsBindFlags::NO_SENSITIVITY].
    pub fn no_sensitivity(mut self) -> Self {
        self.flags |= SettingsBindFlags::NO_SENSITIVITY;
        self
    }

    // rustdoc-stripper-ignore-next
    /// Set the binding flags to [`GET_NO_CHANGES`][crate::SettingsBindFlags::GET_NO_CHANGES].
    pub fn get_no_changes(mut self) -> Self {
        self.flags |= SettingsBindFlags::GET_NO_CHANGES;
        self
    }

    // rustdoc-stripper-ignore-next
    /// Set the binding flags to [`INVERT_BOOLEAN`][crate::SettingsBindFlags::INVERT_BOOLEAN].
    pub fn invert_boolean(mut self) -> Self {
        self.flags |= SettingsBindFlags::INVERT_BOOLEAN;
        self
    }

    #[doc(alias = "get_mapping")]
    pub fn mapping<F: Fn(&glib::Variant, glib::Type) -> Option<glib::Value> + 'static>(
        mut self,
        f: F,
    ) -> Self {
        self.get_mapping = Some(Box::new(f));
        self
    }

    pub fn set_mapping<
        F: Fn(&glib::Value, glib::VariantType) -> Option<glib::Variant> + 'static,
    >(
        mut self,
        f: F,
    ) -> Self {
        self.set_mapping = Some(Box::new(f));
        self
    }

    pub fn build(self) {
        type Mappings = (
            Option<Box<dyn Fn(&glib::Variant, glib::Type) -> Option<glib::Value>>>,
            Option<Box<dyn Fn(&glib::Value, glib::VariantType) -> Option<glib::Variant>>>,
        );
        unsafe extern "C" fn bind_with_mapping_get_trampoline(
            value: *mut glib::gobject_ffi::GValue,
            variant: *mut glib::ffi::GVariant,
            user_data: glib::ffi::gpointer,
        ) -> glib::ffi::gboolean {
            unsafe {
                let user_data = &*(user_data as *const Mappings);
                let f = user_data.0.as_ref().unwrap();
                let value = &mut *(value as *mut glib::Value);
                match f(&from_glib_borrow(variant), value.type_()) {
                    Some(v) => {
                        *value = v;
                        true
                    }
                    _ => false,
                }
                .into_glib()
            }
        }
        unsafe extern "C" fn bind_with_mapping_set_trampoline(
            value: *const glib::gobject_ffi::GValue,
            variant_type: *const glib::ffi::GVariantType,
            user_data: glib::ffi::gpointer,
        ) -> *mut glib::ffi::GVariant {
            unsafe {
                let user_data = &*(user_data as *const Mappings);
                let f = user_data.1.as_ref().unwrap();
                let value = &*(value as *const glib::Value);
                f(value, from_glib_none(variant_type)).into_glib_ptr()
            }
        }
        unsafe extern "C" fn destroy_closure(ptr: *mut libc::c_void) {
            unsafe {
                let _ = Box::<Mappings>::from_raw(ptr as *mut _);
            }
        }

        if self.get_mapping.is_none() && self.set_mapping.is_none() {
            unsafe {
                ffi::g_settings_bind(
                    self.settings.to_glib_none().0,
                    self.key.to_glib_none().0,
                    self.object.to_glib_none().0,
                    self.property.to_glib_none().0,
                    self.flags.into_glib(),
                );
            }
        } else {
            let get_trampoline: Option<unsafe extern "C" fn(_, _, _) -> _> =
                if self.get_mapping.is_none() {
                    None
                } else {
                    Some(bind_with_mapping_get_trampoline)
                };
            let set_trampoline: Option<unsafe extern "C" fn(_, _, _) -> _> =
                if self.set_mapping.is_none() {
                    None
                } else {
                    Some(bind_with_mapping_set_trampoline)
                };
            let mappings: Mappings = (self.get_mapping, self.set_mapping);
            unsafe {
                ffi::g_settings_bind_with_mapping(
                    self.settings.to_glib_none().0,
                    self.key.to_glib_none().0,
                    self.object.to_glib_none().0,
                    self.property.to_glib_none().0,
                    self.flags.into_glib(),
                    get_trampoline,
                    set_trampoline,
                    Box::into_raw(Box::new(mappings)) as *mut libc::c_void,
                    Some(destroy_closure),
                )
            }
        }
    }
}

pub trait SettingsExtManual: IsA<Settings> {
    fn get<U: FromVariant>(&self, key: &str) -> U {
        let val = self.value(key);
        FromVariant::from_variant(&val).unwrap_or_else(|| {
            panic!(
                "Type mismatch: Expected '{}' got '{}'",
                U::static_variant_type().as_str(),
                val.type_()
            )
        })
    }

    fn set(&self, key: &str, value: impl Into<Variant>) -> Result<(), BoolError> {
        self.set_value(key, &value.into())
    }

    #[doc(alias = "g_settings_get_strv")]
    #[doc(alias = "get_strv")]
    fn strv(&self, key: &str) -> StrV {
        unsafe {
            FromGlibPtrContainer::from_glib_full(ffi::g_settings_get_strv(
                self.as_ref().to_glib_none().0,
                key.to_glib_none().0,
            ))
        }
    }

    #[doc(alias = "g_settings_set_strv")]
    fn set_strv(&self, key: &str, value: impl IntoStrV) -> Result<(), glib::error::BoolError> {
        unsafe {
            value.run_with_strv(|value| {
                glib::result_from_gboolean!(
                    ffi::g_settings_set_strv(
                        self.as_ref().to_glib_none().0,
                        key.to_glib_none().0,
                        value.as_ptr() as *mut _,
                    ),
                    "Can't set readonly key"
                )
            })
        }
    }

    #[doc(alias = "g_settings_bind")]
    #[doc(alias = "g_settings_bind_with_mapping")]
    fn bind<'a, P: IsA<glib::Object>>(
        &'a self,
        key: &'a str,
        object: &'a P,
        property: &'a str,
    ) -> BindingBuilder<'a> {
        BindingBuilder {
            settings: self.upcast_ref(),
            key,
            object: object.upcast_ref(),
            property,
            flags: SettingsBindFlags::DEFAULT,
            get_mapping: None,
            set_mapping: None,
        }
    }
}

impl<O: IsA<Settings>> SettingsExtManual for O {}

#[cfg(test)]
mod test {
    use std::{env::set_var, process::Command, str::from_utf8, sync::Once};

    use super::*;

    static INIT: Once = Once::new();

    fn set_env() {
        INIT.call_once(|| {
            let tmp_dir = glib::mkdtemp("gio-rs-test-schemas-XXXXXX").unwrap();

            let output = Command::new("glib-compile-schemas")
                .args([
                    &format!("{}/tests", env!("CARGO_MANIFEST_DIR")),
                    "--targetdir",
                    tmp_dir.to_str().unwrap(),
                ])
                .output()
                .unwrap();

            if !output.status.success() {
                println!("Failed to generate GSchema!");
                println!(
                    "glib-compile-schemas stdout: {}",
                    from_utf8(&output.stdout).unwrap()
                );
                println!(
                    "glib-compile-schemas stderr: {}",
                    from_utf8(&output.stderr).unwrap()
                );
                panic!("Can't test without GSchemas!");
            }

            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { set_var("GSETTINGS_SCHEMA_DIR", tmp_dir) };
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { set_var("GSETTINGS_BACKEND", "memory") };
        });
    }

    #[test]
    #[serial_test::serial]
    fn string_get() {
        set_env();
        let settings = Settings::new("com.github.gtk-rs.test");
        assert_eq!(settings.get::<String>("test-string").as_str(), "Good");
    }

    #[test]
    #[serial_test::serial]
    fn bool_set_get() {
        set_env();
        let settings = Settings::new("com.github.gtk-rs.test");
        settings.set("test-bool", false).unwrap();
        assert!(!settings.get::<bool>("test-bool"));
    }

    #[test]
    #[should_panic]
    #[serial_test::serial]
    fn wrong_type() {
        set_env();
        let settings = Settings::new("com.github.gtk-rs.test");
        settings.get::<u8>("test-string");
    }
}
