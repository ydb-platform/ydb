// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    ffi::OsString,
    fmt,
    ops::{ControlFlow, Deref},
    ptr,
};

use glib::{
    Error, ExitCode, Propagation, VariantDict, prelude::*, subclass::prelude::*, translate::*,
};
use libc::{c_char, c_int, c_void};

use crate::{ActionGroup, ActionMap, Application, DBusConnection, ffi};

pub struct ArgumentList {
    pub(crate) ptr: *mut *mut *mut c_char,
    items: Vec<OsString>,
}

impl ArgumentList {
    pub(crate) fn new(arguments: *mut *mut *mut c_char) -> Self {
        Self {
            ptr: arguments,
            items: unsafe { FromGlibPtrContainer::from_glib_none(ptr::read(arguments)) },
        }
    }

    pub(crate) fn refresh(&mut self) {
        self.items = unsafe { FromGlibPtrContainer::from_glib_none(ptr::read(self.ptr)) };
    }

    // remove the item at index `idx` and shift the raw array
    pub fn remove(&mut self, idx: usize) {
        unsafe {
            let n_args = glib::ffi::g_strv_length(*self.ptr) as usize;
            assert_eq!(n_args, self.items.len());
            assert!(idx < n_args);

            self.items.remove(idx);

            glib::ffi::g_free(*(*self.ptr).add(idx) as *mut c_void);

            for i in idx..n_args - 1 {
                ptr::write((*self.ptr).add(i), *(*self.ptr).add(i + 1))
            }
            ptr::write((*self.ptr).add(n_args - 1), ptr::null_mut());
        }
    }
}

impl Deref for ArgumentList {
    type Target = [OsString];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.items.as_slice()
    }
}

impl fmt::Debug for ArgumentList {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.items.fmt(formatter)
    }
}

impl From<ArgumentList> for Vec<OsString> {
    fn from(list: ArgumentList) -> Vec<OsString> {
        list.items
    }
}

pub trait ApplicationImpl:
    ObjectImpl + ObjectSubclass<Type: IsA<Application> + IsA<ActionGroup> + IsA<ActionMap>>
{
    fn activate(&self) {
        self.parent_activate()
    }

    fn after_emit(&self, platform_data: &glib::Variant) {
        self.parent_after_emit(platform_data)
    }

    fn before_emit(&self, platform_data: &glib::Variant) {
        self.parent_before_emit(platform_data)
    }

    fn command_line(&self, command_line: &crate::ApplicationCommandLine) -> ExitCode {
        self.parent_command_line(command_line)
    }

    fn local_command_line(&self, arguments: &mut ArgumentList) -> ControlFlow<ExitCode> {
        self.parent_local_command_line(arguments)
    }

    fn open(&self, files: &[crate::File], hint: &str) {
        self.parent_open(files, hint)
    }

    fn quit_mainloop(&self) {
        self.parent_quit_mainloop()
    }

    fn run_mainloop(&self) {
        self.parent_run_mainloop()
    }

    fn shutdown(&self) {
        self.parent_shutdown()
    }

    fn startup(&self) {
        self.parent_startup()
    }

    fn handle_local_options(&self, options: &VariantDict) -> ControlFlow<ExitCode> {
        self.parent_handle_local_options(options)
    }

    fn dbus_register(&self, connection: &DBusConnection, object_path: &str) -> Result<(), Error> {
        self.parent_dbus_register(connection, object_path)
    }

    fn dbus_unregister(&self, connection: &DBusConnection, object_path: &str) {
        self.parent_dbus_unregister(connection, object_path)
    }

    fn name_lost(&self) -> Propagation {
        self.parent_name_lost()
    }
}

pub trait ApplicationImplExt: ApplicationImpl {
    fn parent_activate(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .activate
                .expect("No parent class implementation for \"activate\"");
            f(self.obj().unsafe_cast_ref::<Application>().to_glib_none().0)
        }
    }

    fn parent_after_emit(&self, platform_data: &glib::Variant) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .after_emit
                .expect("No parent class implementation for \"after_emit\"");
            f(
                self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                platform_data.to_glib_none().0,
            )
        }
    }

    fn parent_before_emit(&self, platform_data: &glib::Variant) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .before_emit
                .expect("No parent class implementation for \"before_emit\"");
            f(
                self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                platform_data.to_glib_none().0,
            )
        }
    }

    fn parent_command_line(&self, command_line: &crate::ApplicationCommandLine) -> ExitCode {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .command_line
                .expect("No parent class implementation for \"command_line\"");
            f(
                self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                command_line.to_glib_none().0,
            )
            .try_into()
            .unwrap()
        }
    }

    fn parent_local_command_line(&self, arguments: &mut ArgumentList) -> ControlFlow<ExitCode> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .local_command_line
                .expect("No parent class implementation for \"local_command_line\"");

            let mut exit_status = 0;
            let res = f(
                self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                arguments.ptr,
                &mut exit_status,
            );
            arguments.refresh();

            match res {
                glib::ffi::GFALSE => ControlFlow::Continue(()),
                _ => ControlFlow::Break(exit_status.try_into().unwrap()),
            }
        }
    }

    fn parent_open(&self, files: &[crate::File], hint: &str) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .open
                .expect("No parent class implementation for \"open\"");
            f(
                self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                files.to_glib_none().0,
                files.len() as i32,
                hint.to_glib_none().0,
            )
        }
    }

    fn parent_quit_mainloop(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .quit_mainloop
                .expect("No parent class implementation for \"quit_mainloop\"");
            f(self.obj().unsafe_cast_ref::<Application>().to_glib_none().0)
        }
    }

    fn parent_run_mainloop(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .run_mainloop
                .expect("No parent class implementation for \"run_mainloop\"");
            f(self.obj().unsafe_cast_ref::<Application>().to_glib_none().0)
        }
    }

    fn parent_shutdown(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .shutdown
                .expect("No parent class implementation for \"shutdown\"");
            f(self.obj().unsafe_cast_ref::<Application>().to_glib_none().0)
        }
    }

    fn parent_startup(&self) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .startup
                .expect("No parent class implementation for \"startup\"");
            f(self.obj().unsafe_cast_ref::<Application>().to_glib_none().0)
        }
    }

    fn parent_handle_local_options(&self, options: &VariantDict) -> ControlFlow<ExitCode> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            if let Some(f) = (*parent_class).handle_local_options {
                let ret = f(
                    self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                    options.to_glib_none().0,
                );

                match ret {
                    -1 => ControlFlow::Continue(()),
                    _ => ControlFlow::Break(ret.try_into().unwrap()),
                }
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    fn parent_dbus_register(
        &self,
        connection: &DBusConnection,
        object_path: &str,
    ) -> Result<(), glib::Error> {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .dbus_register
                .expect("No parent class implementation for \"dbus_register\"");
            let mut err = ptr::null_mut();
            let res = f(
                self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                connection.to_glib_none().0,
                object_path.to_glib_none().0,
                &mut err,
            );
            if res == glib::ffi::GFALSE {
                Err(from_glib_full(err))
            } else {
                debug_assert!(err.is_null());
                Ok(())
            }
        }
    }

    fn parent_dbus_unregister(&self, connection: &DBusConnection, object_path: &str) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .dbus_unregister
                .expect("No parent class implementation for \"dbus_unregister\"");
            f(
                self.obj().unsafe_cast_ref::<Application>().to_glib_none().0,
                connection.to_glib_none().0,
                object_path.to_glib_none().0,
            );
        }
    }

    fn parent_name_lost(&self) -> Propagation {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GApplicationClass;
            let f = (*parent_class)
                .name_lost
                .expect("No parent class implementation for \"name_lost\"");
            Propagation::from_glib(f(self
                .obj()
                .unsafe_cast_ref::<Application>()
                .to_glib_none()
                .0))
        }
    }
}

impl<T: ApplicationImpl> ApplicationImplExt for T {}

unsafe impl<T: ApplicationImpl> IsSubclassable<T> for Application {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.activate = Some(application_activate::<T>);
        klass.after_emit = Some(application_after_emit::<T>);
        klass.before_emit = Some(application_before_emit::<T>);
        klass.command_line = Some(application_command_line::<T>);
        klass.local_command_line = Some(application_local_command_line::<T>);
        klass.open = Some(application_open::<T>);
        klass.quit_mainloop = Some(application_quit_mainloop::<T>);
        klass.run_mainloop = Some(application_run_mainloop::<T>);
        klass.shutdown = Some(application_shutdown::<T>);
        klass.startup = Some(application_startup::<T>);
        klass.handle_local_options = Some(application_handle_local_options::<T>);
        klass.dbus_register = Some(application_dbus_register::<T>);
        klass.dbus_unregister = Some(application_dbus_unregister::<T>);
        klass.name_lost = Some(application_name_lost::<T>);
    }
}

unsafe extern "C" fn application_activate<T: ApplicationImpl>(ptr: *mut ffi::GApplication) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.activate()
    }
}

unsafe extern "C" fn application_after_emit<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    platform_data: *mut glib::ffi::GVariant,
) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.after_emit(&from_glib_borrow(platform_data))
    }
}
unsafe extern "C" fn application_before_emit<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    platform_data: *mut glib::ffi::GVariant,
) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.before_emit(&from_glib_borrow(platform_data))
    }
}
unsafe extern "C" fn application_command_line<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    command_line: *mut ffi::GApplicationCommandLine,
) -> i32 {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.command_line(&from_glib_borrow(command_line)).into()
    }
}
unsafe extern "C" fn application_local_command_line<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    arguments: *mut *mut *mut c_char,
    exit_status: *mut i32,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let mut args = ArgumentList::new(arguments);
        let res = imp.local_command_line(&mut args);
        args.refresh();

        match res {
            ControlFlow::Break(ret) => {
                *exit_status = ret.into();
                glib::ffi::GTRUE
            }
            ControlFlow::Continue(()) => glib::ffi::GFALSE,
        }
    }
}
unsafe extern "C" fn application_open<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    files: *mut *mut ffi::GFile,
    num_files: i32,
    hint: *const c_char,
) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        let files: Vec<crate::File> =
            FromGlibContainer::from_glib_none_num(files, num_files as usize);
        imp.open(files.as_slice(), &glib::GString::from_glib_borrow(hint))
    }
}
unsafe extern "C" fn application_quit_mainloop<T: ApplicationImpl>(ptr: *mut ffi::GApplication) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.quit_mainloop()
    }
}
unsafe extern "C" fn application_run_mainloop<T: ApplicationImpl>(ptr: *mut ffi::GApplication) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.run_mainloop()
    }
}
unsafe extern "C" fn application_shutdown<T: ApplicationImpl>(ptr: *mut ffi::GApplication) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.shutdown()
    }
}
unsafe extern "C" fn application_startup<T: ApplicationImpl>(ptr: *mut ffi::GApplication) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.startup()
    }
}

unsafe extern "C" fn application_handle_local_options<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    options: *mut glib::ffi::GVariantDict,
) -> c_int {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        imp.handle_local_options(&from_glib_borrow(options))
            .break_value()
            .map(i32::from)
            .unwrap_or(-1)
    }
}

unsafe extern "C" fn application_dbus_register<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    connection: *mut ffi::GDBusConnection,
    object_path: *const c_char,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();

        match imp.dbus_register(
            &from_glib_borrow(connection),
            &glib::GString::from_glib_borrow(object_path),
        ) {
            Ok(()) => glib::ffi::GTRUE,
            Err(e) => {
                if !error.is_null() {
                    *error = e.into_glib_ptr();
                }
                glib::ffi::GFALSE
            }
        }
    }
}

unsafe extern "C" fn application_dbus_unregister<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
    connection: *mut ffi::GDBusConnection,
    object_path: *const c_char,
) {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();
        imp.dbus_unregister(
            &from_glib_borrow(connection),
            &glib::GString::from_glib_borrow(object_path),
        );
    }
}

unsafe extern "C" fn application_name_lost<T: ApplicationImpl>(
    ptr: *mut ffi::GApplication,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(ptr as *mut T::Instance);
        let imp = instance.imp();
        imp.name_lost().into_glib()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    const EXIT_STATUS: u8 = 20;

    mod imp {
        use super::*;

        #[derive(Default)]
        pub struct SimpleApplication;

        #[glib::object_subclass]
        impl ObjectSubclass for SimpleApplication {
            const NAME: &'static str = "SimpleApplication";
            type Type = super::SimpleApplication;
            type ParentType = Application;
        }

        impl ObjectImpl for SimpleApplication {}

        impl ApplicationImpl for SimpleApplication {
            fn command_line(&self, _cmd_line: &crate::ApplicationCommandLine) -> ExitCode {
                #[cfg(not(target_os = "windows"))]
                {
                    let arguments = _cmd_line.arguments();

                    // NOTE: on windows argc and argv are ignored, even if the arguments
                    // were passed explicitly.
                    //
                    // Source: https://gitlab.gnome.org/GNOME/glib/-/blob/e64a93269d09302d7a4facbc164b7fe9c2ad0836/gio/gapplication.c#L2513-2515
                    assert_eq!(arguments.to_vec(), &["--global-1", "--global-2"]);
                };
                EXIT_STATUS.into()
            }

            fn local_command_line(&self, arguments: &mut ArgumentList) -> ControlFlow<ExitCode> {
                let mut rm = Vec::new();

                for (i, line) in arguments.iter().enumerate() {
                    // TODO: we need https://github.com/rust-lang/rust/issues/49802
                    let l = line.to_str().unwrap();
                    if l.starts_with("--local-") {
                        rm.push(i)
                    }
                }

                rm.reverse();

                for i in rm.iter() {
                    arguments.remove(*i);
                }

                ControlFlow::Continue(())
            }
        }
    }

    glib::wrapper! {
        pub struct SimpleApplication(ObjectSubclass<imp::SimpleApplication>)
        @implements Application, ActionMap, ActionGroup;
    }

    #[test]
    fn test_simple_application() {
        let app = glib::Object::builder::<SimpleApplication>()
            .property("application-id", "org.gtk-rs.SimpleApplication")
            .property("flags", crate::ApplicationFlags::empty())
            .build();

        app.set_inactivity_timeout(10000);

        assert_eq!(
            app.run_with_args(&["--local-1", "--global-1", "--local-2", "--global-2"]),
            EXIT_STATUS.into()
        );
    }
}
