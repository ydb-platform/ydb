// Take a look at the license at the top of the repository in the LICENSE file.

use std::path::PathBuf;

use glib::{GString, StrVRef, prelude::*, subclass::prelude::*, translate::*};

use libc::c_char;

use crate::{File, Vfs, ffi};

// Support custom implementation of virtual functions defined in `gio::ffi::GVfsClass`.
pub trait VfsImpl: ObjectImpl + ObjectSubclass<Type: IsA<Vfs>> {
    fn is_active(&self) -> bool {
        self.parent_is_active()
    }

    fn get_file_for_path(&self, path: &std::path::Path) -> File {
        self.parent_get_file_for_path(path)
    }

    fn get_file_for_uri(&self, uri: &str) -> File {
        self.parent_get_file_for_uri(uri)
    }

    fn get_supported_uri_schemes(&self) -> &'static StrVRef {
        self.parent_get_supported_uri_schemes()
    }

    fn parse_name(&self, parse_name: &str) -> File {
        self.parent_parse_name(parse_name)
    }
}

// Support parent implementation of virtual functions defined in `gio::ffi::GVfsClass`.
pub trait VfsImplExt: VfsImpl {
    fn parent_is_active(&self) -> bool {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GVfsClass;

            let f = (*parent_class)
                .is_active
                .expect("No parent class implementation for \"is_active\"");

            let res = f(self.obj().unsafe_cast_ref::<Vfs>().to_glib_none().0);
            from_glib(res)
        }
    }

    fn parent_get_file_for_path(&self, path: &std::path::Path) -> File {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GVfsClass;

            let f = (*parent_class)
                .get_file_for_path
                .expect("No parent class implementation for \"get_file_for_path\"");

            let res = f(
                self.obj().unsafe_cast_ref::<Vfs>().to_glib_none().0,
                path.to_glib_none().0,
            );
            from_glib_full(res)
        }
    }

    fn parent_get_file_for_uri(&self, uri: &str) -> File {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GVfsClass;

            let f = (*parent_class)
                .get_file_for_uri
                .expect("No parent class implementation for \"get_file_for_uri\"");

            let res = f(
                self.obj().unsafe_cast_ref::<Vfs>().to_glib_none().0,
                uri.to_glib_none().0,
            );
            from_glib_full(res)
        }
    }

    fn parent_get_supported_uri_schemes(&self) -> &'static StrVRef {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GVfsClass;

            let f = (*parent_class)
                .get_supported_uri_schemes
                .expect("No parent class implementation for \"get_supported_uri_schemes\"");

            let res = f(self.obj().unsafe_cast_ref::<Vfs>().to_glib_none().0);
            StrVRef::from_glib_borrow(res)
        }
    }

    fn parent_parse_name(&self, parse_name: &str) -> File {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GVfsClass;

            let f = (*parent_class)
                .parse_name
                .expect("No parent class implementation for \"parse_name\"");

            let res = f(
                self.obj().unsafe_cast_ref::<Vfs>().to_glib_none().0,
                parse_name.to_glib_none().0,
            );
            from_glib_full(res)
        }
    }
}

impl<T: VfsImpl> VfsImplExt for T {}

// Implement virtual functions defined in `gio::ffi::GVfsClass`.
unsafe impl<T: VfsImpl> IsSubclassable<T> for Vfs {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.is_active = Some(is_active::<T>);
        klass.get_file_for_path = Some(get_file_for_path::<T>);
        klass.get_file_for_uri = Some(get_file_for_uri::<T>);
        klass.get_supported_uri_schemes = Some(get_supported_uri_schemes::<T>);
        klass.parse_name = Some(parse_name::<T>);
    }
}

unsafe extern "C" fn is_active<T: VfsImpl>(vfs: *mut ffi::GVfs) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(vfs as *mut T::Instance);
        let imp = instance.imp();

        let res = imp.is_active();

        res.into_glib()
    }
}

unsafe extern "C" fn get_file_for_path<T: VfsImpl>(
    vfs: *mut ffi::GVfs,
    path: *const c_char,
) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(vfs as *mut T::Instance);
        let imp = instance.imp();

        let file = imp.get_file_for_path(&PathBuf::from_glib_none(path));

        file.into_glib_ptr()
    }
}

unsafe extern "C" fn get_file_for_uri<T: VfsImpl>(
    vfs: *mut ffi::GVfs,
    uri: *const c_char,
) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(vfs as *mut T::Instance);
        let imp = instance.imp();

        let file = imp.get_file_for_uri(&GString::from_glib_borrow(uri));

        file.into_glib_ptr()
    }
}

unsafe extern "C" fn get_supported_uri_schemes<T: VfsImpl>(
    vfs: *mut ffi::GVfs,
) -> *const *const c_char {
    unsafe {
        let instance = &*(vfs as *mut T::Instance);
        let imp = instance.imp();

        let supported_uri_schemes = imp.get_supported_uri_schemes();

        supported_uri_schemes.as_ptr()
    }
}

unsafe extern "C" fn parse_name<T: VfsImpl>(
    vfs: *mut ffi::GVfs,
    parse_name: *const c_char,
) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(vfs as *mut T::Instance);
        let imp = instance.imp();

        let file = imp.parse_name(&GString::from_glib_borrow(parse_name));

        file.into_glib_ptr()
    }
}

#[cfg(test)]
mod tests {
    // The following tests rely on a custom type `MyCustomVfs` that extends another custom type `MyVfs`.
    // For each virtual method defined in class `gio::ffi::GVfsClass`, a test checks that `MyCustomVfs` and `MyVfs` return the same results.

    use super::*;
    use crate::prelude::*;

    // Define `MyCustomVfs` as a subclass of `MyVfs`.
    mod imp {
        use std::sync::LazyLock;

        use super::*;

        // Defines `MyVfs` as a subclass of `Vfs`.
        #[derive(Default)]
        pub struct MyVfs;

        #[glib::object_subclass]
        impl ObjectSubclass for MyVfs {
            const NAME: &'static str = "MyVfs";
            type Type = super::MyVfs;
            type ParentType = Vfs;
        }

        impl ObjectImpl for MyVfs {}

        // Implements `VfsImpl` with custom implementation.
        impl VfsImpl for MyVfs {
            fn is_active(&self) -> bool {
                true
            }

            fn get_file_for_path(&self, path: &std::path::Path) -> File {
                File::for_path(path)
            }

            fn get_file_for_uri(&self, uri: &str) -> File {
                File::for_uri(uri)
            }

            fn get_supported_uri_schemes(&self) -> &'static StrVRef {
                static SUPPORTED_URI_SCHEMES: LazyLock<glib::StrV> =
                    LazyLock::new(|| glib::StrV::from(["file"]));
                &SUPPORTED_URI_SCHEMES
            }

            fn parse_name(&self, parse_name: &str) -> File {
                File::for_parse_name(parse_name)
            }
        }

        // Defines `MyCustomVfs` as a subclass of `MyVfs`.
        #[derive(Default)]
        pub struct MyCustomVfs;

        #[glib::object_subclass]
        impl ObjectSubclass for MyCustomVfs {
            const NAME: &'static str = "MyCustomVfs";
            type Type = super::MyCustomVfs;
            type ParentType = super::MyVfs;
        }

        impl ObjectImpl for MyCustomVfs {}

        // Implements `VfsImpl` with default implementation, which calls the parent's implementation.
        impl VfsImpl for MyCustomVfs {}

        impl MyVfsImpl for MyCustomVfs {}
    }

    glib::wrapper! {
        pub struct MyVfs(ObjectSubclass<imp::MyVfs>) @extends Vfs;
    }

    pub trait MyVfsImpl: ObjectImpl + ObjectSubclass<Type: IsA<MyVfs> + IsA<Vfs>> {}

    // To make this class subclassable we need to implement IsSubclassable
    unsafe impl<T: MyVfsImpl + VfsImpl> IsSubclassable<T> for MyVfs {}

    glib::wrapper! {
        pub struct MyCustomVfs(ObjectSubclass<imp::MyCustomVfs>) @extends MyVfs, Vfs;
    }

    #[test]
    fn vfs_is_active() {
        // invoke `MyCustomVfs` implementation of `gio::ffi::GVfsClass::is_active`
        let my_custom_vfs = glib::Object::new::<MyCustomVfs>();
        let active = my_custom_vfs.is_active();

        // invoke `MyVfs` implementation of `gio::ffi::GVfsClass::is_active`
        let my_vfs = glib::Object::new::<MyVfs>();
        let expected = my_vfs.is_active();

        // both results should equal
        assert_eq!(active, expected);
    }

    #[test]
    fn vfs_get_file_for_path() {
        // invoke `MyCustomVfs` implementation of `gio::ffi::GVfsClass::get_file_for_path`
        let my_custom_vfs = glib::Object::new::<MyCustomVfs>();
        let file = my_custom_vfs.file_for_path("/path");

        // invoke `MyVfs` implementation of `gio::ffi::GVfsClass::get_file_for_path`
        let my_vfs = glib::Object::new::<MyVfs>();
        let expected = my_vfs.file_for_path("/path");

        // both files should equal
        assert!(file.equal(&expected));
    }

    #[test]
    fn vfs_get_file_for_uri() {
        // invoke `MyCustomVfs` implementation of `gio::ffi::GVfsClass::get_file_for_uri`
        let my_custom_vfs = glib::Object::new::<MyCustomVfs>();
        let file = my_custom_vfs.file_for_uri("file:///path");

        // invoke `MyVfs` implementation of `gio::ffi::GVfsClass::get_file_for_uri`
        let my_vfs = glib::Object::new::<MyVfs>();
        let expected = my_vfs.file_for_uri("file:///path");

        // both files should equal
        assert!(file.equal(&expected));
    }

    #[test]
    fn vfs_get_supported_uri_schemes() {
        // invoke `MyCustomVfs` implementation of `gio::ffi::GVfsClass::supported_uri_schemes`
        let my_custom_vfs = glib::Object::new::<MyCustomVfs>();
        let schemes = my_custom_vfs.supported_uri_schemes();

        // invoke `MyVfs` implementation of `gio::ffi::GVfsClass::supported_uri_schemes`
        let my_vfs = glib::Object::new::<MyVfs>();
        let expected = my_vfs.supported_uri_schemes();

        // both results should equal
        assert_eq!(schemes, expected);
    }

    #[test]
    fn vfs_parse_name() {
        // invoke `MyCustomVfs` implementation of `gio::ffi::GVfsClass::parse_name`
        let my_custom_vfs = glib::Object::new::<MyCustomVfs>();
        let file = my_custom_vfs.parse_name("file:///path");

        // invoke `MyVfs` implementation of `gio::ffi::GVfsClass::parse_name`
        let my_vfs = glib::Object::new::<MyVfs>();
        let expected = my_vfs.parse_name("file:///path");

        // both files should equal
        assert!(file.equal(&expected));
    }
}
