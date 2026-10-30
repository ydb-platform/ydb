// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{
    Error, GString, Interface, Object, prelude::*, subclass::prelude::*, thread_guard, translate::*,
};

use crate::{
    AsyncResult, Cancellable, DriveStartFlags, File, FileAttributeInfoList, FileAttributeValue,
    FileCopyFlags, FileCreateFlags, FileEnumerator, FileIOStream, FileInfo, FileInputStream,
    FileMeasureFlags, FileMonitor, FileMonitorFlags, FileOutputStream, FileQueryInfoFlags,
    IOErrorEnum, Mount, MountMountFlags, MountOperation, MountUnmountFlags, Task, ffi,
};

use libc::{c_char, c_uint};

use std::{boxed::Box, path::PathBuf};

// Support custom implementation of virtual functions defined in `gio::ffi::GFileIface` except pairs `xxx_async/xxx_finish` for which GIO provides a default implementation.
pub trait FileImpl: ObjectImpl + ObjectSubclass<Type: IsA<File>> {
    const SUPPORT_THREAD_CONTEXT: bool = true;

    fn dup(&self) -> File {
        self.parent_dup()
    }

    fn hash(&self) -> u32 {
        self.parent_hash()
    }

    fn equal(&self, file2: &File) -> bool {
        self.parent_equal(file2)
    }

    fn is_native(&self) -> bool {
        self.parent_is_native()
    }

    fn has_uri_scheme(&self, uri_scheme: &str) -> bool {
        self.parent_has_uri_scheme(uri_scheme)
    }

    fn uri_scheme(&self) -> Option<String> {
        self.parent_uri_scheme()
    }

    fn basename(&self) -> Option<PathBuf> {
        self.parent_basename()
    }

    fn path(&self) -> Option<PathBuf> {
        self.parent_path()
    }

    fn uri(&self) -> String {
        self.parent_uri()
    }

    fn parse_name(&self) -> String {
        self.parent_parse_name()
    }

    fn parent(&self) -> Option<File> {
        self.parent_parent()
    }

    fn has_prefix(&self, prefix: &File) -> bool {
        self.parent_has_prefix(prefix)
    }

    fn relative_path(&self, descendant: &File) -> Option<PathBuf> {
        self.parent_relative_path(descendant)
    }

    fn resolve_relative_path(&self, relative_path: impl AsRef<std::path::Path>) -> File {
        self.parent_resolve_relative_path(relative_path)
    }

    fn child_for_display_name(&self, display_name: &str) -> Result<File, Error> {
        self.parent_child_for_display_name(display_name)
    }

    fn enumerate_children(
        &self,
        attributes: &str,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileEnumerator, Error> {
        self.parent_enumerate_children(attributes, flags, cancellable)
    }

    fn query_info(
        &self,
        attributes: &str,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileInfo, Error> {
        self.parent_query_info(attributes, flags, cancellable)
    }

    fn query_filesystem_info(
        &self,
        attributes: &str,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileInfo, Error> {
        self.parent_query_filesystem_info(attributes, cancellable)
    }

    fn find_enclosing_mount(&self, cancellable: Option<&Cancellable>) -> Result<Mount, Error> {
        self.parent_find_enclosing_mount(cancellable)
    }

    fn set_display_name(
        &self,
        display_name: &str,
        cancellable: Option<&Cancellable>,
    ) -> Result<File, Error> {
        self.parent_set_display_name(display_name, cancellable)
    }

    fn query_settable_attributes(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileAttributeInfoList, Error> {
        self.parent_query_settable_attributes(cancellable)
    }

    fn query_writable_namespaces(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileAttributeInfoList, Error> {
        self.parent_query_writable_namespaces(cancellable)
    }

    fn set_attribute<'a>(
        &self,
        attribute: &str,
        value: impl Into<FileAttributeValue<'a>>,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error> {
        self.parent_set_attribute(attribute, value, flags, cancellable)
    }

    fn set_attributes_from_info(
        &self,
        info: &FileInfo,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error> {
        self.parent_set_attributes_from_info(info, flags, cancellable)
    }

    fn read_fn(&self, cancellable: Option<&Cancellable>) -> Result<FileInputStream, Error> {
        self.parent_read_fn(cancellable)
    }

    fn append_to(
        &self,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileOutputStream, Error> {
        self.parent_append_to(flags, cancellable)
    }

    fn create(
        &self,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileOutputStream, Error> {
        self.parent_create(flags, cancellable)
    }

    fn replace(
        &self,
        etag: Option<&str>,
        make_backup: bool,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileOutputStream, Error> {
        self.parent_replace(etag, make_backup, flags, cancellable)
    }

    fn delete(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_delete(cancellable)
    }

    fn trash(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_trash(cancellable)
    }

    fn make_directory(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        self.parent_make_directory(cancellable)
    }

    fn make_symbolic_link(
        &self,
        symlink_value: impl AsRef<std::path::Path>,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error> {
        self.parent_make_symbolic_link(symlink_value, cancellable)
    }

    fn copy(
        source: &File,
        destination: &File,
        flags: FileCopyFlags,
        cancellable: Option<&Cancellable>,
        progress_callback: Option<&mut dyn FnMut(i64, i64)>,
    ) -> Result<(), Error> {
        Self::parent_copy(source, destination, flags, cancellable, progress_callback)
    }

    fn move_(
        source: &File,
        destination: &File,
        flags: FileCopyFlags,
        cancellable: Option<&Cancellable>,
        progress_callback: Option<&mut dyn FnMut(i64, i64)>,
    ) -> Result<(), Error> {
        Self::parent_move(source, destination, flags, cancellable, progress_callback)
    }

    fn mount_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountMountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_mount_mountable(flags, mount_operation, cancellable, callback)
    }

    fn mount_mountable_finish(&self, res: &AsyncResult) -> Result<File, Error> {
        self.parent_mount_mountable_finish(res)
    }

    fn unmount_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_unmount_mountable(flags, cancellable, callback)
    }

    fn unmount_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_unmount_mountable_finish(res)
    }

    fn eject_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_eject_mountable(flags, cancellable, callback)
    }

    fn eject_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_eject_mountable_finish(res)
    }

    fn mount_enclosing_volume<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountMountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_mount_enclosing_volume(flags, mount_operation, cancellable, callback)
    }

    fn mount_enclosing_volume_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_mount_enclosing_volume_finish(res)
    }

    fn monitor_dir(
        &self,
        flags: FileMonitorFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileMonitor, Error> {
        self.parent_monitor_dir(flags, cancellable)
    }

    fn monitor_file(
        &self,
        flags: FileMonitorFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileMonitor, Error> {
        self.parent_monitor_file(flags, cancellable)
    }

    fn open_readwrite(&self, cancellable: Option<&Cancellable>) -> Result<FileIOStream, Error> {
        self.parent_open_readwrite(cancellable)
    }

    fn create_readwrite(
        &self,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileIOStream, Error> {
        self.parent_create_readwrite(flags, cancellable)
    }

    fn replace_readwrite(
        &self,
        etag: Option<&str>,
        make_backup: bool,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileIOStream, Error> {
        self.parent_replace_readwrite(etag, make_backup, flags, cancellable)
    }

    fn start_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: DriveStartFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_start_mountable(flags, mount_operation, cancellable, callback)
    }

    fn start_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_start_mountable_finish(res)
    }

    fn stop_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_stop_mountable(flags, mount_operation, cancellable, callback)
    }

    fn stop_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_stop_mountable_finish(res)
    }

    fn unmount_mountable_with_operation<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_unmount_mountable_with_operation(flags, mount_operation, cancellable, callback)
    }

    fn unmount_mountable_with_operation_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_unmount_mountable_with_operation_finish(res)
    }

    fn eject_mountable_with_operation<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_eject_mountable_with_operation(flags, mount_operation, cancellable, callback)
    }

    fn eject_mountable_with_operation_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_eject_mountable_with_operation_finish(res)
    }

    fn poll_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        self.parent_poll_mountable(cancellable, callback)
    }

    fn poll_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        self.parent_poll_mountable_finish(res)
    }

    fn measure_disk_usage(
        &self,
        flags: FileMeasureFlags,
        cancellable: Option<&Cancellable>,
        progress_callback: Option<&mut dyn FnMut(bool, u64, u64, u64)>,
    ) -> Result<(u64, u64, u64), Error> {
        self.parent_measure_disk_usage(flags, cancellable, progress_callback)
    }

    fn query_exists(&self, cancellable: Option<&Cancellable>) -> bool {
        self.parent_query_exists(cancellable)
    }
}

// Support parent implementation of virtual functions defined in `gio::ffi::GFileIface` except pairs `xxx_async/xxx_finish` for which GIO provides a default implementation.
pub trait FileImplExt: FileImpl {
    fn parent_dup(&self) -> File {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .dup
                .expect("no parent \"dup\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib_full(ret)
        }
    }

    fn parent_hash(&self) -> u32 {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .hash
                .expect("no parent \"hash\" implementation");
            func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0)
        }
    }

    fn parent_equal(&self, file2: &File) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .equal
                .expect("no parent \"equal\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                file2.to_glib_none().0,
            );
            from_glib(ret)
        }
    }

    fn parent_is_native(&self) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .is_native
                .expect("no parent \"is_native\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib(ret)
        }
    }

    fn parent_has_uri_scheme(&self, uri_scheme: &str) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .has_uri_scheme
                .expect("no parent \"has_uri_scheme\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                uri_scheme.to_glib_none().0,
            );
            from_glib(ret)
        }
    }

    fn parent_uri_scheme(&self) -> Option<String> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_uri_scheme
                .expect("no parent \"get_uri_scheme\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib_full(ret)
        }
    }

    fn parent_basename(&self) -> Option<PathBuf> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_basename
                .expect("no parent \"get_basename\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib_full(ret)
        }
    }

    fn parent_path(&self) -> Option<PathBuf> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_path
                .expect("no parent \"get_path\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib_full(ret)
        }
    }

    fn parent_uri(&self) -> String {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_uri
                .expect("no parent \"get_uri\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib_full(ret)
        }
    }

    fn parent_parse_name(&self) -> String {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_parse_name
                .expect("no parent \"get_parse_name\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib_full(ret)
        }
    }

    fn parent_parent(&self) -> Option<File> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_parent
                .expect("no parent \"get_parent\" implementation");
            let ret = func(self.obj().unsafe_cast_ref::<File>().to_glib_none().0);
            from_glib_full(ret)
        }
    }

    fn parent_has_prefix(&self, prefix: &File) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .prefix_matches
                .expect("no parent \"prefix_matches\" implementation");
            let ret = func(
                prefix.to_glib_none().0,
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
            );
            from_glib(ret)
        }
    }

    fn parent_relative_path(&self, descendant: &File) -> Option<PathBuf> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_relative_path
                .expect("no parent \"get_relative_path\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                descendant.to_glib_none().0,
            );
            from_glib_full(ret)
        }
    }

    fn parent_resolve_relative_path(&self, relative_path: impl AsRef<std::path::Path>) -> File {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .resolve_relative_path
                .expect("no parent \"resolve_relative_path\" implementation");
            let ret = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                relative_path.as_ref().to_glib_none().0,
            );
            from_glib_full(ret)
        }
    }

    fn parent_child_for_display_name(&self, display_name: &str) -> Result<File, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .get_child_for_display_name
                .expect("no parent \"get_child_for_display_name\" implementation");
            let mut error = std::ptr::null_mut();
            let ret = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                display_name.to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(from_glib_full(ret))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    fn parent_enumerate_children(
        &self,
        attributes: &str,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileEnumerator, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).enumerate_children {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    attributes.to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_query_info(
        &self,
        attributes: &str,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileInfo, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).query_info {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    attributes.to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_query_filesystem_info(
        &self,
        attributes: &str,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileInfo, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).query_filesystem_info {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    attributes.to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_find_enclosing_mount(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<Mount, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).find_enclosing_mount {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotFound,
                    "Containing mount does not exist",
                ))
            }
        }
    }

    fn parent_set_display_name(
        &self,
        display_name: &str,
        cancellable: Option<&Cancellable>,
    ) -> Result<File, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .set_display_name
                .expect("no parent \"set_display_name\" implementation");
            let mut error = std::ptr::null_mut();
            let ret = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                display_name.to_glib_none().0,
                cancellable.to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(from_glib_full(ret))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    fn parent_query_settable_attributes(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileAttributeInfoList, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).query_settable_attributes {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Ok(FileAttributeInfoList::new())
            }
        }
    }

    fn parent_query_writable_namespaces(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileAttributeInfoList, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).query_writable_namespaces {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Ok(FileAttributeInfoList::new())
            }
        }
    }

    fn parent_set_attribute<'a>(
        &self,
        attribute: &str,
        value: impl Into<FileAttributeValue<'a>>,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).set_attribute {
                let mut error = std::ptr::null_mut();
                let value: FileAttributeValue<'a> = value.into();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    attribute.to_glib_none().0,
                    value.type_().into_glib(),
                    value.as_ptr(),
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_set_attributes_from_info(
        &self,
        info: &FileInfo,
        flags: FileQueryInfoFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .set_attributes_from_info
                .expect("no parent \"set_attributes_from_info\" implementation");
            let mut error = std::ptr::null_mut();
            let is_ok = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                info.to_glib_none().0,
                flags.into_glib(),
                cancellable.to_glib_none().0,
                &mut error,
            );
            debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
            if error.is_null() {
                Ok(())
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    fn parent_read_fn(&self, cancellable: Option<&Cancellable>) -> Result<FileInputStream, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).read_fn {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_append_to(
        &self,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileOutputStream, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).append_to {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_create(
        &self,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileOutputStream, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).create {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_replace(
        &self,
        etag: Option<&str>,
        make_backup: bool,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileOutputStream, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).replace {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    etag.to_glib_none().0,
                    make_backup.into_glib(),
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_delete(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).delete_file {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_trash(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).trash {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_make_directory(&self, cancellable: Option<&Cancellable>) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).make_directory {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_make_symbolic_link(
        &self,
        symlink_value: impl AsRef<std::path::Path>,
        cancellable: Option<&Cancellable>,
    ) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).make_symbolic_link {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    symlink_value.as_ref().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_copy(
        source: &File,
        destination: &File,
        flags: FileCopyFlags,
        cancellable: Option<&Cancellable>,
        progress_callback: Option<&mut dyn FnMut(i64, i64)>,
    ) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).copy {
                let mut super_callback0 = progress_callback;
                let (progress_callback, progress_callback_data) = super_callback0.as_mut().map_or(
                    (None, std::ptr::null_mut()),
                    |progress_callback| {
                        unsafe extern "C" fn progress_callback_trampoline(
                            current_num_bytes: i64,
                            total_num_bytes: i64,
                            user_data: glib::ffi::gpointer,
                        ) {
                            unsafe {
                                let progress_callback: &mut dyn FnMut(i64, i64) =
                                    *(user_data as *mut &mut dyn FnMut(i64, i64));
                                progress_callback(current_num_bytes, total_num_bytes);
                            }
                        }
                        (
                            Some(progress_callback_trampoline as _),
                            progress_callback as *mut &mut dyn FnMut(i64, i64) as *mut _,
                        )
                    },
                );

                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    source.to_glib_none().0,
                    destination.to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    progress_callback,
                    progress_callback_data,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                // give a chance to g_file_copy to call file_copy_fallback
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_move(
        source: &File,
        destination: &File,
        flags: FileCopyFlags,
        cancellable: Option<&Cancellable>,
        progress_callback: Option<&mut dyn FnMut(i64, i64)>,
    ) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).move_ {
                let mut super_callback0 = progress_callback;
                let (progress_callback, progress_callback_data) = super_callback0.as_mut().map_or(
                    (None, std::ptr::null_mut()),
                    |progress_callback| {
                        unsafe extern "C" fn progress_callback_trampoline(
                            current_num_bytes: i64,
                            total_num_bytes: i64,
                            user_data: glib::ffi::gpointer,
                        ) {
                            unsafe {
                                let progress_callback: &mut Box<dyn FnMut(i64, i64) + 'static> =
                                    &mut *(user_data as *mut _);
                                progress_callback(current_num_bytes, total_num_bytes);
                            }
                        }
                        (
                            Some(progress_callback_trampoline as _),
                            progress_callback as *mut &mut dyn FnMut(i64, i64) as *mut _,
                        )
                    },
                );

                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    source.to_glib_none().0,
                    destination.to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    progress_callback,
                    progress_callback_data,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                // give a chance to g_file_move to call g_file_copy
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_mount_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountMountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        unsafe {
            let (callback, user_data) = callback.map_or((None, std::ptr::null_mut()), |callback| {
                let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

                unsafe extern "C" fn callback_trampoline<
                    T: FileImpl,
                    P: FnOnce(&T::Type, &AsyncResult) + 'static,
                >(
                    source_object: *mut glib::gobject_ffi::GObject,
                    res: *mut ffi::GAsyncResult,
                    data: glib::ffi::gpointer,
                ) {
                    unsafe {
                        let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                        let res: &AsyncResult = &from_glib_borrow(res);
                        let callback: Box<thread_guard::ThreadGuard<P>> =
                            Box::from_raw(data as *mut _);
                        let callback: P = callback.into_inner();
                        callback(source, res);
                    }
                }
                let callback = callback_trampoline::<Self, P>;

                (Some(callback as _), Box::into_raw(super_callback) as *mut _)
            });

            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).mount_mountable {
                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    mount_operation.to_glib_none().0,
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                ffi::g_task_report_new_error(
                    self.obj().unsafe_cast_ref::<Object>().to_glib_none().0,
                    callback,
                    user_data,
                    ffi::g_file_mount_mountable as *mut _,
                    IOErrorEnum::domain().into_glib(),
                    IOErrorEnum::NotSupported.into_glib(),
                    "Operation not supported".to_glib_full(),
                );
            }
        }
    }

    fn parent_mount_mountable_finish(&self, res: &AsyncResult) -> Result<File, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).mount_mountable_finish {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else if let Some(task) = res.downcast_ref::<Task<File>>() {
                // get the `Task` result as a `File` or as an error
                task.to_owned().propagate()
            } else {
                // no parent implementation and don't know how to deal with the result so let's panic
                panic!("no parent \"mount_mountable_finish\" implementation")
            }
        }
    }

    fn parent_unmount_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        unsafe {
            let (callback, user_data) = callback.map_or((None, std::ptr::null_mut()), |callback| {
                let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

                unsafe extern "C" fn callback_trampoline<
                    T: FileImpl,
                    P: FnOnce(&T::Type, &AsyncResult) + 'static,
                >(
                    source_object: *mut glib::gobject_ffi::GObject,
                    res: *mut ffi::GAsyncResult,
                    data: glib::ffi::gpointer,
                ) {
                    unsafe {
                        let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                        let res: &AsyncResult = &from_glib_borrow(res);
                        let callback: Box<thread_guard::ThreadGuard<P>> =
                            Box::from_raw(data as *mut _);
                        let callback: P = callback.into_inner();
                        callback(source, res);
                    }
                }
                let callback = callback_trampoline::<Self, P>;

                (Some(callback as _), Box::into_raw(super_callback) as *mut _)
            });

            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).unmount_mountable {
                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                ffi::g_task_report_new_error(
                    self.obj().unsafe_cast_ref::<Object>().to_glib_none().0,
                    callback,
                    user_data,
                    ffi::g_file_unmount_mountable_with_operation as *mut _,
                    IOErrorEnum::domain().into_glib(),
                    IOErrorEnum::NotSupported.into_glib(),
                    "Operation not supported".to_glib_full(),
                );
            }
        }
    }

    fn parent_unmount_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).unmount_mountable_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else if let Some(task) = res.downcast_ref::<Task<File>>() {
                // get the `Task` result as a boolean or as an error
                task.to_owned().propagate().map(|_| ())
            } else {
                // no parent implementation and don't know how to deal with the result so let's panic
                panic!("no parent \"unmount_mountable_finish\" implementation")
            }
        }
    }

    fn parent_eject_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        let (callback, user_data) = callback.map_or((None, std::ptr::null_mut()), |callback| {
            let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

            unsafe extern "C" fn callback_trampoline<
                T: FileImpl,
                P: FnOnce(&T::Type, &AsyncResult) + 'static,
            >(
                source_object: *mut glib::gobject_ffi::GObject,
                res: *mut ffi::GAsyncResult,
                data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                    let res: &AsyncResult = &from_glib_borrow(res);
                    let callback: Box<thread_guard::ThreadGuard<P>> = Box::from_raw(data as *mut _);
                    let callback: P = callback.into_inner();
                    callback(source, res);
                }
            }
            let callback = callback_trampoline::<Self, P>;

            (Some(callback as _), Box::into_raw(super_callback) as *mut _)
        });

        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).eject_mountable {
                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                ffi::g_task_report_new_error(
                    self.obj().unsafe_cast_ref::<Object>().to_glib_none().0,
                    callback,
                    user_data,
                    ffi::g_file_eject_mountable_with_operation as *mut _,
                    IOErrorEnum::domain().into_glib(),
                    IOErrorEnum::NotSupported.into_glib(),
                    "Operation not supported".to_glib_full(),
                );
            }
        }
    }

    fn parent_eject_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).eject_mountable_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else if let Some(task) = res.downcast_ref::<Task<bool>>() {
                // get the `Task` result as a boolean or as an error
                task.to_owned().propagate().map(|_| ())
            } else {
                // no parent implementation and don't know how to deal with the result so let's panic
                panic!("no parent \"eject_mountable_finish\" implementation")
            }
        }
    }

    fn parent_mount_enclosing_volume<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountMountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        let (callback, user_data) = callback.map_or((None, std::ptr::null_mut()), |callback| {
            let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

            unsafe extern "C" fn callback_trampoline<
                T: FileImpl,
                P: FnOnce(&T::Type, &AsyncResult) + 'static,
            >(
                source_object: *mut glib::gobject_ffi::GObject,
                res: *mut ffi::GAsyncResult,
                data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                    let res: &AsyncResult = &from_glib_borrow(res);
                    let callback: Box<thread_guard::ThreadGuard<P>> = Box::from_raw(data as *mut _);
                    let callback: P = callback.into_inner();
                    callback(source, res);
                }
            }
            let callback = callback_trampoline::<Self, P>;

            (Some(callback as _), Box::into_raw(super_callback) as *mut _)
        });

        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).mount_enclosing_volume {
                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    mount_operation.to_glib_none().0,
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                ffi::g_task_report_new_error(
                    self.obj().unsafe_cast_ref::<Object>().to_glib_none().0,
                    callback,
                    user_data,
                    ffi::g_file_mount_enclosing_volume as *mut _,
                    IOErrorEnum::domain().into_glib(),
                    IOErrorEnum::NotSupported.into_glib(),
                    "volume doesn’t implement mount enclosing volume".to_glib_full(),
                );
            }
        }
    }

    fn parent_mount_enclosing_volume_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).mount_enclosing_volume_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else if let Some(task) = res.downcast_ref::<Task<bool>>() {
                // get the `Task` result as a boolean or as an error
                task.to_owned().propagate().map(|_| ())
            } else {
                // no parent implementation and don't know how to deal with the result so let's panic
                panic!("no parent \"mount_enclosing_volume_finish\" implementation")
            }
        }
    }

    fn parent_monitor_dir(
        &self,
        flags: FileMonitorFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileMonitor, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).monitor_dir {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_monitor_file(
        &self,
        flags: FileMonitorFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileMonitor, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).monitor_file {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                // cannot call private _g_poll_file_monitor_new
                panic!("no parent \"monitor_file\" implementation")
            }
        }
    }

    fn parent_open_readwrite(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileIOStream, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).open_readwrite {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_create_readwrite(
        &self,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileIOStream, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).create_readwrite {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_replace_readwrite(
        &self,
        etag: Option<&str>,
        make_backup: bool,
        flags: FileCreateFlags,
        cancellable: Option<&Cancellable>,
    ) -> Result<FileIOStream, Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).replace_readwrite {
                let mut error = std::ptr::null_mut();
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    etag.to_glib_none().0,
                    make_backup.into_glib(),
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                Err(Error::new::<IOErrorEnum>(
                    IOErrorEnum::NotSupported,
                    "Operation not supported",
                ))
            }
        }
    }

    fn parent_start_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: DriveStartFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        let (callback, user_data) = callback.map_or((None, std::ptr::null_mut()), |callback| {
            let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

            unsafe extern "C" fn callback_trampoline<
                T: FileImpl,
                P: FnOnce(&T::Type, &AsyncResult) + 'static,
            >(
                source_object: *mut glib::gobject_ffi::GObject,
                res: *mut ffi::GAsyncResult,
                data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                    let res: &AsyncResult = &from_glib_borrow(res);
                    let callback: Box<thread_guard::ThreadGuard<P>> = Box::from_raw(data as *mut _);
                    let callback: P = callback.into_inner();
                    callback(source, res);
                }
            }
            let callback = callback_trampoline::<Self, P>;

            (Some(callback as _), Box::into_raw(super_callback) as *mut _)
        });

        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).start_mountable {
                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    mount_operation.to_glib_none().0,
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                ffi::g_task_report_new_error(
                    self.obj().unsafe_cast_ref::<Object>().to_glib_none().0,
                    callback,
                    user_data,
                    ffi::g_file_start_mountable as *mut _,
                    IOErrorEnum::domain().into_glib(),
                    IOErrorEnum::NotSupported.into_glib(),
                    "Operation not supported".to_glib_full(),
                );
            }
        }
    }

    fn parent_start_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).start_mountable_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else if let Some(task) = res.downcast_ref::<Task<bool>>() {
                // get the `Task` result as a boolean or as an error
                task.to_owned().propagate().map(|_| ())
            } else {
                // no parent implementation and don't know how to deal with the result so let's panic
                panic!("no parent \"start_mountable_finish\" implementation")
            }
        }
    }

    fn parent_stop_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        let (callback, user_data) = callback.map_or((None, std::ptr::null_mut()), |callback| {
            let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

            unsafe extern "C" fn callback_trampoline<
                T: FileImpl,
                P: FnOnce(&T::Type, &AsyncResult) + 'static,
            >(
                source_object: *mut glib::gobject_ffi::GObject,
                res: *mut ffi::GAsyncResult,
                data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                    let res: &AsyncResult = &from_glib_borrow(res);
                    let callback: Box<thread_guard::ThreadGuard<P>> = Box::from_raw(data as *mut _);
                    let callback: P = callback.into_inner();
                    callback(source, res);
                }
            }
            let callback = callback_trampoline::<Self, P>;

            (Some(callback as _), Box::into_raw(super_callback) as *mut _)
        });

        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).stop_mountable {
                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    mount_operation.to_glib_none().0,
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                ffi::g_task_report_new_error(
                    self.obj().unsafe_cast_ref::<Object>().to_glib_none().0,
                    callback,
                    user_data,
                    ffi::g_file_stop_mountable as *mut _,
                    IOErrorEnum::domain().into_glib(),
                    IOErrorEnum::NotSupported.into_glib(),
                    "Operation not supported".to_glib_full(),
                );
            }
        }
    }

    fn parent_stop_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).stop_mountable_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else if let Some(task) = res.downcast_ref::<Task<bool>>() {
                // get the `Task` result as a boolean or as an error
                task.to_owned().propagate().map(|_| ())
            } else {
                // no parent implementation and don't know how to deal with the result so let's panic
                panic!("no parent \"stop_mountable_finish\" implementation")
            }
        }
    }

    fn parent_unmount_mountable_with_operation<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).unmount_mountable_with_operation {
                let (callback, user_data) =
                    callback.map_or((None, std::ptr::null_mut()), |callback| {
                        let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

                        unsafe extern "C" fn callback_trampoline<
                            T: FileImpl,
                            P: FnOnce(&T::Type, &AsyncResult) + 'static,
                        >(
                            source_object: *mut glib::gobject_ffi::GObject,
                            res: *mut ffi::GAsyncResult,
                            data: glib::ffi::gpointer,
                        ) {
                            unsafe {
                                let source_object: &T::Type =
                                    &from_glib_borrow(source_object as *mut _);
                                let res: &AsyncResult = &from_glib_borrow(res);
                                let callback: Box<thread_guard::ThreadGuard<P>> =
                                    Box::from_raw(data as *mut _);
                                let callback: P = callback.into_inner();
                                callback(source_object, res);
                            }
                        }
                        let callback = callback_trampoline::<Self, P>;

                        (Some(callback as _), Box::into_raw(super_callback) as *mut _)
                    });

                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    mount_operation.to_glib_none().0,
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                )
            } else {
                self.unmount_mountable(flags, cancellable, callback);
            }
        }
    }

    fn parent_unmount_mountable_with_operation_finish(
        &self,
        res: &AsyncResult,
    ) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).unmount_mountable_with_operation_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                self.unmount_mountable_finish(res)
            }
        }
    }

    fn parent_eject_mountable_with_operation<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        flags: MountUnmountFlags,
        mount_operation: Option<&MountOperation>,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).eject_mountable_with_operation {
                let (callback, user_data) =
                    callback.map_or((None, std::ptr::null_mut()), |callback| {
                        let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

                        unsafe extern "C" fn callback_trampoline<
                            T: FileImpl,
                            P: FnOnce(&T::Type, &AsyncResult) + 'static,
                        >(
                            source_object: *mut glib::gobject_ffi::GObject,
                            res: *mut ffi::GAsyncResult,
                            data: glib::ffi::gpointer,
                        ) {
                            unsafe {
                                let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                                let res: &AsyncResult = &from_glib_borrow(res);
                                let callback: Box<thread_guard::ThreadGuard<P>> =
                                    Box::from_raw(data as *mut _);
                                let callback: P = callback.into_inner();
                                callback(source, res);
                            }
                        }
                        let callback = callback_trampoline::<Self, P>;

                        (Some(callback as _), Box::into_raw(super_callback) as *mut _)
                    });

                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    flags.into_glib(),
                    mount_operation.to_glib_none().0,
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                self.eject_mountable(flags, cancellable, callback);
            }
        }
    }

    fn parent_eject_mountable_with_operation_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).eject_mountable_with_operation_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else {
                self.eject_mountable_finish(res)
            }
        }
    }

    fn parent_poll_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
        &self,
        cancellable: Option<&Cancellable>,
        callback: Option<P>,
    ) {
        let (callback, user_data) = callback.map_or((None, std::ptr::null_mut()), |callback| {
            let super_callback = Box::new(thread_guard::ThreadGuard::new(callback));

            unsafe extern "C" fn callback_trampoline<
                T: FileImpl,
                P: FnOnce(&T::Type, &AsyncResult) + 'static,
            >(
                source_object: *mut glib::gobject_ffi::GObject,
                res: *mut ffi::GAsyncResult,
                data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let source: &T::Type = &from_glib_borrow(source_object as *mut _);
                    let res: &AsyncResult = &from_glib_borrow(res);
                    let callback: Box<thread_guard::ThreadGuard<P>> = Box::from_raw(data as *mut _);
                    let callback: P = callback.into_inner();
                    callback(source, res);
                }
            }
            let callback = callback_trampoline::<Self, P>;

            (Some(callback as _), Box::into_raw(super_callback) as *mut _)
        });

        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).poll_mountable {
                func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                    callback,
                    user_data,
                );
            } else {
                ffi::g_task_report_new_error(
                    self.obj().unsafe_cast_ref::<Object>().to_glib_none().0,
                    callback,
                    user_data,
                    ffi::g_file_poll_mountable as *mut _,
                    IOErrorEnum::domain().into_glib(),
                    IOErrorEnum::NotSupported.into_glib(),
                    "Operation not supported".to_glib_full(),
                );
            }
        }
    }

    fn parent_poll_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).poll_mountable_finish {
                let mut error = std::ptr::null_mut();
                let is_ok = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    res.to_glib_none().0,
                    &mut error,
                );
                debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
                if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                }
            } else if let Some(task) = res.downcast_ref::<Task<bool>>() {
                // get the `Task` result as a boolean or as an error
                task.to_owned().propagate().map(|_| ())
            } else {
                // no parent implementation and don't know how to deal with the result so let's panic
                panic!("no parent \"poll_mountable_finish\" implementation")
            }
        }
    }

    fn parent_measure_disk_usage(
        &self,
        flags: FileMeasureFlags,
        cancellable: Option<&Cancellable>,
        progress_callback: Option<&mut dyn FnMut(bool, u64, u64, u64)>,
    ) -> Result<(u64, u64, u64), Error> {
        let mut super_callback0 = progress_callback;
        let (progress_callback, progress_callback_data) =
            super_callback0
                .as_mut()
                .map_or((None, std::ptr::null_mut()), |progress_callback| {
                    unsafe extern "C" fn progress_callback_trampoline(
                        reporting: glib::ffi::gboolean,
                        current_size: u64,
                        num_dirs: u64,
                        num_files: u64,
                        user_data: glib::ffi::gpointer,
                    ) {
                        unsafe {
                            let progress_callback: &mut Box<
                                dyn FnMut(bool, u64, u64, u64) + 'static,
                            > = &mut *(user_data as *mut _);
                            progress_callback(
                                from_glib(reporting),
                                current_size,
                                num_dirs,
                                num_files,
                            )
                        }
                    }
                    (
                        Some(progress_callback_trampoline as _),
                        progress_callback as *mut &mut dyn FnMut(bool, u64, u64, u64) as *mut _,
                    )
                });

        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            let func = (*parent_iface)
                .measure_disk_usage
                .expect("no parent \"measure_disk_usage\" implementation");
            let mut disk_usage = 0u64;
            let mut num_dirs = 0u64;
            let mut num_files = 0u64;
            let mut error = std::ptr::null_mut();
            let is_ok = func(
                self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                flags.into_glib(),
                cancellable.to_glib_none().0,
                progress_callback,
                progress_callback_data,
                &mut disk_usage,
                &mut num_dirs,
                &mut num_files,
                &mut error,
            );
            debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
            if error.is_null() {
                Ok((disk_usage, num_dirs, num_files))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    fn parent_query_exists(&self, cancellable: Option<&Cancellable>) -> bool {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface =
                type_data.as_ref().parent_interface::<File>() as *const ffi::GFileIface;

            if let Some(func) = (*parent_iface).query_exists {
                let ret = func(
                    self.obj().unsafe_cast_ref::<File>().to_glib_none().0,
                    cancellable.to_glib_none().0,
                );
                from_glib(ret)
            } else {
                let file_info =
                    self.query_info("standard::type", FileQueryInfoFlags::NONE, cancellable);
                file_info.is_ok()
            }
        }
    }
}

impl<T: FileImpl> FileImplExt for T {}

// Implement virtual functions defined in `gio::ffi::GFileIface` except pairs `xxx_async/xxx_finish` for which GIO provides a default implementation.
unsafe impl<T: FileImpl> IsImplementable<T> for File {
    fn interface_init(iface: &mut Interface<Self>) {
        let iface = iface.as_mut();

        iface.dup = Some(file_dup::<T>);
        iface.hash = Some(file_hash::<T>);
        iface.equal = Some(file_equal::<T>);
        iface.is_native = Some(file_is_native::<T>);
        iface.has_uri_scheme = Some(file_has_uri_scheme::<T>);
        iface.get_uri_scheme = Some(file_get_uri_scheme::<T>);
        iface.get_basename = Some(file_get_basename::<T>);
        iface.get_path = Some(file_get_path::<T>);
        iface.get_uri = Some(file_get_uri::<T>);
        iface.get_parse_name = Some(file_get_parse_name::<T>);
        iface.get_parent = Some(file_get_parent::<T>);
        iface.prefix_matches = Some(file_prefix_matches::<T>);
        iface.get_relative_path = Some(file_get_relative_path::<T>);
        iface.resolve_relative_path = Some(file_resolve_relative_path::<T>);
        iface.get_child_for_display_name = Some(file_get_child_for_display_name::<T>);
        iface.enumerate_children = Some(file_enumerate_children::<T>);
        iface.query_info = Some(file_query_info::<T>);
        iface.query_filesystem_info = Some(file_query_filesystem_info::<T>);
        iface.find_enclosing_mount = Some(file_find_enclosing_mount::<T>);
        iface.set_display_name = Some(file_set_display_name::<T>);
        iface.query_settable_attributes = Some(file_query_settable_attributes::<T>);
        iface.query_writable_namespaces = Some(file_query_writable_namespaces::<T>);
        iface.set_attribute = Some(file_set_attribute::<T>);
        iface.set_attributes_from_info = Some(file_set_attributes_from_info::<T>);
        iface.read_fn = Some(file_read_fn::<T>);
        iface.append_to = Some(file_append_to::<T>);
        iface.create = Some(file_create::<T>);
        iface.replace = Some(file_replace::<T>);
        iface.delete_file = Some(file_delete_file::<T>);
        iface.trash = Some(file_trash::<T>);
        iface.make_directory = Some(file_make_directory::<T>);
        iface.make_symbolic_link = Some(file_make_symbolic_link::<T>);
        iface.copy = Some(file_copy::<T>);
        iface.move_ = Some(file_move::<T>);
        iface.mount_mountable = Some(file_mount_mountable::<T>);
        iface.mount_mountable_finish = Some(file_mount_mountable_finish::<T>);
        iface.unmount_mountable = Some(file_unmount_mountable::<T>);
        iface.unmount_mountable_finish = Some(file_unmount_mountable_finish::<T>);
        iface.eject_mountable = Some(file_eject_mountable::<T>);
        iface.eject_mountable_finish = Some(file_eject_mountable_finish::<T>);
        iface.mount_enclosing_volume = Some(file_mount_enclosing_volume::<T>);
        iface.mount_enclosing_volume_finish = Some(file_mount_enclosing_volume_finish::<T>);
        iface.monitor_dir = Some(file_monitor_dir::<T>);
        iface.monitor_file = Some(file_monitor_file::<T>);
        iface.open_readwrite = Some(file_open_readwrite::<T>);
        iface.create_readwrite = Some(file_create_readwrite::<T>);
        iface.replace_readwrite = Some(file_replace_readwrite::<T>);
        iface.start_mountable = Some(file_start_mountable::<T>);
        iface.start_mountable_finish = Some(file_start_mountable_finish::<T>);
        iface.stop_mountable = Some(file_stop_mountable::<T>);
        iface.stop_mountable_finish = Some(file_stop_mountable_finish::<T>);
        iface.supports_thread_contexts = T::SUPPORT_THREAD_CONTEXT.into_glib();
        iface.unmount_mountable_with_operation = Some(file_unmount_mountable_with_operation::<T>);
        iface.unmount_mountable_with_operation_finish =
            Some(file_unmount_mountable_with_operation_finish::<T>);
        iface.eject_mountable_with_operation = Some(file_eject_mountable_with_operation::<T>);
        iface.eject_mountable_with_operation_finish =
            Some(file_eject_mountable_with_operation_finish::<T>);
        iface.poll_mountable = Some(file_poll_mountable::<T>);
        iface.poll_mountable_finish = Some(file_poll_mountable_finish::<T>);
        iface.measure_disk_usage = Some(file_measure_disk_usage::<T>);
        #[cfg(feature = "v2_84")]
        {
            iface.query_exists = Some(file_query_exists::<T>);
        }
        // `GFile` already implements `xxx_async/xxx_finish` vfuncs and this should be ok.
        // TODO: when needed, override the `GFile` implementation of the following vfuncs:
        // iface.enumerate_children_async = Some(file_enumerate_children_async::<T>);
        // iface.enumerate_children_finish = Some(file_enumerate_children_finish::<T>);
        // iface.query_info_async = Some(file_query_info_async::<T>);
        // iface.query_info_finish = Some(file_query_info_finish::<T>);
        // iface.query_filesystem_info_async = Some(file_query_filesystem_info_async::<T>);
        // iface.query_filesystem_info_finish = Some(file_query_filesystem_info_finish::<T>);
        // iface.find_enclosing_mount_async = Some(file_find_enclosing_mount_asyncv);
        // iface.find_enclosing_mount_finish = Some(file_find_enclosing_mount_finish::<T>);
        // iface.set_display_name_async = Some(file_set_display_name_async::<T>);
        // iface.set_display_name_finish = Some(file_set_display_name_finish::<T>);
        // iface._query_settable_attributes_async = Some(_file_query_settable_attributes_async::<T>);
        // iface._query_settable_attributes_finish = Some(_file_query_settable_attributes_finish::<T>);
        // iface._query_writable_namespaces_async = Some(_file_query_writable_namespaces_async::<T>);
        // iface._query_writable_namespaces_finish = Some(_file_query_writable_namespaces_finish::<T>);
        // iface.set_attributes_async = Some(file_set_attributes_async::<T>);
        // iface.set_attributes_finish = Some(file_set_attributes_finishv);
        // iface.read_async = Some(file_read_async::<T>);
        // iface.read_finish = Some(file_read_finish::<T>);
        // iface.append_to_async = Some(file_append_to_async::<T>);
        // iface.append_to_finish = Some(file_append_to_finish::<T>);
        // iface.create_async = Some(file_create_async::<T>);
        // iface.create_finish = Some(file_create_finish::<T>);
        // iface.replace_async = Some(file_replace_async::<T>);
        // iface.replace_finish = Some(file_replace_finish::<T>);
        // iface.delete_file_async = Some(file_delete_file_async::<T>);
        // iface.delete_file_finish = Some(file_delete_file_finish::<T>);
        // iface.trash_async = Some(file_trash_async::<T>);
        // iface.trash_finish = Some(file_trash_finish::<T>);
        // iface.make_directory_async = Some(file_make_directory_async::<T>);
        // iface.make_directory_finish = Some(file_make_directory_finish::<T>);
        // iface.make_symbolic_link_async = Some(file_make_symbolic_link_async::<T>);
        // iface.make_symbolic_link_finish = Some(file_make_symbolic_link_finish::<T>);
        // iface.copy_async = Some(file_copy_async::<T>);
        // iface.copy_finish = Some(file_copy_finish::<T>);
        // iface.move_async = Some(file_move_async::<T>);
        // iface.move_finish = Some(file_move_finish::<T>);
        // iface.open_readwrite_async = Some(file_open_readwrite_async::<T>);
        // iface.open_readwrite_finish = Some(file_open_readwrite_finish::<T>);
        // iface.create_readwrite_async = Some(file_create_readwrite_async::<T>);
        // iface.create_readwrite_finish = Some(file_create_readwrite_finish::<T>);
        // iface.replace_readwrite_async = Some(file_replace_readwrite_async::<T>);
        // iface.replace_readwrite_finish = Some(file_replace_readwrite_finish::<T>);
        // iface.measure_disk_usage_async = Some(file_measure_disk_usage_async::<T>);
        // iface.measure_disk_usage_finish = Some(file_measure_disk_usage_finish::<T>);
    }
}

unsafe extern "C" fn file_dup<T: FileImpl>(file: *mut ffi::GFile) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        imp.dup().to_glib_full()
    }
}

unsafe extern "C" fn file_hash<T: FileImpl>(file: *mut ffi::GFile) -> c_uint {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        imp.hash()
    }
}

unsafe extern "C" fn file_equal<T: FileImpl>(
    file1: *mut ffi::GFile,
    file2: *mut ffi::GFile,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file1 as *mut T::Instance);
        let imp = instance.imp();

        imp.equal(&from_glib_borrow(file2)).into_glib()
    }
}

unsafe extern "C" fn file_is_native<T: FileImpl>(file: *mut ffi::GFile) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        imp.is_native().into_glib()
    }
}

unsafe extern "C" fn file_has_uri_scheme<T: FileImpl>(
    file: *mut ffi::GFile,
    uri_scheme: *const c_char,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        imp.has_uri_scheme(&GString::from_glib_borrow(uri_scheme))
            .into_glib()
    }
}

unsafe extern "C" fn file_get_uri_scheme<T: FileImpl>(file: *mut ffi::GFile) -> *mut c_char {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        let res = imp.uri_scheme();
        if let Some(uri_scheme) = res {
            uri_scheme.to_glib_full()
        } else {
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn file_get_basename<T: FileImpl>(file: *mut ffi::GFile) -> *mut c_char {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        let res = imp.basename();
        if let Some(basename) = res {
            basename.to_glib_full()
        } else {
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn file_get_path<T: FileImpl>(file: *mut ffi::GFile) -> *mut c_char {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        let res = imp.path();
        if let Some(path) = res {
            path.to_glib_full()
        } else {
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn file_get_uri<T: FileImpl>(file: *mut ffi::GFile) -> *mut c_char {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        let uri = imp.uri();
        uri.to_glib_full()
    }
}

unsafe extern "C" fn file_get_parse_name<T: FileImpl>(file: *mut ffi::GFile) -> *mut c_char {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        let parse_name = imp.parse_name();
        parse_name.to_glib_full()
    }
}

unsafe extern "C" fn file_get_parent<T: FileImpl>(file: *mut ffi::GFile) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        let res = imp.parent();
        if let Some(parent) = res {
            parent.to_glib_full()
        } else {
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn file_prefix_matches<T: FileImpl>(
    prefix: *mut ffi::GFile,
    file: *mut ffi::GFile,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        imp.has_prefix(&from_glib_borrow(prefix)).into_glib()
    }
}

unsafe extern "C" fn file_get_relative_path<T: FileImpl>(
    parent: *mut ffi::GFile,
    descendant: *mut ffi::GFile,
) -> *mut c_char {
    unsafe {
        let instance = &*(parent as *mut T::Instance);
        let imp = instance.imp();

        let res = imp.relative_path(&from_glib_borrow(descendant));
        if let Some(relative_path) = res {
            relative_path.to_glib_full()
        } else {
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn file_resolve_relative_path<T: FileImpl>(
    file: *mut ffi::GFile,
    relative_path: *const c_char,
) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        let resolved_path =
            imp.resolve_relative_path(GString::from_glib_borrow(relative_path).as_ref());
        resolved_path.to_glib_full()
    }
}

unsafe extern "C" fn file_get_child_for_display_name<T: FileImpl>(
    file: *mut ffi::GFile,
    display_name: *const c_char,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();

        // check display name is a valid ut8 and handle error to avoid rust panicking if it is not
        let basename = glib::ffi::g_filename_from_utf8(
            display_name,
            -1,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        if basename.is_null() {
            if !error.is_null() {
                *error =
                    Error::new::<IOErrorEnum>(IOErrorEnum::InvalidFilename, "Invalid filename")
                        .to_glib_full();
            }
            return std::ptr::null_mut();
        }

        let res = imp.child_for_display_name(&GString::from_glib_borrow(display_name));
        match res {
            Ok(child) => child.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_enumerate_children<T: FileImpl>(
    file: *mut ffi::GFile,
    attributes: *const c_char,
    flags: ffi::GFileQueryInfoFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileEnumerator {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.enumerate_children(
            &GString::from_glib_borrow(attributes),
            from_glib(flags),
            cancellable.as_ref(),
        );
        match res {
            Ok(enumerator) => enumerator.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_query_info<T: FileImpl>(
    file: *mut ffi::GFile,
    attributes: *const c_char,
    flags: ffi::GFileQueryInfoFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileInfo {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.query_info(
            &GString::from_glib_borrow(attributes),
            from_glib(flags),
            cancellable.as_ref(),
        );
        match res {
            Ok(file_info) => file_info.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_query_filesystem_info<T: FileImpl>(
    file: *mut ffi::GFile,
    attributes: *const c_char,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileInfo {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res =
            imp.query_filesystem_info(&GString::from_glib_borrow(attributes), cancellable.as_ref());
        match res {
            Ok(file_info) => file_info.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_find_enclosing_mount<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GMount {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.find_enclosing_mount(cancellable.as_ref());
        match res {
            Ok(mount) => mount.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_set_display_name<T: FileImpl>(
    file: *mut ffi::GFile,
    display_name: *const c_char,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.set_display_name(
            &GString::from_glib_borrow(display_name),
            cancellable.as_ref(),
        );
        match res {
            Ok(renamed_file) => renamed_file.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_query_settable_attributes<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileAttributeInfoList {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.query_settable_attributes(cancellable.as_ref());
        match res {
            Ok(settable_attributes) => settable_attributes.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_query_writable_namespaces<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileAttributeInfoList {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.query_writable_namespaces(cancellable.as_ref());
        match res {
            Ok(writable_namespaces) => writable_namespaces.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_set_attribute<T: FileImpl>(
    file: *mut ffi::GFile,
    attribute: *const c_char,
    type_: ffi::GFileAttributeType,
    value_p: glib::ffi::gpointer,
    flags: ffi::GFileQueryInfoFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res_ = imp.set_attribute(
            &GString::from_glib_borrow(attribute),
            FileAttributeValue::for_pointer(from_glib(type_), value_p),
            from_glib(flags),
            cancellable.as_ref(),
        );

        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_set_attributes_from_info<T: FileImpl>(
    file: *mut ffi::GFile,
    info: *mut ffi::GFileInfo,
    flags: ffi::GFileQueryInfoFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res_ = imp.set_attributes_from_info(
            &from_glib_borrow(info),
            from_glib(flags),
            cancellable.as_ref(),
        );
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_read_fn<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileInputStream {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res_ = imp.read_fn(cancellable.as_ref());
        match res_ {
            Ok(input_stream) => input_stream.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_append_to<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GFileCreateFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileOutputStream {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res_ = imp.append_to(from_glib(flags), cancellable.as_ref());
        match res_ {
            Ok(output_stream) => output_stream.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_create<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GFileCreateFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileOutputStream {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res_ = imp.create(from_glib(flags), cancellable.as_ref());
        match res_ {
            Ok(output_stream) => output_stream.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_replace<T: FileImpl>(
    file: *mut ffi::GFile,
    etag: *const c_char,
    make_backup: glib::ffi::gboolean,
    flags: ffi::GFileCreateFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileOutputStream {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let etag = Option::<GString>::from_glib_none(etag);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res_ = imp.replace(
            etag.as_ref().map(|etag| etag.as_str()),
            from_glib(make_backup),
            from_glib(flags),
            cancellable.as_ref(),
        );
        match res_ {
            Ok(output_stream) => output_stream.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_delete_file<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.delete(cancellable.as_ref());
        match res {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_trash<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.trash(cancellable.as_ref());
        match res {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_make_directory<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.make_directory(cancellable.as_ref());
        match res {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_make_symbolic_link<T: FileImpl>(
    file: *mut ffi::GFile,
    symlink_value: *const c_char,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.make_symbolic_link(
            GString::from_glib_borrow(symlink_value).as_ref(),
            cancellable.as_ref(),
        );
        match res {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_copy<T: FileImpl>(
    source: *mut ffi::GFile,
    destination: *mut ffi::GFile,
    flags: ffi::GFileCopyFlags,
    cancellable: *mut ffi::GCancellable,
    progress_callback: ffi::GFileProgressCallback,
    progress_callback_data: glib::ffi::gpointer,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let mut progress_callback = progress_callback.map(|callback| {
            move |current_num_bytes, total_num_bytes| {
                callback(current_num_bytes, total_num_bytes, progress_callback_data)
            }
        });
        let progress_callback_ref = progress_callback
            .as_mut()
            .map(|f| f as &mut dyn FnMut(i64, i64));

        let res = T::copy(
            &from_glib_borrow(source),
            &from_glib_borrow(destination),
            from_glib(flags),
            cancellable.as_ref(),
            progress_callback_ref,
        );
        match res {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_move<T: FileImpl>(
    source: *mut ffi::GFile,
    destination: *mut ffi::GFile,
    flags: ffi::GFileCopyFlags,
    cancellable: *mut ffi::GCancellable,
    progress_callback: ffi::GFileProgressCallback,
    progress_callback_data: glib::ffi::gpointer,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let mut progress_callback = progress_callback.map(|callback| {
            move |current_num_bytes, total_num_bytes| {
                callback(current_num_bytes, total_num_bytes, progress_callback_data)
            }
        });
        let progress_callback_ref = progress_callback
            .as_mut()
            .map(|f| f as &mut dyn FnMut(i64, i64));

        let res = T::move_(
            &from_glib_borrow(source),
            &from_glib_borrow(destination),
            from_glib(flags),
            cancellable.as_ref(),
            progress_callback_ref,
        );
        match res {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_mount_mountable<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GMountMountFlags,
    mount_operation: *mut ffi::GMountOperation,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let mount_operation = Option::<MountOperation>::from_glib_none(mount_operation);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.mount_mountable(
            from_glib(flags),
            mount_operation.as_ref(),
            cancellable.as_ref(),
            callback,
        );
    }
}

unsafe extern "C" fn file_mount_mountable_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFile {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.mount_mountable_finish(result);
        match res_ {
            Ok(mounted) => mounted.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_unmount_mountable<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GMountUnmountFlags,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.unmount_mountable(from_glib(flags), cancellable.as_ref(), callback);
    }
}

unsafe extern "C" fn file_unmount_mountable_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.unmount_mountable_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_eject_mountable<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GMountUnmountFlags,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.eject_mountable(from_glib(flags), cancellable.as_ref(), callback);
    }
}

unsafe extern "C" fn file_eject_mountable_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.eject_mountable_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_mount_enclosing_volume<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GMountMountFlags,
    mount_operation: *mut ffi::GMountOperation,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let mount_operation = Option::<MountOperation>::from_glib_none(mount_operation);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.mount_enclosing_volume(
            from_glib(flags),
            mount_operation.as_ref(),
            cancellable.as_ref(),
            callback,
        );
    }
}

unsafe extern "C" fn file_mount_enclosing_volume_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.mount_enclosing_volume_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_monitor_dir<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GFileMonitorFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileMonitor {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.monitor_dir(from_glib(flags), cancellable.as_ref());
        match res {
            Ok(monitor) => monitor.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_monitor_file<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GFileMonitorFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileMonitor {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.monitor_file(from_glib(flags), cancellable.as_ref());
        match res {
            Ok(monitor) => monitor.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_open_readwrite<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileIOStream {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.open_readwrite(cancellable.as_ref());
        match res {
            Ok(io_stream) => io_stream.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_create_readwrite<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GFileCreateFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileIOStream {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.create_readwrite(from_glib(flags), cancellable.as_ref());
        match res {
            Ok(io_stream) => io_stream.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_replace_readwrite<T: FileImpl>(
    file: *mut ffi::GFile,
    etag: *const c_char,
    make_backup: glib::ffi::gboolean,
    flags: ffi::GFileCreateFlags,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileIOStream {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let etag = Option::<GString>::from_glib_none(etag);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res_ = imp.replace_readwrite(
            etag.as_ref().map(|etag| etag.as_str()),
            from_glib(make_backup),
            from_glib(flags),
            cancellable.as_ref(),
        );
        match res_ {
            Ok(io_stream) => io_stream.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn file_start_mountable<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GDriveStartFlags,
    mount_operation: *mut ffi::GMountOperation,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let mount_operation = Option::<MountOperation>::from_glib_none(mount_operation);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.start_mountable(
            from_glib(flags),
            mount_operation.as_ref(),
            cancellable.as_ref(),
            callback,
        );
    }
}

unsafe extern "C" fn file_start_mountable_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.start_mountable_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_stop_mountable<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GMountUnmountFlags,
    mount_operation: *mut ffi::GMountOperation,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let mount_operation = Option::<MountOperation>::from_glib_none(mount_operation);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.stop_mountable(
            from_glib(flags),
            mount_operation.as_ref(),
            cancellable.as_ref(),
            callback,
        );
    }
}

unsafe extern "C" fn file_stop_mountable_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.stop_mountable_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_unmount_mountable_with_operation<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GMountUnmountFlags,
    mount_operation: *mut ffi::GMountOperation,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let mount_operation = Option::<MountOperation>::from_glib_none(mount_operation);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.unmount_mountable_with_operation(
            from_glib(flags),
            mount_operation.as_ref(),
            cancellable.as_ref(),
            callback,
        );
    }
}

unsafe extern "C" fn file_unmount_mountable_with_operation_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.unmount_mountable_with_operation_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_eject_mountable_with_operation<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GMountUnmountFlags,
    mount_operation: *mut ffi::GMountOperation,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let mount_operation = Option::<MountOperation>::from_glib_none(mount_operation);
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.eject_mountable_with_operation(
            from_glib(flags),
            mount_operation.as_ref(),
            cancellable.as_ref(),
            callback,
        );
    }
}

unsafe extern "C" fn file_eject_mountable_with_operation_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.eject_mountable_with_operation_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_poll_mountable<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let callback = callback.map(|callback| {
            move |source: &T::Type, res: &AsyncResult| {
                callback(
                    source.upcast_ref::<Object>().to_glib_none().0,
                    res.to_glib_none().0,
                    user_data,
                )
            }
        });

        imp.poll_mountable(cancellable.as_ref(), callback)
    }
}

unsafe extern "C" fn file_poll_mountable_finish<T: FileImpl>(
    file: *mut ffi::GFile,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let result: &AsyncResult = &from_glib_borrow(res);

        let res_ = imp.poll_mountable_finish(result);
        match res_ {
            Ok(_) => true.into_glib(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

unsafe extern "C" fn file_measure_disk_usage<T: FileImpl>(
    file: *mut ffi::GFile,
    flags: ffi::GFileMeasureFlags,
    cancellable: *mut ffi::GCancellable,
    progress_callback: ffi::GFileMeasureProgressCallback,
    progress_callback_data: glib::ffi::gpointer,
    disk_usage: *mut u64,
    num_dirs: *mut u64,
    num_files: *mut u64,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(file as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);
        let mut progress_callback = progress_callback.map(|callback| {
            move |reporting: bool, current_size: u64, num_dirs: u64, num_files: u64| {
                callback(
                    reporting.into_glib(),
                    current_size,
                    num_dirs,
                    num_files,
                    progress_callback_data,
                )
            }
        });
        let progress_callback_ref = progress_callback
            .as_mut()
            .map(|f| f as &mut dyn FnMut(bool, u64, u64, u64));

        let res = imp.measure_disk_usage(
            from_glib(flags),
            cancellable.as_ref(),
            progress_callback_ref,
        );
        match res {
            Ok((disk_usage_, num_dirs_, num_files_)) => {
                if !disk_usage.is_null() {
                    *disk_usage = disk_usage_
                }
                if !num_dirs.is_null() {
                    *num_dirs = num_dirs_
                }
                if !num_files.is_null() {
                    *num_files = num_files_
                }
                true.into_glib()
            }
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                false.into_glib()
            }
        }
    }
}

#[cfg(feature = "v2_84")]
unsafe extern "C" fn file_query_exists<T: FileImpl>(
    file: *mut ffi::GFile,
    cancellable: *mut ffi::GCancellable,
) -> glib::ffi::gboolean {
    let instance = &*(file as *mut T::Instance);
    let imp = instance.imp();
    let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

    let res = imp.query_exists(cancellable.as_ref());
    res.into_glib()
}

#[cfg(test)]
mod tests;
