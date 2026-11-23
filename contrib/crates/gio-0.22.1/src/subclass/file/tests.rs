// Take a look at the license at the top of the repository in the LICENSE file.

// The following tests rely on a custom type `MyCustomFile` that extends the existing GIO type `MyFile`. Both types implement the interface `gio::auto::File`.
// For each virtual method defined in interface `crate::ffi::GFileIface`, a test checks that `MyCustomFile` and `MyFile` return the same results.

use std::path::{Path, PathBuf};

use futures_channel::oneshot;

use super::*;
use crate::{FileMonitorEvent, FileType, prelude::*};

mod imp {
    use super::*;
    use crate::{FileAttributeInfoFlags, FileAttributeType, subclass::prelude::*};
    use glib::ValueDelegate;
    use std::{
        cell::{Cell, RefCell},
        hash::{self, Hash, Hasher},
    };

    const SCHEME: &str = "myfile";

    // Define custom types to use for properties in `MyFile`.
    #[derive(Copy, Clone, Debug, PartialEq, ValueDelegate)]
    #[value_delegate(from = ffi::GFileType)]
    pub struct MyFileType(pub FileType);

    impl Default for MyFileType {
        fn default() -> Self {
            Self(FileType::Unknown)
        }
    }

    impl From<ffi::GFileType> for MyFileType {
        fn from(v: ffi::GFileType) -> Self {
            Self(unsafe { FileType::from_glib(v) })
        }
    }

    impl<'a> From<&'a MyFileType> for ffi::GFileType {
        fn from(v: &'a MyFileType) -> Self {
            v.0.into_glib()
        }
    }

    impl From<MyFileType> for ffi::GFileType {
        fn from(v: MyFileType) -> Self {
            From::from(&v)
        }
    }

    #[derive(Default, Copy, Clone, Debug, PartialEq, ValueDelegate)]
    #[value_delegate(from = u8)]
    pub enum MyFileState {
        #[default]
        DoesNotExist,
        Exist,
        Deleted,
        Trashed,
    }

    impl From<u8> for MyFileState {
        fn from(v: u8) -> Self {
            match v {
                1 => Self::Exist,
                2 => Self::Deleted,
                3 => Self::Trashed,
                _ => Self::DoesNotExist,
            }
        }
    }

    impl<'a> From<&'a MyFileState> for u8 {
        fn from(v: &'a MyFileState) -> Self {
            match v {
                MyFileState::DoesNotExist => 0,
                MyFileState::Exist => 1,
                MyFileState::Deleted => 2,
                MyFileState::Trashed => 3,
            }
        }
    }

    impl From<MyFileState> for u8 {
        fn from(v: MyFileState) -> Self {
            From::from(&v)
        }
    }

    // Define `MyFile` as a subclass of `File`.
    #[derive(glib::Properties, Default, Debug)]
    #[properties(wrapper_type = super::MyFile)]
    pub struct MyFile {
        #[property(construct_only)]
        path: RefCell<PathBuf>,
        #[property(construct_only)]
        xattrs: RefCell<Vec<String>>,
        #[property(construct_only)]
        children: RefCell<Vec<String>>,
        #[property(construct_only)]
        type_: Cell<MyFileType>,
        #[property(construct_only)]
        state: Cell<MyFileState>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for MyFile {
        const NAME: &'static str = "MyFile";
        type Type = super::MyFile;
        type Interfaces = (File,);
    }

    #[glib::derived_properties]
    impl ObjectImpl for MyFile {}

    // Implements `FileImpl` with custom implementation.
    impl FileImpl for MyFile {
        const SUPPORT_THREAD_CONTEXT: bool = true;

        fn dup(&self) -> File {
            Self::Type::new(self.path.borrow().clone()).upcast()
        }

        fn hash(&self) -> u32 {
            let mut hasher = hash::DefaultHasher::new();
            self.path.borrow().hash(&mut hasher);
            hasher.finish() as u32
        }

        fn equal(&self, file2: &File) -> bool {
            match file2.downcast_ref::<Self::Type>() {
                Some(file2) => self.path == file2.imp().path,
                None => false,
            }
        }

        fn is_native(&self) -> bool {
            false
        }

        fn has_uri_scheme(&self, uri_scheme: &str) -> bool {
            uri_scheme == SCHEME
        }

        fn uri_scheme(&self) -> Option<String> {
            Some(SCHEME.to_owned())
        }

        fn basename(&self) -> Option<PathBuf> {
            self.path.borrow().file_name().map(PathBuf::from)
        }

        fn path(&self) -> Option<PathBuf> {
            Some(self.path.borrow().to_path_buf())
        }

        fn uri(&self) -> String {
            format!("{}://{}", SCHEME, self.path.borrow().to_string_lossy())
        }

        fn parse_name(&self) -> String {
            self.uri()
        }

        fn parent(&self) -> Option<File> {
            self.path
                .borrow()
                .parent()
                .map(|parent| Self::Type::new(parent).upcast())
        }

        fn has_prefix(&self, prefix: &File) -> bool {
            if let Some(prefix_path) = prefix.path() {
                self.path.borrow().starts_with(prefix_path)
            } else {
                false
            }
        }

        fn relative_path(&self, descendant: &File) -> Option<PathBuf> {
            match descendant.downcast_ref::<Self::Type>() {
                Some(descendant) => descendant
                    .imp()
                    .path
                    .borrow()
                    .as_path()
                    .strip_prefix(self.path.borrow().as_path())
                    .ok()
                    .map(PathBuf::from),
                None => None,
            }
        }

        fn resolve_relative_path(&self, relative_path: impl AsRef<std::path::Path>) -> File {
            let relative_pathbuf = PathBuf::from(relative_path.as_ref());
            let path = if relative_pathbuf.is_absolute() {
                relative_pathbuf
            } else {
                self.path.borrow().join(relative_pathbuf)
            };
            Self::Type::new(path).upcast()
        }

        fn child_for_display_name(&self, display_name: &str) -> Result<File, Error> {
            let path = self.path.borrow().join(display_name);
            Ok(Self::Type::new(path).upcast())
        }

        fn enumerate_children(
            &self,
            _attributes: &str,
            _flags: FileQueryInfoFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileEnumerator, Error> {
            if self.type_.get().0 != FileType::Directory {
                Err(Error::new(
                    IOErrorEnum::NotDirectory,
                    "File is not a directory",
                ))
            } else if self.state.get() != MyFileState::Exist {
                Err(Error::new(IOErrorEnum::NotFound, "File does not exist"))
            } else {
                let enumerator =
                    super::MyFileEnumerator::new(self.children.borrow().clone()).upcast();
                Ok(enumerator)
            }
        }

        fn query_info(
            &self,
            attributes: &str,
            _flags: FileQueryInfoFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileInfo, Error> {
            let file_info = FileInfo::new();
            let (mut name, mut xattr) = (false, Vec::new());
            for attribute in attributes.split(",") {
                let (n, x) = match attribute {
                    "*" => (true, "*"),
                    "standard::*" | "standard::name" => (true, ""),
                    attribute if attribute.starts_with("xattr::") => (false, attribute),
                    _ => {
                        return Err(Error::new(
                            IOErrorEnum::InvalidArgument,
                            &format!("Querying attributes {attribute} not supported for MyFile"),
                        ));
                    }
                };
                name |= n;
                xattr.push(x);
            }
            if name {
                file_info.set_name(
                    self.path
                        .borrow()
                        .file_name()
                        .map_or("none", |s| s.to_str().unwrap_or("none")),
                );
            }
            if !xattr.is_empty() {
                let all = xattr.contains(&"xattr::*");
                for xattr in self.xattrs.borrow().iter() {
                    let (key, value) = xattr.split_once('=').unwrap_or((xattr, ""));
                    if all || xattr.contains(key) {
                        file_info.set_attribute(key, value);
                    }
                }
            }
            Ok(file_info)
        }

        fn query_filesystem_info(
            &self,
            attributes: &str,
            cancellable: Option<&Cancellable>,
        ) -> Result<FileInfo, Error> {
            self.query_info(attributes, FileQueryInfoFlags::NONE, cancellable)
        }

        fn find_enclosing_mount(&self, _cancellable: Option<&Cancellable>) -> Result<Mount, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Find enclosing mount not supported for MyFile",
            ))
        }

        fn set_display_name(
            &self,
            display_name: &str,
            _query_writable_namespaces_async: Option<&Cancellable>,
        ) -> Result<File, Error> {
            let path = match self.path.borrow().parent() {
                Some(parent) => parent.join(display_name),
                None => PathBuf::from(display_name),
            };
            Ok(Self::Type::new(path).upcast())
        }

        fn query_settable_attributes(
            &self,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileAttributeInfoList, Error> {
            let file_attribute_info_list = FileAttributeInfoList::new();
            file_attribute_info_list.add(
                "standard::name",
                FileAttributeType::String,
                FileAttributeInfoFlags::NONE,
            );
            for xattr in self.xattrs.borrow().iter() {
                let (key, _value) = xattr.split_once('=').unwrap_or((xattr, ""));
                file_attribute_info_list.add(
                    &format!("xattr::{key}"),
                    FileAttributeType::String,
                    FileAttributeInfoFlags::NONE,
                );
            }
            Ok(file_attribute_info_list)
        }

        fn query_writable_namespaces(
            &self,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileAttributeInfoList, Error> {
            let file_attribute_info_list = FileAttributeInfoList::new();
            file_attribute_info_list.add(
                "xattr",
                FileAttributeType::String,
                FileAttributeInfoFlags::NONE,
            );
            Ok(file_attribute_info_list)
        }

        fn set_attribute<'a>(
            &self,
            attribute: &str,
            value: impl Into<FileAttributeValue<'a>>,
            _flags: FileQueryInfoFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<(), Error> {
            let value = value.into();
            match (attribute, value.type_(), value.as_ptr()) {
                ("standard::name", FileAttributeType::String, p) => {
                    let name = unsafe {
                        <String as FromGlibPtrNone<*mut libc::c_char>>::from_glib_none(p as *mut _)
                    };
                    self.path.borrow_mut().set_file_name(name);
                    Ok(())
                }
                (attribute, FileAttributeType::String, p) if attribute.starts_with("xattr:") => {
                    let value = unsafe {
                        <String as FromGlibPtrNone<*mut libc::c_char>>::from_glib_none(p as *mut _)
                    };
                    self.xattrs
                        .borrow_mut()
                        .push(format!("{attribute}={value}"));
                    Ok(())
                }
                (attribute, type_, _) => Err(Error::new(
                    IOErrorEnum::InvalidArgument,
                    &format!(
                        "Setting attribute '{attribute}' ({type_:?}) not supported for MyFile"
                    ),
                )),
            }
        }

        fn set_attributes_from_info(
            &self,
            info: &FileInfo,
            flags: FileQueryInfoFlags,
            cancellable: Option<&Cancellable>,
        ) -> Result<(), Error> {
            for attribute in info.list_attributes(None) {
                let value = info.attribute_as_string(attribute.as_str()).unwrap();
                self.set_attribute(attribute.as_str(), value.as_str(), flags, cancellable)?;
            }
            Ok(())
        }

        fn read_fn(&self, _cancellable: Option<&Cancellable>) -> Result<FileInputStream, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Reading not supported for MyFile",
            ))
        }

        fn append_to(
            &self,
            _flags: FileCreateFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileOutputStream, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Appending to file not supported for MyFile",
            ))
        }

        fn create(
            &self,
            _flags: FileCreateFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileOutputStream, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Creating file not supported for MyFile",
            ))
        }

        fn replace(
            &self,
            _etag: Option<&str>,
            _make_backup: bool,
            _flags: FileCreateFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileOutputStream, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Replacing file not supported for MyFile",
            ))
        }

        fn delete(&self, _cancellable: Option<&Cancellable>) -> Result<(), Error> {
            if self.state.get() != MyFileState::Exist {
                Err(Error::new(IOErrorEnum::NotFound, "File does not exist"))
            } else {
                self.state.set(MyFileState::Deleted);
                Ok(())
            }
        }

        fn trash(&self, _cancellable: Option<&Cancellable>) -> Result<(), Error> {
            if self.state.get() != MyFileState::Exist {
                Err(Error::new(IOErrorEnum::NotFound, "File does not exist"))
            } else {
                self.state.set(MyFileState::Trashed);
                Ok(())
            }
        }

        fn make_directory(&self, _cancellable: Option<&Cancellable>) -> Result<(), Error> {
            if self.state.get() == MyFileState::Exist {
                Err(Error::new(IOErrorEnum::Exists, "File already exists"))
            } else {
                self.state.set(MyFileState::Exist);
                self.type_.set(MyFileType(FileType::Directory));
                Ok(())
            }
        }

        fn make_symbolic_link(
            &self,
            _symlink_value: impl AsRef<std::path::Path>,
            _cancellable: Option<&Cancellable>,
        ) -> Result<(), Error> {
            if self.state.get() == MyFileState::Exist {
                Err(Error::new(IOErrorEnum::Exists, "File already exists"))
            } else {
                self.state.set(MyFileState::Exist);
                self.type_.set(MyFileType(FileType::SymbolicLink));
                Ok(())
            }
        }

        fn copy(
            source: &File,
            destination: &File,
            flags: FileCopyFlags,
            cancellable: Option<&Cancellable>,
            progress_callback: Option<&mut dyn FnMut(i64, i64)>,
        ) -> Result<(), Error> {
            let source_exists = source.downcast_ref::<Self::Type>().map_or_else(
                || source.query_exists(cancellable),
                |f| f.imp().state.get() == MyFileState::Exist,
            );
            let source_type = source.downcast_ref::<Self::Type>().map_or_else(
                || source.query_file_type(FileQueryInfoFlags::NONE, cancellable),
                |f| f.imp().type_.get().0,
            );
            let destination_exists = destination.downcast_ref::<Self::Type>().map_or_else(
                || destination.query_exists(cancellable),
                |f| f.imp().state.get() == MyFileState::Exist,
            );
            let destination_type = destination.downcast_ref::<Self::Type>().map_or_else(
                || destination.query_file_type(FileQueryInfoFlags::NONE, cancellable),
                |f| f.imp().type_.get().0,
            );
            let overwrite = flags.contains(FileCopyFlags::OVERWRITE);
            match (
                source_exists,
                source_type,
                destination_exists,
                destination_type,
                overwrite,
            ) {
                (false, _, _, _, _) => {
                    Err(Error::new(IOErrorEnum::NotFound, "Source does not exist"))
                }
                (true, _, true, _, false) => Err(Error::new(
                    IOErrorEnum::Exists,
                    "Destination already exists",
                )),
                (true, FileType::Regular, true, FileType::Directory, true) => Err(Error::new(
                    IOErrorEnum::IsDirectory,
                    "Cannot overwrite a directory with a file",
                )),
                (true, FileType::Directory, true, FileType::Directory, true) => Err(Error::new(
                    IOErrorEnum::WouldMerge,
                    "Cannot overwrite a directory with a directory",
                )),
                (true, FileType::Directory, false, _, _) => Err(Error::new(
                    IOErrorEnum::WouldRecurse,
                    "Cannot handle recursive copy of source directory",
                )),
                (true, FileType::Directory, true, FileType::Regular, true) => Err(Error::new(
                    IOErrorEnum::WouldRecurse,
                    "Cannot handle recursive copy of source directory",
                )),
                _ => {
                    // Simulate a copy operation
                    if let Some(callback) = progress_callback {
                        for i in 0..10 {
                            // Simulate progress
                            callback(i * 10_i64, 100);
                        }
                    }
                    if let Some(dest) = destination.downcast_ref::<Self::Type>() {
                        dest.imp().state.set(MyFileState::Exist);
                        dest.imp().type_.set(MyFileType(source_type));
                    }
                    Ok(())
                }
            }
        }

        fn move_(
            source: &File,
            destination: &File,
            flags: FileCopyFlags,
            cancellable: Option<&Cancellable>,
            progress_callback: Option<&mut dyn FnMut(i64, i64)>,
        ) -> Result<(), Error> {
            let source_exists = source.downcast_ref::<Self::Type>().map_or_else(
                || source.query_exists(cancellable),
                |f| f.imp().state.get() == MyFileState::Exist,
            );
            let source_type = source.downcast_ref::<Self::Type>().map_or_else(
                || source.query_file_type(FileQueryInfoFlags::NONE, cancellable),
                |f| f.imp().type_.get().0,
            );
            let destination_exists = destination.downcast_ref::<Self::Type>().map_or_else(
                || destination.query_exists(cancellable),
                |f| f.imp().state.get() == MyFileState::Exist,
            );
            let destination_type = destination.downcast_ref::<Self::Type>().map_or_else(
                || destination.query_file_type(FileQueryInfoFlags::NONE, cancellable),
                |f| f.imp().type_.get().0,
            );
            let overwrite = flags.contains(FileCopyFlags::OVERWRITE);
            match (
                source_exists,
                source_type,
                destination_exists,
                destination_type,
                overwrite,
            ) {
                (false, _, _, _, _) => {
                    Err(Error::new(IOErrorEnum::NotFound, "Source does not exist"))
                }
                (true, _, true, _, false) => Err(Error::new(
                    IOErrorEnum::Exists,
                    "Destination already exists",
                )),
                (true, FileType::Regular, true, FileType::Directory, true) => Err(Error::new(
                    IOErrorEnum::IsDirectory,
                    "Cannot overwrite a directory with a file",
                )),
                (true, FileType::Directory, true, FileType::Directory, true) => Err(Error::new(
                    IOErrorEnum::WouldMerge,
                    "Cannot overwrite a directory with a directory",
                )),
                (true, FileType::Directory, false, _, _) => Err(Error::new(
                    IOErrorEnum::WouldRecurse,
                    "Cannot handle recursive copy of source directory",
                )),
                (true, FileType::Directory, true, FileType::Regular, true) => Err(Error::new(
                    IOErrorEnum::WouldRecurse,
                    "Cannot handle recursive copy of source directory",
                )),
                _ => {
                    // Simulate a move operation
                    if let Some(callback) = progress_callback {
                        for i in 0..10 {
                            // Simulate progress
                            callback(i * 10_i64, 100);
                        }
                    }
                    if let Some(dest) = destination.downcast_ref::<Self::Type>() {
                        dest.imp().state.set(MyFileState::Exist);
                        dest.imp().type_.set(MyFileType(source_type));
                    }
                    Ok(())
                }
            }
        }

        fn mount_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: MountMountFlags,
            _mount_operation: Option<&MountOperation>,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<File>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Mounting mountable not supported for MyFile",
            )));
        }

        fn mount_mountable_finish(&self, res: &AsyncResult) -> Result<File, Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<File>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
            }
        }

        fn unmount_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: MountUnmountFlags,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Unmounting mountable not supported for MyFile",
            )));
        }

        fn unmount_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn eject_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: MountUnmountFlags,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Ejecting mountable not supported for MyFile",
            )));
        }

        fn eject_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn mount_enclosing_volume<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: MountMountFlags,
            _mount_operation: Option<&MountOperation>,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Mounting enclosing volume not supported for MyFile",
            )));
        }

        fn mount_enclosing_volume_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn monitor_dir(
            &self,
            _flags: FileMonitorFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileMonitor, Error> {
            if self.type_.get().0 != FileType::Directory {
                Err(Error::new(
                    IOErrorEnum::NotDirectory,
                    "File is not a directory",
                ))
            } else if self.state.get() != MyFileState::Exist {
                Err(Error::new(IOErrorEnum::NotFound, "File does not exist"))
            } else {
                let monitor = super::MyFileMonitor::new().upcast();
                Ok(monitor)
            }
        }

        fn monitor_file(
            &self,
            _flags: FileMonitorFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileMonitor, Error> {
            if self.type_.get().0 != FileType::Regular {
                Err(Error::new(
                    IOErrorEnum::NotRegularFile,
                    "File is not a file",
                ))
            } else if self.state.get() != MyFileState::Exist {
                Err(Error::new(IOErrorEnum::NotFound, "File does not exist"))
            } else {
                let monitor = super::MyFileMonitor::new().upcast();
                Ok(monitor)
            }
        }

        fn open_readwrite(
            &self,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileIOStream, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Opening file for read/write not supported for MyFile",
            ))
        }

        fn create_readwrite(
            &self,
            _flags: FileCreateFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileIOStream, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Creating file for read/write not supported for MyFile",
            ))
        }

        fn replace_readwrite(
            &self,
            _etag: Option<&str>,
            _make_backup: bool,
            _flags: FileCreateFlags,
            _cancellable: Option<&Cancellable>,
        ) -> Result<FileIOStream, Error> {
            Err(Error::new(
                IOErrorEnum::NotSupported,
                "Replacing file for read/write not supported for MyFile",
            ))
        }

        fn start_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: DriveStartFlags,
            _mount_operation: Option<&MountOperation>,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Starting mountable not supported for MyFile",
            )));
        }

        fn start_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn stop_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: MountUnmountFlags,
            _mount_operation: Option<&MountOperation>,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Stopping mountable not supported for MyFile",
            )));
        }

        fn stop_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn unmount_mountable_with_operation<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: MountUnmountFlags,
            _mount_operation: Option<&MountOperation>,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Unmounting mountable with operation not supported for MyFile",
            )));
        }

        fn unmount_mountable_with_operation_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn eject_mountable_with_operation<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            _flags: MountUnmountFlags,
            _mount_operation: Option<&MountOperation>,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Ejecting mountable with operation not supported for MyFile",
            )));
        }

        fn eject_mountable_with_operation_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn poll_mountable<P: FnOnce(&Self::Type, &AsyncResult) + 'static>(
            &self,
            cancellable: Option<&Cancellable>,
            callback: Option<P>,
        ) {
            let callback = callback.expect("callback is required");
            let task = unsafe {
                crate::LocalTask::new(
                    Some(self.obj().as_ref()),
                    cancellable,
                    move |task: crate::LocalTask<bool>, source: Option<&Self::Type>| {
                        callback(source.unwrap(), task.upcast_ref::<AsyncResult>())
                    },
                )
            };
            task.return_result(Err(Error::new(
                IOErrorEnum::NotSupported,
                "Polling mountable not supported for MyFile",
            )));
        }

        fn poll_mountable_finish(&self, res: &AsyncResult) -> Result<(), Error> {
            unsafe {
                res.downcast_ref::<crate::LocalTask<bool>>()
                    .expect("res expected to be a LocalTask")
                    .to_owned()
                    .propagate()
                    .map(|_| ())
            }
        }

        fn measure_disk_usage(
            &self,
            _flags: FileMeasureFlags,
            _cancellable: Option<&Cancellable>,
            progress_callback: Option<&mut dyn FnMut(bool, u64, u64, u64)>,
        ) -> Result<(u64, u64, u64), Error> {
            // Simulate a measure disk operation
            if let Some(callback) = progress_callback {
                for i in 0..10u64 {
                    // Simulate progress
                    callback(true, i * 10, i, i * 5);
                }
                callback(false, 100u64, 10u64, 50u64);
            }
            Ok((100u64, 10u64, 50u64))
        }

        fn query_exists(&self, _cancellable: Option<&Cancellable>) -> bool {
            true
        }
    }

    #[derive(Default)]
    pub struct MyCustomFile;

    // Define `MyCustomFile` as a subclass of `MyFile`.
    #[glib::object_subclass]
    impl ObjectSubclass for MyCustomFile {
        const NAME: &'static str = "MyCustomFile";
        type Type = super::MyCustomFile;
        type ParentType = super::MyFile;
        type Interfaces = (File,);
    }

    impl ObjectImpl for MyCustomFile {}

    // Implements `FileImpl` with default implementation, which calls the parent's implementation.
    impl FileImpl for MyCustomFile {}

    impl MyFileImpl for MyCustomFile {}

    pub(super) mod file_enumerator {
        use super::*;

        // Define `MyFileEnumerator` as a subclass of `FileEnumerator`.
        #[derive(glib::Properties, Default, Debug)]
        #[properties(wrapper_type = super::MyFileEnumerator)]
        pub struct MyFileEnumerator {
            #[property(construct_only)]
            children: RefCell<Vec<String>>,
            index: Cell<i32>,
        }

        #[glib::object_subclass]
        impl ObjectSubclass for MyFileEnumerator {
            const NAME: &'static str = "MyFileMyEnumerator"; // Different name than `MyFileEnumerator` defined in `FileEnumerator` tests.
            type Type = super::MyFileEnumerator;
            type ParentType = FileEnumerator;
        }

        #[glib::derived_properties]
        impl ObjectImpl for MyFileEnumerator {
            fn dispose(&self) {
                let _ = self.obj().close(Cancellable::NONE);
            }
        }

        // Implements `FileEnumeratorImpl` with custom implementation.
        impl FileEnumeratorImpl for MyFileEnumerator {
            fn next_file(
                &self,
                _cancellable: Option<&Cancellable>,
            ) -> Result<Option<FileInfo>, glib::Error> {
                match self.index.get() {
                    -1 => Err(glib::Error::new::<IOErrorEnum>(
                        IOErrorEnum::Closed,
                        "Enumerator is closed",
                    )),
                    i if (i as usize) < self.children.borrow().len() => {
                        let file_info = FileInfo::new();
                        file_info.set_name(self.children.borrow()[i as usize].as_str());
                        self.index.set(i + 1);
                        Ok(Some(file_info))
                    }
                    _ => Ok(None),
                }
            }

            fn close(&self, _cancellable: Option<&Cancellable>) -> (bool, Option<glib::Error>) {
                self.index.set(-1);
                (true, None)
            }
        }
    }

    pub(super) mod file_monitor {
        use super::*;

        // Define `MyFileMonitor` as a subclass of `FileMonitor`.
        #[derive(Default, Debug)]
        pub struct MyFileMonitor;

        #[glib::object_subclass]
        impl ObjectSubclass for MyFileMonitor {
            const NAME: &'static str = "MyFileMyMonitor"; // Different name than `MyFileMonitor` defined in `FileMonitor` tests.
            type Type = super::MyFileMonitor;
            type ParentType = FileMonitor;
        }

        impl ObjectImpl for MyFileMonitor {}

        // Implements `FileMonitorImpl` with custom implementation.
        impl FileMonitorImpl for MyFileMonitor {
            fn cancel(&self) {}
        }
    }
}

use imp::{MyFileState, MyFileType};

glib::wrapper! {
    pub struct MyFile(ObjectSubclass<imp::MyFile>) @implements File;
}

impl MyFile {
    fn new<P: AsRef<Path>>(path: P) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .build()
    }

    fn with_xattr<'a, P: AsRef<Path>, X: AsRef<[&'a str]>>(path: P, xattrs: X) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .property("xattrs", xattrs.as_ref())
            .build()
    }

    fn with_children<'a, P: AsRef<Path>, C: AsRef<[&'a str]>>(path: P, children: C) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .property("children", children.as_ref())
            .property("type", MyFileType(FileType::Directory))
            .property("state", MyFileState::Exist)
            .build()
    }

    fn with_type_state<P: AsRef<Path>>(path: P, type_: FileType, state: MyFileState) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .property("type", MyFileType(type_))
            .property("state", state)
            .build()
    }
}

pub trait MyFileImpl: ObjectImpl + ObjectSubclass<Type: IsA<MyFile> + IsA<File>> {}

// To make this class subclassable we need to implement IsSubclassable
unsafe impl<T: MyFileImpl + FileImpl> IsSubclassable<T> for MyFile {}

glib::wrapper! {
    pub struct MyCustomFile(ObjectSubclass<imp::MyCustomFile>) @extends MyFile, @implements File;
}

impl MyCustomFile {
    fn new<P: AsRef<Path>>(path: P) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .build()
    }

    fn with_xattr<'a, P: AsRef<Path>, X: AsRef<[&'a str]>>(path: P, xattrs: X) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .property("xattrs", xattrs.as_ref())
            .build()
    }

    fn with_children<'a, P: AsRef<Path>, C: AsRef<[&'a str]>>(path: P, children: C) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .property("children", children.as_ref())
            .property("type", MyFileType(FileType::Directory))
            .property("state", MyFileState::Exist)
            .build()
    }

    fn with_type_state<P: AsRef<Path>>(path: P, type_: FileType, state: MyFileState) -> Self {
        Object::builder()
            .property("path", path.as_ref().to_path_buf())
            .property("type", MyFileType(type_))
            .property("state", state)
            .build()
    }
}

glib::wrapper! {
    pub struct MyFileEnumerator(ObjectSubclass<imp::file_enumerator::MyFileEnumerator>) @extends FileEnumerator;
}

impl MyFileEnumerator {
    pub fn new(children: Vec<String>) -> Self {
        Object::builder().property("children", children).build()
    }
}

glib::wrapper! {
    pub struct MyFileMonitor(ObjectSubclass<imp::file_monitor::MyFileMonitor>) @extends FileMonitor;
}

impl MyFileMonitor {
    pub fn new() -> Self {
        Object::builder().build()
    }
}

impl Default for MyFileMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[test]
fn file_dup() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::dup`
    let my_custom_file = MyCustomFile::new("/my_file");
    let dup = my_custom_file.dup();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::dup`
    let my_file = MyFile::new("/my_file");
    let expected = my_file.dup();

    // both results should equal
    assert!(dup.equal(&expected));
}

// checker-ignore-item
#[test]
fn file_hash() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::hash`
    let my_custom_file = MyCustomFile::new("/my_file");
    let hash = unsafe {
        crate::ffi::g_file_hash(
            <MyCustomFile as ToGlibPtr<
                *mut glib::subclass::basic::InstanceStruct<imp::MyCustomFile>,
            >>::to_glib_none(&my_custom_file)
            .0 as glib::ffi::gconstpointer,
        )
    };

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::hash`
    let my_file = MyFile::new("/my_file");
    let expected = unsafe {
        crate::ffi::g_file_hash(
            <MyFile as ToGlibPtr<
                *mut glib::subclass::basic::InstanceStruct<imp::MyFile>,
            >>::to_glib_none(&my_file)
            .0 as glib::ffi::gconstpointer,
        )
    };

    // both hash values should equal
    assert_eq!(hash, expected);
}

#[test]
fn file_equal() {
    // 2 instances of `MyCustomFile` with same path should equal
    let my_custom_file = MyCustomFile::new("/my_file");
    let expected = MyCustomFile::new("/my_file");
    assert!(my_custom_file.equal(&expected));

    // instances of `MyCustomFile` and of `MyFile` with same path should not equal (because type is different)
    let expected = File::for_path(PathBuf::from("/my_file"));
    assert!(!my_custom_file.equal(&expected));
}

#[test]
fn file_is_native() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::is_native`
    let my_custom_file = MyCustomFile::new("/my_file");
    let is_native = my_custom_file.is_native();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::is_native`
    let my_file = MyFile::new("/my_file");
    let expected = my_file.is_native();

    // both results should equal
    assert_eq!(is_native, expected);
}

#[test]
fn file_has_uri_scheme() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::has_uri_scheme`
    let my_custom_file = MyCustomFile::new("/my_file");
    let has_file = my_custom_file.has_uri_scheme("file");
    let has_foo = my_custom_file.has_uri_scheme("foo");

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::has_uri_scheme`
    let my_file = MyFile::new("/my_file");

    let expected_file = my_file.has_uri_scheme("file");
    let expected_foo = my_file.has_uri_scheme("foo");

    // both results should equal
    assert_eq!(has_file, expected_file);
    assert_eq!(has_foo, expected_foo);
}

#[test]
fn file_uri_scheme() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::uri_scheme`
    let my_custom_file = MyCustomFile::new("/my_file");
    let uri_scheme = my_custom_file.uri_scheme();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::uri_scheme`
    let my_file = MyFile::new("/my_file");
    let expected = my_file.uri_scheme();

    // both uri schemes should equal
    assert_eq!(uri_scheme, expected);
}

#[test]
fn file_basename() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::basename`
    let my_custom_file = MyCustomFile::new("/my_file");
    let basename = my_custom_file.basename();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::basename`
    let my_file = MyFile::new("/my_file");
    let expected = my_file.basename();

    // both basenames should equal
    assert_eq!(basename, expected);
}

#[test]
fn file_path() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::path`
    let my_custom_file = MyCustomFile::new("/my_file");
    let path = my_custom_file.path();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::path`
    let my_file = MyFile::new("/my_file");
    let expected = my_file.path();

    // both paths should equal
    assert_eq!(path, expected);
}

#[test]
fn file_uri() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::uri`
    let my_custom_file = MyCustomFile::new("/my_file");
    let uri = my_custom_file.uri();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::uri`
    let my_file = MyFile::new("/my_file");
    let expected = my_file.uri();

    // both uris should equal
    assert_eq!(uri, expected);
}

#[test]
fn file_parse_name() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::parse_name`
    let my_custom_file = MyCustomFile::new("/my_file");
    let parse_name = my_custom_file.parse_name();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::parse_name`
    let my_file = MyFile::new("/my_file");
    let expected = my_file.parse_name();

    // both parse names should equal
    assert_eq!(parse_name, expected);
}

#[test]
fn file_parent() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::parent`
    let my_custom_file = MyCustomFile::new("/my_parent/my_file");
    let res = my_custom_file.parent();
    assert!(res.is_some(), "unexpected None");
    let parent = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::parent`
    let my_file = MyFile::new("/my_parent/my_file");
    let res = my_file.parent();
    assert!(res.is_some(), "unexpected None");
    let expected = res.unwrap();

    // both parents should equal
    assert!(parent.equal(&expected));
}

#[test]
fn file_has_prefix() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::has_prefix`
    let my_custom_file = MyCustomFile::new("/my_prefix/my_file");
    let my_custom_prefix = MyCustomFile::new("/my_prefix");
    let has_prefix = my_custom_file.has_prefix(&my_custom_prefix);

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::has_prefix`
    let my_file = MyFile::new("/my_prefix/my_file");
    let my_prefix = MyFile::new("/my_prefix");
    let expected = my_file.has_prefix(&my_prefix);

    // both results should equal
    assert_eq!(has_prefix, expected);
}

#[test]
fn file_relative_path() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::relative_path`
    let my_custom_parent = MyCustomFile::new("/my_parent");
    let my_custom_descendant = MyCustomFile::new("/my_parent/my_descendant");
    let relative_path = my_custom_parent.relative_path(&my_custom_descendant);

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::relative_path`
    let my_parent = MyFile::new("/my_parent");
    let my_descendant = MyFile::new("/my_parent/my_descendant");
    let expected = my_parent.relative_path(&my_descendant);

    // both relative paths should equal
    assert_eq!(relative_path, expected);
}

#[test]
fn file_resolve_relative_path() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::resolve_relative_path`
    let my_custom_prefix = MyCustomFile::new("/my_prefix");
    let resolved_path = my_custom_prefix.resolve_relative_path("my_file");

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::resolve_relative_path`
    let my_prefix = MyFile::new("/my_prefix");
    let expected = my_prefix.resolve_relative_path("my_file");

    // both resolved path result should equal
    assert!(resolved_path.equal(&expected));
}

#[test]
fn file_child_for_display_name() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::child_for_display_name`
    let my_custom_parent = MyCustomFile::new("/my_parent");
    let res = my_custom_parent.child_for_display_name("my_file");
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let child = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::child_for_display_name`
    let my_parent = MyFile::new("/my_parent");
    let res = my_parent.child_for_display_name("my_file");
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both children should equal
    assert!(child.equal(&expected))
}

#[test]
fn file_enumerate_children() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::enumerate_children`
    let my_custom_file =
        MyCustomFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);
    let res = my_custom_file.enumerate_children("*", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_err(), "unexpected enumerator");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::enumerate_children`
    let my_file = MyFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);
    let res = my_file.enumerate_children("*", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_err(), "unexpected enumerator");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::enumerate_children`
    let my_custom_parent = MyCustomFile::with_children("/my_parent", vec!["my_file1", "my_file2"]);
    let res = my_custom_parent.enumerate_children("*", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let my_custom_enumerator = res.unwrap();
    let res = my_custom_enumerator
        .map(|res| res.map(|file_info| file_info.name()))
        .collect::<Result<Vec<_>, _>>();
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let my_custom_children = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::enumerate_children`
    let my_parent = MyFile::with_children("/my_parent", vec!["my_file1", "my_file2"]);
    let res = my_parent.enumerate_children("*", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let my_enumerator = res.unwrap();
    let res = my_enumerator
        .map(|res| res.map(|file_info| file_info.name()))
        .collect::<Result<Vec<_>, _>>();
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both children should equal
    assert_eq!(my_custom_children, expected)
}

#[test]
fn file_query_info() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::query_info`
    let my_custom_file =
        MyCustomFile::with_xattr("/my_file", vec!["xattr::key1=value1", "xattr::key2=value2"]);
    let res = my_custom_file.query_info("*", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let file_info = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::query_info`
    let my_file = MyFile::with_xattr("/my_file", vec!["xattr::key1=value1", "xattr::key2=value2"]);
    let res = my_file.query_info("*", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both results should equal
    assert_eq!(file_info.name(), expected.name());
    assert_eq!(
        file_info.attribute_as_string("xattr::key1"),
        expected.attribute_as_string("xattr::key1")
    );
    assert_eq!(
        file_info.attribute_as_string("xattr::key2"),
        expected.attribute_as_string("xattr::key2")
    );
}

#[test]
fn file_query_filesystem_info() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::query_filesystem_info`
    let my_custom_file =
        MyCustomFile::with_xattr("/my_file", vec!["xattr::key1=value1", "xattr::key2=value2"]);
    let res = my_custom_file.query_filesystem_info("*", Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let file_info = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::query_filesystem_info`
    let my_file = MyFile::with_xattr("/my_file", vec!["xattr::key1=value1", "xattr::key2=value2"]);
    let res = my_file.query_filesystem_info("*", Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both results should equal
    assert_eq!(file_info.name(), expected.name());
    assert_eq!(
        file_info.attribute_as_string("xattr::key1"),
        expected.attribute_as_string("xattr::key1")
    );
    assert_eq!(
        file_info.attribute_as_string("xattr::key2"),
        expected.attribute_as_string("xattr::key2")
    );
}

#[test]
fn file_find_enclosing_mount() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::find_enclosing_mount`
    let my_custom_dir = MyCustomFile::new("/my_directory");
    let res = my_custom_dir.find_enclosing_mount(Cancellable::NONE);
    assert!(res.is_err(), "unexpected mount {:?}", res.ok().unwrap());
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::find_enclosing_mount`
    let my_dir = MyFile::new("/my_directory");
    let res = my_dir.find_enclosing_mount(Cancellable::NONE);
    assert!(res.is_err(), "unexpected mount {:?}", res.ok().unwrap());
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_set_display_name() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::set_display_name`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.set_display_name("my_file_new_name", Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let renamed = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::set_display_name`
    let my_file = MyFile::new("/my_file");
    let res = my_file.set_display_name("my_file_new_name", Cancellable::NONE);
    let expected = res.unwrap();

    // both new paths should equal
    assert_eq!(renamed.path(), expected.path());
}

#[test]
fn file_query_settable_attributes() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::query_settable_attributes`
    let my_custom_file =
        MyCustomFile::with_xattr("/my_file", vec!["xattr::key1=value1", "xattr::key2=value2"]);
    let res = my_custom_file.query_settable_attributes(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let file_attribute_infos = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::query_settable_attributes`
    let my_file = MyFile::with_xattr("/my_file", vec!["xattr::key1=value1", "xattr::key2=value2"]);
    let res = my_file.query_settable_attributes(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both file attribute infos should equal
    assert_eq!(
        file_attribute_infos
            .attributes()
            .iter()
            .map(|attr| attr.name().to_owned())
            .collect::<Vec<_>>(),
        expected
            .attributes()
            .iter()
            .map(|attr| attr.name().to_owned())
            .collect::<Vec<_>>()
    );
}

#[test]
fn file_query_writable_namespaces() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::query_writable_namespaces`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.query_writable_namespaces(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let file_attribute_infos = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::query_writable_namespaces`
    let my_file = MyFile::new("/my_file");
    let res = my_file.query_writable_namespaces(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both file attribute infos should equal
    assert_eq!(
        file_attribute_infos
            .attributes()
            .iter()
            .map(|attr| attr.name().to_owned())
            .collect::<Vec<_>>(),
        expected
            .attributes()
            .iter()
            .map(|attr| attr.name().to_owned())
            .collect::<Vec<_>>()
    );
}

#[test]
fn file_set_attribute() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::set_attribute`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.set_attribute(
        "xattr::key1",
        "value1",
        FileQueryInfoFlags::NONE,
        Cancellable::NONE,
    );
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let res = my_custom_file.query_info("xattr::key1", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let file_info = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::set_attribute`
    let my_file = MyFile::new("/my_file");
    let res = my_file.set_attribute(
        "xattr::key1",
        "value1",
        FileQueryInfoFlags::NONE,
        Cancellable::NONE,
    );
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let res = my_file.query_info("xattr::key1", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both file attributes should equal
    assert_eq!(
        file_info.attribute_as_string("xattr::key1"),
        expected.attribute_as_string("xattr::key1")
    );
}

#[test]
fn file_set_attributes_from_info() {
    let file_info = FileInfo::new();
    file_info.set_attribute_string("xattr::key1", "value1");

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::set_attributes_from_info`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.set_attributes_from_info(
        &file_info,
        FileQueryInfoFlags::NONE,
        Cancellable::NONE,
    );
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let res = my_custom_file.query_info("xattr::key1", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let file_info = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::set_attributes_from_info`
    let my_file = MyFile::new("/my_file");
    let res =
        my_file.set_attributes_from_info(&file_info, FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let res = my_file.query_info("xattr::key1", FileQueryInfoFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both file attributes should equal
    assert_eq!(
        file_info.attribute_as_string("xattr::key1"),
        expected.attribute_as_string("xattr::key1")
    );
}

#[test]
fn file_read_fn() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::read_fn`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.read(Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file input stream {:?}",
        res.ok().unwrap()
    );
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::read_fn`
    let my_file = MyFile::new("/my_file");
    let res = my_file.read(Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file input stream {:?}",
        res.ok().unwrap()
    );
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_append_to() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::append_to`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.append_to(FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file output stream {:?}",
        res.ok().unwrap()
    );
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::append_to`
    let my_file = MyFile::new("/my_file");
    let res = my_file.append_to(FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file output stream {:?}",
        res.ok().unwrap()
    );
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_create() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::create`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.create(FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file output stream {:?}",
        res.ok().unwrap()
    );
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::create`
    let my_file = MyFile::new("/my_file");
    let res = my_file.create(FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file output stream {:?}",
        res.ok().unwrap()
    );
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_replace() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::replace`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.replace(None, false, FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file output stream {:?}",
        res.ok().unwrap()
    );
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::replace`
    let my_file = MyFile::new("/my_file");
    let res = my_file.replace(None, false, FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file output stream {:?}",
        res.ok().unwrap()
    );
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_delete() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::delete`
    let my_custom_file =
        MyCustomFile::with_type_state("/my_file", FileType::Unknown, MyFileState::Exist);
    let res = my_custom_file.delete(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::delete`
    let my_file = MyFile::with_type_state("/my_file", FileType::Unknown, MyFileState::Exist);
    let res = my_file.delete(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::delete`
    let res = my_custom_file.delete(Cancellable::NONE);
    assert!(res.is_err(), "unexpected deleted file");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::delete`
    let res = my_custom_file.delete(Cancellable::NONE);
    assert!(res.is_err(), "unexpected deleted file");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_trash() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::trash`
    let my_custom_file =
        MyCustomFile::with_type_state("/my_file", FileType::Unknown, MyFileState::Exist);
    let res = my_custom_file.trash(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::trash`
    let my_file = MyFile::with_type_state("/my_file", FileType::Unknown, MyFileState::Exist);
    let res = my_file.trash(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::trash`
    let res = my_custom_file.trash(Cancellable::NONE);
    assert!(res.is_err(), "unexpected trashed file");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::trash`
    let res = my_custom_file.trash(Cancellable::NONE);
    assert!(res.is_err(), "unexpected trashed file");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_make_directory() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::make_directory`
    let my_custom_dir = MyCustomFile::new("/my_directory");
    let res = my_custom_dir.make_directory(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::make_directory`
    let my_dir = MyFile::new("/my_directory");
    let res = my_dir.make_directory(Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::make_directory`
    let res = my_custom_dir.make_directory(Cancellable::NONE);
    assert!(res.is_err(), "unexpected created directory");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::make_directory`
    let res = my_custom_dir.make_directory(Cancellable::NONE);
    assert!(res.is_err(), "unexpected created directory");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_make_symbolic_link() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::make_symbolic_link`
    let my_custom_symlink = MyCustomFile::new("/my_symbolic_link");
    let res = my_custom_symlink.make_symbolic_link("/my_target", Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::make_symbolic_link`
    let my_symlink = MyFile::new("/my_symbolic_link");
    let res = my_symlink.make_symbolic_link("/my_target", Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::make_symbolic_link`
    let res = my_custom_symlink.make_symbolic_link("/my_target", Cancellable::NONE);
    assert!(res.is_err(), "unexpected created directory");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::make_symbolic_link`
    let res = my_custom_symlink.make_symbolic_link("/my_target", Cancellable::NONE);
    assert!(res.is_err(), "unexpected created directory");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_copy() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::copy`
    let my_custom_source_file = MyCustomFile::new("/my_file1");
    let my_custom_destination_file = MyCustomFile::new("/my_file2");
    let res = my_custom_source_file.copy(
        &my_custom_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::copy`
    let my_source_file = MyFile::new("/my_file1");
    let my_destination_file = MyFile::new("/my_file2");
    let res = my_source_file.copy(
        &my_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::NotFound));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::copy`
    let my_custom_source_file =
        MyCustomFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_custom_destination_file =
        MyCustomFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_custom_source_file.copy(
        &my_custom_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::copy`
    let my_source_file =
        MyFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_destination_file =
        MyFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_source_file.copy(
        &my_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::Exists));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::copy`
    let my_custom_source_file =
        MyCustomFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_custom_destination_directory =
        MyCustomFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_custom_source_file.copy(
        &my_custom_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::copy`
    let my_source_file =
        MyFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_destination_directory =
        MyFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_source_file.copy(
        &my_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::IsDirectory));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::copy`
    let my_custom_source_directory =
        MyCustomFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_custom_destination_directory =
        MyCustomFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_custom_source_directory.copy(
        &my_custom_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::copy`
    let my_source_directory =
        MyFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_destination_directory =
        MyFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_source_directory.copy(
        &my_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::WouldMerge));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::copy`
    let my_custom_source_directory =
        MyCustomFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_custom_destination_directory =
        MyCustomFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::DoesNotExist);
    let res = my_custom_source_directory.copy(
        &my_custom_destination_directory,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::copy`
    let my_source_directory =
        MyFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_destination_directory =
        MyFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::DoesNotExist);
    let res = my_source_directory.copy(
        &my_destination_directory,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::WouldRecurse));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::copy`
    let my_custom_source_directory =
        MyCustomFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_custom_destination_file =
        MyCustomFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_custom_source_directory.copy(
        &my_custom_destination_file,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::copy`
    let my_source_directory =
        MyFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_destination_file =
        MyFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_source_directory.copy(
        &my_destination_file,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected copy");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::WouldRecurse));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::copy`
    let my_custom_source_file =
        MyCustomFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_custom_destination_file =
        MyCustomFile::with_type_state("/my_file2", FileType::Regular, MyFileState::DoesNotExist);
    let (mut copied, mut total) = (0, 0);
    let res = my_custom_source_file.copy(
        &my_custom_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        Some(&mut move |current_num_bytes, total_num_bytes| {
            assert!(current_num_bytes >= copied);
            assert!(total_num_bytes >= total);
            copied = current_num_bytes;
            total = total_num_bytes;
        }),
    );
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::copy`
    let my_source_file =
        MyFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_destination_file =
        MyFile::with_type_state("/my_file2", FileType::Regular, MyFileState::DoesNotExist);
    let (mut copied, mut total) = (0, 0);
    let expected = my_source_file.copy(
        &my_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        Some(&mut move |current_num_bytes, total_num_bytes| {
            assert!(current_num_bytes >= copied);
            assert!(total_num_bytes >= total);
            copied = current_num_bytes;
            total = total_num_bytes;
        }),
    );
    assert!(expected.is_ok(), "{}", expected.unwrap_err());

    // both results should equal
    assert_eq!(res, expected);
}

#[test]
fn file_move_() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::move_`
    let my_custom_source_file = MyCustomFile::new("/my_file1");
    let my_custom_destination_file = MyCustomFile::new("/my_file2");
    let res = my_custom_source_file.move_(
        &my_custom_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::move_`
    let my_source_file = MyFile::new("/my_file1");
    let my_destination_file = MyFile::new("/my_file2");
    let res = my_source_file.move_(
        &my_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::NotFound));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::move_`
    let my_custom_source_file =
        MyCustomFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_custom_destination_file =
        MyCustomFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_custom_source_file.move_(
        &my_custom_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::move_`
    let my_source_file =
        MyFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_destination_file =
        MyFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_source_file.move_(
        &my_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::Exists));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::move_`
    let my_custom_source_file =
        MyCustomFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_custom_destination_directory =
        MyCustomFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_custom_source_file.move_(
        &my_custom_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::move_`
    let my_source_file =
        MyFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_destination_directory =
        MyFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_source_file.move_(
        &my_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::IsDirectory));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::move_`
    let my_custom_source_directory =
        MyCustomFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_custom_destination_directory =
        MyCustomFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_custom_source_directory.move_(
        &my_custom_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::move_`
    let my_source_directory =
        MyFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_destination_directory =
        MyFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::Exist);
    let res = my_source_directory.move_(
        &my_destination_directory,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::WouldMerge));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::move_`
    let my_custom_source_directory =
        MyCustomFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_custom_destination_directory =
        MyCustomFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::DoesNotExist);
    let res = my_custom_source_directory.move_(
        &my_custom_destination_directory,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::move_`
    let my_source_directory =
        MyFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_destination_directory =
        MyFile::with_type_state("/my_dir2", FileType::Directory, MyFileState::DoesNotExist);
    let res = my_source_directory.move_(
        &my_destination_directory,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::WouldRecurse));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::move_`
    let my_custom_source_directory =
        MyCustomFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_custom_destination_file =
        MyCustomFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_custom_source_directory.move_(
        &my_custom_destination_file,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::move_`
    let my_source_directory =
        MyFile::with_type_state("/my_dir1", FileType::Directory, MyFileState::Exist);
    let my_destination_file =
        MyFile::with_type_state("/my_file2", FileType::Regular, MyFileState::Exist);
    let res = my_source_directory.move_(
        &my_destination_file,
        FileCopyFlags::OVERWRITE,
        Cancellable::NONE,
        None,
    );
    assert!(res.is_err(), "unexpected move_");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    assert_eq!(err.kind::<IOErrorEnum>(), Some(IOErrorEnum::WouldRecurse));

    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::move_`
    let my_custom_source_file =
        MyCustomFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_custom_destination_file =
        MyCustomFile::with_type_state("/my_file2", FileType::Regular, MyFileState::DoesNotExist);
    let (mut copied, mut total) = (0, 0);
    let res = my_custom_source_file.move_(
        &my_custom_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        Some(&mut move |current_num_bytes, total_num_bytes| {
            assert!(current_num_bytes >= copied);
            assert!(total_num_bytes >= total);
            copied = current_num_bytes;
            total = total_num_bytes;
        }),
    );
    assert!(res.is_ok(), "{}", res.unwrap_err());

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::move_`
    let my_source_file =
        MyFile::with_type_state("/my_file1", FileType::Regular, MyFileState::Exist);
    let my_destination_file =
        MyFile::with_type_state("/my_file2", FileType::Regular, MyFileState::DoesNotExist);
    let (mut copied, mut total) = (0, 0);
    let expected = my_source_file.move_(
        &my_destination_file,
        FileCopyFlags::NONE,
        Cancellable::NONE,
        Some(&mut move |current_num_bytes, total_num_bytes| {
            assert!(current_num_bytes >= copied);
            assert!(total_num_bytes >= total);
            copied = current_num_bytes;
            total = total_num_bytes;
        }),
    );
    assert!(expected.is_ok(), "{}", expected.unwrap_err());

    // both results should equal
    assert_eq!(res, expected);
}

#[test]
fn file_mount_mountable() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::mount_mountable`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_custom_path.mount_mountable(
                MountMountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file mount mountable success");
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::mount_mountable`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_path.mount_mountable(
                MountMountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file mount mountable success");
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

// checker-ignore-item
#[test]
fn file_unmount_mountable() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // implement the deprecated function unmount_mountable which is useful for this test
        fn unmount_mountable<T: IsA<File>, P: FnOnce(Result<(), glib::Error>) + 'static>(
            file: &T,
            flags: MountUnmountFlags,
            cancellable: Option<&Cancellable>,
            callback: P,
        ) {
            use glib::translate::{IntoGlib, from_glib_full};
            use std::boxed::Box as Box_;
            let main_context = glib::MainContext::ref_thread_default();
            let is_main_context_owner = main_context.is_owner();
            let has_acquired_main_context = (!is_main_context_owner)
                .then(|| main_context.acquire().ok())
                .flatten();
            assert!(
                is_main_context_owner || has_acquired_main_context.is_some(),
                "Async operations only allowed if the thread is owning the MainContext"
            );

            let user_data: Box_<glib::thread_guard::ThreadGuard<P>> =
                Box_::new(glib::thread_guard::ThreadGuard::new(callback));
            unsafe extern "C" fn unmount_mountable_trampoline<
                P: FnOnce(Result<(), glib::Error>) + 'static,
            >(
                _source_object: *mut glib::gobject_ffi::GObject,
                res: *mut crate::ffi::GAsyncResult,
                user_data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let mut error = std::ptr::null_mut();
                    crate::ffi::g_file_unmount_mountable_finish(
                        _source_object as *mut _,
                        res,
                        &mut error,
                    );
                    let result = if error.is_null() {
                        Ok(())
                    } else {
                        Err(from_glib_full(error))
                    };
                    let callback: Box_<glib::thread_guard::ThreadGuard<P>> =
                        Box_::from_raw(user_data as *mut _);
                    let callback: P = callback.into_inner();
                    callback(result);
                }
            }
            let callback = unmount_mountable_trampoline::<P>;
            unsafe {
                crate::ffi::g_file_unmount_mountable(
                    file.as_ref().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    Some(callback),
                    Box_::into_raw(user_data) as *mut _,
                );
            }
        }

        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::unmount_mountable`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            unmount_mountable(
                &my_custom_path,
                MountUnmountFlags::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file unmount mountable success");
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::unmount_mountable`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            unmount_mountable(
                &my_path,
                MountUnmountFlags::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file unmount mountable success");
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

// checker-ignore-item
#[test]
fn file_eject_mountable() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // implement the deprecated function eject_mountable which is useful for this test
        fn eject_mountable<T: IsA<File>, P: FnOnce(Result<(), glib::Error>) + 'static>(
            file: &T,
            flags: MountUnmountFlags,
            cancellable: Option<&Cancellable>,
            callback: P,
        ) {
            use glib::translate::{IntoGlib, from_glib_full};
            use std::boxed::Box as Box_;
            let main_context = glib::MainContext::ref_thread_default();
            let is_main_context_owner = main_context.is_owner();
            let has_acquired_main_context = (!is_main_context_owner)
                .then(|| main_context.acquire().ok())
                .flatten();
            assert!(
                is_main_context_owner || has_acquired_main_context.is_some(),
                "Async operations only allowed if the thread is owning the MainContext"
            );

            let user_data: Box_<glib::thread_guard::ThreadGuard<P>> =
                Box_::new(glib::thread_guard::ThreadGuard::new(callback));
            unsafe extern "C" fn eject_mountable_trampoline<
                P: FnOnce(Result<(), glib::Error>) + 'static,
            >(
                _source_object: *mut glib::gobject_ffi::GObject,
                res: *mut crate::ffi::GAsyncResult,
                user_data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let mut error = std::ptr::null_mut();
                    crate::ffi::g_file_eject_mountable_finish(
                        _source_object as *mut _,
                        res,
                        &mut error,
                    );
                    let result = if error.is_null() {
                        Ok(())
                    } else {
                        Err(from_glib_full(error))
                    };
                    let callback: Box_<glib::thread_guard::ThreadGuard<P>> =
                        Box_::from_raw(user_data as *mut _);
                    let callback: P = callback.into_inner();
                    callback(result);
                }
            }
            let callback = eject_mountable_trampoline::<P>;
            unsafe {
                crate::ffi::g_file_eject_mountable(
                    file.as_ref().to_glib_none().0,
                    flags.into_glib(),
                    cancellable.to_glib_none().0,
                    Some(callback),
                    Box_::into_raw(user_data) as *mut _,
                );
            }
        }

        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::eject_mountable`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            eject_mountable(
                &my_custom_path,
                MountUnmountFlags::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file eject mountable success");
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::eject_mountable`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            eject_mountable(
                &my_path,
                MountUnmountFlags::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file eject mountable success");
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

#[test]
fn file_mount_enclosing_volume() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::mount_enclosing_volume`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_custom_path.mount_enclosing_volume(
                MountMountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(
            res.is_err(),
            "unexpected file mount enclosing volume success"
        );
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::mount_enclosing_volume`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_path.mount_enclosing_volume(
                MountMountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(
            res.is_err(),
            "unexpected file mount enclosing volume success"
        );
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

#[test]
fn file_monitor_dir() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::monitor_dir`
    let my_custom_file =
        MyCustomFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);
    let res = my_custom_file.monitor_directory(FileMonitorFlags::NONE, Cancellable::NONE);
    assert!(res.is_err(), "unexpected monitor");
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::monitor_dir`
    let my_file = MyFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);
    let res = my_file.monitor_directory(FileMonitorFlags::NONE, Cancellable::NONE);
    assert!(res.is_err(), "unexpected monitor");
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());

    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::monitor_dir`
        let my_custom_dir =
            MyCustomFile::with_type_state("/my_directory", FileType::Directory, MyFileState::Exist);
        let res = my_custom_dir.monitor_directory(FileMonitorFlags::NONE, Cancellable::NONE);
        assert!(res.is_ok(), "{}", res.unwrap_err());
        let my_custom_dir_monitor = res.unwrap();

        let rx = {
            let (tx, rx) = async_channel::bounded(3);
            my_custom_dir_monitor.connect_changed(move |_, file, other_file, event_type| {
                let res = glib::MainContext::ref_thread_default().block_on(tx.send((
                    file.uri(),
                    other_file.map(File::uri),
                    event_type,
                )));
                assert!(res.is_ok(), "{}", res.err().unwrap());
            });
            rx
        };
        // emit 1st event
        let my_custom_child =
            MyCustomFile::with_type_state("/my_child", FileType::Regular, MyFileState::Exist);
        my_custom_dir_monitor.emit_event(
            &my_custom_child,
            None::<&MyCustomFile>,
            FileMonitorEvent::Created,
        );
        // emit 2nd event
        my_custom_dir_monitor.emit_event(
            &my_custom_child,
            None::<&MyCustomFile>,
            FileMonitorEvent::Changed,
        );
        // emit 3rd event
        my_custom_dir_monitor.emit_event(
            &my_custom_child,
            None::<&MyCustomFile>,
            FileMonitorEvent::ChangesDoneHint,
        );
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let event1 = res.unwrap();
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let event2 = res.unwrap();
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let event3 = res.unwrap();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::monitor_dir`
        let my_dir =
            MyFile::with_type_state("/my_directory", FileType::Directory, MyFileState::Exist);
        let res = my_dir.monitor_directory(FileMonitorFlags::NONE, Cancellable::NONE);
        assert!(res.is_ok(), "{}", res.unwrap_err());
        let my_dir_monitor = res.unwrap();

        let rx = {
            let (tx, rx) = async_channel::bounded(3);
            my_dir_monitor.connect_changed(move |_, file, other_file, event_type| {
                let res = glib::MainContext::ref_thread_default().block_on(tx.send((
                    file.uri(),
                    other_file.map(File::uri),
                    event_type,
                )));
                assert!(res.is_ok(), "{}", res.err().unwrap());
            });
            rx
        };
        // emit an event
        let my_child = MyFile::with_type_state("/my_child", FileType::Regular, MyFileState::Exist);
        my_dir_monitor.emit_event(&my_child, None::<&MyFile>, FileMonitorEvent::Created);
        my_dir_monitor.emit_event(&my_child, None::<&MyFile>, FileMonitorEvent::Changed);
        my_dir_monitor.emit_event(
            &my_child,
            None::<&MyFile>,
            FileMonitorEvent::ChangesDoneHint,
        );
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let expected1 = res.unwrap();
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let expected2 = res.unwrap();
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let expected3 = res.unwrap();

        // both events should equal
        assert_eq!(event1, expected1);
        assert_eq!(event2, expected2);
        assert_eq!(event3, expected3);
    });
}

#[test]
fn file_monitor_file() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::monitor_file`
    let my_custom_dir =
        MyCustomFile::with_type_state("/my_directory", FileType::Directory, MyFileState::Exist);
    let res = my_custom_dir.monitor_file(FileMonitorFlags::NONE, Cancellable::NONE);
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let monitor = res.unwrap();
    assert_ne!(monitor.type_(), MyFileMonitor::static_type());

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::monitor_file`
    let my_dir = MyFile::with_type_state("/my_directory", FileType::Directory, MyFileState::Exist);
    let res = my_dir.monitor_file(FileMonitorFlags::NONE, Cancellable::NONE);
    let expected = res.unwrap();
    assert_ne!(expected.type_(), MyFileMonitor::static_type());

    // both results should equal
    assert_eq!(monitor.type_(), expected.type_());

    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::monitor_file`
        let my_custom_file =
            MyCustomFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);
        let res = my_custom_file.monitor_file(FileMonitorFlags::NONE, Cancellable::NONE);
        assert!(res.is_ok(), "{}", res.unwrap_err());
        let my_custom_file_monitor = res.unwrap();

        let rx = {
            let (tx, rx) = async_channel::bounded(2);
            my_custom_file_monitor.connect_changed(move |_, file, other_file, event_type| {
                let res = glib::MainContext::ref_thread_default().block_on(tx.send((
                    file.uri(),
                    other_file.map(File::uri),
                    event_type,
                )));
                assert!(res.is_ok(), "{}", res.err().unwrap());
            });
            rx
        };
        // emit 1st event
        my_custom_file_monitor.emit_event(
            &my_custom_file,
            None::<&MyCustomFile>,
            FileMonitorEvent::Changed,
        );
        // emit 2nd event
        my_custom_file_monitor.emit_event(
            &my_custom_file,
            None::<&MyCustomFile>,
            FileMonitorEvent::ChangesDoneHint,
        );
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let event1 = res.unwrap();
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let event2 = res.unwrap();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::monitor_file`
        let my_file = MyFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);
        let res = my_file.monitor_file(FileMonitorFlags::NONE, Cancellable::NONE);
        assert!(res.is_ok(), "{}", res.unwrap_err());
        let my_file_monitor = res.unwrap();

        let rx = {
            let (tx, rx) = async_channel::bounded(2);
            my_file_monitor.connect_changed(move |_, file, other_file, event_type| {
                let res = glib::MainContext::ref_thread_default().block_on(tx.send((
                    file.uri(),
                    other_file.map(File::uri),
                    event_type,
                )));
                assert!(res.is_ok(), "{}", res.err().unwrap());
            });
            rx
        };
        // emit an event
        my_file_monitor.emit_event(&my_file, None::<&MyFile>, FileMonitorEvent::Changed);
        my_file_monitor.emit_event(&my_file, None::<&MyFile>, FileMonitorEvent::ChangesDoneHint);
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let expected1 = res.unwrap();
        let res = glib::MainContext::ref_thread_default().block_on(rx.recv());
        assert!(res.is_ok(), "{}", res.err().unwrap());
        let expected2 = res.unwrap();

        // both events should equal
        assert_eq!(event1, expected1);
        assert_eq!(event2, expected2);
    });
}

#[test]
fn file_open_readwrite() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::open_readwrite`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.open_readwrite(Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file open read write success {:?}",
        res.ok().unwrap()
    );
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::open_readwrite`
    let my_file = MyFile::new("/my_file");
    let res = my_file.open_readwrite(Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file open read write success {:?}",
        res.ok().unwrap()
    );
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_create_readwrite() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::create_readwrite`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res = my_custom_file.create_readwrite(FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file create read write success {:?}",
        res.ok().unwrap()
    );
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::create_readwrite`
    let my_file = MyFile::new("/my_file");
    let res = my_file.create_readwrite(FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file create read write success {:?}",
        res.ok().unwrap()
    );
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_replace_readwrite() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::replace_readwrite`
    let my_custom_file = MyCustomFile::new("/my_file");
    let res =
        my_custom_file.replace_readwrite(None, false, FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file replace read write success {:?}",
        res.ok().unwrap()
    );
    let err = res.unwrap_err();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::replace_readwrite`
    let my_file = MyFile::new("/my_file");
    let res = my_file.replace_readwrite(None, false, FileCreateFlags::NONE, Cancellable::NONE);
    assert!(
        res.is_err(),
        "unexpected file replace read write success {:?}",
        res.ok().unwrap()
    );
    let expected = res.unwrap_err();

    // both errors should equal
    assert_eq!(err.message(), expected.message());
    assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
}

#[test]
fn file_start_mountable() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::start_mountable`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_custom_path.start_mountable(
                DriveStartFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file start mountable success");
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::start_mountable`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_path.start_mountable(
                DriveStartFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file start mountable success");
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

#[test]
fn file_stop_mountable() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::stop_mountable`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_custom_path.stop_mountable(
                MountUnmountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file stop mountable success");
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::stop_mountable`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_path.stop_mountable(
                MountUnmountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file stop mountable success");
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

#[test]
fn file_unmount_mountable_with_operation() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::unmount_mountable_with_operation`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_custom_path.unmount_mountable_with_operation(
                MountUnmountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(
            res.is_err(),
            "unexpected file unmount mountable with operation success"
        );
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::unmount_mountable_with_operation`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_path.unmount_mountable_with_operation(
                MountUnmountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(
            res.is_err(),
            "unexpected file unmount mountable with operation success"
        );
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

#[test]
fn file_eject_mountable_with_operation() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::eject_mountable_with_operation`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_custom_path.eject_mountable_with_operation(
                MountUnmountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(
            res.is_err(),
            "unexpected file eject mountable with operation success"
        );
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::eject_mountable_with_operation`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_path.eject_mountable_with_operation(
                MountUnmountFlags::NONE,
                MountOperation::NONE,
                Cancellable::NONE,
                move |res| tx.send(res).unwrap(),
            );
            rx.await.unwrap()
        });
        assert!(
            res.is_err(),
            "unexpected file eject mountable with operation success"
        );
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

#[test]
fn file_poll_mountable() {
    // run test in a main context dedicated and configured as the thread default one
    let _ = glib::MainContext::new().with_thread_default(|| {
        // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::poll_mountable`
        let my_custom_path = MyCustomFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_custom_path.poll_mountable(Cancellable::NONE, move |res| tx.send(res).unwrap());
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file poll mountable success");
        let err = res.unwrap_err();

        // invoke `MyFile` implementation of `crate::ffi::GFileIface::poll_mountable`
        let my_path = MyFile::new("/my_path");

        let (tx, rx) = oneshot::channel();
        let res = glib::MainContext::ref_thread_default().block_on(async {
            my_path.poll_mountable(Cancellable::NONE, move |res| tx.send(res).unwrap());
            rx.await.unwrap()
        });
        assert!(res.is_err(), "unexpected file poll mountable success");
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.message(), expected.message());
        assert_eq!(err.kind::<IOErrorEnum>(), expected.kind::<IOErrorEnum>());
    });
}

#[test]
fn file_measure_disk_usage() {
    // invoke `MyCustomFile` implementation of `crate::ffi::GFileIface::measure_disk_usage`
    let my_custom_file =
        MyCustomFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);

    let (mut completed, mut size, mut nb_dirs, mut nb_files) = (false, 0u64, 0u64, 0u64);
    let res = my_custom_file.measure_disk_usage(
        FileMeasureFlags::NONE,
        Cancellable::NONE,
        Some(
            &mut move |reporting: bool, current_size, num_dirs, num_files| {
                if reporting {
                    assert!(!completed);
                }
                assert!(current_size >= size);
                assert!(num_dirs >= nb_dirs);
                assert!(num_files >= nb_files);
                completed = !reporting;
                size = current_size;
                nb_dirs = num_dirs;
                nb_files = num_files;
            },
        ),
    );
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let result = res.unwrap();

    // invoke `MyFile` implementation of `crate::ffi::GFileIface::measure_disk_usage`
    let my_file = MyFile::with_type_state("/my_file", FileType::Regular, MyFileState::Exist);
    let (mut completed, mut size, mut nb_dirs, mut nb_files) = (false, 0u64, 0u64, 0u64);
    let res = my_file.measure_disk_usage(
        FileMeasureFlags::NONE,
        Cancellable::NONE,
        Some(
            &mut move |reporting: bool, current_size, num_dirs, num_files| {
                if reporting {
                    assert!(!completed);
                }
                assert!(current_size >= size);
                assert!(num_dirs >= nb_dirs);
                assert!(num_files >= nb_files);
                completed = !reporting;
                size = current_size;
                nb_dirs = num_dirs;
                nb_files = num_files;
            },
        ),
    );
    assert!(res.is_ok(), "{}", res.unwrap_err());
    let expected = res.unwrap();

    // both results should equal
    assert_eq!(result, expected);
}
