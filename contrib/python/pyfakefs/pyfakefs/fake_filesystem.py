# Copyright 2009 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A fake filesystem implementation for unit testing.

:Usage:

>>> from pyfakefs import fake_filesystem, fake_os
>>> filesystem = fake_filesystem.FakeFilesystem()
>>> os_module = fake_os.FakeOsModule(filesystem)
>>> pathname = '/a/new/dir/new-file'

Create a new file object, creating parent directory objects as needed:

>>> os_module.path.exists(pathname)
False
>>> new_file = filesystem.create_file(pathname)

File objects can't be overwritten:

>>> os_module.path.exists(pathname)
True
>>> try:
...   filesystem.create_file(pathname)
... except OSError as e:
...   assert e.errno == errno.EEXIST, 'unexpected errno: %d' % e.errno
...   assert e.strerror == 'File exists in the fake filesystem'

Remove a file object:

>>> filesystem.remove_object(pathname)
>>> os_module.path.exists(pathname)
False

Create a new file object at the previous path:

>>> beatles_file = filesystem.create_file(pathname,
...     contents='Dear Prudence\\nWon\\'t you come out to play?\\n')
>>> os_module.path.exists(pathname)
True

Use the FakeFileOpen class to read fake file objects:

>>> file_module = fake_filesystem.FakeFileOpen(filesystem)
>>> for line in file_module(pathname):
...     print(line.rstrip())
...
Dear Prudence
Won't you come out to play?

File objects cannot be treated like directory objects:

>>> try:
...   os_module.listdir(pathname)
... except OSError as e:
...   assert e.errno == errno.ENOTDIR, 'unexpected errno: %d' % e.errno
...   assert e.strerror == 'Not a directory in the fake filesystem'

The FakeOsModule can list fake directory objects:

>>> os_module.listdir(os_module.path.dirname(pathname))
['new-file']

The FakeOsModule also supports stat operations:

>>> import stat
>>> stat.S_ISREG(os_module.stat(pathname).st_mode)
True
>>> stat.S_ISDIR(os_module.stat(os_module.path.dirname(pathname)).st_mode)
True
"""

import contextlib
import dataclasses
import errno
import heapq
import os
import random
import sys
import tempfile
from collections import namedtuple, OrderedDict
from doctest import TestResults
from enum import Enum
from stat import (
    S_IFREG,
    S_IFDIR,
    S_ISLNK,
    S_IFMT,
    S_ISDIR,
    S_IFLNK,
    S_ISREG,
)
from typing import (
    List,
    Callable,
    Union,
    Any,
    Dict,
    Tuple,
    cast,
    AnyStr,
    overload,
    NoReturn,
    Optional,
)

from pyfakefs import fake_file, fake_path, fake_io, fake_os, helpers, fake_open
from pyfakefs.fake_file import AnyFileWrapper, AnyFile
from pyfakefs.helpers import (
    is_int_type,
    make_string_path,
    to_string,
    matching_string,
    AnyPath,
    AnyString,
    WINDOWS_PROPERTIES,
    POSIX_PROPERTIES,
    FSType,
)

if sys.platform.startswith("linux"):
    # on newer Linux system, the default maximum recursion depth is 40
    # we ignore older systems here
    _MAX_LINK_DEPTH = 40
else:
    # on MacOS and Windows, the maximum recursion depth is 32
    _MAX_LINK_DEPTH = 32


class OSType(Enum):
    """Defines the real or simulated OS of the underlying file system."""

    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"


# definitions for backwards compatibility
FakeFile = fake_file.FakeFile
FakeNullFile = fake_file.FakeNullFile
FakeFileFromRealFile = fake_file.FakeFileFromRealFile
FakeDirectory = fake_file.FakeDirectory
FakeDirectoryFromRealDirectory = fake_file.FakeDirectoryFromRealDirectory
FakeFileWrapper = fake_file.FakeFileWrapper
StandardStreamWrapper = fake_file.StandardStreamWrapper
FakeDirWrapper = fake_file.FakeDirWrapper
FakePipeWrapper = fake_file.FakePipeWrapper

FakePathModule = fake_path.FakePathModule
FakeOsModule = fake_os.FakeOsModule
FakeFileOpen = fake_open.FakeFileOpen
FakeIoModule = fake_io.FakeIoModule
if sys.platform != "win32":
    FakeFcntlModule = fake_io.FakeFcntlModule
PatchMode = fake_io.PatchMode

is_root = helpers.is_root
get_uid = helpers.get_uid
set_uid = helpers.set_uid
get_gid = helpers.get_gid
set_gid = helpers.set_gid
reset_ids = helpers.reset_ids

PERM_READ = helpers.PERM_READ
PERM_WRITE = helpers.PERM_WRITE
PERM_EXE = helpers.PERM_EXE
PERM_DEF = helpers.PERM_DEF
PERM_DEF_FILE = helpers.PERM_DEF_FILE
PERM_ALL = helpers.PERM_ALL


class FakeFilesystem:
    """Provides the appearance of a real directory tree for unit testing.

    Attributes:
        is_case_sensitive: `True` if a case-sensitive file system is assumed.
        root: The root :py:class:`FakeDirectory<pyfakefs.fake_file.FakeDirectory>` entry
            of the file system.
        umask: The umask used for newly created files, see `os.umask`.
        patcher: Holds the Patcher object if created from it. Allows access
            to the patcher object if using the pytest fs fixture.
        patch_open_code: Defines how `io.open_code` will be patched;
            patching can be on, off, or in automatic mode.
        shuffle_listdir_results: If `True`, `os.listdir` will not sort the
            results to match the real file system behavior.
    """

    def __init__(
        self,
        path_separator: str = os.path.sep,
        total_size: Optional[int] = None,
        patcher: Any = None,
        create_temp_dir: bool = False,
    ) -> None:
        """
        Args:
            path_separator:  optional substitute for os.path.sep
            total_size: if not None, the total size in bytes of the
                root filesystem.
            patcher: the Patcher instance if created from the Patcher
            create_temp_dir: If True, a temp directory is created on initialization.
                Under Posix, if the temp directory is not `/tmp`, a link to the temp
                path is additionally created at `/tmp`.

        Example usage to use the same path separator under all systems:

        >>> filesystem = FakeFilesystem(path_separator='/')

        """
        self.patcher = patcher
        self.create_temp_dir = create_temp_dir

        # is_windows_fs can be used to test the behavior of pyfakefs under
        # Windows fs on non-Windows systems and vice verse;
        # is it used to support drive letters, UNC paths and some other
        # Windows-specific features
        self._is_windows_fs = sys.platform == "win32"

        # can be used to test some MacOS-specific behavior under other systems
        self._is_macos = sys.platform == "darwin"

        # is_case_sensitive can be used to test pyfakefs for case-sensitive
        # file systems on non-case-sensitive systems and vice verse
        self.is_case_sensitive: bool = not (self._is_windows_fs or self._is_macos)

        # by default, we use the configured filesystem
        self.fs_type = FSType.DEFAULT
        base_properties = (
            WINDOWS_PROPERTIES if self._is_windows_fs else POSIX_PROPERTIES
        )
        self.fs_properties = [
            dataclasses.replace(base_properties),
            POSIX_PROPERTIES,
            WINDOWS_PROPERTIES,
        ]
        self.path_separator = path_separator

        self.root: FakeDirectory
        self._cwd = ""

        # We can't query the current value without changing it:
        self.umask: int = os.umask(0o22)
        os.umask(self.umask)

        # A list of open file objects. Their position in the list is their
        # file descriptor number
        self.open_files: List[Optional[List[AnyFileWrapper]]] = []
        # A heap containing all free positions in self.open_files list
        self._free_fd_heap: List[int] = []
        # last used numbers for inodes (st_ino) and devices (st_dev)
        self.last_ino: int = 0
        self.last_dev: int = 0
        self.mount_points: Dict[AnyString, Dict] = OrderedDict()
        self.dev_null: Any = None
        self.reset(total_size=total_size, init_pathlib=False)

        # set from outside if needed
        self.patch_open_code = PatchMode.OFF
        self.shuffle_listdir_results = False

    @property
    def is_linux(self) -> bool:
        """Returns `True` in a real or faked Linux file system."""
        return not self.is_windows_fs and not self.is_macos

    @property
    def is_windows_fs(self) -> bool:
        """Returns `True` in a real or faked Windows file system."""
        return self.fs_type == FSType.WINDOWS or (
            self.fs_type == FSType.DEFAULT and self._is_windows_fs
        )

    @is_windows_fs.setter
    def is_windows_fs(self, value: bool) -> None:
        if self._is_windows_fs != value:
            self._is_windows_fs = value
            if value:
                self._is_macos = False
            self.reset()
            FakePathModule.reset(self)

    @property
    def is_macos(self) -> bool:
        """Returns `True` in a real or faked macOS file system."""
        return self._is_macos

    @is_macos.setter
    def is_macos(self, value: bool) -> None:
        if self._is_macos != value:
            self._is_macos = value
            if value:
                self._is_windows_fs = False
            self.reset()
            FakePathModule.reset(self)

    @property
    def path_separator(self) -> str:
        """Returns the path separator, corresponds to `os.path.sep`."""
        return self.fs_properties[self.fs_type.value].sep

    @path_separator.setter
    def path_separator(self, value: str) -> None:
        self.fs_properties[0].sep = value
        if value != os.sep:
            self.alternative_path_separator = None

    @property
    def alternative_path_separator(self) -> Optional[str]:
        """Returns the alternative path separator, corresponds to `os.path.altsep`."""
        return self.fs_properties[self.fs_type.value].altsep

    @alternative_path_separator.setter
    def alternative_path_separator(self, value: Optional[str]) -> None:
        self.fs_properties[0].altsep = value

    @property
    def devnull(self) -> str:
        return self.fs_properties[self.fs_type.value].devnull

    @property
    def pathsep(self) -> str:
        return self.fs_properties[self.fs_type.value].pathsep

    @property
    def line_separator(self) -> str:
        return self.fs_properties[self.fs_type.value].linesep

    @property
    def cwd(self) -> str:
        """Return the current working directory of the fake filesystem."""
        return self._cwd

    @cwd.setter
    def cwd(self, value: str) -> None:
        """Set the current working directory of the fake filesystem.
        Make sure a new drive or share is auto-mounted under Windows.
        """
        _cwd = make_string_path(value)
        self._cwd = _cwd.replace(
            matching_string(_cwd, os.sep), matching_string(_cwd, self.path_separator)
        )
        self._auto_mount_drive_if_needed(value)

    @property
    def root_dir(self) -> FakeDirectory:
        """Return the root directory, which represents "/" under POSIX,
        and the current drive under Windows."""
        if self.is_windows_fs:
            return self._mount_point_dir_for_cwd()
        return self.root

    @property
    def root_dir_name(self) -> str:
        """Return the root directory name, which is "/" under POSIX,
        and the root path of the current drive under Windows."""
        root_dir = to_string(self.root_dir.name)
        if not root_dir.endswith(self.path_separator):
            return root_dir + self.path_separator
        return root_dir

    @property
    def os(self) -> OSType:
        """Return the real or simulated type of operating system."""
        return (
            OSType.WINDOWS
            if self.is_windows_fs
            else OSType.MACOS
            if self.is_macos
            else OSType.LINUX
        )

    @os.setter
    def os(self, value: OSType) -> None:
        """Set the simulated type of operating system underlying the fake
        file system."""
        self._is_windows_fs = value == OSType.WINDOWS
        self._is_macos = value == OSType.MACOS
        self.is_case_sensitive = value == OSType.LINUX
        self.fs_type = FSType.DEFAULT
        base_properties = (
            WINDOWS_PROPERTIES if self._is_windows_fs else POSIX_PROPERTIES
        )
        self.fs_properties[0] = base_properties
        self.reset()
        FakePathModule.reset(self)

    def reset(self, total_size: Optional[int] = None, init_pathlib: bool = True):
        """Remove all file system contents and reset the root."""
        self.root = FakeDirectory(self.path_separator, filesystem=self)

        self.dev_null = FakeNullFile(self)
        self.open_files.clear()
        self._free_fd_heap.clear()
        self.last_ino = 0
        self.last_dev = 0
        self.mount_points.clear()
        self._add_root_mount_point(total_size)
        self._add_standard_streams()
        if self.create_temp_dir:
            self._create_temp_dir()
        if init_pathlib:
            from pyfakefs import fake_pathlib

            fake_pathlib.init_module(self)

    @contextlib.contextmanager
    def use_fs_type(self, fs_type: FSType):
        old_fs_type = self.fs_type
        try:
            self.fs_type = fs_type
            yield
        finally:
            self.fs_type = old_fs_type

    def _add_root_mount_point(self, total_size):
        mount_point = "C:" if self.is_windows_fs else self.path_separator
        self._cwd = mount_point
        if not self.cwd.endswith(self.path_separator):
            self._cwd += self.path_separator
        self.add_mount_point(mount_point, total_size)

    def pause(self) -> None:
        """Pause the patching of the file system modules until :py:meth:`resume` is
        called. After that call, all file system calls are executed in the
        real file system.
        Calling `pause()` twice is silently ignored.
        Only allowed if the file system object was created by a
        `Patcher` object. This is also the case for the pytest `fs` fixture.

        Raises:
            RuntimeError: if the file system was not created by a `Patcher`.
        """
        if self.patcher is None:
            raise RuntimeError(
                "pause() can only be called from a fake file "
                "system object created by a Patcher object"
            )
        self.patcher.pause()

    def resume(self) -> None:
        """Resume the patching of the file system modules if :py:meth:`pause` has
        been called before. After that call, all file system calls are
        executed in the fake file system.
        Does nothing if patching is not paused.
        Raises:
            RuntimeError: if the file system has not been created by `Patcher`.
        """
        if self.patcher is None:
            raise RuntimeError(
                "resume() can only be called from a fake file "
                "system object created by a Patcher object"
            )
        self.patcher.resume()

    def clear_cache(self) -> None:
        """Clear the cache of non-patched modules."""
        if self.patcher:
            self.patcher.clear_cache()

    def raise_os_error(
        self,
        err_no: int,
        filename: Optional[AnyString] = None,
        winerror: Optional[int] = None,
    ) -> NoReturn:
        """Raises OSError.
        The error message is constructed from the given error code and shall
        start with the error string issued in the real system.
        Note: this is not true under Windows if winerror is given - in this
        case a localized message specific to winerror will be shown in the
        real file system.

        Args:
            err_no: A numeric error code from the C variable errno.
            filename: The name of the affected file, if any.
            winerror: Windows only - the specific Windows error code.
        """
        message = os.strerror(err_no) + " in the fake filesystem"
        if winerror is not None and sys.platform == "win32" and self.is_windows_fs:
            raise OSError(err_no, message, filename, winerror)
        raise OSError(err_no, message, filename)

    def get_path_separator(self, path: AnyStr) -> AnyStr:
        """Return the path separator as the same type as path"""
        return matching_string(path, self.path_separator)

    def _alternative_path_separator(self, path: AnyStr) -> Optional[AnyStr]:
        """Return the alternative path separator as the same type as path"""
        return matching_string(path, self.alternative_path_separator)

    def starts_with_sep(self, path: AnyStr) -> bool:
        """Return True if path starts with a path separator."""
        sep = self.get_path_separator(path)
        altsep = self._alternative_path_separator(path)
        return path.startswith(sep) or altsep is not None and path.startswith(altsep)

    def add_mount_point(
        self,
        path: AnyStr,
        total_size: Optional[int] = None,
        can_exist: bool = False,
    ) -> Dict:
        """Add a new mount point for a filesystem device.
        The mount point gets a new unique device number.

        Args:
            path: The root path for the new mount path.

            total_size: The new total size of the added filesystem device
                in bytes. Defaults to infinite size.

            can_exist: If `True`, no error is raised if the mount point
                already exists.

        Returns:
            The newly created mount point dict.

        Raises:
            OSError: if trying to mount an existing mount point again,
                and `can_exist` is False.
        """
        path = self.normpath(self.normcase(path))
        for mount_point in self.mount_points:
            if (
                self.is_case_sensitive
                and path == matching_string(path, mount_point)
                or not self.is_case_sensitive
                and path.lower() == matching_string(path, mount_point.lower())
            ):
                if can_exist:
                    return self.mount_points[mount_point]
                self.raise_os_error(errno.EEXIST, path)

        self.last_dev += 1
        self.mount_points[path] = {
            "idev": self.last_dev,
            "total_size": total_size,
            "used_size": 0,
        }
        if path == matching_string(path, self.root.name):
            # special handling for root path: has been created before
            root_dir = self.root
            self.last_ino += 1
            root_dir.st_ino = self.last_ino
        else:
            root_dir = self._create_mount_point_dir(path)
        root_dir.st_dev = self.last_dev
        return self.mount_points[path]

    def _create_mount_point_dir(self, directory_path: AnyPath) -> FakeDirectory:
        """A version of `create_dir` for the mount point directory creation,
        which avoids circular calls and unneeded checks.
        """
        dir_path = self.make_string_path(directory_path)
        path_components = self._path_components(dir_path)
        current_dir = self.root

        new_dirs = []
        for component in [to_string(p) for p in path_components]:
            directory = self._directory_content(current_dir, to_string(component))[1]
            if not directory:
                new_dir = FakeDirectory(component, filesystem=self)
                new_dirs.append(new_dir)
                current_dir.add_entry(new_dir)
                current_dir = new_dir
            else:
                current_dir = cast(FakeDirectory, directory)

        for new_dir in new_dirs:
            new_dir.st_mode = S_IFDIR | helpers.PERM_DEF

        return current_dir

    def _auto_mount_drive_if_needed(self, path: AnyStr) -> Optional[Dict]:
        """Windows only: if `path` is located on an unmounted drive or UNC
        mount point, the drive/mount point is added to the mount points."""
        if self.is_windows_fs:
            drive = self.splitdrive(path)[0]
            if drive:
                return self.add_mount_point(path=drive, can_exist=True)
        return None

    def _mount_point_for_path(self, path: AnyStr) -> Dict:
        path = self.absnormpath(self._original_path(path))
        for mount_path in self.mount_points:
            if path == matching_string(path, mount_path):
                return self.mount_points[mount_path]
        mount_path = matching_string(path, "")
        drive = self.splitdrive(path)[0]
        for root_path in self.mount_points:
            root_path = matching_string(path, root_path)
            if drive and not root_path.startswith(drive):
                continue
            if path.startswith(root_path) and len(root_path) > len(mount_path):
                mount_path = root_path
        if mount_path:
            return self.mount_points[to_string(mount_path)]
        mount_point = self._auto_mount_drive_if_needed(path)
        assert mount_point
        return mount_point

    def _mount_point_dir_for_cwd(self) -> FakeDirectory:
        """Return the fake directory object of the mount point where the
        current working directory points to."""

        def object_from_path(file_path) -> FakeDirectory:
            path_components = self._path_components(file_path)
            target = self.root
            for component in path_components:
                target = cast(FakeDirectory, target.get_entry(component))
            return target

        path = to_string(self.cwd)
        for mount_path in self.mount_points:
            if path == to_string(mount_path):
                return object_from_path(mount_path)
        mount_path = ""
        drive = to_string(self.splitdrive(path)[0])
        for root_path in self.mount_points:
            str_root_path = to_string(root_path)
            if drive and not str_root_path.startswith(drive):
                continue
            if path.startswith(str_root_path) and len(str_root_path) > len(mount_path):
                mount_path = root_path
        return object_from_path(mount_path)

    def _mount_point_for_device(self, idev: int) -> Optional[Dict]:
        for mount_point in self.mount_points.values():
            if mount_point["idev"] == idev:
                return mount_point
        return None

    def get_disk_usage(self, path: Optional[AnyStr] = None) -> Tuple[int, int, int]:
        """Return the total, used and free disk space in bytes as named tuple,
        or placeholder values simulating unlimited space if not set.

        .. note:: This matches the return value of ``shutil.disk_usage()``.

        Args:
            path: The disk space is returned for the file system device where
                `path` resides.
                Defaults to the root path (e.g. '/' on Unix systems).
        """
        DiskUsage = namedtuple("DiskUsage", "total, used, free")
        if path is None:
            mount_point = next(iter(self.mount_points.values()))
        else:
            file_path = make_string_path(path)
            mount_point = self._mount_point_for_path(file_path)
        if mount_point and mount_point["total_size"] is not None:
            return DiskUsage(
                mount_point["total_size"],
                mount_point["used_size"],
                mount_point["total_size"] - mount_point["used_size"],
            )
        return DiskUsage(1024 * 1024 * 1024 * 1024, 0, 1024 * 1024 * 1024 * 1024)

    def set_disk_usage(self, total_size: int, path: Optional[AnyStr] = None) -> None:
        """Changes the total size of the file system, preserving the
        used space.
        Example usage: set the size of an auto-mounted Windows drive.

        Args:
            total_size: The new total size of the filesystem in bytes.

            path: The disk space is changed for the file system device where
                `path` resides.
                Defaults to the root path (e.g. '/' on Unix systems).

        Raises:
            OSError: if the new space is smaller than the used size.
        """
        file_path: AnyStr = (
            path if path is not None else self.root_dir_name  # type: ignore
        )
        mount_point = self._mount_point_for_path(file_path)
        if (
            mount_point["total_size"] is not None
            and mount_point["used_size"] > total_size
        ):
            self.raise_os_error(errno.ENOSPC, path)
        mount_point["total_size"] = total_size

    def change_disk_usage(
        self, usage_change: int, file_path: AnyStr, st_dev: int
    ) -> None:
        """Change the used disk space by the given amount.

        Args:
            usage_change: Number of bytes added to the used space.
                If negative, the used space will be decreased.

            file_path: The path of the object needing the disk space.

            st_dev: The device ID for the respective file system.

        Raises:
            OSError: if `usage_change` exceeds the free file system space
        """
        mount_point = self._mount_point_for_device(st_dev)
        if mount_point:
            total_size = mount_point["total_size"]
            if total_size is not None:
                if total_size - mount_point["used_size"] < usage_change:
                    self.raise_os_error(errno.ENOSPC, file_path)
            mount_point["used_size"] += usage_change

    def stat(self, entry_path: AnyStr, follow_symlinks: bool = True):
        """Return the os.stat-like tuple for the FakeFile object of entry_path.

        Args:
            entry_path:  Path to filesystem object to retrieve.
            follow_symlinks: If False and entry_path points to a symlink,
                the link itself is inspected instead of the linked object.

        Returns:
            The FakeStatResult object corresponding to entry_path.

        Raises:
            OSError: if the filesystem object doesn't exist.
        """
        # stat should return the tuple representing return value of os.stat
        try:
            file_object = self.resolve(
                entry_path,
                follow_symlinks,
                allow_fd=True,
                check_read_perm=False,
                check_exe_perm=False,
            )
        except TypeError:
            file_object = self.resolve(entry_path)
        if not is_root():
            # make sure stat raises if a parent dir is not readable
            parent_dir = file_object.parent_dir
            if parent_dir:
                self._get_object(parent_dir.path, check_read_perm=False)  # type: ignore[arg-type]

        self.raise_for_filepath_ending_with_separator(
            entry_path, file_object, follow_symlinks
        )

        return file_object.stat_result.copy()

    def raise_for_filepath_ending_with_separator(
        self,
        entry_path: AnyStr,
        file_object: FakeFile,
        follow_symlinks: bool = True,
        macos_handling: bool = False,
    ) -> None:
        if self.ends_with_path_separator(entry_path):
            if S_ISLNK(file_object.st_mode):
                try:
                    link_object = self.resolve(entry_path)
                except OSError as exc:
                    if self.is_macos and exc.errno != errno.ENOENT:
                        return
                    if self.is_windows_fs:
                        self.raise_os_error(errno.EINVAL, entry_path)
                    raise
                if not follow_symlinks or self.is_windows_fs or self.is_macos:
                    file_object = link_object
            if self.is_windows_fs:
                is_error = S_ISREG(file_object.st_mode)
            elif self.is_macos and macos_handling:
                is_error = not S_ISLNK(file_object.st_mode)
            else:
                is_error = not S_ISDIR(file_object.st_mode)
            if is_error:
                error_nr = errno.EINVAL if self.is_windows_fs else errno.ENOTDIR
                self.raise_os_error(error_nr, entry_path)

    def chmod(
        self,
        path: Union[AnyStr, int],
        mode: int,
        follow_symlinks: bool = True,
        force_unix_mode: bool = False,
    ) -> None:
        """Change the permissions of a file as encoded in integer mode.

        Args:
            path: (str | int) Path to the file or file descriptor.
            mode: (int) Permissions.
            follow_symlinks: If `False` and `path` points to a symlink,
                the link itself is affected instead of the linked object.
            force_unix_mode: if True and run under Windows, the mode is not
                adapted for Windows to allow making dirs unreadable
        """
        allow_fd = not self.is_windows_fs or sys.version_info >= (3, 13)
        file_object = self.resolve(
            path, follow_symlinks, allow_fd=allow_fd, check_owner=True
        )
        if self.is_windows_fs and not force_unix_mode:
            if mode & helpers.PERM_WRITE:
                file_object.st_mode = file_object.st_mode | 0o222
            else:
                file_object.st_mode = file_object.st_mode & 0o777555
        else:
            file_object.st_mode = (file_object.st_mode & ~helpers.PERM_ALL) | (
                mode & helpers.PERM_ALL
            )
        file_object.st_ctime = helpers.now()

    def utime(
        self,
        path: AnyStr,
        times: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        *,
        ns: Optional[Tuple[int, int]] = None,
        follow_symlinks: bool = True,
    ) -> None:
        """Change the access and modified times of a file.

        Args:
            path: (str) Path to the file.
            times: 2-tuple of int or float numbers, of the form (atime, mtime)
                which is used to set the access and modified times in seconds.
                If None, both times are set to the current time.
            ns: 2-tuple of int numbers, of the form (atime, mtime)  which is
                used to set the access and modified times in nanoseconds.
                If `None`, both times are set to the current time.
            follow_symlinks: If `False` and entry_path points to a symlink,
                the link itself is queried instead of the linked object.

            Raises:
                TypeError: If anything other than the expected types is
                    specified in the passed `times` or `ns` tuple,
                    or if the tuple length is not equal to 2.
                ValueError: If both times and ns are specified.
        """
        self._handle_utime_arg_errors(ns, times)

        file_object = self.resolve(path, follow_symlinks, allow_fd=True)
        if times is not None:
            for file_time in times:
                if not isinstance(file_time, (int, float)):
                    raise TypeError("atime and mtime must be numbers")

            file_object.st_atime = times[0]
            file_object.st_mtime = times[1]
        elif ns is not None:
            for file_time in ns:
                if not isinstance(file_time, int):
                    raise TypeError("atime and mtime must be ints")

            file_object.st_atime_ns = ns[0]
            file_object.st_mtime_ns = ns[1]
        else:
            current_time = helpers.now()
            file_object.st_atime = current_time
            file_object.st_mtime = current_time

    @staticmethod
    def _handle_utime_arg_errors(
        ns: Optional[Tuple[int, int]],
        times: Optional[Tuple[Union[int, float], Union[int, float]]],
    ):
        if times is not None and ns is not None:
            raise ValueError(
                "utime: you may specify either 'times' or 'ns' but not both"
            )
        if times is not None and len(times) != 2:
            raise TypeError("utime: 'times' must be either a tuple of two ints or None")
        if ns is not None and len(ns) != 2:
            raise TypeError("utime: 'ns' must be a tuple of two ints")

    def add_open_file(self, file_obj: AnyFileWrapper, new_fd: int = -1) -> int:
        """Add file_obj to the list of open files on the filesystem.
        Used internally to manage open files.

        The position in the open_files array is the file descriptor number.

        Args:
            file_obj: File object to be added to open files list.
            new_fd: The optional new file descriptor.

        Returns:
            File descriptor number for the file object.
        """
        if new_fd >= 0:
            size = len(self.open_files)
            if new_fd < size:
                open_files = self.open_files[new_fd]
                if open_files:
                    for f in open_files:
                        try:
                            f.close()
                        except OSError:
                            pass
                if new_fd in self._free_fd_heap:
                    self._free_fd_heap.remove(new_fd)
                self.open_files[new_fd] = [file_obj]
            else:
                for fd in range(size, new_fd):
                    self.open_files.append([])
                    heapq.heappush(self._free_fd_heap, fd)
                self.open_files.append([file_obj])
            return new_fd

        if self._free_fd_heap:
            open_fd = heapq.heappop(self._free_fd_heap)
            self.open_files[open_fd] = [file_obj]
            return open_fd

        self.open_files.append([file_obj])
        return len(self.open_files) - 1

    def close_open_file(self, file_des: int) -> None:
        """Remove file object with given descriptor from the list
        of open files.

        Sets the entry in open_files to None.

        Args:
            file_des: Descriptor of file object to be removed from
            open files list.
        """
        self.open_files[file_des] = None
        heapq.heappush(self._free_fd_heap, file_des)

    def get_open_file(self, file_des: int) -> AnyFileWrapper:
        """Return an open file.

        Args:
            file_des: File descriptor of the open file.

        Raises:
            OSError: an invalid file descriptor.
            TypeError: filedes is not an integer.

        Returns:
            Open file object.
        """
        try:
            return self.get_open_files(file_des)[0]
        except IndexError:
            self.raise_os_error(errno.EBADF, str(file_des))

    def get_open_files(self, file_des: int) -> List[AnyFileWrapper]:
        """Return the list of open files for a file descriptor.

        Args:
            file_des: File descriptor of the open files.

        Raises:
            OSError: an invalid file descriptor.
            TypeError: filedes is not an integer.

        Returns:
            List of open file objects.
        """
        if not is_int_type(file_des):
            raise TypeError("an integer is required")
        valid = file_des < len(self.open_files)
        if valid:
            return self.open_files[file_des] or []
        self.raise_os_error(errno.EBADF, str(file_des))

    def has_open_file(self, file_object: FakeFile) -> bool:
        """Return True if the given file object is in the list of open files.

        Args:
            file_object: The FakeFile object to be checked.

        Returns:
            `True` if the file is open.
        """
        return file_object in [
            wrappers[0].get_object() for wrappers in self.open_files if wrappers
        ]

    def _normalize_path_sep(self, path: AnyStr) -> AnyStr:
        alt_sep = self._alternative_path_separator(path)
        if alt_sep is not None:
            return path.replace(alt_sep, self.get_path_separator(path))
        return path

    def normcase(self, path: AnyStr) -> AnyStr:
        """Replace all appearances of alternative path separator
        with path separator.

        Do nothing if no alternative separator is set.

        Args:
            path: The path to be normalized.

        Returns:
            The normalized path that will be used internally.
        """
        file_path = make_string_path(path)
        return self._normalize_path_sep(file_path)

    def normpath(self, path: AnyStr) -> AnyStr:
        """Mimic os.path.normpath using the specified path_separator.

        Mimics os.path.normpath using the path_separator that was specified
        for this FakeFilesystem. Normalizes the path, but unlike the method
        absnormpath, does not make it absolute.  Eliminates dot components
        (. and ..) and combines repeated path separators (//).  Initial ..
        components are left in place for relative paths.
        If the result is an empty path, '.' is returned instead.

        This also replaces alternative path separator with path separator.
        That is, it behaves like the real os.path.normpath on Windows if
        initialized with '\\' as path separator and  '/' as alternative
        separator.

        Args:
            path:  (str) The path to normalize.

        Returns:
            (str) A copy of path with empty components and dot components
            removed.
        """
        path_str = self.normcase(path)
        drive, path_str = self.splitdrive(path_str)
        sep = self.get_path_separator(path_str)
        is_absolute_path = path_str.startswith(sep)
        path_components: List[AnyStr] = path_str.split(
            sep
        )  # pytype: disable=invalid-annotation
        collapsed_path_components: List[
            AnyStr
        ] = []  # pytype: disable=invalid-annotation
        dot = matching_string(path_str, ".")
        dotdot = matching_string(path_str, "..")
        for component in path_components:
            if (not component) or (component == dot):
                continue
            if component == dotdot:
                if collapsed_path_components and (
                    collapsed_path_components[-1] != dotdot
                ):
                    # Remove an up-reference: directory/..
                    collapsed_path_components.pop()
                    continue
                elif is_absolute_path:
                    # Ignore leading .. components if starting from the
                    # root directory.
                    continue
            collapsed_path_components.append(component)
        collapsed_path = sep.join(collapsed_path_components)
        if is_absolute_path:
            collapsed_path = sep + collapsed_path
        return drive + collapsed_path or dot

    def _original_path(self, path: AnyStr) -> AnyStr:
        """Return a normalized case version of the given path for
        case-insensitive file systems. For case-sensitive file systems,
        return path unchanged.

        Args:
            path: the file path to be transformed

        Returns:
            A version of path matching the case of existing path elements.
        """

        def components_to_path():
            if len(path_components) > len(normalized_components):
                normalized_components.extend(
                    to_string(p) for p in path_components[len(normalized_components) :]
                )
            sep = self.path_separator
            normalized_path = sep.join(normalized_components)
            if self.starts_with_sep(path) and not self.starts_with_sep(normalized_path):
                normalized_path = sep + normalized_path
            if len(normalized_path) == 2 and self.starts_with_drive_letter(
                normalized_path
            ):
                normalized_path += sep
            return normalized_path

        if self.is_case_sensitive or not path:
            return path
        path = self.replace_windows_root(path)
        path_components = self._path_components(path)
        normalized_components = []
        current_dir = self.root
        for component in path_components:
            if not isinstance(current_dir, FakeDirectory):
                return components_to_path()
            dir_name, directory = self._directory_content(
                current_dir, to_string(component)
            )
            if directory is None or (
                isinstance(directory, FakeDirectory)
                and directory._byte_contents is None
                and directory.st_size == 0
            ):
                return components_to_path()
            current_dir = cast(FakeDirectory, directory)
            normalized_components.append(dir_name)
        return components_to_path()

    def absnormpath(self, path: AnyStr) -> AnyStr:
        """Absolutize and minimalize the given path.

        Forces all relative paths to be absolute, and normalizes the path to
        eliminate dot and empty components.

        Args:
            path:  Path to normalize.

        Returns:
            The normalized path relative to the current working directory,
            or the root directory if path is empty.
        """
        path = self.normcase(path)
        cwd = matching_string(path, self.cwd)
        if not path:
            path = self.get_path_separator(path)
        if path == matching_string(path, "."):
            path = cwd
        elif not self._starts_with_root_path(path):
            # Prefix relative paths with cwd, if cwd is not root.
            root_name = matching_string(path, self.root.name)
            empty = matching_string(path, "")
            path = self.get_path_separator(path).join(
                (cwd != root_name and cwd or empty, path)
            )
        else:
            path = self.replace_windows_root(path)
        return self.normpath(path)

    def splitpath(self, path: AnyStr) -> Tuple[AnyStr, AnyStr]:
        """Mimic os.path.split using the specified path_separator.

        Mimics os.path.split using the path_separator that was specified
        for this FakeFilesystem.

        Args:
            path:  (str) The path to split.

        Returns:
            (str) A duple (pathname, basename) for which pathname does not
            end with a slash, and basename does not contain a slash.
        """
        path = make_string_path(path)
        sep = self.get_path_separator(path)
        alt_sep = self._alternative_path_separator(path)
        seps = sep if alt_sep is None else sep + alt_sep
        drive, path = self.splitdrive(path)
        i = len(path)
        while i and path[i - 1] not in seps:
            i -= 1
        head, tail = path[:i], path[i:]  # now tail has no slashes
        # remove trailing slashes from head, unless it's all slashes
        head = head.rstrip(seps) or head
        return drive + head, tail

    def splitdrive(self, path: AnyStr) -> Tuple[AnyStr, AnyStr]:
        """Splits the path into the drive part and the rest of the path.

        Taken from Windows specific implementation in Python 3.5
        and slightly adapted.

        Args:
            path: the full path to be splitpath.

        Returns:
            A tuple of the drive part and the rest of the path, or of
            an empty string and the full path if drive letters are
            not supported or no drive is present.
        """
        path_str = make_string_path(path)
        if self.is_windows_fs:
            if len(path_str) >= 2:
                norm_str = self.normcase(path_str)
                sep = self.get_path_separator(path_str)
                # UNC path_str handling
                if (norm_str[0:2] == sep * 2) and (norm_str[2:3] != sep):
                    # UNC path_str handling - splits off the mount point
                    # instead of the drive
                    sep_index = norm_str.find(sep, 2)
                    if sep_index == -1:
                        return path_str[:0], path_str
                    sep_index2 = norm_str.find(sep, sep_index + 1)
                    if sep_index2 == sep_index + 1:
                        return path_str[:0], path_str
                    if sep_index2 == -1:
                        sep_index2 = len(path_str)
                    return path_str[:sep_index2], path_str[sep_index2:]
                if path_str[1:2] == matching_string(path_str, ":"):
                    return path_str[:2], path_str[2:]
        return path_str[:0], path_str

    def splitroot(self, path: AnyStr):
        """Split a pathname into drive, root and tail.
        Implementation taken from ntpath and posixpath.
        """
        p = os.fspath(path)
        if isinstance(p, bytes):
            sep = self.path_separator.encode()
            altsep = None
            alternative_path_separator = self.alternative_path_separator
            if alternative_path_separator is not None:
                altsep = alternative_path_separator.encode()
            colon = b":"
            unc_prefix = b"\\\\?\\UNC\\"
            empty = b""
        else:
            sep = self.path_separator
            altsep = self.alternative_path_separator
            colon = ":"
            unc_prefix = "\\\\?\\UNC\\"
            empty = ""
        if self.is_windows_fs:
            normp = p.replace(altsep, sep) if altsep else p
            if normp[:1] == sep:
                if normp[1:2] == sep:
                    # UNC drives, e.g. \\server\share or \\?\UNC\server\share
                    # Device drives, e.g. \\.\device or \\?\device
                    start = 8 if normp[:8].upper() == unc_prefix else 2
                    index = normp.find(sep, start)
                    if index == -1:
                        return p, empty, empty
                    index2 = normp.find(sep, index + 1)
                    if index2 == -1:
                        return p, empty, empty
                    return p[:index2], p[index2 : index2 + 1], p[index2 + 1 :]
                else:
                    # Relative path with root, e.g. \Windows
                    return empty, p[:1], p[1:]
            elif normp[1:2] == colon:
                if normp[2:3] == sep:
                    # Absolute drive-letter path, e.g. X:\Windows
                    return p[:2], p[2:3], p[3:]
                else:
                    # Relative path with drive, e.g. X:Windows
                    return p[:2], empty, p[2:]
            else:
                # Relative path, e.g. Windows
                return empty, empty, p
        else:
            if p[:1] != sep:
                # Relative path, e.g.: 'foo'
                return empty, empty, p
            elif p[1:2] != sep or p[2:3] == sep:
                # Absolute path, e.g.: '/foo', '///foo', '////foo', etc.
                return empty, sep, p[1:]
            else:
                return empty, p[:2], p[2:]

    def _join_paths_with_drive_support(self, *all_paths: AnyStr) -> AnyStr:
        """Taken from Python 3.5 os.path.join() code in ntpath.py
        and slightly adapted"""
        base_path = all_paths[0]
        paths_to_add = all_paths[1:]
        sep = self.get_path_separator(base_path)
        seps = [sep, self._alternative_path_separator(base_path)]
        result_drive, result_path = self.splitdrive(base_path)
        for path in paths_to_add:
            drive_part, path_part = self.splitdrive(path)
            if path_part and path_part[:1] in seps:
                # Second path is absolute
                if drive_part or not result_drive:
                    result_drive = drive_part
                result_path = path_part
                continue
            elif drive_part and drive_part != result_drive:
                if self.is_case_sensitive or drive_part.lower() != result_drive.lower():
                    # Different drives => ignore the first path entirely
                    result_drive = drive_part
                    result_path = path_part
                    continue
                # Same drive in different case
                result_drive = drive_part
            # Second path is relative to the first
            if result_path and result_path[-1:] not in seps:
                result_path = result_path + sep
            result_path = result_path + path_part
        # add separator between UNC and non-absolute path
        colon = matching_string(base_path, ":")
        if (
            result_path
            and result_path[:1] not in seps
            and result_drive
            and result_drive[-1:] != colon
        ):
            return result_drive + sep + result_path
        return result_drive + result_path

    def joinpaths(self, *paths: AnyStr) -> AnyStr:
        """Mimic os.path.join using the specified path_separator.

        Args:
            *paths:  (str) Zero or more paths to join.

        Returns:
            (str) The paths joined by the path separator, starting with
            the last absolute path in paths.
        """
        file_paths = [os.fspath(path) for path in paths]
        if len(file_paths) == 1:
            return paths[0]
        if self.is_windows_fs:
            return self._join_paths_with_drive_support(*file_paths)
        path = file_paths[0]
        sep = self.get_path_separator(file_paths[0])
        for path_segment in file_paths[1:]:
            if path_segment.startswith(sep) or not path:
                # An absolute path
                path = path_segment
            elif path.endswith(sep):
                path += path_segment
            else:
                path += sep + path_segment
        return path

    @overload
    def _path_components(self, path: str) -> List[str]: ...

    @overload
    def _path_components(self, path: bytes) -> List[bytes]: ...

    def _path_components(self, path: AnyStr) -> List[AnyStr]:
        """Breaks the path into a list of component names.

        Does not include the root directory as a component, as all paths
        are considered relative to the root directory for the FakeFilesystem.
        Callers should basically follow this pattern:

        .. code:: python

            file_path = self.absnormpath(file_path)
            path_components = self._path_components(file_path)
            current_dir = self.root
            for component in path_components:
                if component not in current_dir.entries:
                    raise OSError
                _do_stuff_with_component(current_dir, component)
                current_dir = current_dir.get_entry(component)

        Args:
            path:  Path to tokenize.

        Returns:
            The list of names split from path.
        """
        if not path or path == self.get_path_separator(path):
            return []
        drive, path = self.splitdrive(path)
        sep = self.get_path_separator(path)
        # handle special case of Windows emulated under POSIX
        if self.is_windows_fs and sys.platform != "win32":
            path = path.replace(matching_string(sep, "\\"), sep)
        path_components = path.split(sep)
        assert drive or path_components
        if not path_components[0]:
            if len(path_components) > 1 and not path_components[1]:
                path_components = []
            else:
                # This is an absolute path.
                path_components = path_components[1:]
        if drive:
            path_components.insert(0, drive)
        return path_components

    def starts_with_drive_letter(self, file_path: AnyStr) -> bool:
        """Return True if file_path starts with a drive letter.

        Args:
            file_path: the full path to be examined.

        Returns:
            `True` if drive letter support is enabled in the filesystem and
            the path starts with a drive letter.
        """
        colon = matching_string(file_path, ":")
        if len(file_path) >= 2 and file_path[0:1].isalpha() and file_path[1:2] == colon:
            if self.is_windows_fs:
                return True
            if os.name == "nt":
                # special case if we are emulating Posix under Windows
                # check if the path exists because it has been mapped in
                # this is not foolproof, but handles most cases
                try:
                    if len(file_path) == 2:
                        # avoid recursion, check directly in the entries
                        return any(
                            [
                                entry.upper() == file_path.upper()
                                for entry in self.root_dir.entries
                            ]
                        )
                    self.get_object_from_normpath(file_path)
                    return True
                except OSError:
                    return False
        return False

    def _starts_with_root_path(self, file_path: AnyStr) -> bool:
        root_name = matching_string(file_path, self.root.name)
        file_path = self._normalize_path_sep(file_path)
        return (
            file_path.startswith(root_name)
            or not self.is_case_sensitive
            and file_path.lower().startswith(root_name.lower())
            or self.starts_with_drive_letter(file_path)
        )

    def replace_windows_root(self, path: AnyStr) -> AnyStr:
        """In windows, if a path starts with a single separator,
        it points to the root dir of the current mount point, usually a
        drive - replace it with that mount point path to get the real path.
        """
        if path and self.is_windows_fs and self.root_dir:
            sep = self.get_path_separator(path)
            # ignore UNC paths
            if path[0:1] == sep and (len(path) == 1 or path[1:2] != sep):
                # check if we already have a mount point for that path
                for root_path in self.mount_points:
                    root_path = matching_string(path, root_path)
                    if path.startswith(root_path):
                        return path
                # must be a pointer to the current drive - replace it
                mount_point = matching_string(path, self.root_dir_name)
                path = mount_point + path[1:]
        return path

    def _is_root_path(self, file_path: AnyStr) -> bool:
        root_name = matching_string(file_path, self.root.name)
        return file_path == root_name or self.is_mount_point(file_path)

    def is_mount_point(self, file_path: AnyStr) -> bool:
        """Return `True` if `file_path` points to a mount point."""
        for mount_point in self.mount_points:
            mount_point = matching_string(file_path, mount_point)
            if (
                file_path == mount_point
                or not self.is_case_sensitive
                and file_path.lower() == mount_point.lower()
            ):
                return True
            if (
                self.is_windows_fs
                and len(file_path) == 3
                and len(mount_point) == 2
                and self.starts_with_drive_letter(file_path)
                and file_path[:2].lower() == mount_point.lower()
            ):
                return True
        return False

    def ends_with_path_separator(self, path: Union[int, AnyPath]) -> bool:
        """Return True if ``file_path`` ends with a valid path separator."""
        if isinstance(path, int):
            return False
        file_path = make_string_path(path)
        if not file_path:
            return False
        sep = self.get_path_separator(file_path)
        altsep = self._alternative_path_separator(file_path)
        return file_path not in (sep, altsep) and (
            file_path.endswith(sep) or altsep is not None and file_path.endswith(altsep)
        )

    def is_filepath_ending_with_separator(self, path: AnyStr) -> bool:
        if not self.ends_with_path_separator(path):
            return False
        return self.isfile(self._path_without_trailing_separators(path))

    def _directory_content(
        self, directory: FakeDirectory, component: str
    ) -> Tuple[Optional[str], Optional[AnyFile]]:
        if not isinstance(directory, FakeDirectory):
            return None, None
        if component in directory.entries:
            return component, directory.entries[component]
        if not self.is_case_sensitive:
            matching_content = [
                (subdir, directory.entries[subdir])
                for subdir in directory.entries
                if subdir.lower() == component.lower()
            ]
            if matching_content:
                return matching_content[0]

        return None, None

    def exists(self, file_path: AnyPath, check_link: bool = False) -> bool:
        """Return true if a path points to an existing file system object.

        Args:
            file_path:  The path to examine.
            check_link: If True, links are not followed

        Returns:
            (bool) True if the corresponding object exists.

        Raises:
            TypeError: if file_path is None.
        """
        if check_link and self.islink(file_path):
            return True
        path = to_string(self.make_string_path(file_path))
        if path is None:
            raise TypeError
        if not path:
            return False
        if path == self.devnull:
            return not self.is_windows_fs or sys.version_info >= (3, 8)
        try:
            if self.is_filepath_ending_with_separator(path):
                return False
            path = self.resolve_path(path)
        except OSError:
            return False
        if self._is_root_path(path):
            return True

        path_components: List[str] = self._path_components(path)
        current_dir = self.root
        for component in path_components:
            directory = self._directory_content(current_dir, to_string(component))[1]
            if directory is None:
                return False
            current_dir = cast(FakeDirectory, directory)
        return True

    def resolve_path(self, file_path: AnyStr, allow_fd: bool = False) -> AnyStr:
        """Follow a path, resolving symlinks.

        ResolvePath traverses the filesystem along the specified file path,
        resolving file names and symbolic links until all elements of the path
        are exhausted, or we reach a file which does not exist.
        If all the elements are not consumed, they just get appended to the
        path resolved so far.
        This gives us the path which is as resolved as it can be, even if the
        file does not exist.

        This behavior mimics Unix semantics, and is best shown by example.
        Given a file system that looks like this:

              /a/b/
              /a/b/c -> /a/b2          c is a symlink to /a/b2
              /a/b2/x
              /a/c   -> ../d
              /a/x   -> y

         Then:
              /a/b/x      =>  /a/b/x
              /a/c        =>  /a/d
              /a/x        =>  /a/y
              /a/b/c/d/e  =>  /a/b2/d/e

        Args:
            file_path: The path to examine.
            allow_fd: If `True`, `file_path` may be open file descriptor.

        Returns:
            The resolved_path (str or byte).

        Raises:
            TypeError: if `file_path` is `None`.
            OSError: if `file_path` is '' or a part of the path doesn't exist.
        """

        if allow_fd and isinstance(file_path, int):
            return self.get_open_file(file_path).get_object().path
        path = make_string_path(file_path)
        if path is None:
            # file.open(None) raises TypeError, so mimic that.
            raise TypeError("Expected file system path string, received None")
        if sys.platform == "win32" and self.os != OSType.WINDOWS:
            path = path.replace(
                matching_string(path, os.sep),
                matching_string(path, self.path_separator),
            )
        if not path or not self._valid_relative_path(path):
            # file.open('') raises OSError, so mimic that, and validate that
            # all parts of a relative path exist.
            self.raise_os_error(errno.ENOENT, path)
        path = self.absnormpath(self._original_path(path))
        path = self.replace_windows_root(path)
        if self._is_root_path(path):
            return path
        if path == matching_string(path, self.devnull):
            return path
        path_components = self._path_components(path)
        resolved_components = self._resolve_components(path_components)
        path = self._components_to_path(resolved_components)
        # after resolving links, we have to check again for Windows root
        return self.replace_windows_root(path)  # pytype: disable=bad-return-type

    def _components_to_path(self, component_folders):
        sep = (
            self.get_path_separator(component_folders[0])
            if component_folders
            else self.path_separator
        )
        path = sep.join(component_folders)
        if not self._starts_with_root_path(path):
            path = sep + path
        return path

    def _resolve_components(self, components: List[AnyStr]) -> List[str]:
        current_dir = self.root
        link_depth = 0
        path_components = [to_string(comp) for comp in components]
        resolved_components: List[str] = []
        while path_components:
            component = path_components.pop(0)
            resolved_components.append(component)
            directory = self._directory_content(current_dir, component)[1]
            if directory is None:
                # The component of the path at this point does not actually
                # exist in the folder.  We can't resolve the path any more.
                # It is legal to link to a file that does not yet exist, so
                # rather than raise an error, we just append the remaining
                # components to what return path we have built so far and
                # return that.
                resolved_components.extend(path_components)
                break
            # Resolve any possible symlinks in the current path component.
            elif S_ISLNK(directory.st_mode):
                # This link_depth check is not really meant to be an accurate
                # check. It is just a quick hack to prevent us from looping
                # forever on cycles.
                if link_depth > _MAX_LINK_DEPTH:
                    self.raise_os_error(
                        errno.ELOOP,
                        self._components_to_path(resolved_components),
                    )
                link_path = self._follow_link(resolved_components, directory)

                # Following the link might result in the complete replacement
                # of the current_dir, so we evaluate the entire resulting path.
                target_components = self._path_components(link_path)
                path_components = target_components + path_components
                resolved_components = []
                current_dir = self.root
                link_depth += 1
            else:
                current_dir = cast(FakeDirectory, directory)
        return resolved_components

    def _valid_relative_path(self, file_path: AnyStr) -> bool:
        if self.is_windows_fs:
            return True
        slash_dotdot = matching_string(file_path, self.path_separator + "..")
        while file_path and slash_dotdot in file_path:
            file_path = file_path[: file_path.rfind(slash_dotdot)]
            if not self.exists(self.absnormpath(file_path)):
                return False
        return True

    def _follow_link(self, link_path_components: List[str], link: AnyFile) -> str:
        """Follow a link w.r.t. a path resolved so far.

        The component is either a real file, which is a no-op, or a
        symlink. In the case of a symlink, we have to modify the path
        as built up so far
          /a/b => ../c  should yield /a/../c (which will normalize to /a/c)
          /a/b => x     should yield /a/x
          /a/b => /x/y/z should yield /x/y/z
        The modified path may land us in a new spot which is itself a
        link, so we may repeat the process.

        Args:
            link_path_components: The resolved path built up to the link
                so far.
            link: The link object itself.

        Returns:
            (string) The updated path resolved after following the link.

        Raises:
            OSError: if there are too many levels of symbolic link
        """
        link_path = link.contents
        if link_path is not None:
            # ignore UNC prefix for local files
            if self.is_windows_fs and link_path.startswith("\\\\?\\"):
                link_path = link_path[4:]
            sep = self.get_path_separator(link_path)
            # For links to absolute paths, we want to throw out everything
            # in the path built so far and replace with the link. For relative
            # links, we have to append the link to what we have so far,
            if not self._starts_with_root_path(link_path):
                # Relative path. Append remainder of path to what we have
                # processed so far, excluding the name of the link itself.
                # /a/b => ../c  should yield /a/../c
                # (which will normalize to /c)
                # /a/b => d should yield a/d
                components = link_path_components[:-1]
                components.append(link_path)
                link_path = sep.join(components)
            # Don't call self.NormalizePath(), as we don't want to prepend
            # self.cwd.
            return self.normpath(link_path)  # pytype: disable=bad-return-type
        raise ValueError("Invalid link")

    def get_object_from_normpath(
        self,
        file_path: AnyPath,
        check_read_perm: bool = True,
        check_exe_perm: bool = True,
        check_owner: bool = False,
    ) -> AnyFile:
        """Search for the specified filesystem object within the fake
        filesystem.

        Args:
            file_path: Specifies target FakeFile object to retrieve, with a
                path that has already been normalized/resolved.
            check_read_perm: If True, raises OSError if a parent directory
                does not have read permission
            check_exe_perm: If True, raises OSError if a parent directory
                does not have execute (e.g. search) permission
            check_owner: If True, and check_read_perm is also True,
                only checks read permission if the current user id is
                different from the file object user id

        Returns:
            The FakeFile object corresponding to file_path.

        Raises:
            OSError: if the object is not found.
        """
        path = make_string_path(file_path)
        if path == matching_string(path, self.root.name):
            return self.root
        if path == matching_string(path, self.devnull):
            return self.dev_null

        path = self._original_path(path)
        path_components = self._path_components(path)
        target = self.root
        try:
            for component in path_components:
                if S_ISLNK(target.st_mode):
                    if target.contents:
                        target = cast(FakeDirectory, self.resolve(target.contents))
                if not S_ISDIR(target.st_mode):
                    if not self.is_windows_fs:
                        self.raise_os_error(errno.ENOTDIR, path)
                    self.raise_os_error(errno.ENOENT, path)
                target = target.get_entry(component)  # type: ignore
                if (
                    not is_root()
                    and (check_read_perm or check_exe_perm)
                    and target
                    and not self._can_read(
                        target, check_read_perm, check_exe_perm, check_owner
                    )
                ):
                    self.raise_os_error(errno.EACCES, target.path)
        except KeyError:
            self.raise_os_error(errno.ENOENT, path)
        return target

    @staticmethod
    def _can_read(target, check_read_perm, check_exe_perm, owner_can_read):
        if owner_can_read and target.st_uid == helpers.get_uid():
            return True
        permission = helpers.PERM_READ if check_read_perm else 0
        if S_ISDIR(target.st_mode) and check_exe_perm:
            permission |= helpers.PERM_EXE
        if not permission:
            return True
        return target.has_permission(permission)

    def _get_object(
        self,
        file_path: AnyPath,
        check_read_perm: bool = True,
        check_exe_perm: bool = True,
    ) -> FakeFile:
        """Search for the specified filesystem object within the fake
        filesystem. By default, consider read and execute permissions.

        Args:
            file_path: Specifies the target
                :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object to retrieve.
            check_read_perm: If True, raises OSError if a parent directory
                does not have read permission
            check_exe_perm: If True, raises OSError if a parent directory
                does not have execute (e.g. search) permission

        Returns:
            The :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object corresponding
            to `file_path`.

        Raises:
            OSError: if the object is not found.
        """
        path = make_string_path(file_path)
        path = self.absnormpath(self._original_path(path))
        return self.get_object_from_normpath(path, check_read_perm, check_exe_perm)

    def get_object(self, file_path: AnyPath) -> FakeFile:
        """Search for the specified filesystem object within the fake
        filesystem and return it. Ignore any read permissions.

        Args:
            file_path: Specifies the target
                :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object to retrieve.

        Returns:
            The :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object corresponding
            to `file_path`.

        Raises:
            OSError: if the object is not found.
        """
        return self._get_object(file_path, check_read_perm=False, check_exe_perm=False)

    def resolve(
        self,
        file_path: Union[AnyStr, int],
        follow_symlinks: bool = True,
        allow_fd: bool = False,
        check_read_perm: bool = True,
        check_exe_perm: bool = True,
        check_owner: bool = False,
    ) -> FakeFile:
        """Search for the specified filesystem object, resolving all links.

        Args:
            file_path: Specifies the target FakeFile object to retrieve.
            follow_symlinks: If `False`, the link itself is resolved,
                otherwise the object linked to.
            allow_fd: If `True`, `file_path` may be an open file descriptor
            check_read_perm: If True, raises OSError if a parent directory
                does not have read permission
            check_read_perm: If True, raises OSError if a parent directory
                does not have execute permission
            check_owner: If True, and check_read_perm is also True,
                only checks read permission if the current user id is
                different from the file object user id

        Returns:
          The FakeFile object corresponding to `file_path`.

        Raises:
            OSError: if the object is not found.
        """
        if isinstance(file_path, int):
            if allow_fd:
                open_file = self.get_open_file(file_path).get_object()
                assert isinstance(open_file, FakeFile)
                return open_file
            raise TypeError("path should be string, bytes or os.PathLike, not int")

        if follow_symlinks:
            return self.get_object_from_normpath(
                self.resolve_path(file_path, allow_fd),
                check_read_perm,
                check_exe_perm,
                check_owner,
            )
        return self.lresolve(file_path)

    def lresolve(self, path: AnyPath) -> FakeFile:
        """Search for the specified object, resolving only parent links.

        This is analogous to the stat/lstat difference.  This resolves links
        *to* the object but not of the final object itself.

        Args:
            path: Specifies target FakeFile object to retrieve.

        Returns:
            The FakeFile object corresponding to path.

        Raises:
            OSError: if the object is not found.
        """
        path_str = make_string_path(path)
        if not path_str:
            raise OSError(errno.ENOENT, path_str)
        if path_str == matching_string(path_str, self.root.name):
            # The root directory will never be a link
            return self.root

        # remove trailing separator
        path_str = self._path_without_trailing_separators(path_str)
        if path_str == matching_string(path_str, "."):
            path_str = matching_string(path_str, self.cwd)
        path_str = self._original_path(path_str)

        parent_directory, child_name = self.splitpath(path_str)
        if not parent_directory:
            parent_directory = matching_string(path_str, self.cwd)
        try:
            parent_obj = self.resolve(parent_directory)
            assert parent_obj
            if not isinstance(parent_obj, FakeDirectory):
                if not self.is_windows_fs and isinstance(parent_obj, FakeFile):
                    self.raise_os_error(errno.ENOTDIR, path_str)
                self.raise_os_error(errno.ENOENT, path_str)
            if not parent_obj.has_permission(helpers.PERM_READ):
                self.raise_os_error(errno.EACCES, parent_directory)
            return (
                parent_obj.get_entry(to_string(child_name))
                if child_name
                else parent_obj
            )
        except KeyError:
            pass
        raise OSError(errno.ENOENT, path_str)

    def add_object(self, file_path: AnyStr, file_object: AnyFile) -> None:
        """Add a fake file or directory into the filesystem at file_path.

        Args:
            file_path: The path to the file to be added relative to self.
            file_object: File or directory to add.

        Raises:
            OSError: if file_path does not correspond to a
                directory.
        """
        if not file_path:
            target_directory = self.root_dir
        else:
            target_directory = cast(
                FakeDirectory,
                self.resolve(file_path, check_read_perm=False, check_exe_perm=True),
            )
            if not S_ISDIR(target_directory.st_mode):
                error = errno.ENOENT if self.is_windows_fs else errno.ENOTDIR
                self.raise_os_error(error, file_path)
        target_directory.add_entry(file_object)

    def rename(
        self,
        old_file_path: AnyPath,
        new_file_path: AnyPath,
        force_replace: bool = False,
    ) -> None:
        """Renames a FakeFile object at old_file_path to new_file_path,
        preserving all properties.

        Args:
            old_file_path: Path to filesystem object to rename.
            new_file_path: Path to where the filesystem object will live
                after this call.
            force_replace: If set and destination is an existing file, it
                will be replaced even under Windows if the user has
                permissions, otherwise replacement happens under Unix only.

        Raises:
            OSError: if old_file_path does not exist.
            OSError: if new_file_path is an existing directory
                (Windows, or Posix if old_file_path points to a regular file)
            OSError: if old_file_path is a directory and new_file_path a file
            OSError: if new_file_path is an existing file and force_replace
                not set (Windows only).
            OSError: if new_file_path is an existing file and could not be
                removed (Posix, or Windows with force_replace set).
            OSError: if dirname(new_file_path) does not exist.
            OSError: if the file would be moved to another filesystem
                (e.g. mount point).
        """
        old_path = make_string_path(old_file_path)
        new_path = make_string_path(new_file_path)
        ends_with_sep = self.ends_with_path_separator(old_path)
        old_path = self.absnormpath(old_path)
        new_path = self.absnormpath(new_path)
        if not self.exists(old_path, check_link=True):
            self.raise_os_error(errno.ENOENT, old_path, 2)
        if ends_with_sep:
            self._handle_broken_link_with_trailing_sep(old_path)

        old_object = self.lresolve(old_path)
        if not self.is_windows_fs:
            self._handle_posix_dir_link_errors(new_path, old_path, ends_with_sep)

        if self.exists(new_path, check_link=True):
            renamed_path = self._rename_to_existing_path(
                force_replace, new_path, old_path, old_object, ends_with_sep
            )

            if renamed_path is None:
                return
            else:
                new_path = renamed_path

        old_dir, old_name = self.splitpath(old_path)
        new_dir, new_name = self.splitpath(new_path)
        if not self.exists(new_dir):
            self.raise_os_error(errno.ENOENT, new_dir)
        old_dir_object = self.resolve(old_dir)
        new_dir_object = self.resolve(new_dir)
        if old_dir_object.st_dev != new_dir_object.st_dev:
            self.raise_os_error(errno.EXDEV, old_path)
        if not S_ISDIR(new_dir_object.st_mode):
            self.raise_os_error(
                errno.EACCES if self.is_windows_fs else errno.ENOTDIR, new_path
            )
        if new_dir_object.has_parent_object(old_object):
            self.raise_os_error(errno.EINVAL, new_path)

        self._do_rename(old_dir_object, old_name, new_dir_object, new_name)

    def _do_rename(self, old_dir_object, old_name, new_dir_object, new_name):
        object_to_rename = old_dir_object.get_entry(old_name)
        old_dir_object.remove_entry(old_name, recursive=False)
        object_to_rename.name = new_name
        new_name = new_dir_object._normalized_entryname(new_name)
        old_entry = (
            new_dir_object.get_entry(new_name)
            if new_name in new_dir_object.entries
            else None
        )
        try:
            if old_entry:
                # in case of overwriting remove the old entry first
                new_dir_object.remove_entry(new_name)
            new_dir_object.add_entry(object_to_rename)
        except OSError:
            # adding failed, roll back the changes before re-raising
            if old_entry and new_name not in new_dir_object.entries:
                new_dir_object.add_entry(old_entry)
            object_to_rename.name = old_name
            old_dir_object.add_entry(object_to_rename)
            raise

    def _handle_broken_link_with_trailing_sep(self, path: AnyStr) -> None:
        # note that the check for trailing sep has to be done earlier
        if self.islink(path):
            if not self.exists(path):
                error = (
                    errno.ENOENT
                    if self.is_macos
                    else errno.EINVAL
                    if self.is_windows_fs
                    else errno.ENOTDIR
                )
                self.raise_os_error(error, path)

    def _handle_posix_dir_link_errors(
        self, new_file_path: AnyStr, old_file_path: AnyStr, ends_with_sep: bool
    ) -> None:
        if self.isdir(old_file_path, follow_symlinks=False) and self.islink(
            new_file_path
        ):
            self.raise_os_error(errno.ENOTDIR, new_file_path)
        if self.isdir(new_file_path, follow_symlinks=False) and self.islink(
            old_file_path
        ):
            if ends_with_sep and self.is_macos:
                return
            error = errno.ENOTDIR if ends_with_sep else errno.EISDIR
            self.raise_os_error(error, new_file_path)
        if (
            ends_with_sep
            and self.islink(old_file_path)
            and old_file_path == new_file_path
            and not self.is_windows_fs
        ):
            self.raise_os_error(errno.ENOTDIR, new_file_path)

    def _rename_to_existing_path(
        self,
        force_replace: bool,
        new_file_path: AnyStr,
        old_file_path: AnyStr,
        old_object: FakeFile,
        ends_with_sep: bool,
    ) -> Optional[AnyStr]:
        new_object = self._get_object(new_file_path)
        if old_file_path == new_file_path:
            if not S_ISLNK(new_object.st_mode) and ends_with_sep:
                error = errno.EINVAL if self.is_windows_fs else errno.ENOTDIR
                self.raise_os_error(error, old_file_path)
            return None  # Nothing to do here

        if old_object == new_object:
            return self._rename_same_object(new_file_path, old_file_path)
        if S_ISDIR(new_object.st_mode) or S_ISLNK(new_object.st_mode):
            self._handle_rename_error_for_dir_or_link(
                force_replace,
                new_file_path,
                new_object,
                old_object,
                ends_with_sep,
            )
        elif S_ISDIR(old_object.st_mode):
            error = errno.EEXIST if self.is_windows_fs else errno.ENOTDIR
            self.raise_os_error(error, new_file_path)
        elif self.is_windows_fs and not force_replace:
            self.raise_os_error(errno.EEXIST, new_file_path)
        else:
            self.remove_object(new_file_path)
        return new_file_path

    def _handle_rename_error_for_dir_or_link(
        self,
        force_replace: bool,
        new_file_path: AnyStr,
        new_object: FakeFile,
        old_object: FakeFile,
        ends_with_sep: bool,
    ) -> None:
        if self.is_windows_fs:
            if force_replace:
                self.raise_os_error(errno.EACCES, new_file_path)
            else:
                self.raise_os_error(errno.EEXIST, new_file_path)
        if not S_ISLNK(new_object.st_mode):
            if new_object.entries:
                if (
                    not S_ISLNK(old_object.st_mode)
                    or not ends_with_sep
                    or not self.is_macos
                ):
                    self.raise_os_error(errno.ENOTEMPTY, new_file_path)
            if S_ISREG(old_object.st_mode):
                self.raise_os_error(errno.EISDIR, new_file_path)

    def _rename_same_object(
        self, new_file_path: AnyStr, old_file_path: AnyStr
    ) -> Optional[AnyStr]:
        do_rename = old_file_path.lower() == new_file_path.lower()
        if not do_rename:
            try:
                real_old_path = self.resolve_path(old_file_path)
                original_old_path = self._original_path(real_old_path)
                real_new_path = self.resolve_path(new_file_path)
                if real_new_path == original_old_path and (
                    new_file_path == real_old_path
                ) == (new_file_path.lower() == real_old_path.lower()):
                    real_object = self.resolve(old_file_path, follow_symlinks=False)
                    do_rename = (
                        os.path.basename(old_file_path) == real_object.name
                        or not self.is_macos
                    )
                else:
                    do_rename = real_new_path.lower() == real_old_path.lower()
                if do_rename:
                    # only case is changed in case-insensitive file
                    # system - do the rename
                    parent, file_name = self.splitpath(new_file_path)
                    new_file_path = self.joinpaths(
                        self._original_path(parent), file_name
                    )
            except OSError:
                # ResolvePath may fail due to symlink loop issues or
                # similar - in this case just assume the paths
                # to be different
                pass
        if not do_rename:
            # hard links to the same file - nothing to do
            return None
        return new_file_path

    def remove_object(self, file_path: AnyStr) -> None:
        """Remove an existing file or directory.

        Args:
            file_path: The path to the file relative to self.

        Raises:
            OSError: if file_path does not correspond to an existing file, or
                if part of the path refers to something other than a directory.
            OSError: if the directory is in use (eg, if it is '/').
        """
        file_path = self.absnormpath(self._original_path(file_path))
        if self._is_root_path(file_path):
            self.raise_os_error(errno.EBUSY, file_path)
        try:
            dirname, basename = self.splitpath(file_path)
            target_directory = self.resolve(dirname, check_read_perm=False)
            target_directory.remove_entry(basename)
        except KeyError:
            self.raise_os_error(errno.ENOENT, file_path)
        except AttributeError:
            self.raise_os_error(errno.ENOTDIR, file_path)

    def make_string_path(self, path: AnyPath) -> AnyStr:  # type: ignore[type-var]
        path_str = make_string_path(path)
        os_sep = matching_string(path_str, os.sep)
        fake_sep = self.get_path_separator(path_str)
        return path_str.replace(os_sep, fake_sep)  # type: ignore[return-value]

    def create_dir(
        self,
        directory_path: AnyPath,
        perm_bits: int = helpers.PERM_DEF,
        apply_umask: bool = True,
    ) -> FakeDirectory:
        """Create `directory_path` and all the parent directories, and return
        the created :py:class:`FakeDirectory<pyfakefs.fake_file.FakeDirectory>` object.

        Helper method to set up your test faster.

        Args:
            directory_path: The full directory path to create.
            perm_bits: The permission bits as set by ``chmod``.
            apply_umask: If `True` (default), the current umask is applied
                to `perm_bits`.

        Returns:
            The newly created
            :py:class:`FakeDirectory<pyfakefs.fake_file.FakeDirectory>` object.

        Raises:
            OSError: if the directory already exists.
        """
        dir_path = self.make_string_path(directory_path)
        dir_path = self.absnormpath(dir_path)
        self._auto_mount_drive_if_needed(dir_path)
        if self.exists(dir_path, check_link=True) and dir_path not in self.mount_points:
            self.raise_os_error(errno.EEXIST, dir_path)
        path_components = self._path_components(dir_path)
        current_dir = self.root

        new_dirs = []
        for component in [to_string(p) for p in path_components]:
            directory = self._directory_content(current_dir, to_string(component))[1]
            if not directory:
                new_dir = FakeDirectory(component, filesystem=self)
                new_dirs.append(new_dir)
                if self.is_windows_fs and current_dir == self.root:
                    current_dir = self.root_dir
                current_dir.add_entry(new_dir)
                current_dir = new_dir
            else:
                if S_ISLNK(directory.st_mode):
                    assert directory.contents
                    directory = self.resolve(directory.contents)
                    assert directory
                current_dir = cast(FakeDirectory, directory)
                if directory.st_mode & S_IFDIR != S_IFDIR:
                    self.raise_os_error(errno.ENOTDIR, current_dir.path)

        # set the permission after creating the directories
        # to allow directory creation inside a read-only directory
        for new_dir in new_dirs:
            if apply_umask:
                perm_bits &= ~self.umask
            new_dir.st_mode = S_IFDIR | perm_bits

        return current_dir

    def create_file(
        self,
        file_path: AnyPath,
        st_mode: int = S_IFREG | helpers.PERM_DEF_FILE,
        contents: AnyString = "",
        st_size: Optional[int] = None,
        create_missing_dirs: bool = True,
        apply_umask: bool = True,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        side_effect: Optional[Callable] = None,
    ) -> FakeFile:
        """Create `file_path`, including all the parent directories along
        the way, and return the created
        :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object.

        This helper method can be used to set up tests more easily.

        Args:
            file_path: The path to the file to create.
            st_mode: The `stat` constant representing the file type.
            contents: the contents of the file. If not given and `st_size` is
                `None`, an empty file is assumed.
            st_size: file size; only valid if contents not given. If given,
                the file is considered to be in "large file mode" and trying
                to read from or write to the file will result in an exception.
            create_missing_dirs: If `True`, auto create missing directories.
            apply_umask: If `True` (default), the current umask is applied
                to `st_mode`.
            encoding: If `contents` is of type `str`, the encoding used
                for serialization.
            errors: The error mode used for encoding/decoding errors.
            side_effect: function handle that is executed when the file is written,
                must accept the file object as an argument.

        Returns:
            The newly created :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object.

        Raises:
            OSError: if the file already exists.
            OSError: if the containing directory is required and missing.
        """
        return self.create_file_internally(
            file_path,
            st_mode,
            contents,
            st_size,
            create_missing_dirs,
            apply_umask,
            encoding,
            errors,
            side_effect=side_effect,
        )

    def add_real_file(
        self,
        source_path: AnyPath,
        read_only: bool = True,
        target_path: Optional[AnyPath] = None,
    ) -> FakeFile:
        """Create `file_path`, including all the parent directories along the
        way, for an existing real file, and return the created
        :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object.
        The contents of the real file are read only on demand.

        Args:
            source_path: Path to an existing file in the real file system
            read_only: If `True` (the default), writing to the fake file
                raises an exception.  Otherwise, writing to the file changes
                the fake file only.
            target_path: If given, the path of the target direction,
                otherwise it is equal to `source_path`.

        Returns:
            the newly created :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object.

        Raises:
            OSError: if the file does not exist in the real file system.
            OSError: if the file already exists in the fake file system.

        .. note:: On most systems, accessing the fake file's contents may
            update both the real and fake files' `atime` (access time).
            In this particular case, `add_real_file()` violates the rule
            that `pyfakefs` must not modify the real file system.
        """
        target_path = target_path or source_path
        source_path_str = make_string_path(source_path)
        real_stat = os.stat(source_path_str)
        fake_file = self.create_file_internally(target_path, read_from_real_fs=True)

        # for read-only mode, remove the write/executable permission bits
        fake_file.stat_result.set_from_stat_result(real_stat)
        if read_only:
            fake_file.st_mode &= 0o777444
        fake_file.file_path = source_path_str
        self.change_disk_usage(fake_file.size, fake_file.name, fake_file.st_dev)
        return fake_file

    def add_real_symlink(
        self, source_path: AnyPath, target_path: Optional[AnyPath] = None
    ) -> FakeFile:
        """Create a symlink at `source_path` (or `target_path`, if given) and return
        the created :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object.
        It will point to the same path as the symlink on the real filesystem.
        Relative symlinks will point relative to their new location.  Absolute symlinks
        will point to the same, absolute path as on the real filesystem.

        Args:
            source_path: The path to the existing symlink.
            target_path: If given, the name of the symlink in the fake
                filesystem, otherwise, the same as `source_path`.

        Returns:
            the newly created :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object.

        Raises:
            OSError: if the directory does not exist in the real file system.
            OSError: if the symlink could not be created
                (see :py:meth:`create_file`).
            OSError: if the directory already exists in the fake file system.
        """
        source_path_str = make_string_path(source_path)  # TODO: add test
        source_path_str = self._path_without_trailing_separators(source_path_str)
        if not os.path.exists(source_path_str) and not os.path.islink(source_path_str):
            self.raise_os_error(errno.ENOENT, source_path_str)

        target = os.readlink(source_path_str)

        if target_path:
            return self.create_symlink(target_path, target)
        else:
            return self.create_symlink(source_path_str, target)

    def add_real_directory(
        self,
        source_path: AnyPath,
        read_only: bool = True,
        lazy_read: bool = True,
        target_path: Optional[AnyPath] = None,
    ) -> FakeDirectory:
        """Create a fake directory corresponding to the real directory at the
        specified path, and return the created
        :py:class:`FakeDirectory<pyfakefs.fake_file.FakeDirectory>` object.
        Add entries in the fake directory corresponding to
        the entries in the real directory.  Symlinks are supported.
        If the target directory already exists in the fake filesystem, the directory
        contents are merged. Overwriting existing files is not allowed.

        Args:
            source_path: The path to the existing directory.
            read_only: If set, all files under the directory are treated as
                read-only, e.g. a write access raises an exception;
                otherwise, writing to the files changes the fake files only
                as usually.
            lazy_read: If set (default), directory contents are only read when
                accessed, and only until the needed subdirectory level.

                .. note:: This means that the file system size is only updated
                  at the time the directory contents are read; set this to
                  `False` only if you are dependent on accurate file system
                  size in your test
            target_path: If given, the target directory, otherwise,
                the target directory is the same as `source_path`.

        Returns:
            the newly created
            :py:class:`FakeDirectory<pyfakefs.fake_file.FakeDirectory>` object.

        Raises:
            OSError: if the directory does not exist in the real filesystem.
            OSError: if a file or link exists in the fake filesystem where a real
                file or directory shall be mapped.
        """
        source_path_str = make_string_path(source_path)
        source_path_str = self._path_without_trailing_separators(source_path_str)
        if not os.path.exists(source_path_str):
            self.raise_os_error(errno.ENOENT, source_path_str)
        target_path_str = make_string_path(target_path or source_path_str)

        # get rid of inconsistencies between real and fake path separators
        if os.altsep is not None:
            target_path_str = os.path.normpath(target_path_str)
        if os.sep != self.path_separator:
            target_path_str = target_path_str.replace(os.sep, self.path_separator)

        self._auto_mount_drive_if_needed(target_path_str)
        if lazy_read:
            self._create_fake_from_real_dir_lazily(
                source_path_str, target_path_str, read_only
            )
        else:
            self._create_fake_from_real_dir(source_path_str, target_path_str, read_only)
        return cast(FakeDirectory, self._get_object(target_path_str))

    def _create_fake_from_real_dir(self, source_path_str, target_path_str, read_only):
        if not self.exists(target_path_str):
            self.create_dir(target_path_str)
        for base, _, files in os.walk(source_path_str):
            new_base = os.path.join(
                target_path_str,
                os.path.relpath(base, source_path_str),
            )
            for file_entry in os.listdir(base):
                file_path = os.path.join(base, file_entry)
                if os.path.islink(file_path):
                    self.add_real_symlink(file_path, os.path.join(new_base, file_entry))
            for file_entry in files:
                path = os.path.join(base, file_entry)
                if not os.path.islink(path):
                    self.add_real_file(
                        path, read_only, os.path.join(new_base, file_entry)
                    )

    def _create_fake_from_real_dir_lazily(
        self, source_path_str, target_path_str, read_only
    ):
        if self.exists(target_path_str):
            if not self.isdir(target_path_str):
                raise OSError(errno.ENOTDIR, "Mapping target is not a directory")
            for entry in os.listdir(source_path_str):
                src_entry_path = os.path.join(source_path_str, entry)
                target_entry_path = os.path.join(target_path_str, entry)
                if os.path.isdir(src_entry_path):
                    self.add_real_directory(
                        src_entry_path, read_only, True, target_entry_path
                    )
                elif os.path.islink(src_entry_path):
                    self.add_real_symlink(src_entry_path, target_entry_path)
                elif os.path.isfile(src_entry_path):
                    self.add_real_file(src_entry_path, read_only, target_entry_path)
            return self._get_object(target_path_str)

        parent_path = os.path.split(target_path_str)[0]
        if self.exists(parent_path):
            parent_dir = self._get_object(parent_path)
        else:
            parent_dir = self.create_dir(parent_path)
        new_dir = FakeDirectoryFromRealDirectory(
            source_path_str, self, read_only, target_path_str
        )
        parent_dir.add_entry(new_dir)
        return new_dir

    def add_real_paths(
        self,
        path_list: List[AnyStr],
        read_only: bool = True,
        lazy_dir_read: bool = True,
    ) -> None:
        """This convenience method adds multiple files and/or directories from
        the real file system to the fake file system. See :py:meth:`add_real_file` and
        :py:meth:`add_real_directory`.

        Args:
            path_list: List of file and directory paths in the real file
                system.
            read_only: If set, all files and files under the directories
                are treated as read-only, e.g. a write access raises an
                exception; otherwise, writing to the files changes the fake
                files only as usually.
            lazy_dir_read: Uses lazy reading of directory contents if set
                (see :py:meth:`add_real_directory`)

        Raises:
            OSError: if any of the files and directories in the list
                does not exist in the real file system.
            OSError: if a file or link exists in the fake filesystem where a real
                file or directory shall be mapped.
        """
        for path in path_list:
            if os.path.isdir(path):
                self.add_real_directory(path, read_only, lazy_dir_read)
            else:
                self.add_real_file(path, read_only)

    def create_file_internally(
        self,
        file_path: AnyPath,
        st_mode: int = S_IFREG | helpers.PERM_DEF_FILE,
        contents: AnyString = "",
        st_size: Optional[int] = None,
        create_missing_dirs: bool = True,
        apply_umask: bool = True,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        read_from_real_fs: bool = False,
        side_effect: Optional[Callable] = None,
    ) -> FakeFile:
        """Internal fake file creator that supports both normal fake files
        and fake files based on real files.

        Args:
            file_path: path to the file to create.
            st_mode: the stat.S_IF constant representing the file type.
            contents: the contents of the file. If not given and st_size is
                None, an empty file is assumed.
            st_size: file size; only valid if contents not given. If given,
                the file is considered to be in "large file mode" and trying
                to read from or write to the file will result in an exception.
            create_missing_dirs: if True, auto create missing directories.
            apply_umask: whether or not the current umask must be applied
                on st_mode.
            encoding: if contents is a unicode string, the encoding used for
                serialization.
            errors: the error mode used for encoding/decoding errors
            read_from_real_fs: if True, the contents are read from the real
                file system on demand.
            side_effect: function handle that is executed when file is written,
                must accept the file object as an argument.
        """
        path = self.make_string_path(file_path)
        path = self.absnormpath(path)
        if not is_int_type(st_mode):
            raise TypeError(
                "st_mode must be of int type - did you mean to set contents?"
            )

        if self.exists(path, check_link=True):
            self.raise_os_error(errno.EEXIST, path)
        parent_directory, new_file = self.splitpath(path)
        if not parent_directory:
            parent_directory = matching_string(path, self.cwd)
        self._auto_mount_drive_if_needed(parent_directory)
        if not self.exists(parent_directory):
            if not create_missing_dirs:
                self.raise_os_error(errno.ENOENT, parent_directory)
            parent_directory = matching_string(
                path,
                self.create_dir(parent_directory).path,  # type: ignore
            )
        else:
            parent_directory = self._original_path(parent_directory)
        if apply_umask:
            st_mode &= ~self.umask
        file_object: FakeFile
        if read_from_real_fs:
            file_object = FakeFileFromRealFile(
                to_string(path), filesystem=self, side_effect=side_effect
            )
        else:
            file_object = FakeFile(
                new_file,
                st_mode,
                filesystem=self,
                encoding=encoding,
                errors=errors,
                side_effect=side_effect,
            )

        self.add_object(parent_directory, file_object)

        if st_size is None and contents is None:
            contents = ""
        if not read_from_real_fs and (contents is not None or st_size is not None):
            try:
                if st_size is not None:
                    file_object.set_large_file_size(st_size)
                else:
                    file_object.set_initial_contents(contents)  # type: ignore
            except OSError:
                self.remove_object(path)
                raise

        return file_object

    def create_symlink(
        self,
        file_path: AnyPath,
        link_target: AnyPath,
        create_missing_dirs: bool = True,
    ) -> FakeFile:
        """Create the specified symlink, pointed at the specified link target,
        and return the created :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object
        representing the link.

        Args:
            file_path:  path to the symlink to create
            link_target:  the target of the symlink
            create_missing_dirs: If `True`, any missing parent directories of
                `file_path` will be created

        Returns:
            The newly created :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object.

        Raises:
            OSError: if the symlink could not be created
                (see :py:meth:`create_file`).
        """
        link_path = self.make_string_path(file_path)
        link_target_path = self.make_string_path(link_target)
        link_path = self.normcase(link_path)
        # the link path cannot end with a path separator
        if self.ends_with_path_separator(link_path):
            if self.exists(link_path):
                self.raise_os_error(errno.EEXIST, link_path)
            if self.exists(link_target_path):
                if not self.is_windows_fs:
                    self.raise_os_error(errno.ENOENT, link_path)
            else:
                if self.is_windows_fs:
                    self.raise_os_error(errno.EINVAL, link_target_path)
                if not self.exists(
                    self._path_without_trailing_separators(link_path),
                    check_link=True,
                ):
                    self.raise_os_error(errno.ENOENT, link_target_path)
                if self.is_macos:
                    # to avoid EEXIST exception, remove the link
                    # if it already exists
                    if self.exists(link_path, check_link=True):
                        self.remove_object(link_path)
                else:
                    self.raise_os_error(errno.EEXIST, link_target_path)

        # resolve the link path only if it is not a link itself
        if not self.islink(link_path):
            link_path = self.resolve_path(link_path)
        permission = helpers.PERM_DEF_FILE if self.is_windows_fs else helpers.PERM_DEF
        return self.create_file_internally(
            link_path,
            st_mode=S_IFLNK | permission,
            contents=link_target_path,
            create_missing_dirs=create_missing_dirs,
            apply_umask=self.is_macos,
        )

    def create_link(
        self,
        old_path: AnyPath,
        new_path: AnyPath,
        follow_symlinks: bool = True,
        create_missing_dirs: bool = True,
    ) -> FakeFile:
        """Create a hard link at `new_path`, pointing at `old_path`,
        and return the created :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object
        representing the link.

        Args:
            old_path: An existing link to the target file.
            new_path: The destination path to create a new link at.
            follow_symlinks: If `False` and `old_path` is a symlink, link the
                symlink instead of the object it points to.
            create_missing_dirs: If `True`, any missing parent directories of
                `file_path` will be created

        Returns:
            The :py:class:`FakeFile<pyfakefs.fake_file.FakeFile>` object referred to
            by `old_path`.

        Raises:
            OSError:  if something already exists at `new_path`.
            OSError:  if `old_path` is a directory.
            OSError:  if the parent directory doesn't exist.
        """
        old_path_str = make_string_path(old_path)
        new_path_str = make_string_path(new_path)
        new_path_normalized = self.absnormpath(new_path_str)
        if self.exists(new_path_normalized, check_link=True):
            self.raise_os_error(errno.EEXIST, new_path_str)

        new_parent_directory, new_basename = self.splitpath(new_path_normalized)
        if not new_parent_directory:
            new_parent_directory = matching_string(new_path_str, self.cwd)

        if not self.exists(new_parent_directory):
            if create_missing_dirs:
                self.create_dir(new_parent_directory)
            else:
                self.raise_os_error(errno.ENOENT, new_parent_directory)

        if self.ends_with_path_separator(old_path_str):
            error = errno.EINVAL if self.is_windows_fs else errno.ENOTDIR
            self.raise_os_error(error, old_path_str)

        if not self.is_windows_fs and self.ends_with_path_separator(new_path):
            self.raise_os_error(errno.ENOENT, old_path_str)

        # Retrieve the target file
        try:
            old_file = self.resolve(old_path_str, follow_symlinks=follow_symlinks)
        except OSError:
            self.raise_os_error(errno.ENOENT, old_path_str)

        if old_file.st_mode & S_IFDIR:
            self.raise_os_error(
                errno.EACCES if self.is_windows_fs else errno.EPERM,
                old_path_str,
            )

        # abuse the name field to control the filename of the
        # newly created link
        old_file.name = new_basename  # type: ignore[assignment]
        self.add_object(new_parent_directory, old_file)
        return old_file

    def link(
        self,
        old_path: AnyPath,
        new_path: AnyPath,
        follow_symlinks: bool = True,
    ) -> FakeFile:
        """Create a hard link at new_path, pointing at old_path.

        Args:
            old_path: An existing link to the target file.
            new_path: The destination path to create a new link at.
            follow_symlinks: If False and old_path is a symlink, link the
                symlink instead of the object it points to.

        Returns:
            The FakeFile object referred to by old_path.

        Raises:
            OSError:  if something already exists at new_path.
            OSError:  if old_path is a directory.
            OSError:  if the parent directory doesn't exist.
        """
        return self.create_link(
            old_path, new_path, follow_symlinks, create_missing_dirs=False
        )

    def _is_circular_link(self, link_obj: FakeFile) -> bool:
        try:
            assert link_obj.contents
            self.resolve_path(link_obj.contents)
        except OSError as exc:
            return exc.errno == errno.ELOOP
        return False

    def readlink(self, path: AnyPath) -> str:
        """Read the target of a symlink.

        Args:
            path:  symlink to read the target of.

        Returns:
            the string representing the path to which the symbolic link points.

        Raises:
            TypeError: if path is None
            OSError: (with errno=ENOENT) if path is not a valid path, or
                (with errno=EINVAL) if path is valid, but is not a symlink,
                or if the path ends with a path separator (Posix only)
        """
        if path is None:
            raise TypeError
        link_path = make_string_path(path)
        link_obj = self.lresolve(link_path)
        if S_IFMT(link_obj.st_mode) != S_IFLNK:
            self.raise_os_error(errno.EINVAL, link_path)

        if self.ends_with_path_separator(link_path):
            if not self.is_windows_fs and self.exists(link_path):
                self.raise_os_error(errno.EINVAL, link_path)
            if not self.exists(link_obj.path):  # type: ignore
                if self.is_windows_fs:
                    error = errno.EINVAL
                elif self._is_circular_link(link_obj):
                    if self.is_macos:
                        return link_obj.path  # type: ignore[return-value]
                    error = errno.ELOOP
                else:
                    error = errno.ENOENT
                self.raise_os_error(error, link_obj.path)

        assert link_obj.contents
        return link_obj.contents

    def makedir(self, dir_path: AnyPath, mode: int = helpers.PERM_DEF) -> None:
        """Create a leaf Fake directory.

        Args:
            dir_path: (str) Name of directory to create.
                Relative paths are assumed to be relative to '/'.
            mode: (int) Mode to create directory with.  This argument defaults
                to 0o777. The umask is applied to this mode.

        Raises:
            OSError: if the directory name is invalid or parent directory is
                read only or as per :py:meth:`add_object`.
        """
        dir_name = make_string_path(dir_path)
        ends_with_sep = self.ends_with_path_separator(dir_name)
        dir_name = self._path_without_trailing_separators(dir_name)
        if not dir_name:
            self.raise_os_error(errno.ENOENT, "")

        if self.is_windows_fs:
            dir_name = self.absnormpath(dir_name)
        parent_dir, rest = self.splitpath(dir_name)
        if parent_dir:
            base_dir = self.normpath(parent_dir)
            ellipsis = matching_string(parent_dir, self.path_separator + "..")
            if parent_dir.endswith(ellipsis) and not self.is_windows_fs:
                base_dir, dummy_dotdot, _ = parent_dir.partition(ellipsis)
            if self.is_windows_fs and not rest and not self.exists(base_dir):
                # under Windows, the parent dir may be a drive or UNC path
                # which has to be mounted
                self._auto_mount_drive_if_needed(parent_dir)
            if not self.exists(base_dir):
                self.raise_os_error(errno.ENOENT, base_dir)

        dir_name = self.absnormpath(dir_name)
        if self.exists(dir_name, check_link=True):
            if self.is_windows_fs and dir_name == self.root_dir_name:
                error_nr = errno.EACCES
            else:
                error_nr = errno.EEXIST
            if ends_with_sep and self.is_macos and not self.exists(dir_name):
                # to avoid EEXIST exception, remove the link
                self.remove_object(dir_name)
            else:
                self.raise_os_error(error_nr, dir_name)
        head, tail = self.splitpath(dir_name)

        self.add_object(
            to_string(head),
            FakeDirectory(to_string(tail), mode & ~self.umask, filesystem=self),
        )

    def _path_without_trailing_separators(self, path: AnyStr) -> AnyStr:
        while self.ends_with_path_separator(path):
            path = path[:-1]
        return path

    def makedirs(
        self, dir_name: AnyStr, mode: int = helpers.PERM_DEF, exist_ok: bool = False
    ) -> None:
        """Create a leaf Fake directory and create any non-existent
        parent dirs.

        Args:
            dir_name: (str) Name of directory to create.
            mode: (int) Mode to create directory (and any necessary parent
                directories) with. This argument defaults to 0o777.
                The umask is applied to this mode.
          exist_ok: (boolean) If exist_ok is False (the default), an OSError is
                raised if the target directory already exists.

        Raises:
            OSError: if the directory already exists and exist_ok=False,
                or as per :py:meth:`create_dir`.
        """
        if not dir_name:
            self.raise_os_error(errno.ENOENT, "")
        ends_with_sep = self.ends_with_path_separator(dir_name)
        dir_name = self.absnormpath(dir_name)
        if (
            ends_with_sep
            and self.is_macos
            and self.exists(dir_name, check_link=True)
            and not self.exists(dir_name)
        ):
            # to avoid EEXIST exception, remove the link
            self.remove_object(dir_name)

        dir_name_str = to_string(dir_name)
        path_components = self._path_components(dir_name_str)

        # Raise a permission denied error if the first existing directory
        # is not writeable.
        current_dir = self.root_dir
        for component in path_components:
            if (
                not hasattr(current_dir, "entries")
                or component not in current_dir.entries
            ):
                break
            else:
                current_dir = cast(FakeDirectory, current_dir.entries[component])
        try:
            self.create_dir(dir_name, mode)
        except OSError as e:
            if e.errno == errno.EACCES:
                # permission denied - propagate exception
                raise
            if not exist_ok or not isinstance(self.resolve(dir_name), FakeDirectory):
                if self.is_windows_fs and e.errno == errno.ENOTDIR:
                    e.errno = errno.ENOENT
                # mypy thinks that errno may be None
                self.raise_os_error(cast(int, e.errno), e.filename)

    def _is_of_type(
        self,
        path: AnyPath,
        st_flag: int,
        follow_symlinks: bool = True,
    ) -> bool:
        """Helper function to implement isdir(), islink(), etc.

        See the stat(2) man page for valid stat.S_I* flag values

        Args:
            path: Path to file to stat and test
            st_flag: The stat.S_I* flag checked for the file's st_mode
            follow_symlinks: If `False` and path points to a symlink,
                the link itself is checked instead of the linked object.

        Returns:
            (boolean) `True` if the st_flag is set in path's st_mode.

        Raises:
          TypeError: if path is None
        """
        if path is None:
            raise TypeError
        file_path = make_string_path(path)
        try:
            obj = self.resolve(file_path, follow_symlinks, check_read_perm=False)
            if obj:
                self.raise_for_filepath_ending_with_separator(
                    file_path, obj, macos_handling=not follow_symlinks
                )
                return S_IFMT(obj.st_mode) == st_flag
        except OSError:
            return False
        return False

    def isdir(self, path: AnyPath, follow_symlinks: bool = True) -> bool:
        """Determine if path identifies a directory.

        Args:
            path: Path to filesystem object.
            follow_symlinks: If `False` and path points to a symlink,
                the link itself is checked instead of the linked object.

        Returns:
            `True` if path points to a directory (following symlinks).

        Raises:
            TypeError: if path is None.
        """
        return self._is_of_type(path, S_IFDIR, follow_symlinks)

    def isfile(self, path: AnyPath, follow_symlinks: bool = True) -> bool:
        """Determine if path identifies a regular file.

        Args:
            path: Path to filesystem object.
            follow_symlinks: If `False` and path points to a symlink,
                the link itself is checked instead of the linked object.

        Returns:
            `True` if path points to a regular file (following symlinks).

        Raises:
            TypeError: if path is None.
        """
        return self._is_of_type(path, S_IFREG, follow_symlinks)

    def islink(self, path: AnyPath) -> bool:
        """Determine if path identifies a symbolic link.

        Args:
            path: Path to filesystem object.

        Returns:
            `True` if path points to a symlink (S_IFLNK set in st_mode)

        Raises:
            TypeError: if path is None.
        """
        return self._is_of_type(path, S_IFLNK, follow_symlinks=False)

    if sys.version_info >= (3, 12):

        def isjunction(self, path: AnyPath) -> bool:
            """Returns False. Junctions are never faked."""
            return False

    def confirmdir(
        self,
        target_directory: AnyStr,
        check_read_perm: bool = True,
        check_exe_perm: bool = True,
        check_owner: bool = False,
    ) -> FakeDirectory:
        """Test that the target is actually a directory, raising OSError
        if not.

        Args:
            target_directory: Path to the target directory within the fake
                filesystem.
            check_read_perm: If True, raises OSError if the directory
                does not have read permission
            check_exe_perm: If True, raises OSError if the directory
                does not have execute (e.g. search) permission
            check_owner: If True, only checks read permission if the current
                user id is different from the file object user id

        Returns:
            The FakeDirectory object corresponding to target_directory.

        Raises:
            OSError: if the target is not a directory.
        """
        directory = cast(
            FakeDirectory,
            self.resolve(
                target_directory,
                check_read_perm=check_read_perm,
                check_exe_perm=check_exe_perm,
                check_owner=check_owner,
            ),
        )
        if not directory.st_mode & S_IFDIR:
            self.raise_os_error(errno.ENOTDIR, target_directory, 267)
        return directory

    def remove(self, path: AnyStr) -> None:
        """Remove the FakeFile object at the specified file path.

        Args:
            path: Path to file to be removed.

        Raises:
            OSError: if path points to a directory.
            OSError: if path does not exist.
            OSError: if removal failed.
        """
        norm_path = make_string_path(path)
        norm_path = self.absnormpath(norm_path)
        if self.ends_with_path_separator(path):
            self._handle_broken_link_with_trailing_sep(norm_path)
        if self.exists(norm_path):
            obj = self.resolve(norm_path, check_read_perm=False)
            if S_IFMT(obj.st_mode) == S_IFDIR:
                link_obj = self.lresolve(norm_path)
                if S_IFMT(link_obj.st_mode) != S_IFLNK:
                    if self.is_windows_fs:
                        error = errno.EACCES
                    elif self.is_macos:
                        error = errno.EPERM
                    else:
                        error = errno.EISDIR
                    self.raise_os_error(error, norm_path)

                if path.endswith(self.get_path_separator(path)):
                    if self.is_windows_fs:
                        error = errno.EACCES
                    elif self.is_macos:
                        error = errno.EPERM
                    else:
                        error = errno.ENOTDIR
                    self.raise_os_error(error, norm_path)
            else:
                self.raise_for_filepath_ending_with_separator(path, obj)

        self.remove_object(norm_path)

    def rmdir(self, target_directory: AnyStr, allow_symlink: bool = False) -> None:
        """Remove a leaf Fake directory.

        Args:
            target_directory: (str) Name of directory to remove.
            allow_symlink: (bool) if `target_directory` is a symlink,
                the function just returns, otherwise it raises (Posix only)

        Raises:
            OSError: if target_directory does not exist.
            OSError: if target_directory does not point to a directory.
            OSError: if removal failed per FakeFilesystem.RemoveObject.
                Cannot remove '.'.
        """
        if target_directory == matching_string(target_directory, "."):
            error_nr = errno.EACCES if self.is_windows_fs else errno.EINVAL
            self.raise_os_error(error_nr, target_directory)
        ends_with_sep = self.ends_with_path_separator(target_directory)
        target_directory = self.absnormpath(target_directory)
        if self.confirmdir(target_directory, check_owner=True):
            if not self.is_windows_fs and self.islink(target_directory):
                if allow_symlink:
                    return
                if not ends_with_sep or not self.is_macos:
                    self.raise_os_error(errno.ENOTDIR, target_directory)

            dir_object = self.resolve(target_directory, check_owner=True)
            if dir_object.entries:
                self.raise_os_error(errno.ENOTEMPTY, target_directory)
            self.remove_object(target_directory)

    def listdir(self, target_directory: AnyStr) -> List[AnyStr]:
        """Return a list of file names in target_directory.

        Args:
            target_directory: Path to the target directory within the
                fake filesystem.

        Returns:
            A list of file names within the target directory in arbitrary
            order. If `shuffle_listdir_results` is set, the order is not the
            same in subsequent calls to avoid tests relying on any ordering.

        Raises:
            OSError: if the target is not a directory.
        """
        if target_directory is None:
            return []
        target_directory = self.resolve_path(target_directory, allow_fd=True)
        directory = self.confirmdir(target_directory, check_exe_perm=False)
        directory_contents = list(directory.entries.keys())
        if self.shuffle_listdir_results:
            random.shuffle(directory_contents)
        return directory_contents  # type: ignore[return-value]

    def __str__(self) -> str:
        return str(self.root_dir)

    if sys.version_info >= (3, 13):
        # used for emulating Windows
        _WIN_RESERVED_NAMES = frozenset(
            {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"}
            | {f"COM{c}" for c in "123456789\xb9\xb2\xb3"}
            | {f"LPT{c}" for c in "123456789\xb9\xb2\xb3"}
        )
        _WIN_RESERVED_CHARS = frozenset(
            {chr(i) for i in range(32)} | {'"', "*", ":", "<", ">", "?", "|", "/", "\\"}
        )

        def isreserved(self, path):
            if not self.is_windows_fs:
                return False

            def is_reserved_name(name):
                if sys.platform == "win32":
                    from os.path import _isreservedname  # type: ignore[import-error]

                    return _isreservedname(name)

                if name[-1:] in (".", " "):
                    return name not in (".", "..")
                if self._WIN_RESERVED_CHARS.intersection(name):
                    return True
                name = name.partition(".")[0].rstrip(" ").upper()
                return name in self._WIN_RESERVED_NAMES

            path = os.fsdecode(self.splitroot(path)[2])
            if self.alternative_path_separator is not None:
                path = path.replace(
                    self.alternative_path_separator, self.path_separator
                )

            return any(
                is_reserved_name(name)
                for name in reversed(path.split(self.path_separator))
            )

    def _add_standard_streams(self) -> None:
        self.add_open_file(StandardStreamWrapper(sys.stdin))
        self.add_open_file(StandardStreamWrapper(sys.stdout))
        self.add_open_file(StandardStreamWrapper(sys.stderr))

    def _tempdir_name(self):
        """This logic is extracted from tempdir._candidate_tempdir_list.
        We cannot rely on tempdir.gettempdir() in an empty filesystem, as it tries
        to write to the filesystem to ensure that the tempdir is valid.
        """
        # reset the cached tempdir in tempfile
        tempfile.tempdir = None
        for env_name in "TMPDIR", "TEMP", "TMP":
            dir_name = os.getenv(env_name)
            if dir_name:
                return dir_name
        # we have to check the real OS temp path here, as this is what
        # tempfile assumes
        if os.name == "nt":
            return os.path.expanduser(r"~\AppData\Local\Temp")
        return "/tmp"

    def _create_temp_dir(self):
        # the temp directory is assumed to exist at least in `tempfile`,
        # so we create it here for convenience
        temp_dir = self._tempdir_name()
        if not self.exists(temp_dir):
            self.create_dir(temp_dir)
        if sys.platform != "win32" and not self.exists("/tmp"):
            # under Posix, we also create a link in /tmp if the path does not exist
            self.create_symlink("/tmp", temp_dir)
            # reset the used size to 0 to avoid having the link size counted
            # which would make disk size tests more complicated
            next(iter(self.mount_points.values()))["used_size"] = 0


def _run_doctest() -> TestResults:
    import doctest
    import pyfakefs

    return doctest.testmod(pyfakefs.fake_filesystem)


def __getattr__(name):
    # backwards compatibility for read access to globals moved to helpers
    if name == "USER_ID":
        return helpers.USER_ID
    if name == "GROUP_ID":
        return helpers.GROUP_ID
    raise AttributeError(f"No attribute {name!r}.")


if __name__ == "__main__":
    _run_doctest()
