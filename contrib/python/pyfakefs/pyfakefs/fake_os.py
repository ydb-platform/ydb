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

"""Uses :py:class:`FakeOsModule` to provide a
fake :py:mod:`os` module replacement.
"""

import errno
import functools
import inspect
import os
import sys
import uuid
from contextlib import contextmanager
from stat import (
    S_IFREG,
    S_IFSOCK,
)
from typing import (
    List,
    Optional,
    Callable,
    Union,
    Any,
    Tuple,
    cast,
    AnyStr,
    TYPE_CHECKING,
    Set,
)

from pyfakefs.fake_file import (
    FakeDirectory,
    FakeDirWrapper,
    StandardStreamWrapper,
    FakeFileWrapper,
    FakePipeWrapper,
    FakeFile,
    AnyFileWrapper,
)
from pyfakefs.fake_open import FakeFileOpen, _OpenModes
from pyfakefs.fake_path import FakePathModule
from pyfakefs.fake_scandir import scandir, walk, ScanDirIter
from pyfakefs.helpers import (
    FakeStatResult,
    is_called_from_skipped_module,
    is_int_type,
    is_byte_string,
    make_string_path,
    IS_PYPY,
    to_string,
    matching_string,
    AnyString,
    to_bytes,
    PERM_EXE,
    PERM_DEF,
    is_root,
    get_uid,
    get_gid,
)

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem

NR_STD_STREAMS = 3


class FakeOsModule:
    """Uses FakeFilesystem to provide a fake os module replacement.

    Do not create os.path separately from os, as there is a necessary circular
    dependency between os and os.path to replicate the behavior of the standard
    Python modules.  What you want to do is to just let FakeOsModule take care
    of `os.path` setup itself.

    # You always want to do this.
    filesystem = fake_filesystem.FakeFilesystem()
    my_os_module = fake_os.FakeOsModule(filesystem)
    """

    use_original = False

    @staticmethod
    def dir() -> List[str]:
        """Return the list of patched function names. Used for patching
        functions imported from the module.
        """
        _dir = [
            "access",
            "chdir",
            "chmod",
            "chown",
            "close",
            "dup",
            "dup2",
            "fstat",
            "fsync",
            "getcwd",
            "lchmod",
            "link",
            "listdir",
            "lseek",
            "lstat",
            "makedirs",
            "mkdir",
            "mknod",
            "open",
            "read",
            "readlink",
            "remove",
            "removedirs",
            "rename",
            "rmdir",
            "scandir",
            "stat",
            "symlink",
            "umask",
            "unlink",
            "utime",
            "walk",
            "write",
            "getcwdb",
            "replace",
        ]
        if sys.platform.startswith("linux"):
            _dir += [
                "fdatasync",
                "getxattr",
                "listxattr",
                "removexattr",
                "setxattr",
            ]
        if sys.platform != "win32":
            _dir += [
                "getgid",
                "getuid",
            ]
        return _dir

    def __init__(self, filesystem: "FakeFilesystem"):
        """Also exposes self.path (to fake os.path).

        Args:
            filesystem: FakeFilesystem used to provide file system information
        """
        self.filesystem = filesystem
        self.os_module: Any = os
        self.path = FakePathModule(self.filesystem, self)
        self._supports_follow_symlinks: Optional[Set] = None
        self._supports_dir_fd: Optional[Set] = None
        self._supports_effective_ids: Optional[Set] = None
        self._supports_fd: Optional[Set] = None

    @property
    def devnull(self) -> str:
        return self.path.devnull

    @property
    def sep(self) -> str:
        return self.path.sep

    @property
    def altsep(self) -> Optional[str]:
        return self.path.altsep

    @property
    def linesep(self) -> str:
        return self.path.linesep

    @property
    def pathsep(self) -> str:
        return self.path.pathsep

    def fdopen(self, fd: int, *args: Any, **kwargs: Any) -> AnyFileWrapper:
        """Redirector to open() builtin function.

        Args:
            fd: The file descriptor of the file to open.
            *args: Pass through args.
            **kwargs: Pass through kwargs.

        Returns:
            File object corresponding to file_des.

        Raises:
            TypeError: if file descriptor is not an integer.
        """
        if not is_int_type(fd):
            raise TypeError("an integer is required")
        return FakeFileOpen(self.filesystem)(fd, *args, **kwargs)

    def _umask(self) -> int:
        """Return the current umask."""
        if self.filesystem.is_windows_fs:
            # windows always returns 0 - it has no real notion of umask
            return 0
        if sys.platform == "win32":
            # if we are testing Unix under Windows we assume a default mask
            return 0o002
        else:
            # under Unix, we return the real umask;
            # there is no pure getter for umask, so we have to first
            # set a mode to get the previous one and then re-set that
            mask = os.umask(0)
            os.umask(mask)
            return mask

    def open(
        self,
        path: AnyStr,
        flags: int,
        mode: Optional[int] = None,
        *,
        dir_fd: Optional[int] = None,
    ) -> int:
        """Return the file descriptor for a FakeFile.

        Args:
            path: the path to the file
            flags: low-level bits to indicate io operation
            mode: bits to define default permissions
                Note: only basic modes are supported, OS-specific modes are
                ignored
            dir_fd: If not `None`, the file descriptor of a directory,
                with `file_path` being relative to this directory.

        Returns:
            A file descriptor.

        Raises:
            OSError: if the path cannot be found
            ValueError: if invalid mode is given
            NotImplementedError: if `os.O_EXCL` is used without `os.O_CREAT`
        """
        path = self._path_with_dir_fd(path, self.open, dir_fd)
        if mode is None:
            if self.filesystem.is_windows_fs:
                mode = 0o666
            else:
                mode = 0o777 & ~self._umask()

        has_directory_flag = (
            hasattr(os, "O_DIRECTORY") and flags & os.O_DIRECTORY == os.O_DIRECTORY
        )
        if (
            has_directory_flag
            and self.filesystem.exists(path)
            and not self.filesystem.isdir(path)
        ):
            raise OSError(errno.ENOTDIR, "path is not a directory", path)

        has_follow_flag = (
            hasattr(os, "O_NOFOLLOW") and flags & os.O_NOFOLLOW == os.O_NOFOLLOW
        )
        if has_follow_flag and self.filesystem.islink(path):
            raise OSError(errno.ELOOP, "path is a symlink", path)

        has_tmpfile_flag = (
            hasattr(os, "O_TMPFILE") and flags & os.O_TMPFILE == os.O_TMPFILE
        )
        open_modes = _OpenModes(
            must_exist=not flags & os.O_CREAT and not has_tmpfile_flag,
            can_read=not flags & os.O_WRONLY,
            can_write=flags & (os.O_RDWR | os.O_WRONLY) != 0,
            truncate=flags & os.O_TRUNC != 0,
            append=flags & os.O_APPEND != 0,
            must_not_exist=flags & os.O_EXCL != 0,
        )
        if open_modes.must_not_exist and open_modes.must_exist:
            raise NotImplementedError("O_EXCL without O_CREAT mode is not supported")
        if has_tmpfile_flag:
            # this is a workaround for tempfiles that do not have a filename
            # as we do not support this directly, we just add a unique filename
            # and set the file to delete on close
            path = self.filesystem.joinpaths(
                path, matching_string(path, str(uuid.uuid4()))
            )

        if not self.filesystem.is_windows_fs and self.filesystem.exists(path):
            # handle opening directory - only allowed under Posix
            # with read-only mode
            obj = self.filesystem.resolve(path)
            if isinstance(obj, FakeDirectory):
                if (
                    not open_modes.must_exist and not self.filesystem.is_macos
                ) or open_modes.can_write:
                    self.filesystem.raise_os_error(errno.EISDIR, path)
                dir_wrapper = FakeDirWrapper(obj, path, self.filesystem)
                file_des = self.filesystem.add_open_file(dir_wrapper)
                dir_wrapper.filedes = file_des
                return file_des

        # low level open is always binary
        str_flags = "b"
        delete_on_close = has_tmpfile_flag
        if hasattr(os, "O_TEMPORARY"):
            delete_on_close = flags & os.O_TEMPORARY == os.O_TEMPORARY
        fake_file = FakeFileOpen(
            self.filesystem, delete_on_close=delete_on_close, raw_io=True
        )(path, str_flags, open_modes=open_modes)
        assert not isinstance(fake_file, StandardStreamWrapper)
        if fake_file.file_object != self.filesystem.dev_null:
            self.chmod(path, mode)
        return fake_file.fileno()

    def close(self, fd: int) -> None:
        """Close a file descriptor.

        Args:
            fd: An integer file descriptor for the file object requested.

        Raises:
            OSError: bad file descriptor.
            TypeError: if file descriptor is not an integer.
        """
        file_handle = self.filesystem.get_open_file(fd)
        file_handle.close_fd(fd)

    def dup(self, fd: int) -> int:
        file_handle = self.filesystem.get_open_file(fd)
        return self.filesystem.add_open_file(file_handle)

    def dup2(self, fd: int, fd2: int, inheritable: bool = True) -> int:
        if fd == fd2:
            return fd
        file_handle = self.filesystem.get_open_file(fd)
        return self.filesystem.add_open_file(file_handle, fd2)

    def read(self, fd: int, n: int) -> bytes:
        """Read number of bytes from a file descriptor, returns bytes read.

        Args:
            fd: An integer file descriptor for the file object requested.
            n: Number of bytes to read from file.

        Returns:
            Bytes read from file.

        Raises:
            OSError: bad file descriptor.
            TypeError: if file descriptor is not an integer.
        """
        file_handle = self.filesystem.get_open_file(fd)
        if isinstance(file_handle, FakeFileWrapper):
            file_handle.raw_io = True
        if isinstance(file_handle, FakeDirWrapper):
            self.filesystem.raise_os_error(errno.EBADF, file_handle.file_path)
        return file_handle.read(n)

    def write(self, fd: int, contents: bytes) -> int:
        """Write string to file descriptor, returns number of bytes written.

        Args:
            fd: An integer file descriptor for the file object requested.
            contents: String of bytes to write to file.

        Returns:
            Number of bytes written.

        Raises:
            OSError: bad file descriptor.
            TypeError: if file descriptor is not an integer.
        """
        file_handle = cast(FakeFileWrapper, self.filesystem.get_open_file(fd))
        if isinstance(file_handle, FakeDirWrapper):
            self.filesystem.raise_os_error(errno.EBADF, file_handle.file_path)

        if isinstance(file_handle, FakePipeWrapper):
            return file_handle.write(contents)

        file_handle.raw_io = True
        file_handle._sync_io()
        file_handle.update_flush_pos()
        file_handle.write(contents)
        file_handle.flush()
        return len(contents)

    def lseek(self, fd: int, pos: int, whence: int):
        file_handle = self.filesystem.get_open_file(fd)
        if isinstance(file_handle, FakeFileWrapper):
            file_handle.seek(pos, whence)
        else:
            raise OSError(errno.EBADF, "Bad file descriptor for fseek")

    def pipe(self) -> Tuple[int, int]:
        read_fd, write_fd = os.pipe()
        read_wrapper = FakePipeWrapper(self.filesystem, read_fd, False)
        file_des = self.filesystem.add_open_file(read_wrapper)
        read_wrapper.filedes = file_des
        write_wrapper = FakePipeWrapper(self.filesystem, write_fd, True)
        file_des = self.filesystem.add_open_file(write_wrapper)
        write_wrapper.filedes = file_des
        return read_wrapper.filedes, write_wrapper.filedes

    def fstat(self, fd: int) -> FakeStatResult:
        """Return the os.stat-like tuple for the FakeFile object of file_des.

        Args:
            fd: The file descriptor of filesystem object to retrieve.

        Returns:
            The FakeStatResult object corresponding to entry_path.

        Raises:
            OSError: if the filesystem object doesn't exist.
        """
        # stat should return the tuple representing return value of os.stat
        file_object = self.filesystem.get_open_file(fd).get_object()
        assert isinstance(file_object, FakeFile)
        return file_object.stat_result.copy()

    def umask(self, mask: int) -> int:
        """Change the current umask.

        Args:
            mask: (int) The new umask value.

        Returns:
            The old umask.

        Raises:
            TypeError: if new_mask is of an invalid type.
        """
        if not is_int_type(mask):
            raise TypeError("an integer is required")
        old_umask = self.filesystem.umask
        self.filesystem.umask = mask
        return old_umask

    def chdir(self, path: AnyStr) -> None:
        """Change current working directory to target directory.

        Args:
            path: The path to new current working directory.

        Raises:
            OSError: if user lacks permission to enter the argument directory
                or if the target is not a directory.
        """
        try:
            path = self.filesystem.resolve_path(path, allow_fd=True)
        except OSError as exc:
            if self.filesystem.is_macos and exc.errno == errno.EBADF:
                raise OSError(errno.ENOTDIR, "Not a directory: " + str(path))
            elif (
                self.filesystem.is_windows_fs
                and exc.errno == errno.ENOENT
                and path == ""
            ):
                raise OSError(errno.EINVAL, "Invalid argument:  + str(path)")
            raise
        try:
            self.filesystem.confirmdir(path)
        except OSError as exc:
            if exc.errno == errno.EACCES:
                # no access rights to the parent directory - do nothing
                return
            raise
        directory = self.filesystem.resolve(path)
        # A full implementation would check permissions all the way
        # up the tree.
        if not is_root() and not directory.has_permission(PERM_EXE):
            self.filesystem.raise_os_error(errno.EACCES, directory.name)
        self.filesystem.cwd = path  # type: ignore[assignment]

    def getcwd(self) -> str:
        """Return current working directory."""
        return to_string(self.filesystem.cwd)

    def getcwdb(self) -> bytes:
        """Return current working directory as bytes."""
        return to_bytes(self.filesystem.cwd)

    def listdir(self, path: AnyStr) -> List[AnyStr]:
        """Return a list of file names in target_directory.

        Args:
            path: Path to the target directory within the fake
                filesystem.

        Returns:
            A list of file names within the target directory in arbitrary
                order.

        Raises:
          OSError:  if the target is not a directory.
        """
        return self.filesystem.listdir(path)

    XATTR_CREATE = 1
    XATTR_REPLACE = 2

    def getxattr(
        self, path: AnyStr, attribute: AnyString, *, follow_symlinks: bool = True
    ) -> Optional[bytes]:
        """Return the value of the given extended filesystem attribute for
        `path`.

        Args:
            path: File path, file descriptor or path-like object.
            attribute: (str or bytes) The attribute name.
            follow_symlinks: (bool) If True (the default), symlinks in the
                path are traversed.

        Returns:
            The contents of the extended attribute as bytes or None if
            the attribute does not exist.

        Raises:
            OSError: if the path does not exist.
        """
        if not self.filesystem.is_linux:
            raise AttributeError("module 'os' has no attribute 'getxattr'")

        if isinstance(attribute, bytes):
            attribute = attribute.decode(sys.getfilesystemencoding())
        file_obj = self.filesystem.resolve(path, follow_symlinks, allow_fd=True)
        if attribute not in file_obj.xattr:
            raise OSError(errno.ENODATA, "No data available", path)
        return file_obj.xattr.get(attribute)

    def listxattr(
        self, path: Optional[AnyStr] = None, *, follow_symlinks: bool = True
    ) -> List[str]:
        """Return a list of the extended filesystem attributes on `path`.

        Args:
            path: File path, file descriptor or path-like object.
               If None, the current directory is used.
            follow_symlinks: (bool) If True (the default), symlinks in the
                path are traversed.

        Returns:
            A list of all attribute names for the given path as str.

        Raises:
            OSError: if the path does not exist.
        """
        if not self.filesystem.is_linux:
            raise AttributeError("module 'os' has no attribute 'listxattr'")

        path_str = self.filesystem.cwd if path is None else path
        file_obj = self.filesystem.resolve(
            cast(AnyStr, path_str),  # pytype: disable=invalid-annotation
            follow_symlinks,
            allow_fd=True,
        )
        return list(file_obj.xattr.keys())

    def removexattr(
        self, path: AnyStr, attribute: AnyString, *, follow_symlinks: bool = True
    ) -> None:
        """Removes the extended filesystem attribute from `path`.

        Args:
            path: File path, file descriptor or path-like object
            attribute: (str or bytes) The attribute name.
            follow_symlinks: (bool) If True (the default), symlinks in the
                path are traversed.

        Raises:
            OSError: if the path does not exist.
        """
        if not self.filesystem.is_linux:
            raise AttributeError("module 'os' has no attribute 'removexattr'")

        if isinstance(attribute, bytes):
            attribute = attribute.decode(sys.getfilesystemencoding())
        file_obj = self.filesystem.resolve(path, follow_symlinks, allow_fd=True)
        if attribute in file_obj.xattr:
            del file_obj.xattr[attribute]

    def setxattr(
        self,
        path: AnyStr,
        attribute: AnyString,
        value: bytes,
        flags: int = 0,
        *,
        follow_symlinks: bool = True,
    ) -> None:
        """Sets the value of the given extended filesystem attribute for
        `path`.

        Args:
            path: File path, file descriptor or path-like object.
            attribute: The attribute name (str or bytes).
            value: (byte-like) The value to be set.
            follow_symlinks: (bool) If True (the default), symlinks in the
                path are traversed.

        Raises:
            OSError: if the path does not exist.
            TypeError: if `value` is not a byte-like object.
        """
        if not self.filesystem.is_linux:
            raise AttributeError("module 'os' has no attribute 'setxattr'")

        if isinstance(attribute, bytes):
            attribute = attribute.decode(sys.getfilesystemencoding())
        if not is_byte_string(value):
            raise TypeError("a bytes-like object is required")
        file_obj = self.filesystem.resolve(path, follow_symlinks, allow_fd=True)
        exists = attribute in file_obj.xattr
        if exists and flags == self.XATTR_CREATE:
            self.filesystem.raise_os_error(errno.ENODATA, file_obj.path)
        if not exists and flags == self.XATTR_REPLACE:
            self.filesystem.raise_os_error(errno.EEXIST, file_obj.path)
        file_obj.xattr[attribute] = value

    def scandir(self, path: str = ".") -> ScanDirIter:
        """Return an iterator of DirEntry objects corresponding to the
        entries in the directory given by path.

        Args:
            path: Path to the target directory within the fake filesystem.

        Returns:
            An iterator to an unsorted list of os.DirEntry objects for
            each entry in path.

        Raises:
            OSError: if the target is not a directory.
        """
        return scandir(self.filesystem, path)

    def walk(
        self,
        top: AnyStr,
        topdown: bool = True,
        onerror: Optional[bool] = None,
        followlinks: bool = False,
    ):
        """Perform an os.walk operation over the fake filesystem.

        Args:
            top: The root directory from which to begin walk.
            topdown: Determines whether to return the tuples with the root as
                the first entry (`True`) or as the last, after all the child
                directory tuples (`False`).
          onerror: If not `None`, function which will be called to handle the
                `os.error` instance provided when `os.listdir()` fails.
          followlinks: If `True`, symbolic links are followed.

        Yields:
            (path, directories, nondirectories) for top and each of its
            subdirectories.  See the documentation for the builtin os module
            for further details.
        """
        return walk(self.filesystem, top, topdown, onerror, followlinks)

    def readlink(self, path: AnyStr, dir_fd: Optional[int] = None) -> str:
        """Read the target of a symlink.

        Args:
            path:  Symlink to read the target of.
            dir_fd: If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.

        Returns:
            the string representing the path to which the symbolic link points.

        Raises:
            TypeError: if `path` is None
            OSError: (with errno=ENOENT) if path is not a valid path, or
                     (with errno=EINVAL) if path is valid, but is not a symlink
        """
        path = self._path_with_dir_fd(path, self.readlink, dir_fd)
        return self.filesystem.readlink(path)

    def stat(
        self,
        path: AnyStr,
        *,
        dir_fd: Optional[int] = None,
        follow_symlinks: bool = True,
    ) -> FakeStatResult:
        """Return the os.stat-like tuple for the FakeFile object of entry_path.

        Args:
            path:  path to filesystem object to retrieve.
            dir_fd: (int) If not `None`, the file descriptor of a directory,
                with `entry_path` being relative to this directory.
            follow_symlinks: (bool) If `False` and `entry_path` points to a
                symlink, the link itself is changed instead of the linked
                object.

        Returns:
            The FakeStatResult object corresponding to entry_path.

        Raises:
            OSError: if the filesystem object doesn't exist.
        """
        path = self._path_with_dir_fd(path, self.stat, dir_fd)
        return self.filesystem.stat(path, follow_symlinks)

    def lstat(self, path: AnyStr, *, dir_fd: Optional[int] = None) -> FakeStatResult:
        """Return the os.stat-like tuple for entry_path,
        not following symlinks.

        Args:
            path:  path to filesystem object to retrieve.
            dir_fd: If not `None`, the file descriptor of a directory, with
                `path` being relative to this directory.

        Returns:
            the FakeStatResult object corresponding to `path`.

        Raises:
            OSError: if the filesystem object doesn't exist.
        """
        # stat should return the tuple representing return value of os.stat
        path = self._path_with_dir_fd(path, self.lstat, dir_fd, check_supported=False)
        return self.filesystem.stat(path, follow_symlinks=False)

    def remove(self, path: AnyStr, dir_fd: Optional[int] = None) -> None:
        """Remove the FakeFile object at the specified file path.

        Args:
            path: Path to file to be removed.
            dir_fd: If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.

        Raises:
            OSError: if path points to a directory.
            OSError: if path does not exist.
            OSError: if removal failed.
        """
        path = self._path_with_dir_fd(path, self.remove, dir_fd, check_supported=False)
        self.filesystem.remove(path)

    def unlink(self, path: AnyStr, *, dir_fd: Optional[int] = None) -> None:
        """Remove the FakeFile object at the specified file path.

        Args:
            path: Path to file to be removed.
            dir_fd: If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.

        Raises:
            OSError: if path points to a directory.
            OSError: if path does not exist.
            OSError: if removal failed.
        """
        path = self._path_with_dir_fd(path, self.unlink, dir_fd)
        self.filesystem.remove(path)

    def rename(
        self,
        src: AnyStr,
        dst: AnyStr,
        *,
        src_dir_fd: Optional[int] = None,
        dst_dir_fd: Optional[int] = None,
    ) -> None:
        """Rename a FakeFile object at old_file_path to new_file_path,
        preserving all properties.
        Also replaces existing new_file_path object, if one existed
        (Unix only).

        Args:
            src: Path to filesystem object to rename.
            dst: Path to where the filesystem object will live
                after this call.
            src_dir_fd: If not `None`, the file descriptor of a directory,
                with `src` being relative to this directory.
            dst_dir_fd: If not `None`, the file descriptor of a directory,
                with `dst` being relative to this directory.

        Raises:
            OSError: if old_file_path does not exist.
            OSError: if new_file_path is an existing directory.
            OSError: if new_file_path is an existing file (Windows only)
            OSError: if new_file_path is an existing file and could not
                be removed (Unix)
            OSError: if `dirname(new_file)` does not exist
            OSError: if the file would be moved to another filesystem
                (e.g. mount point)
        """
        src = self._path_with_dir_fd(src, self.rename, src_dir_fd)
        dst = self._path_with_dir_fd(dst, self.rename, dst_dir_fd)
        self.filesystem.rename(src, dst)

    def renames(self, old: AnyStr, new: AnyStr):
        """Fakes `os.renames`, documentation taken from there.

        Super-rename; create directories as necessary and delete any left
        empty.  Works like rename, except creation of any intermediate
        directories needed to make the new pathname good is attempted
        first.  After the rename, directories corresponding to rightmost
        path segments of the old name will be pruned until either the
        whole path is consumed or a nonempty directory is found.

        Note: this function can fail with the new directory structure made
        if you lack permissions needed to unlink the leaf directory or
        file.

        """
        head, tail = self.filesystem.splitpath(new)
        if head and tail and not self.filesystem.exists(head):
            self.makedirs(head)
        self.rename(old, new)
        head, tail = self.filesystem.splitpath(old)
        if head and tail:
            try:
                self.removedirs(head)
            except OSError:
                pass

    def replace(
        self,
        src: AnyStr,
        dst: AnyStr,
        *,
        src_dir_fd: Optional[int] = None,
        dst_dir_fd: Optional[int] = None,
    ) -> None:
        """Renames a FakeFile object at old_file_path to new_file_path,
        preserving all properties.
        Also replaces existing new_file_path object, if one existed.

        Arg
            src: Path to filesystem object to rename.
            dst: Path to where the filesystem object will live
                after this call.
            src_dir_fd: If not `None`, the file descriptor of a directory,
                with `src` being relative to this directory.
            dst_dir_fd: If not `None`, the file descriptor of a directory,
                with `dst` being relative to this directory.

        Raises:
            OSError: if old_file_path does not exist.
            OSError: if new_file_path is an existing directory.
            OSError: if new_file_path is an existing file and could
                not be removed
            OSError: if `dirname(new_file)` does not exist
            OSError: if the file would be moved to another filesystem
                (e.g. mount point)
        """
        src = self._path_with_dir_fd(
            src, self.rename, src_dir_fd, check_supported=False
        )
        dst = self._path_with_dir_fd(
            dst, self.rename, dst_dir_fd, check_supported=False
        )
        self.filesystem.rename(src, dst, force_replace=True)

    def rmdir(self, path: AnyStr, *, dir_fd: Optional[int] = None) -> None:
        """Remove a leaf Fake directory.

        Args:
            path: (str) Name of directory to remove.
            dir_fd: If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.

        Raises:
            OSError: if `path` does not exist or is not a directory,
            or as per FakeFilesystem.remove_object. Cannot remove '.'.
        """
        path = self._path_with_dir_fd(path, self.rmdir, dir_fd)
        self.filesystem.rmdir(path)

    def removedirs(self, name: AnyStr) -> None:
        """Remove a leaf fake directory and all empty intermediate ones.

        Args:
            name: the directory to be removed.

        Raises:
            OSError: if target_directory does not exist or is not a directory.
            OSError: if target_directory is not empty.
        """
        name = self.filesystem.absnormpath(name)
        directory = self.filesystem.confirmdir(name)
        if directory.entries:
            self.filesystem.raise_os_error(errno.ENOTEMPTY, self.path.basename(name))
        else:
            self.rmdir(name)
        head, tail = self.path.split(name)
        if not tail:
            head, tail = self.path.split(head)
        while head and tail:
            head_dir = self.filesystem.confirmdir(head)
            if head_dir.entries:
                break
            # only the top-level dir may not be a symlink
            self.filesystem.rmdir(head, allow_symlink=True)
            head, tail = self.path.split(head)

    def mkdir(
        self, path: AnyStr, mode: int = PERM_DEF, *, dir_fd: Optional[int] = None
    ) -> None:
        """Create a leaf Fake directory.

        Args:
            path: (str) Name of directory to create.
                Relative paths are assumed to be relative to '/'.
            mode: (int) Mode to create directory with.  This argument defaults
                to 0o777.  The umask is applied to this mode.
            dir_fd: If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.

        Raises:
            OSError: if the directory name is invalid or parent directory is
                read only or as per FakeFilesystem.add_object.
        """
        path = self._path_with_dir_fd(path, self.mkdir, dir_fd)
        try:
            self.filesystem.makedir(path, mode)
        except OSError as e:
            if e.errno == errno.EACCES:
                self.filesystem.raise_os_error(e.errno, path)
            raise

    def makedirs(
        self, name: AnyStr, mode: int = PERM_DEF, exist_ok: Optional[bool] = None
    ) -> None:
        """Create a leaf Fake directory + create any non-existent parent dirs.

        Args:
            name: (str) Name of directory to create.
            mode: (int) Mode to create directory (and any necessary parent
                directories) with. This argument defaults to 0o777.
                The umask is applied to this mode.
            exist_ok: (boolean) If exist_ok is False (the default), an OSError
                is raised if the target directory already exists.

        Raises:
            OSError: if the directory already exists and exist_ok=False, or as
                per :py:meth:`FakeFilesystem.create_dir`.
        """
        if exist_ok is None:
            exist_ok = False

        # copied and adapted from real implementation in os.py (Python 3.12)
        head, tail = self.filesystem.splitpath(name)
        if not tail:
            head, tail = self.filesystem.splitpath(head)
        if head and tail and not self.filesystem.exists(head):
            try:
                self.makedirs(head, exist_ok=exist_ok)
            except FileExistsError:
                pass
            cdir = self.filesystem.cwd
            if isinstance(tail, bytes):
                if tail == bytes(cdir, "ASCII"):
                    return
            elif tail == cdir:
                return
        try:
            self.mkdir(name, mode)
        except OSError:
            if not exist_ok or not self.filesystem.isdir(name):
                raise

    def _path_with_dir_fd(
        self,
        path: AnyStr,
        fct: Callable,
        dir_fd: Optional[int],
        check_supported: bool = True,
    ) -> AnyStr:
        """Return the path considering dir_fd. Raise on invalid parameters."""
        try:
            path = make_string_path(path)
        except TypeError:
            # the error is handled later
            path = path
        if dir_fd is not None:
            # check if fd is supported for the built-in real function
            if check_supported and (fct not in self.supports_dir_fd):
                raise NotImplementedError("dir_fd unavailable on this platform")
            if isinstance(path, int):
                raise ValueError(
                    "%s: Can't specify dir_fd without matching path_str" % fct.__name__
                )
            if not self.path.isabs(path):
                open_file = self.filesystem.get_open_file(dir_fd)
                return self.path.join(  # type: ignore[type-var, return-value]
                    cast(FakeFile, open_file.get_object()).path, path
                )
        return path

    def truncate(self, path: AnyStr, length: int) -> None:
        """Truncate the file corresponding to path, so that it is
         length bytes in size. If length is larger than the current size,
         the file is filled up with zero bytes.

        Args:
            path: (str or int) Path to the file, or an integer file
                descriptor for the file object.
            length: (int) Length of the file after truncating it.

        Raises:
            OSError: if the file does not exist or the file descriptor is
                invalid.
        """
        file_object = self.filesystem.resolve(path, allow_fd=True)
        file_object.size = length

    def ftruncate(self, fd: int, length: int) -> None:
        """Truncate the file corresponding to fd, so that it is
         length bytes in size. If length is larger than the current size,
         the file is filled up with zero bytes.

        Args:
            fd: (int) File descriptor for the file object.
            length: (int) Maximum length of the file after truncating it.

        Raises:
            OSError: if the file descriptor is invalid
        """
        file_object = self.filesystem.get_open_file(fd).get_object()
        if isinstance(file_object, FakeFileWrapper):
            file_object.size = length
        else:
            raise OSError(errno.EBADF, "Invalid file descriptor")

    def access(
        self,
        path: AnyStr,
        mode: int,
        *,
        dir_fd: Optional[int] = None,
        effective_ids: Optional[bool] = None,
        follow_symlinks: Optional[bool] = None,
    ) -> bool:
        """Check if a file exists and has the specified permissions.

        Args:
            path: (str) Path to the file.
            mode: (int) Permissions represented as a bitwise-OR combination of
                os.F_OK, os.R_OK, os.W_OK, and os.X_OK.
            dir_fd: If not `None`, the file descriptor of a directory, with
                `path` being relative to this directory.
            effective_ids: (bool) Unused. Only here to match the signature.
            follow_symlinks: (bool) If `False` and `path` points to a symlink,
                the link itself is queried instead of the linked object.

        Returns:
            bool, `True` if file is accessible, `False` otherwise.
        """
        if effective_ids is not None and self.filesystem.is_windows_fs:
            raise NotImplementedError(
                "access: effective_ids unavailable on this platform"
            )
        if follow_symlinks is None:
            # different behavior under Windows
            follow_symlinks = not self.filesystem.is_windows_fs
        elif self.filesystem.is_windows_fs:
            raise NotImplementedError(
                "access: follow_symlinks unavailable on this platform"
            )
        path = self._path_with_dir_fd(path, self.access, dir_fd)
        try:
            stat_result = self.stat(path, follow_symlinks=follow_symlinks)
        except OSError as os_error:
            if os_error.errno == errno.ENOENT:
                return False
            raise
        if is_root():
            if mode & os.X_OK:
                return stat_result.st_mode & 0o111 != 0
            return True
        return (mode & ((stat_result.st_mode >> 6) & 7)) == mode

    def fchmod(
        self,
        fd: int,
        mode: int,
    ) -> None:
        """Change the permissions of an open file as encoded in integer mode.

        Args:
            fd: (int) File descriptor.
            mode: (int) Permissions.
        """
        if self.filesystem.is_windows_fs and sys.version_info < (3, 13):
            raise AttributeError(
                "module 'os' has no attribute 'fchmod'. Did you mean: 'chmod'?"
            )
        self.filesystem.chmod(fd, mode)

    def chmod(
        self,
        path: AnyStr,
        mode: int,
        *,
        dir_fd: Optional[int] = None,
        follow_symlinks: bool = True,
    ) -> None:
        """Change the permissions of a file as encoded in integer mode.

        Args:
            path: (str) Path to the file.
            mode: (int) Permissions.
            dir_fd: If not `None`, the file descriptor of a directory, with
                `path` being relative to this directory.
            follow_symlinks: (bool) If `False` and `path` points to a symlink,
                the link itself is queried instead of the linked object.
        """
        if not follow_symlinks and (
            self.chmod not in self.supports_follow_symlinks or IS_PYPY
        ):
            raise NotImplementedError(
                "`follow_symlinks` for chmod() is not available on this system"
            )
        path = self._path_with_dir_fd(path, self.chmod, dir_fd)
        self.filesystem.chmod(path, mode, follow_symlinks)

    def lchmod(self, path: AnyStr, mode: int) -> None:
        """Change the permissions of a file as encoded in integer mode.
        If the file is a link, the permissions of the link are changed.

        Args:
          path: (str) Path to the file.
          mode: (int) Permissions.
        """
        if self.filesystem.is_windows_fs:
            raise NameError("name 'lchmod' is not defined")
        self.filesystem.chmod(path, mode, follow_symlinks=False)

    def utime(
        self,
        path: AnyStr,
        times: Optional[Tuple[Union[int, float], Union[int, float]]] = None,
        ns: Optional[Tuple[int, int]] = None,
        dir_fd: Optional[int] = None,
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
                If None, both times are set to the current time.
            dir_fd: If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.
            follow_symlinks: (bool) If `False` and `path` points to a symlink,
                the link itself is queried instead of the linked object.

            Raises:
                TypeError: If anything other than the expected types is
                    specified in the passed `times` or `ns` tuple,
                    or if the tuple length is not equal to 2.
                ValueError: If both times and ns are specified.
        """
        path = self._path_with_dir_fd(path, self.utime, dir_fd)
        self.filesystem.utime(path, times=times, ns=ns, follow_symlinks=follow_symlinks)

    def chown(
        self,
        path: AnyStr,
        uid: int,
        gid: int,
        *,
        dir_fd: Optional[int] = None,
        follow_symlinks: bool = True,
    ) -> None:
        """Set ownership of a faked file.

        Args:
            path: (str) Path to the file or directory.
            uid: (int) Numeric uid to set the file or directory to.
            gid: (int) Numeric gid to set the file or directory to.
            dir_fd: (int) If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.
            follow_symlinks: (bool) If `False` and path points to a symlink,
                the link itself is changed instead of the linked object.

        Raises:
            OSError: if path does not exist.

        `None` is also allowed for `uid` and `gid`.  This permits `os.rename`
        to use `os.chown` even when the source file `uid` and `gid` are
        `None` (unset).
        """
        path = self._path_with_dir_fd(path, self.chown, dir_fd)
        file_object = self.filesystem.resolve(path, follow_symlinks, allow_fd=True)
        if not isinstance(uid, int) or not isinstance(gid, int):
            raise TypeError("An integer is required")
        if uid != -1:
            file_object.st_uid = uid
        if gid != -1:
            file_object.st_gid = gid

    def mknod(
        self,
        path: AnyStr,
        mode: Optional[int] = None,
        device: int = 0,
        *,
        dir_fd: Optional[int] = None,
    ) -> None:
        """Create a filesystem node named 'filename'.

        Does not support device special files or named pipes as the real os
        module does.

        Args:
            path: (str) Name of the file to create
            mode: (int) Permissions to use and type of file to be created.
                Default permissions are 0o666.  Only the stat.S_IFREG file type
                is supported by the fake implementation.  The umask is applied
                to this mode.
            device: not supported in fake implementation
            dir_fd: If not `None`, the file descriptor of a directory,
                with `path` being relative to this directory.

        Raises:
          OSError: if called with unsupported options or the file can not be
          created.
        """
        if self.filesystem.is_windows_fs:
            raise AttributeError("module 'os' has no attribute 'mknode'")
        if mode is None:
            # note that a default value of 0o600 without a device type is
            # documented - this is not how it seems to work
            mode = S_IFREG | 0o600
        if device or not mode & S_IFREG and not is_root():
            self.filesystem.raise_os_error(errno.EPERM)

        path = self._path_with_dir_fd(path, self.mknod, dir_fd)
        head, tail = self.path.split(path)
        if not tail:
            if self.filesystem.exists(head, check_link=True):
                self.filesystem.raise_os_error(errno.EEXIST, path)
            self.filesystem.raise_os_error(errno.ENOENT, path)
        if tail in (matching_string(tail, "."), matching_string(tail, "..")):
            self.filesystem.raise_os_error(errno.ENOENT, path)
        if self.filesystem.exists(path, check_link=True):
            self.filesystem.raise_os_error(errno.EEXIST, path)
        self.filesystem.add_object(
            head,
            FakeFile(tail, mode & ~self.filesystem.umask, filesystem=self.filesystem),
        )

    def symlink(
        self,
        src: AnyStr,
        dst: AnyStr,
        target_is_directory: bool = False,
        *,
        dir_fd: Optional[int] = None,
    ) -> None:
        """Creates the specified symlink, pointed at the specified link target.

        Args:
            src: The target of the symlink.
            dst: Path to the symlink to create.
            target_is_directory: Currently ignored.
            dir_fd: If not `None`, the file descriptor of a directory,
                with `dst` being relative to this directory.

        Raises:
            OSError:  if the file already exists.
        """
        dst = self._path_with_dir_fd(dst, self.symlink, dir_fd)
        self.filesystem.create_symlink(dst, src, create_missing_dirs=False)

    def link(
        self,
        src: AnyStr,
        dst: AnyStr,
        *,
        src_dir_fd: Optional[int] = None,
        dst_dir_fd: Optional[int] = None,
        follow_symlinks: Optional[bool] = None,
    ) -> None:
        """Create a hard link at dst, pointing at src.

        Args:
            src: An existing path to the target file.
            dst: The destination path to create a new link at.
            src_dir_fd: If not `None`, the file descriptor of a directory,
                with `src` being relative to this directory.
            dst_dir_fd: If not `None`, the file descriptor of a directory,
                with `dst` being relative to this directory.
            follow_symlinks: (bool) If True (the default), symlinks in the
                path are traversed.

        Raises:
            OSError:  if something already exists at new_path.
            OSError:  if the parent directory doesn't exist.
        """
        if IS_PYPY and follow_symlinks is not None:
            raise OSError(errno.EINVAL, "Invalid argument: follow_symlinks")
        if follow_symlinks is None:
            follow_symlinks = True

        src = self._path_with_dir_fd(src, self.link, src_dir_fd)
        dst = self._path_with_dir_fd(dst, self.link, dst_dir_fd)
        self.filesystem.link(src, dst, follow_symlinks=follow_symlinks)

    def fsync(self, fd: int) -> None:
        """Perform fsync for a fake file (in other words, do nothing).

        Args:
            fd: The file descriptor of the open file.

        Raises:
            OSError: file_des is an invalid file descriptor.
            TypeError: file_des is not an integer.
        """
        # Throw an error if file_des isn't valid
        if 0 <= fd < NR_STD_STREAMS:
            self.filesystem.raise_os_error(errno.EINVAL)
        file_object = cast(FakeFileWrapper, self.filesystem.get_open_file(fd))
        if self.filesystem.is_windows_fs:
            if not hasattr(file_object, "allow_update") or not file_object.allow_update:
                self.filesystem.raise_os_error(errno.EBADF, file_object.file_path)

    def fdatasync(self, fd: int) -> None:
        """Perform fdatasync for a fake file (in other words, do nothing).

        Args:
            fd: The file descriptor of the open file.

        Raises:
            OSError: `fd` is an invalid file descriptor.
            TypeError: `fd` is not an integer.
        """
        if self.filesystem.is_windows_fs or self.filesystem.is_macos:
            raise AttributeError("module 'os' has no attribute 'fdatasync'")
        # Throw an error if file_des isn't valid
        if 0 <= fd < NR_STD_STREAMS:
            self.filesystem.raise_os_error(errno.EINVAL)
        self.filesystem.get_open_file(fd)

    def sendfile(self, fd_out: int, fd_in: int, offset: int, count: int) -> int:
        """Copy count bytes from file descriptor fd_in to file descriptor
        fd_out starting at offset.

        Args:
            fd_out: The file descriptor of the destination file.
            fd_in: The file descriptor of the source file.
            offset: The offset in bytes where to start the copy in the
                source file. If `None` (Linux only), copying is started at
                the current position, and the position is updated.
            count: The number of bytes to copy. If 0, all remaining bytes
                are copied (MacOs only).

        Raises:
            OSError: If `fd_in` or `fd_out` is an invalid file descriptor.
            TypeError: If `fd_in` or `fd_out` is not an integer.
            TypeError: If `offset` is None under MacOs.
        """
        if self.filesystem.is_windows_fs:
            raise AttributeError("module 'os' has no attribute 'sendfile'")
        if 0 <= fd_in < NR_STD_STREAMS:
            self.filesystem.raise_os_error(errno.EINVAL)
        if 0 <= fd_out < NR_STD_STREAMS:
            self.filesystem.raise_os_error(errno.EINVAL)
        source = cast(FakeFileWrapper, self.filesystem.get_open_file(fd_in))
        dest = cast(FakeFileWrapper, self.filesystem.get_open_file(fd_out))
        if self.filesystem.is_macos:
            if dest.get_object().stat_result.st_mode & 0o777000 != S_IFSOCK:
                raise OSError("Socket operation on non-socket")
        if offset is None:
            if self.filesystem.is_macos:
                raise TypeError("None is not a valid offset")
            contents = source.read(count)
        else:
            position = source.tell()
            source.seek(offset)
            if count == 0 and self.filesystem.is_macos:
                contents = source.read()
            else:
                contents = source.read(count)
            source.seek(position)
        if contents:
            written = dest.write(contents)
            dest.flush()
            return written
        return 0

    def getuid(self) -> int:
        """Returns the user id set in the fake filesystem.
        If not changed using ``set_uid``, this is the uid of the real system.
        """
        if self.filesystem.is_windows_fs:
            raise NameError("name 'getuid' is not defined")
        return get_uid()

    def getgid(self) -> int:
        """Returns the group id set in the fake filesystem.
        If not changed using ``set_gid``, this is the gid of the real system.
        """
        if self.filesystem.is_windows_fs:
            raise NameError("name 'getgid' is not defined")
        return get_gid()

    def fake_functions(self, original_functions) -> Set:
        functions = set()
        for fn in original_functions:
            if hasattr(self, fn.__name__):
                functions.add(getattr(self, fn.__name__))
            else:
                functions.add(fn)
        return functions

    @property
    def supports_follow_symlinks(self) -> Set[Callable]:
        if self._supports_follow_symlinks is None:
            self._supports_follow_symlinks = self.fake_functions(
                self.os_module.supports_follow_symlinks
            )
        return self._supports_follow_symlinks

    @property
    def supports_dir_fd(self) -> Set[Callable]:
        if self._supports_dir_fd is None:
            self._supports_dir_fd = self.fake_functions(self.os_module.supports_dir_fd)
        return self._supports_dir_fd

    @property
    def supports_fd(self) -> Set[Callable]:
        if self._supports_fd is None:
            self._supports_fd = self.fake_functions(self.os_module.supports_fd)
        return self._supports_fd

    @property
    def supports_effective_ids(self) -> Set[Callable]:
        if self._supports_effective_ids is None:
            self._supports_effective_ids = self.fake_functions(
                self.os_module.supports_effective_ids
            )
        return self._supports_effective_ids

    def __getattr__(self, name: str) -> Any:
        """Forwards any unfaked calls to the standard os module."""
        return getattr(self.os_module, name)


def handle_original_call(f: Callable) -> Callable:
    """Decorator used for real pathlib Path methods to ensure that
    real os functions instead of faked ones are used.
    Applied to all non-private methods of `FakeOsModule`."""

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        should_use_original = FakeOsModule.use_original

        if not should_use_original and args:
            self = args[0]
            fs: FakeFilesystem = self.filesystem
            if self.filesystem.patcher:
                skip_names = fs.patcher.skip_names
                if is_called_from_skipped_module(
                    skip_names=skip_names,
                    case_sensitive=fs.is_case_sensitive,
                ):
                    should_use_original = True

        if should_use_original:
            # remove the `self` argument for FakeOsModule methods
            if args and isinstance(args[0], FakeOsModule):
                args = args[1:]
            return getattr(os, f.__name__)(*args, **kwargs)

        return f(*args, **kwargs)

    return wrapped


for name, fn in inspect.getmembers(FakeOsModule, inspect.isfunction):
    if not fn.__name__.startswith("_"):
        setattr(FakeOsModule, name, handle_original_call(fn))


@contextmanager
def use_original_os():
    """Temporarily use original os functions instead of faked ones.
    Used to ensure that skipped modules do not use faked calls.
    """
    try:
        FakeOsModule.use_original = True
        yield
    finally:
        FakeOsModule.use_original = False
