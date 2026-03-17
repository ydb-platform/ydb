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

"""Faked ``os.path`` module replacement. See ``fake_filesystem`` for usage."""

import errno
import functools
import inspect
import os
import sys
from stat import (
    S_IFDIR,
    S_IFMT,
)
from types import ModuleType
from typing import (
    Callable,
    List,
    Optional,
    Union,
    Any,
    Dict,
    Tuple,
    AnyStr,
    overload,
    ClassVar,
    TYPE_CHECKING,
)

from pyfakefs.helpers import (
    is_called_from_skipped_module,
    make_string_path,
    to_string,
    matching_string,
    to_bytes,
)

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem
    from pyfakefs.fake_os import FakeOsModule


def _copy_module(old: ModuleType) -> ModuleType:
    """Recompiles and creates new module object."""
    saved = sys.modules.pop(old.__name__, None)
    new = __import__(old.__name__)
    if saved is not None:
        sys.modules[old.__name__] = saved
    return new


class FakePathModule:
    """Faked os.path module replacement.

    FakePathModule should *only* be instantiated by FakeOsModule.  See the
    FakeOsModule docstring for details.
    """

    _OS_PATH_COPY: Any = _copy_module(os.path)

    devnull: ClassVar[str] = ""
    sep: ClassVar[str] = ""
    altsep: ClassVar[Optional[str]] = None
    linesep: ClassVar[str] = ""
    pathsep: ClassVar[str] = ""

    @staticmethod
    def dir() -> List[str]:
        """Return the list of patched function names. Used for patching
        functions imported from the module.
        """
        dir_list = [
            "abspath",
            "dirname",
            "exists",
            "expanduser",
            "getatime",
            "getctime",
            "getmtime",
            "getsize",
            "isabs",
            "isdir",
            "isfile",
            "islink",
            "ismount",
            "join",
            "lexists",
            "normcase",
            "normpath",
            "realpath",
            "relpath",
            "split",
            "splitdrive",
            "samefile",
        ]
        if sys.version_info >= (3, 12):
            dir_list += ["isjunction", "splitroot"]
        return dir_list

    def __init__(self, filesystem: "FakeFilesystem", os_module: "FakeOsModule"):
        """Init.

        Args:
            filesystem: FakeFilesystem used to provide file system information
        """
        self.filesystem = filesystem
        self._os_path = self._OS_PATH_COPY
        self._os_path.os = self.os = os_module  # type: ignore[attr-defined]
        self.reset(filesystem)

    @classmethod
    def reset(cls, filesystem: "FakeFilesystem") -> None:
        cls.sep = filesystem.path_separator
        cls.altsep = filesystem.alternative_path_separator
        cls.linesep = filesystem.line_separator
        cls.devnull = filesystem.devnull
        cls.pathsep = filesystem.pathsep

    def exists(self, path: AnyStr) -> bool:
        """Determine whether the file object exists within the fake filesystem.

        Args:
            path: The path to the file object.

        Returns:
            (bool) `True` if the file exists.
        """
        return self.filesystem.exists(path)

    def lexists(self, path: AnyStr) -> bool:
        """Test whether a path exists.  Returns True for broken symbolic links.

        Args:
          path:  path to the symlink object.

        Returns:
          bool (if file exists).
        """
        return self.filesystem.exists(path, check_link=True)

    def getsize(self, path: AnyStr):
        """Return the file object size in bytes.

        Args:
          path:  path to the file object.

        Returns:
          file size in bytes.
        """
        file_obj = self.filesystem.resolve(path)
        if (
            self.filesystem.ends_with_path_separator(path)
            and S_IFMT(file_obj.st_mode) != S_IFDIR
        ):
            error_nr = errno.EINVAL if self.filesystem.is_windows_fs else errno.ENOTDIR
            self.filesystem.raise_os_error(error_nr, path)
        return file_obj.st_size

    def isabs(self, path: AnyStr) -> bool:
        """Return True if path is an absolute pathname."""
        empty = matching_string(path, "")
        if self.filesystem.is_windows_fs:
            drive, path = self.splitdrive(path)
        else:
            drive = empty
        path = make_string_path(path)
        if not self.filesystem.starts_with_sep(path):
            return False
        if self.filesystem.is_windows_fs and sys.version_info >= (3, 13):
            # from Python 3.13 on, a path under Windows starting with a single separator
            # (e.g. not a drive and not an UNC path) is no more considered absolute
            return drive != empty
        return True

    def isdir(self, path: AnyStr) -> bool:
        """Determine if path identifies a directory."""
        return self.filesystem.isdir(path)

    def isfile(self, path: AnyStr) -> bool:
        """Determine if path identifies a regular file."""
        return self.filesystem.isfile(path)

    def islink(self, path: AnyStr) -> bool:
        """Determine if path identifies a symbolic link.

        Args:
            path: Path to filesystem object.

        Returns:
            `True` if path points to a symbolic link.

        Raises:
            TypeError: if path is None.
        """
        return self.filesystem.islink(path)

    if sys.version_info >= (3, 12):

        def isjunction(self, path: AnyStr) -> bool:
            """Returns False. Junctions are never faked."""
            return self.filesystem.isjunction(path)

        def splitroot(self, path: AnyStr):
            """Split a pathname into drive, root and tail.
            Implementation taken from ntpath and posixpath.
            """
            return self.filesystem.splitroot(path)

    if sys.version_info >= (3, 13):

        def isreserved(self, path):
            if not self.filesystem.is_windows_fs:
                raise AttributeError("module 'os' has no attribute 'isreserved'")

            return self.filesystem.isreserved(path)

    def getmtime(self, path: AnyStr) -> float:
        """Returns the modification time of the fake file.

        Args:
            path: the path to fake file.

        Returns:
            (int, float) the modification time of the fake file
                         in number of seconds since the epoch.

        Raises:
            OSError: if the file does not exist.
        """
        try:
            file_obj = self.filesystem.resolve(path)
            return file_obj.st_mtime
        except OSError:
            self.filesystem.raise_os_error(
                errno.ENOENT, winerror=3
            )  # pytype: disable=bad-return-type

    def getatime(self, path: AnyStr) -> float:
        """Returns the last access time of the fake file.

        Note: Access time is not set automatically in fake filesystem
            on access.

        Args:
            path: the path to fake file.

        Returns:
            (int, float) the access time of the fake file in number of seconds
                since the epoch.

        Raises:
            OSError: if the file does not exist.
        """
        try:
            file_obj = self.filesystem.resolve(path)
        except OSError:
            self.filesystem.raise_os_error(errno.ENOENT)
        return file_obj.st_atime  # pytype: disable=name-error

    def getctime(self, path: AnyStr) -> float:
        """Returns the creation time of the fake file.

        Args:
            path: the path to fake file.

        Returns:
            (int, float) the creation time of the fake file in number of
                seconds since the epoch.

        Raises:
            OSError: if the file does not exist.
        """
        try:
            file_obj = self.filesystem.resolve(path)
        except OSError:
            self.filesystem.raise_os_error(errno.ENOENT)
        return file_obj.st_ctime  # pytype: disable=name-error

    def abspath(self, path: AnyStr) -> AnyStr:
        """Return the absolute version of a path."""

        def getcwd():
            """Return the current working directory."""
            # pylint: disable=undefined-variable
            if isinstance(path, bytes):
                return self.os.getcwdb()
            else:
                return self.os.getcwd()

        path = make_string_path(path)
        if not self.isabs(path):
            path = self.join(getcwd(), path)
        elif self.filesystem.is_windows_fs and self.filesystem.starts_with_sep(path):
            cwd = getcwd()
            if self.filesystem.starts_with_drive_letter(cwd):
                path = self.join(cwd[:2], path)
        return self.normpath(path)

    def join(self, *p: AnyStr) -> AnyStr:
        """Return the completed path with a separator of the parts."""
        return self.filesystem.joinpaths(*p)

    def split(self, path: AnyStr) -> Tuple[AnyStr, AnyStr]:
        """Split the path into the directory and the filename of the path."""
        return self.filesystem.splitpath(path)

    def splitdrive(self, path: AnyStr) -> Tuple[AnyStr, AnyStr]:
        """Split the path into the drive part and the rest of the path, if
        supported."""
        return self.filesystem.splitdrive(path)

    def normpath(self, path: AnyStr) -> AnyStr:
        """Normalize path, eliminating double slashes, etc."""
        return self.filesystem.normpath(path)

    def normcase(self, path: AnyStr) -> AnyStr:
        """Convert to lower case under windows, replaces additional path
        separator."""
        path = self.filesystem.normcase(path)
        if self.filesystem.is_windows_fs:
            path = path.lower()
        return path

    def relpath(self, path: AnyStr, start: Optional[AnyStr] = None) -> AnyStr:
        """We mostly rely on the native implementation and adapt the
        path separator."""
        if not path:
            raise ValueError("no path specified")
        path = make_string_path(path)
        path = self.filesystem.replace_windows_root(path)
        sep = matching_string(path, self.filesystem.path_separator)
        if start is not None:
            start = make_string_path(start)
        else:
            start = matching_string(path, self.filesystem.cwd)
        start = self.filesystem.replace_windows_root(start)
        system_sep = matching_string(path, self._os_path.sep)
        if self.filesystem.alternative_path_separator is not None:
            altsep = matching_string(path, self.filesystem.alternative_path_separator)
            path = path.replace(altsep, system_sep)
            start = start.replace(altsep, system_sep)
        path = path.replace(sep, system_sep)
        start = start.replace(sep, system_sep)
        path = self._os_path.relpath(path, start)
        return path.replace(system_sep, sep)

    def realpath(self, filename: AnyStr, strict: Optional[bool] = None) -> AnyStr:
        """Return the canonical path of the specified filename, eliminating any
        symbolic links encountered in the path.
        """
        if strict is not None and sys.version_info < (3, 10):
            raise TypeError("realpath() got an unexpected keyword argument 'strict'")
        if strict:
            # raises in strict mode if the file does not exist
            self.filesystem.resolve(filename)
        if self.filesystem.is_windows_fs:
            return self.abspath(filename)
        filename = make_string_path(filename)
        path, ok = self._join_real_path(filename[:0], filename, {})
        path = self.abspath(path)
        return path

    def samefile(self, path1: AnyStr, path2: AnyStr) -> bool:
        """Return whether path1 and path2 point to the same file.

        Args:
            path1: first file path or path object
            path2: second file path or path object

        Raises:
            OSError: if one of the paths does not point to an existing
                file system object.
        """
        stat1 = self.filesystem.stat(path1)
        stat2 = self.filesystem.stat(path2)
        return stat1.st_ino == stat2.st_ino and stat1.st_dev == stat2.st_dev

    @overload
    def _join_real_path(
        self, path: str, rest: str, seen: Dict[str, Optional[str]]
    ) -> Tuple[str, bool]: ...

    @overload
    def _join_real_path(
        self, path: bytes, rest: bytes, seen: Dict[bytes, Optional[bytes]]
    ) -> Tuple[bytes, bool]: ...

    def _join_real_path(
        self, path: AnyStr, rest: AnyStr, seen: Dict[AnyStr, Optional[AnyStr]]
    ) -> Tuple[AnyStr, bool]:
        """Join two paths, normalizing and eliminating any symbolic links
        encountered in the second path.
        Taken from Python source and adapted.
        """
        curdir = matching_string(path, ".")
        pardir = matching_string(path, "..")

        sep = self.filesystem.get_path_separator(path)
        if self.isabs(rest):
            rest = rest[1:]
            path = sep

        while rest:
            name, _, rest = rest.partition(sep)
            if not name or name == curdir:
                # current dir
                continue
            if name == pardir:
                # parent dir
                if path:
                    path, name = self.filesystem.splitpath(path)
                    if name == pardir:
                        path = self.filesystem.joinpaths(path, pardir, pardir)
                else:
                    path = pardir
                continue
            newpath = self.filesystem.joinpaths(path, name)
            if not self.filesystem.islink(newpath):
                path = newpath
                continue
            # Resolve the symbolic link
            if newpath in seen:
                # Already seen this path
                seen_path = seen[newpath]
                if seen_path is not None:
                    # use cached value
                    path = seen_path
                    continue
                # The symlink is not resolved, so we must have a symlink loop.
                # Return already resolved part + rest of the path unchanged.
                return self.filesystem.joinpaths(newpath, rest), False
            seen[newpath] = None  # not resolved symlink
            path, ok = self._join_real_path(
                path,
                matching_string(path, self.filesystem.readlink(newpath)),
                seen,
            )
            if not ok:
                return self.filesystem.joinpaths(path, rest), False
            seen[newpath] = path  # resolved symlink
        return path, True

    def dirname(self, path: AnyStr) -> AnyStr:
        """Returns the first part of the result of `split()`."""
        return self.split(path)[0]

    def expanduser(self, path: AnyStr) -> AnyStr:
        """Return the argument with an initial component of ~ or ~user
        replaced by that user's home directory.
        """
        path = self._os_path.expanduser(path)
        return path.replace(
            matching_string(path, self._os_path.sep),
            matching_string(path, self.sep),
        )

    def ismount(self, path: AnyStr) -> bool:
        """Return true if the given path is a mount point.

        Args:
            path: Path to filesystem object to be checked

        Returns:
            `True` if path is a mount point added to the fake file system.
            Under Windows also returns True for drive and UNC roots
            (independent of their existence).
        """
        if not path:
            return False
        path_str = to_string(make_string_path(path))
        normed_path = self.filesystem.absnormpath(path_str)
        sep = self.filesystem.path_separator
        if self.filesystem.is_windows_fs:
            path_seps: Union[Tuple[str, Optional[str]], Tuple[str]]
            if self.filesystem.alternative_path_separator is not None:
                path_seps = (sep, self.filesystem.alternative_path_separator)
            else:
                path_seps = (sep,)
            drive, rest = self.filesystem.splitdrive(normed_path)
            if drive and drive[:1] in path_seps:
                return (not rest) or (rest in path_seps)
            if rest in path_seps:
                return True
        for mount_point in self.filesystem.mount_points:
            if to_string(normed_path).rstrip(sep) == to_string(mount_point).rstrip(sep):
                return True
        return False

    def __getattr__(self, name: str) -> Any:
        """Forwards any non-faked calls to the real os.path."""
        return getattr(self._os_path, name)


if sys.platform == "win32":

    class FakeNtModule:
        """Under windows, a few function of `os.path` are taken from the `nt` module
        for performance reasons. These are patched here.
        """

        @staticmethod
        def dir():
            if sys.version_info >= (3, 12):
                return ["_path_exists", "_path_isfile", "_path_isdir", "_path_islink"]
            else:
                return ["_isdir"]

        def __init__(self, filesystem: "FakeFilesystem"):
            """Init.

            Args:
                filesystem: FakeFilesystem used to provide file system information
            """
            import nt  # type:ignore[import]

            self.filesystem = filesystem
            self.nt_module: Any = nt

        def getcwd(self) -> str:
            """Return current working directory."""
            return to_string(self.filesystem.cwd)

        def getcwdb(self) -> bytes:
            """Return current working directory as bytes."""
            return to_bytes(self.filesystem.cwd)

        if sys.version_info >= (3, 12):

            def _path_isdir(self, path: AnyStr) -> bool:
                return self.filesystem.isdir(path)

            def _path_isfile(self, path: AnyStr) -> bool:
                return self.filesystem.isfile(path)

            def _path_islink(self, path: AnyStr) -> bool:
                return self.filesystem.islink(path)

            def _path_exists(self, path: AnyStr) -> bool:
                return self.filesystem.exists(path)

        else:

            def _isdir(self, path: AnyStr) -> bool:
                return self.filesystem.isdir(path)

        def __getattr__(self, name: str) -> Any:
            """Forwards any non-faked calls to the real nt module."""
            return getattr(self.nt_module, name)


def handle_original_call(f: Callable) -> Callable:
    """Decorator used for real pathlib Path methods to ensure that
    real os functions instead of faked ones are used.
    Applied to all non-private methods of `FakePathModule`."""

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if args:
            self = args[0]
            should_use_original = self.os.use_original
            if not should_use_original and self.filesystem.patcher:
                skip_names = self.filesystem.patcher.skip_names
                if is_called_from_skipped_module(
                    skip_names=skip_names,
                    case_sensitive=self.filesystem.is_case_sensitive,
                ):
                    should_use_original = True

            if should_use_original:
                # remove the `self` argument for FakePathModule methods
                if args and isinstance(args[0], FakePathModule):
                    args = args[1:]
                return getattr(os.path, f.__name__)(*args, **kwargs)

        return f(*args, **kwargs)

    return wrapped


for name, fn in inspect.getmembers(FakePathModule, inspect.isfunction):
    if not fn.__name__.startswith("_"):
        setattr(FakePathModule, name, handle_original_call(fn))
