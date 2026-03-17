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

"""Helper classes use for fake file system implementation."""

import ctypes
import importlib
import io
import locale
import os
import platform
import stat
import sys
import sysconfig
import time
import traceback
from collections import namedtuple
from copy import copy
from dataclasses import dataclass
from enum import Enum
from stat import S_IFLNK
from typing import Union, Optional, Any, AnyStr, overload, cast

AnyString = Union[str, bytes]
AnyPath = Union[AnyStr, os.PathLike]

IS_PYPY = platform.python_implementation() == "PyPy"
IS_WIN = sys.platform == "win32"
IN_DOCKER = os.path.exists("/.dockerenv")

PERM_READ = 0o400  # Read permission bit.
PERM_WRITE = 0o200  # Write permission bit.
PERM_EXE = 0o100  # Execute permission bit.
PERM_DEF = 0o777  # Default permission bits.
PERM_DEF_FILE = 0o666  # Default permission bits (regular file)
PERM_ALL = 0o7777  # All permission bits.

STDLIB_PATH = os.path.realpath(sysconfig.get_path("stdlib"))
PYFAKEFS_PATH = os.path.dirname(__file__)
PYFAKEFS_TEST_PATHS = [
    os.path.join(PYFAKEFS_PATH, "tests"),
    os.path.join(PYFAKEFS_PATH, "pytest_tests"),
]

_OpenModes = namedtuple(
    "_OpenModes",
    "must_exist can_read can_write truncate append must_not_exist",
)

if sys.platform == "win32":
    fake_id = 0 if ctypes.windll.shell32.IsUserAnAdmin() else 1
    USER_ID = fake_id
    GROUP_ID = fake_id
else:
    USER_ID = os.getuid()
    GROUP_ID = os.getgid()


def get_uid() -> int:
    """Get the global user id. Same as ``os.getuid()``"""
    return USER_ID


def set_uid(uid: int) -> None:
    """Set the global user id. This is used as st_uid for new files
    and to differentiate between a normal user and the root user (uid 0).
    For the root user, some permission restrictions are ignored.

    Args:
        uid: (int) the user ID of the user calling the file system functions.
    """
    global USER_ID
    USER_ID = uid


def get_gid() -> int:
    """Get the global group id. Same as ``os.getgid()``"""
    return GROUP_ID


def set_gid(gid: int) -> None:
    """Set the global group id. This is only used to set st_gid for new files,
    no permission checks are performed.

    Args:
        gid: (int) the group ID of the user calling the file system functions.
    """
    global GROUP_ID
    GROUP_ID = gid


def reset_ids() -> None:
    """Set the global user ID and group ID back to default values."""
    if sys.platform == "win32":
        reset_id = 0 if ctypes.windll.shell32.IsUserAnAdmin() else 1
        set_uid(reset_id)
        set_gid(reset_id)
    else:
        set_uid(os.getuid())
        set_gid(os.getgid())


def is_root() -> bool:
    """Return True if the current user is the root user."""
    return USER_ID == 0


def is_int_type(val: Any) -> bool:
    """Return True if `val` is of integer type."""
    return isinstance(val, int)


def is_byte_string(val: Any) -> bool:
    """Return True if `val` is a bytes-like object, False for a unicode
    string."""
    return not hasattr(val, "encode")


def is_unicode_string(val: Any) -> bool:
    """Return True if `val` is a unicode string, False for a bytes-like
    object."""
    return hasattr(val, "encode")


def get_locale_encoding():
    if sys.version_info >= (3, 11):
        return locale.getencoding()
    return locale.getpreferredencoding(False)


@overload
def make_string_path(dir_name: AnyStr) -> AnyStr: ...


@overload
def make_string_path(dir_name: os.PathLike) -> str: ...


def make_string_path(dir_name: AnyPath) -> AnyStr:  # type: ignore[type-var]
    return cast(AnyStr, os.fspath(dir_name))  # pytype: disable=invalid-annotation


def to_string(path: Union[AnyStr, Union[str, bytes]]) -> str:
    """Return the string representation of a byte string using the preferred
    encoding, or the string itself if path is a str."""
    if isinstance(path, bytes):
        return path.decode(get_locale_encoding())
    return path


def to_bytes(path: Union[AnyStr, Union[str, bytes]]) -> bytes:
    """Return the bytes representation of a string using the preferred
    encoding, or the byte string itself if path is a byte string."""
    if isinstance(path, str):
        return bytes(path, get_locale_encoding())
    return path


def join_strings(s1: AnyStr, s2: AnyStr) -> AnyStr:
    """This is a bit of a hack to satisfy mypy - may be refactored."""
    return s1 + s2


def real_encoding(encoding: Optional[str]) -> Optional[str]:
    """Since Python 3.10, the new function ``io.text_encoding`` returns
    "locale" as the encoding if None is defined. This will be handled
    as no encoding in pyfakefs."""
    if sys.version_info >= (3, 10):
        return encoding if encoding != "locale" else None
    return encoding


def now():
    return time.time()


@overload
def matching_string(matched: bytes, string: AnyStr) -> bytes: ...


@overload
def matching_string(matched: str, string: AnyStr) -> str: ...


@overload
def matching_string(matched: AnyStr, string: None) -> None: ...


def matching_string(  # type: ignore[misc]
    matched: AnyStr, string: Optional[AnyStr]
) -> Optional[AnyString]:
    """Return the string as byte or unicode depending
    on the type of matched, assuming string is an ASCII string.
    """
    if string is None:
        return string
    if isinstance(matched, bytes) and isinstance(string, str):
        return string.encode(get_locale_encoding())
    return string  # pytype: disable=bad-return-type


@dataclass
class FSProperties:
    sep: str
    altsep: Optional[str]
    pathsep: str
    linesep: str
    devnull: str


# pure POSIX file system properties, for use with PosixPath
POSIX_PROPERTIES = FSProperties(
    sep="/",
    altsep=None,
    pathsep=":",
    linesep="\n",
    devnull="/dev/null",
)

# pure Windows file system properties, for use with WindowsPath
WINDOWS_PROPERTIES = FSProperties(
    sep="\\",
    altsep="/",
    pathsep=";",
    linesep="\r\n",
    devnull="NUL",
)


class FSType(Enum):
    """Defines which file system properties to use."""

    DEFAULT = 0  # use current OS file system + modifications in fake file system
    POSIX = 1  # pure POSIX properties, for use in PosixPath
    WINDOWS = 2  # pure Windows properties, for use in WindowsPath


class FakeStatResult:
    """Mimics os.stat_result for use as return type of `stat()` and similar.
    This is needed as `os.stat_result` has no possibility to set
    nanosecond times directly.
    """

    def __init__(
        self,
        is_windows: bool,
        user_id: int,
        group_id: int,
        initial_time: Optional[float] = None,
    ):
        self.st_mode: int = 0
        self.st_ino: Optional[int] = None
        self.st_dev: int = 0
        self.st_nlink: int = 0
        self.st_uid: int = user_id
        self.st_gid: int = group_id
        self._st_size: int = 0
        self.is_windows: bool = is_windows
        self._st_atime_ns: int = int((initial_time or 0) * 1e9)
        self._st_mtime_ns: int = self._st_atime_ns
        self._st_ctime_ns: int = self._st_atime_ns

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FakeStatResult)
            and self._st_atime_ns == other._st_atime_ns
            and self._st_ctime_ns == other._st_ctime_ns
            and self._st_mtime_ns == other._st_mtime_ns
            and self.st_size == other.st_size
            and self.st_gid == other.st_gid
            and self.st_uid == other.st_uid
            and self.st_nlink == other.st_nlink
            and self.st_dev == other.st_dev
            and self.st_ino == other.st_ino
            and self.st_mode == other.st_mode
        )

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def copy(self) -> "FakeStatResult":
        """Return a copy where the float usage is hard-coded to mimic the
        behavior of the real os.stat_result.
        """
        stat_result = copy(self)
        return stat_result

    def set_from_stat_result(self, stat_result: os.stat_result) -> None:
        """Set values from a real os.stat_result.
        Note: values that are controlled by the fake filesystem are not set.
        This includes st_ino, st_dev and st_nlink.
        """
        self.st_mode = stat_result.st_mode
        self.st_uid = stat_result.st_uid
        self.st_gid = stat_result.st_gid
        self._st_size = stat_result.st_size
        self._st_atime_ns = stat_result.st_atime_ns
        self._st_mtime_ns = stat_result.st_mtime_ns
        self._st_ctime_ns = stat_result.st_ctime_ns

    @property
    def st_ctime(self) -> Union[int, float]:
        """Return the creation time in seconds."""
        return self._st_ctime_ns / 1e9

    @st_ctime.setter
    def st_ctime(self, val: Union[int, float]) -> None:
        """Set the creation time in seconds."""
        self._st_ctime_ns = int(val * 1e9)

    @property
    def st_atime(self) -> Union[int, float]:
        """Return the access time in seconds."""
        return self._st_atime_ns / 1e9

    @st_atime.setter
    def st_atime(self, val: Union[int, float]) -> None:
        """Set the access time in seconds."""
        self._st_atime_ns = int(val * 1e9)

    @property
    def st_mtime(self) -> Union[int, float]:
        """Return the modification time in seconds."""
        return self._st_mtime_ns / 1e9

    @st_mtime.setter
    def st_mtime(self, val: Union[int, float]) -> None:
        """Set the modification time in seconds."""
        self._st_mtime_ns = int(val * 1e9)

    @property
    def st_size(self) -> int:
        if self.st_mode & S_IFLNK == S_IFLNK and self.is_windows:
            return 0
        return self._st_size

    @st_size.setter
    def st_size(self, val: int) -> None:
        self._st_size = val

    @property
    def st_blocks(self) -> int:
        """Return the number of 512-byte blocks allocated for the file.
        Assumes a page size of 4096 (matches most systems).
        Ignores that this may not be available under some systems,
        and that the result may differ if the file has holes.
        """
        if self.is_windows:
            raise AttributeError("'os.stat_result' object has no attribute 'st_blocks'")
        page_size = 4096
        blocks_in_page = page_size // 512
        pages = self._st_size // page_size
        if self._st_size % page_size:
            pages += 1
        return pages * blocks_in_page

    @property
    def st_file_attributes(self) -> int:
        if not self.is_windows:
            raise AttributeError(
                "module 'os.stat_result' has no attribute 'st_file_attributes'"
            )
        mode = 0
        st_mode = self.st_mode
        if st_mode & stat.S_IFDIR:
            mode |= stat.FILE_ATTRIBUTE_DIRECTORY  # type:ignore[attr-defined]
        if st_mode & stat.S_IFREG:
            mode |= stat.FILE_ATTRIBUTE_NORMAL  # type:ignore[attr-defined]
        if st_mode & (stat.S_IFCHR | stat.S_IFBLK):
            mode |= stat.FILE_ATTRIBUTE_DEVICE  # type:ignore[attr-defined]
        if st_mode & stat.S_IFLNK:
            mode |= stat.FILE_ATTRIBUTE_REPARSE_POINT  # type:ignore
        return mode

    @property
    def st_reparse_tag(self) -> int:
        if not self.is_windows or sys.version_info < (3, 8):
            raise AttributeError(
                "module 'os.stat_result' has no attribute 'st_reparse_tag'"
            )
        if self.st_mode & stat.S_IFLNK:
            return stat.IO_REPARSE_TAG_SYMLINK  # type: ignore[attr-defined]
        return 0

    def __getitem__(self, item: int) -> Optional[int]:
        """Implement item access to mimic `os.stat_result` behavior."""
        import stat

        if item == stat.ST_MODE:
            return self.st_mode
        if item == stat.ST_INO:
            return self.st_ino
        if item == stat.ST_DEV:
            return self.st_dev
        if item == stat.ST_NLINK:
            return self.st_nlink
        if item == stat.ST_UID:
            return self.st_uid
        if item == stat.ST_GID:
            return self.st_gid
        if item == stat.ST_SIZE:
            return self.st_size
        if item == stat.ST_ATIME:
            # item access always returns int for backward compatibility
            return int(self.st_atime)
        if item == stat.ST_MTIME:
            return int(self.st_mtime)
        if item == stat.ST_CTIME:
            return int(self.st_ctime)
        raise ValueError("Invalid item")

    @property
    def st_atime_ns(self) -> int:
        """Return the access time in nanoseconds."""
        return self._st_atime_ns

    @st_atime_ns.setter
    def st_atime_ns(self, val: int) -> None:
        """Set the access time in nanoseconds."""
        self._st_atime_ns = val

    @property
    def st_mtime_ns(self) -> int:
        """Return the modification time in nanoseconds."""
        return self._st_mtime_ns

    @st_mtime_ns.setter
    def st_mtime_ns(self, val: int) -> None:
        """Set the modification time of the fake file in nanoseconds."""
        self._st_mtime_ns = val

    @property
    def st_ctime_ns(self) -> int:
        """Return the creation time in nanoseconds."""
        return self._st_ctime_ns

    @st_ctime_ns.setter
    def st_ctime_ns(self, val: int) -> None:
        """Set the creation time of the fake file in nanoseconds."""
        self._st_ctime_ns = val


class BinaryBufferIO(io.BytesIO):
    """Stream class that handles byte contents for files."""

    def __init__(self, contents: Optional[bytes]):
        super().__init__(contents or b"")

    def putvalue(self, value: bytes) -> None:
        self.write(value)


class TextBufferIO(io.TextIOWrapper):
    """Stream class that handles Python string contents for files."""

    def __init__(
        self,
        contents: Optional[bytes] = None,
        newline: Optional[str] = None,
        encoding: Optional[str] = None,
        errors: str = "strict",
    ):
        self._bytestream = io.BytesIO(contents or b"")
        super().__init__(self._bytestream, encoding, errors, newline)

    def getvalue(self) -> bytes:
        return self._bytestream.getvalue()

    def putvalue(self, value: bytes) -> None:
        self._bytestream.write(value)


def is_called_from_skipped_module(
    skip_names: list, case_sensitive: bool, check_open_code: bool = False
) -> bool:
    def starts_with(path, string):
        if case_sensitive:
            return path.startswith(string)
        return path.lower().startswith(string.lower())

    # in most cases we don't have skip names and won't need the overhead
    # of analyzing the traceback, except when checking for open_code
    if not skip_names and not check_open_code:
        return False

    stack = traceback.extract_stack()

    # handle the case that we try to call the original `open_code`
    # (since Python 3.12)
    # The stack in this case is:
    # -1: helpers.is_called_from_skipped_module: 'stack = traceback.extract_stack()'
    # -2: fake_open.fake_open: 'if is_called_from_skipped_module('
    # -3: fake_io.open: 'return fake_open('
    # -4: fake_io.open_code : 'return self._io_module.open_code(path)'
    if (
        check_open_code
        and stack[-4].name == "open_code"
        and stack[-4].line == "return self._io_module.open_code(path)"
    ):
        return True

    if not skip_names:
        return False

    caller_filename = next(
        (
            frame.filename
            for frame in stack[::-1]
            if not frame.filename.startswith("<frozen ")
            and not starts_with(frame.filename, STDLIB_PATH)
            and (
                not starts_with(frame.filename, PYFAKEFS_PATH)
                or any(
                    starts_with(frame.filename, test_path)
                    for test_path in PYFAKEFS_TEST_PATHS
                )
            )
        ),
        None,
    )

    if caller_filename:
        caller_module_name = os.path.splitext(caller_filename)[0]
        caller_module_name = caller_module_name.replace(os.sep, ".")

        if any(
            [
                caller_module_name == sn or caller_module_name.endswith("." + sn)
                for sn in skip_names
            ]
        ):
            return True
    return False


def reload_cleanup_handler(name):
    """Cleanup handler that reloads the module with the given name.
    Maybe needed in cases where a module is imported locally.
    """
    if name in sys.modules:
        importlib.reload(sys.modules[name])
    return True
