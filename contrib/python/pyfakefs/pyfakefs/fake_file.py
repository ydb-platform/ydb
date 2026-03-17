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

"""Fake implementations for different file objects."""

import errno
import io
import os
import sys
import traceback
from stat import (
    S_IFREG,
    S_IFDIR,
)
from types import TracebackType
from typing import (
    List,
    Optional,
    Callable,
    Union,
    Any,
    Dict,
    cast,
    AnyStr,
    NoReturn,
    Iterator,
    TextIO,
    Type,
    TYPE_CHECKING,
)

from pyfakefs import helpers
from pyfakefs.helpers import (
    FakeStatResult,
    BinaryBufferIO,
    TextBufferIO,
    is_int_type,
    is_unicode_string,
    to_string,
    matching_string,
    real_encoding,
    AnyPath,
    AnyString,
    get_locale_encoding,
    _OpenModes,
    is_root,
)

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem


# Work around pyupgrade auto-rewriting `io.open()` to `open()`.
io_open = io.open

AnyFileWrapper = Union[
    "FakeFileWrapper",
    "FakeDirWrapper",
    "StandardStreamWrapper",
    "FakePipeWrapper",
]
AnyFile = Union["FakeFile", "FakeDirectory"]


class FakeLargeFileIoException(Exception):
    """Exception thrown on unsupported operations for fake large files.
    Fake large files have a size with no real content.
    """

    def __init__(self, file_path: str) -> None:
        super().__init__(
            "Read and write operations not supported for "
            "fake large file: %s" % file_path
        )


class FakeFile:
    """Provides the appearance of a real file.

    Attributes currently faked out:
      * `st_mode`: user-specified, otherwise S_IFREG
      * `st_ctime`: the time.time() timestamp of the file change time (updated
        each time a file's attributes is modified).
      * `st_atime`: the time.time() timestamp when the file was last accessed.
      * `st_mtime`: the time.time() timestamp when the file was last modified.
      * `st_size`: the size of the file
      * `st_nlink`: the number of hard links to the file
      * `st_ino`: the inode number - a unique number identifying the file
      * `st_dev`: a unique number identifying the (fake) file system device
        the file belongs to
      * `st_uid`: always set to USER_ID, which can be changed globally using
            `set_uid`
      * `st_gid`: always set to GROUP_ID, which can be changed globally using
            `set_gid`

    .. note:: The resolution for `st_ctime`, `st_mtime` and `st_atime` in the
        real file system depends on the used file system (for example it is
        only 1s for HFS+ and older Linux file systems, but much higher for
        ext4 and NTFS). This is currently ignored by pyfakefs, which uses
        the resolution of `time.time()`.

        Under Windows, `st_atime` is not updated for performance reasons by
        default. pyfakefs never updates `st_atime` under Windows, assuming
        the default setting.
    """

    stat_types = (
        "st_mode",
        "st_ino",
        "st_dev",
        "st_nlink",
        "st_uid",
        "st_gid",
        "st_size",
        "st_atime",
        "st_mtime",
        "st_ctime",
        "st_atime_ns",
        "st_mtime_ns",
        "st_ctime_ns",
    )

    def __init__(
        self,
        name: AnyStr,
        st_mode: int = S_IFREG | helpers.PERM_DEF_FILE,
        contents: Optional[AnyStr] = None,
        filesystem: Optional["FakeFilesystem"] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        side_effect: Optional[Callable[["FakeFile"], None]] = None,
        open_modes: Optional[_OpenModes] = None,
    ):
        """
        Args:
            name: Name of the file/directory, without parent path information
            st_mode: The stat.S_IF* constant representing the file type (i.e.
                stat.S_IFREG, stat.S_IFDIR), and the file permissions.
                If no file type is set (e.g. permission flags only), a
                regular file type is assumed.
            contents: The contents of the filesystem object; should be a string
                or byte object for regular files, and a dict of other
                FakeFile or FakeDirectory objects with the file names as
                keys for FakeDirectory objects
            filesystem: The fake filesystem where the file is created.
            encoding: If contents is a unicode string, the encoding used
                for serialization.
            errors: The error mode used for encoding/decoding errors.
            side_effect: function handle that is executed when file is written,
                must accept the file object as an argument.
            open_modes: The modes the file was opened with (e.g. can read, write etc.)
        """
        # to be backwards compatible regarding argument order, we raise on None
        if filesystem is None:
            raise ValueError("filesystem shall not be None")
        self.filesystem: "FakeFilesystem" = filesystem
        self._side_effect: Optional[Callable] = side_effect
        self.name: AnyStr = name  # type: ignore[assignment]
        self.stat_result = FakeStatResult(
            filesystem.is_windows_fs,
            helpers.get_uid(),
            helpers.get_gid(),
            helpers.now(),
        )
        if st_mode >> 12 == 0:
            st_mode |= S_IFREG
        self.stat_result.st_mode = st_mode
        self.st_size: int = 0
        self.encoding: Optional[str] = real_encoding(encoding)
        self.errors: str = errors or "strict"
        self._byte_contents: Optional[bytes] = self._encode_contents(contents)
        self.stat_result.st_size = (
            len(self._byte_contents) if self._byte_contents is not None else 0
        )
        self.epoch: int = 0
        self.parent_dir: Optional[FakeDirectory] = None
        # Linux specific: extended file system attributes
        self.xattr: Dict = {}
        self.opened_as: AnyString = ""
        self.open_modes = open_modes

    @property
    def byte_contents(self) -> Optional[bytes]:
        """Return the contents as raw byte array."""
        return self._byte_contents

    @property
    def contents(self) -> Optional[str]:
        """Return the contents as string with the original encoding."""
        if isinstance(self.byte_contents, bytes):
            return self.byte_contents.decode(
                self.encoding or get_locale_encoding(),
                errors=self.errors,
            )
        return None

    @property
    def st_ctime(self) -> float:
        """Return the creation time of the fake file."""
        return self.stat_result.st_ctime

    @st_ctime.setter
    def st_ctime(self, val: float) -> None:
        """Set the creation time of the fake file."""
        self.stat_result.st_ctime = val

    @property
    def st_atime(self) -> float:
        """Return the access time of the fake file."""
        return self.stat_result.st_atime

    @st_atime.setter
    def st_atime(self, val: float) -> None:
        """Set the access time of the fake file."""
        self.stat_result.st_atime = val

    @property
    def st_mtime(self) -> float:
        """Return the modification time of the fake file."""
        return self.stat_result.st_mtime

    @st_mtime.setter
    def st_mtime(self, val: float) -> None:
        """Set the modification time of the fake file."""
        self.stat_result.st_mtime = val

    def set_large_file_size(self, st_size: int) -> None:
        """Sets the self.st_size attribute and replaces self.content with None.

        Provided specifically to simulate very large files without regards
        to their content (which wouldn't fit in memory).
        Note that read/write operations with such a file raise
            :py:class:`FakeLargeFileIoException`.

        Args:
          st_size: (int) The desired file size

        Raises:
          OSError: if the st_size is not a non-negative integer,
                   or if st_size exceeds the available file system space
        """
        self._check_positive_int(st_size)
        if self.st_size:
            self.size = 0
        if self.filesystem:
            self.filesystem.change_disk_usage(st_size, self.name, self.st_dev)
        self.st_size = st_size
        self._byte_contents = None

    def _check_positive_int(self, size: int) -> None:
        # the size should be an positive integer value
        if not is_int_type(size) or size < 0:
            self.filesystem.raise_os_error(errno.ENOSPC, self.name)

    def is_large_file(self) -> bool:
        """Return `True` if this file was initialized with size
        but no contents.
        """
        return self._byte_contents is None

    def _encode_contents(self, contents: Union[str, bytes, None]) -> Optional[bytes]:
        if is_unicode_string(contents):
            contents = bytes(
                cast(str, contents),
                self.encoding or get_locale_encoding(),
                self.errors,
            )
        return cast(bytes, contents)

    def set_initial_contents(self, contents: AnyStr) -> bool:
        """Sets the file contents and size.
           Called internally after initial file creation.

        Args:
            contents: string, new content of file.

        Returns:
            True if the contents have been changed.

        Raises:
              OSError: if the st_size is not a non-negative integer,
                   or if st_size exceeds the available file system space
        """
        byte_contents = self._encode_contents(contents)
        changed = self._byte_contents != byte_contents
        st_size = len(byte_contents) if byte_contents else 0

        current_size = self.st_size or 0
        self.filesystem.change_disk_usage(
            st_size - current_size, self.name, self.st_dev
        )
        self._byte_contents = byte_contents
        self.st_size = st_size
        self.epoch += 1
        return changed

    def set_contents(self, contents: AnyStr, encoding: Optional[str] = None) -> bool:
        """Sets the file contents and size and increases the modification time.
        Also executes the side_effects if available.

        Args:
          contents: (str, bytes) new content of file.
          encoding: (str) the encoding to be used for writing the contents
                    if they are a unicode string.
                    If not given, the locale preferred encoding is used.

        Returns:
            True if the contents have been changed.

        Raises:
          OSError: if `st_size` is not a non-negative integer,
                   or if it exceeds the available file system space.
        """
        self.encoding = real_encoding(encoding)
        changed = self.set_initial_contents(contents)
        if self._side_effect is not None:
            self._side_effect(self)
        return changed

    @property
    def size(self) -> int:
        """Return the size in bytes of the file contents."""
        return self.st_size

    @size.setter
    def size(self, st_size: int) -> None:
        """Resizes file content, padding with nulls if new size exceeds the
        old size.

        Args:
          st_size: The desired size for the file.

        Raises:
          OSError: if the st_size arg is not a non-negative integer
                   or if st_size exceeds the available file system space
        """

        self._check_positive_int(st_size)
        current_size = self.st_size or 0
        self.filesystem.change_disk_usage(
            st_size - current_size, self.name, self.st_dev
        )
        if self._byte_contents:
            if st_size < current_size:
                self._byte_contents = self._byte_contents[:st_size]
            else:
                self._byte_contents += b"\0" * (st_size - current_size)
        self.st_size = st_size
        self.epoch += 1

    @property
    def path(self) -> AnyStr:  # type: ignore[type-var]
        """Return the full path of the current object."""
        names: List[AnyStr] = []  # pytype: disable=invalid-annotation
        obj: Optional[FakeFile] = self
        while obj:
            names.insert(0, matching_string(self.name, obj.name))  # type: ignore
            obj = obj.parent_dir
        sep = self.filesystem.get_path_separator(names[0])
        if names[0] == sep:
            names.pop(0)
            dir_path = sep.join(names)
            drive = self.filesystem.splitdrive(dir_path)[0]
            # if a Windows path already starts with a drive or UNC path,
            # no extra separator is needed
            if not drive:
                dir_path = sep + dir_path
        else:
            dir_path = sep.join(names)
        return self.filesystem.absnormpath(dir_path)

    if sys.version_info >= (3, 12):

        @property
        def is_junction(self) -> bool:
            return self.filesystem.isjunction(self.path)

    def __getattr__(self, item: str) -> Any:
        """Forward some properties to stat_result."""
        if item in self.stat_types:
            return getattr(self.stat_result, item)
        return super().__getattribute__(item)

    def __setattr__(self, key: str, value: Any) -> None:
        """Forward some properties to stat_result."""
        if key in self.stat_types:
            return setattr(self.stat_result, key, value)
        return super().__setattr__(key, value)

    def __str__(self) -> str:
        return f"{self.name!r}({self.st_mode:o})"

    def has_permission(self, permission_bits: int) -> bool:
        """Checks if the given permissions are set in the fake file.

        Args:
            permission_bits: The permission bits as set for the user.

        Returns:
            True if the permissions are set in the correct class (user/group/other).
        """
        if helpers.get_uid() == self.stat_result.st_uid:
            return self.st_mode & permission_bits == permission_bits
        if helpers.get_gid() == self.stat_result.st_gid:
            return self.st_mode & (permission_bits >> 3) == permission_bits >> 3
        return self.st_mode & (permission_bits >> 6) == permission_bits >> 6


class FakeNullFile(FakeFile):
    def __init__(self, filesystem: "FakeFilesystem") -> None:
        super().__init__(filesystem.devnull, filesystem=filesystem, contents="")

    @property
    def byte_contents(self) -> bytes:
        return b""

    def set_initial_contents(self, contents: AnyStr) -> bool:
        return False


class FakeFileFromRealFile(FakeFile):
    """Represents a fake file copied from the real file system.

    The contents of the file are read on demand only.
    """

    def __init__(
        self,
        file_path: str,
        filesystem: "FakeFilesystem",
        side_effect: Optional[Callable] = None,
    ) -> None:
        """
        Args:
            file_path: Path to the existing file.
            filesystem: The fake filesystem where the file is created.

        Raises:
            OSError: if the file does not exist in the real file system.
            OSError: if the file already exists in the fake file system.
        """
        super().__init__(
            name=os.path.basename(file_path),
            filesystem=filesystem,
            side_effect=side_effect,
        )
        self.contents_read = False

    @property
    def byte_contents(self) -> Optional[bytes]:
        if not self.contents_read:
            self.contents_read = True
            with io_open(self.file_path, "rb") as f:
                self._byte_contents = f.read()
        # On MacOS and BSD, the above io.open() updates atime on the real file
        self.st_atime = os.stat(self.file_path).st_atime
        return self._byte_contents

    def set_contents(self, contents, encoding=None):
        self.contents_read = True
        super().set_contents(contents, encoding)

    def is_large_file(self):
        """The contents are never faked."""
        return False


class FakeDirectory(FakeFile):
    """Provides the appearance of a real directory."""

    def __init__(
        self,
        name: str,
        perm_bits: int = helpers.PERM_DEF,
        filesystem: Optional["FakeFilesystem"] = None,
    ):
        """
        Args:
            name:  name of the file/directory, without parent path information
            perm_bits: permission bits. defaults to 0o777.
            filesystem: if set, the fake filesystem where the directory
                is created
        """
        FakeFile.__init__(self, name, S_IFDIR | perm_bits, "", filesystem=filesystem)
        # directories have the link count of contained entries,
        # including '.' and '..'
        self.st_nlink += 1
        self._entries: Dict[str, AnyFile] = {}

    def set_contents(self, contents: AnyStr, encoding: Optional[str] = None) -> bool:
        raise self.filesystem.raise_os_error(errno.EISDIR, self.path)

    @property
    def entries(self) -> Dict[str, FakeFile]:
        """Return the list of contained directory entries."""
        return self._entries

    @property
    def ordered_dirs(self) -> List[str]:
        """Return the list of contained directory entry names ordered by
        creation order.
        """
        return [
            item[0]
            for item in sorted(self._entries.items(), key=lambda entry: entry[1].st_ino)
        ]

    def add_entry(self, path_object: FakeFile) -> None:
        """Adds a child FakeFile to this directory.

        Args:
            path_object: FakeFile instance to add as a child of this directory.

        Raises:
            OSError: if the directory has no write permission (Posix only)
            OSError: if the file or directory to be added already exists
        """
        if (
            not helpers.is_root()
            and not self.filesystem.is_windows_fs
            and not self.has_permission(helpers.PERM_WRITE)
        ):
            raise OSError(errno.EACCES, "Permission Denied", self.path)

        path_object_name: str = to_string(path_object.name)
        if path_object_name in self.entries:
            self.filesystem.raise_os_error(errno.EEXIST, self.path)

        self._entries[path_object_name] = path_object
        path_object.parent_dir = self
        if path_object.st_ino is None:
            self.filesystem.last_ino += 1
            path_object.st_ino = self.filesystem.last_ino
        self.st_nlink += 1
        path_object.st_nlink += 1
        path_object.st_dev = self.st_dev
        if path_object.st_nlink == 1:
            self.filesystem.change_disk_usage(
                path_object.size, path_object.name, self.st_dev
            )

    def get_entry(self, pathname_name: str) -> AnyFile:
        """Retrieves the specified child file or directory entry.

        Args:
            pathname_name: The basename of the child object to retrieve.

        Returns:
            The fake file or directory object.

        Raises:
            KeyError: if no child exists by the specified name.
        """
        pathname_name = self._normalized_entryname(pathname_name)
        return self.entries[to_string(pathname_name)]

    def _normalized_entryname(self, pathname_name: str) -> str:
        if not self.filesystem.is_case_sensitive:
            matching_names = [
                name for name in self.entries if name.lower() == pathname_name.lower()
            ]
            if matching_names:
                pathname_name = matching_names[0]
        return pathname_name

    def remove_entry(self, pathname_name: str, recursive: bool = True) -> None:
        """Removes the specified child file or directory.

        Args:
            pathname_name: Basename of the child object to remove.
            recursive: If True (default), the entries in contained directories
                are deleted first. Used to propagate removal errors
                (e.g. permission problems) from contained entries.

        Raises:
            KeyError: if no child exists by the specified name.
            OSError: if user lacks permission to delete the file,
                or (Windows only) the file is open.
        """
        pathname_name = self._normalized_entryname(pathname_name)
        entry = self.get_entry(pathname_name)
        if self.filesystem.is_windows_fs:
            if not is_root() and entry.st_mode & helpers.PERM_WRITE == 0:
                self.filesystem.raise_os_error(errno.EACCES, pathname_name)
            if self.filesystem.has_open_file(entry):
                raise_error = True
                if os.name == "posix" and not hasattr(os, "O_TMPFILE"):
                    # special handling for emulating Windows under macOS and PyPi
                    # tempfile uses unlink based on the real OS while deleting
                    # a temporary file, so we ignore that error in this specific case
                    st = traceback.extract_stack(limit=6)
                    if sys.version_info < (3, 10):
                        if (
                            st[0].name == "TemporaryFile"
                            and st[0].line == "_os.unlink(name)"
                        ):
                            raise_error = False
                    else:
                        # TemporaryFile implementation has changed in Python 3.10
                        if st[0].name == "opener" and st[0].line == "_os.unlink(name)":
                            raise_error = False
                if raise_error:
                    self.filesystem.raise_os_error(errno.EACCES, pathname_name)
        else:
            if not helpers.is_root() and not self.has_permission(
                helpers.PERM_WRITE | helpers.PERM_EXE
            ):
                self.filesystem.raise_os_error(errno.EACCES, pathname_name)

        if recursive and isinstance(entry, FakeDirectory):
            while entry.entries:
                entry.remove_entry(list(entry.entries)[0])
        elif entry.st_nlink == 1:
            self.filesystem.change_disk_usage(-entry.size, pathname_name, entry.st_dev)

        self.st_nlink -= 1
        entry.st_nlink -= 1
        assert entry.st_nlink >= 0

        del self.entries[to_string(pathname_name)]

    @property
    def size(self) -> int:
        """Return the total size of all files contained
        in this directory tree.
        """
        return sum([item[1].size for item in self.entries.items()])

    @size.setter
    def size(self, st_size: int) -> None:
        """Setting the size is an error for a directory."""
        raise self.filesystem.raise_os_error(errno.EISDIR, self.path)

    def has_parent_object(self, dir_object: "FakeDirectory") -> bool:
        """Return `True` if dir_object is a direct or indirect parent
        directory, or if both are the same object."""
        obj: Optional[FakeDirectory] = self
        while obj:
            if obj == dir_object:
                return True
            obj = obj.parent_dir
        return False

    def __str__(self) -> str:
        description = super().__str__() + ":\n"
        for item in self.entries:
            item_desc = self.entries[item].__str__()
            for line in item_desc.split("\n"):
                if line:
                    description = description + "  " + line + "\n"
        return description


class FakeDirectoryFromRealDirectory(FakeDirectory):
    """Represents a fake directory copied from the real file system.

    The contents of the directory are read on demand only.
    """

    def __init__(
        self,
        source_path: AnyPath,
        filesystem: "FakeFilesystem",
        read_only: bool,
        target_path: Optional[AnyPath] = None,
    ):
        """
        Args:
            source_path: Full directory path.
            filesystem: The fake filesystem where the directory is created.
            read_only: If set, all files under the directory are treated
                as read-only, e.g. a write access raises an exception;
                otherwise, writing to the files changes the fake files
                only as usually.
            target_path: If given, the target path of the directory,
                otherwise the target is the same as `source_path`.

        Raises:
            OSError: if the directory does not exist in the real file system
        """
        target_path = target_path or source_path
        real_stat = os.stat(source_path)
        super().__init__(
            name=to_string(os.path.split(target_path)[1]),
            perm_bits=real_stat.st_mode,
            filesystem=filesystem,
        )

        self.st_ctime = real_stat.st_ctime
        self.st_atime = real_stat.st_atime
        self.st_mtime = real_stat.st_mtime
        self.st_gid = real_stat.st_gid
        self.st_uid = real_stat.st_uid
        self.source_path = source_path  # type: ignore
        self.read_only = read_only
        self.contents_read = False

    @property
    def entries(self) -> Dict[str, FakeFile]:
        """Return the list of contained directory entries, loading them
        if not already loaded."""
        if not self.contents_read:
            self.contents_read = True
            base = self.path
            for entry in os.listdir(self.source_path):
                source_path = os.path.join(self.source_path, entry)
                target_path = os.path.join(base, entry)  # type: ignore
                if os.path.islink(source_path):
                    self.filesystem.add_real_symlink(source_path, target_path)
                elif os.path.isdir(source_path):
                    self.filesystem.add_real_directory(
                        source_path, self.read_only, target_path=target_path
                    )
                else:
                    self.filesystem.add_real_file(
                        source_path, self.read_only, target_path=target_path
                    )
        return self._entries

    @property
    def size(self) -> int:
        # we cannot get the size until the contents are loaded
        if not self.contents_read:
            return 0
        return super().size

    @size.setter
    def size(self, st_size: int) -> None:
        raise self.filesystem.raise_os_error(errno.EISDIR, self.path)


class FakeFileWrapper:
    """Wrapper for a stream object for use by a FakeFile object.

    If the wrapper has any data written to it, it will propagate to
    the FakeFile object on close() or flush().
    """

    def __init__(
        self,
        file_object: FakeFile,
        file_path: AnyStr,
        update: bool,
        read: bool,
        append: bool,
        delete_on_close: bool,
        filesystem: "FakeFilesystem",
        newline: Optional[str],
        binary: bool,
        closefd: bool,
        encoding: Optional[str],
        errors: Optional[str],
        buffering: int,
        raw_io: bool,
        opened_as_fd: bool,
        is_stream: bool = False,
    ):
        self.file_object = file_object
        self.file_path = file_path  # type: ignore[var-annotated]
        self._append = append
        self._read = read
        self.allow_update = update
        self._closefd = closefd
        self._file_epoch = file_object.epoch
        self.raw_io = raw_io
        self._binary = binary
        self.opened_as_fd = opened_as_fd
        self.is_stream = is_stream
        self._changed = False
        self._buffer_size = buffering
        if self._buffer_size == 0 and not binary:
            raise ValueError("can't have unbuffered text I/O")
        # buffer_size is ignored in text mode
        elif self._buffer_size == -1 or not binary:
            self._buffer_size = io.DEFAULT_BUFFER_SIZE
        self._use_line_buffer = not binary and buffering == 1

        contents = file_object.byte_contents
        self._encoding = encoding or get_locale_encoding()
        errors = errors or "strict"
        self._io: Union[BinaryBufferIO, TextBufferIO] = (
            BinaryBufferIO(contents)
            if binary
            else TextBufferIO(
                contents, encoding=encoding, newline=newline, errors=errors
            )
        )
        self._read_whence = 0
        self._read_seek = 0
        self._flush_pos = 0
        if contents:
            self._flush_pos = len(contents)
            if update:
                if not append:
                    self._io.seek(0)
                else:
                    self._io.seek(self._flush_pos)
                    self._read_seek = self._io.tell()

        if delete_on_close:
            assert filesystem, "delete_on_close=True requires filesystem"
        self._filesystem = filesystem
        self.delete_on_close = delete_on_close
        # override, don't modify FakeFile.name, as FakeFilesystem expects
        # it to be the file name only, no directories.
        self.name = file_object.opened_as
        self.filedes: Optional[int] = None

    def __enter__(self) -> "FakeFileWrapper":
        """To support usage of this fake file with the 'with' statement."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """To support usage of this fake file with the 'with' statement."""
        self.close()

    def _raise(self, message: str) -> NoReturn:
        if self.raw_io:
            self._filesystem.raise_os_error(errno.EBADF, self.file_path)
        raise io.UnsupportedOperation(message)

    def get_object(self) -> FakeFile:
        """Return the FakeFile object that is wrapped
        by the current instance.
        """
        return self.file_object

    def fileno(self) -> int:
        """Return the file descriptor of the file object."""
        if self.filedes is not None:
            return self.filedes
        raise OSError(errno.EBADF, "Invalid file descriptor")

    def close(self) -> None:
        """Close the file."""
        self.close_fd(self.filedes)

    def close_fd(self, fd: Optional[int]) -> None:
        """Close the file for the given file descriptor."""

        # ignore closing a closed file
        if not self._is_open():
            return

        # for raw io, all writes are flushed immediately
        if not self.raw_io:
            try:
                self.flush()
            except OSError as e:
                if e.errno == errno.EBADF:
                    # if we get here, we have an open file descriptor
                    # without write permission, which has to be closed
                    assert self.filedes
                    self._filesystem.close_open_file(self.filedes)
                raise

            if self._filesystem.is_windows_fs and self._changed:
                self.file_object.st_mtime = helpers.now()

        assert fd is not None
        if self._closefd:
            self._filesystem.close_open_file(fd)
        else:
            open_files = self._filesystem.open_files[fd]
            assert open_files is not None
            open_files.remove(self)
        if self.delete_on_close:
            self._filesystem.remove_object(
                self.get_object().path  # type: ignore[arg-type]
            )

    @property
    def closed(self) -> bool:
        """Simulate the `closed` attribute on file."""
        return not self._is_open()

    def _try_flush(self, old_pos: int) -> None:
        """Try to flush and reset the position if it fails."""
        flush_pos = self._flush_pos
        try:
            self.flush()
        except OSError:
            # write failed - reset to previous position
            self._io.seek(old_pos)
            self._io.truncate()
            self._flush_pos = flush_pos
            raise

    def flush(self) -> None:
        """Flush file contents to 'disk'."""
        if self.is_stream:
            return

        self._check_open_file()

        if self.allow_update:
            if self._append:
                contents = self._io.getvalue()
                self._sync_io()
                old_contents = self.file_object.byte_contents
                assert old_contents is not None
                contents = old_contents + contents[self._flush_pos :]
                self._set_stream_contents(contents)
            else:
                self._io.flush()
                contents = self._io.getvalue()
            changed = self.file_object.set_contents(contents, self._encoding)
            self.update_flush_pos()
            if changed:
                if self._filesystem.is_windows_fs:
                    self._changed = True
                else:
                    current_time = helpers.now()
                    self.file_object.st_ctime = current_time
                    self.file_object.st_mtime = current_time
            self._file_epoch = self.file_object.epoch
            self._flush_related_files()
        else:
            buf_length = len(self._io.getvalue())
            content_length = 0
            if self.file_object.byte_contents is not None:
                content_length = len(self.file_object.byte_contents)
            # an error is only raised if there is something to flush
            if content_length != buf_length:
                self._filesystem.raise_os_error(errno.EBADF)

    def update_flush_pos(self) -> None:
        self._flush_pos = self._io.tell()

    def _flush_related_files(self) -> None:
        for open_files in self._filesystem.open_files[3:]:
            if open_files is not None:
                for open_file in open_files:
                    if (
                        open_file is not self
                        and isinstance(open_file, FakeFileWrapper)
                        and self.file_object == open_file.file_object
                        and not open_file._append
                    ):
                        open_file._sync_io()

    def seek(self, offset: int, whence: int = 0) -> None:
        """Move read/write pointer in 'file'."""
        self._check_open_file()
        if not self._append:
            self._io.seek(offset, whence)
        else:
            self._read_seek = offset
            self._read_whence = whence
        if not self.is_stream:
            self.flush()

    def tell(self) -> int:
        """Return the file's current position.

        Returns:
          int, file's current position in bytes.
        """
        self._check_open_file()
        if not self.is_stream:
            self.flush()

        if not self._append:
            return self._io.tell()
        if self._read_whence:
            write_seek = self._io.tell()
            self._io.seek(self._read_seek, self._read_whence)
            self._read_seek = self._io.tell()
            self._read_whence = 0
            self._io.seek(write_seek)
        return self._read_seek

    def _sync_io(self) -> None:
        """Update the stream with changes to the file object contents."""
        if self._file_epoch == self.file_object.epoch:
            return

        contents = self.file_object.byte_contents
        assert contents is not None
        self._set_stream_contents(contents)
        self._file_epoch = self.file_object.epoch

    def _set_stream_contents(self, contents: bytes) -> None:
        whence = self._io.tell()
        self._io.seek(0)
        self._io.truncate()
        self._io.putvalue(contents)
        if not self._append:
            self._io.seek(whence)

    def _read_wrappers(self, name: str) -> Callable:
        """Wrap a stream attribute in a read wrapper.

        Returns a read_wrapper which tracks our own read pointer since the
        stream object has no concept of a different read and write pointer.

        Args:
            name: The name of the attribute to wrap. Should be a read call.

        Returns:
            The read_wrapper function.
        """
        io_attr = getattr(self._io, name)

        def read_wrapper(*args, **kwargs):
            """Wrap all read calls to the stream object.

            We do this to track the read pointer separate from the write
            pointer.  Anything that wants to read from the stream object
            while we're in append mode goes through this.

            Args:
                *args: pass through args
                **kwargs: pass through kwargs
            Returns:
                Wrapped stream object method
            """
            self._io.seek(self._read_seek, self._read_whence)
            ret_value = io_attr(*args, **kwargs)
            self._read_seek = self._io.tell()
            self._read_whence = 0
            self._io.seek(0, 2)
            return ret_value

        return read_wrapper

    def _other_wrapper(self, name: str) -> Callable:
        """Wrap a stream attribute in an other_wrapper.

        Args:
          name: the name of the stream attribute to wrap.

        Returns:
          other_wrapper which is described below.
        """
        io_attr = getattr(self._io, name)

        def other_wrapper(*args, **kwargs):
            """Wrap all other calls to the stream Object.

            We do this to track changes to the write pointer.  Anything that
            moves the write pointer in a file open for appending should move
            the read pointer as well.

            Args:
                *args: Pass through args.
                **kwargs: Pass through kwargs.

            Returns:
                Wrapped stream object method.
            """
            write_seek = self._io.tell()
            ret_value = io_attr(*args, **kwargs)
            if write_seek != self._io.tell():
                self._read_seek = self._io.tell()
                self._read_whence = 0

            return ret_value

        return other_wrapper

    def _write_wrapper(self, name: str) -> Callable:
        """Wrap a stream attribute in a write_wrapper.

        Args:
          name: the name of the stream attribute to wrap.

        Returns:
          write_wrapper which is described below.
        """
        io_attr = getattr(self._io, name)

        def write_wrapper(*args, **kwargs):
            """Wrap all other calls to the stream Object.

            We do this to track changes to the write pointer.  Anything that
            moves the write pointer in a file open for appending should move
            the read pointer as well.

            Args:
                *args: Pass through args.
                **kwargs: Pass through kwargs.

            Returns:
                Wrapped stream object method.
            """
            old_pos = self._io.tell()
            ret_value = io_attr(*args, **kwargs)
            new_pos = self._io.tell()

            # if the buffer size is exceeded, we flush
            use_line_buf = self._use_line_buffer and "\n" in args[0]
            if new_pos - self._flush_pos > self._buffer_size or use_line_buf:
                flush_all = new_pos - old_pos > self._buffer_size or use_line_buf
                # if the current write does not exceed the buffer size,
                # we revert to the previous position and flush that,
                # otherwise we flush all
                if not flush_all:
                    self._io.seek(old_pos)
                    self._io.truncate()
                self._try_flush(old_pos)
                if not flush_all:
                    ret_value = io_attr(*args, **kwargs)
            if self._append:
                self._read_seek = self._io.tell()
                self._read_whence = 0
            return ret_value

        return write_wrapper

    def _adapt_size_for_related_files(self, size: int) -> None:
        for open_files in self._filesystem.open_files[3:]:
            if open_files is not None:
                for open_file in open_files:
                    if (
                        open_file is not self
                        and isinstance(open_file, FakeFileWrapper)
                        and self.file_object == open_file.file_object
                        and cast(FakeFileWrapper, open_file)._append
                    ):
                        open_file._read_seek += size

    def _truncate_wrapper(self) -> Callable:
        """Wrap truncate() to allow flush after truncate.

        Returns:
            Wrapper which is described below.
        """
        io_attr = self._io.truncate

        def truncate_wrapper(*args, **kwargs):
            """Wrap truncate call to call flush after truncate."""
            if self._append:
                self._io.seek(self._read_seek, self._read_whence)
            size = io_attr(*args, **kwargs)
            self.flush()
            if not self.is_stream:
                self.file_object.size = size
                buffer_size = len(self._io.getvalue())
                if buffer_size < size:
                    self._io.seek(buffer_size)
                    self._io.putvalue(b"\0" * (size - buffer_size))
                    self.file_object.set_contents(self._io.getvalue(), self._encoding)
                    self._flush_pos = size
                    self._adapt_size_for_related_files(size - buffer_size)

            self.flush()
            return size

        return truncate_wrapper

    def size(self) -> int:
        """Return the content size in bytes of the wrapped file."""
        return self.file_object.st_size

    def __getattr__(self, name: str) -> Any:
        if self.file_object.is_large_file():
            raise FakeLargeFileIoException(self.file_path)

        reading = name.startswith("read") or name == "next"
        truncate = name == "truncate"
        writing = name.startswith("write") or truncate

        if reading or writing:
            self._check_open_file()
        if not self._read and reading:
            return self._read_error()
        if not self.opened_as_fd and not self.allow_update and writing:
            return self._write_error()

        if reading:
            self._sync_io()
            if not self.is_stream:
                self.flush()
            if not self._filesystem.is_windows_fs:
                self.file_object.st_atime = helpers.now()
        if truncate:
            return self._truncate_wrapper()
        if self._append:
            if reading:
                return self._read_wrappers(name)
            elif not writing:
                return self._other_wrapper(name)
        if writing:
            return self._write_wrapper(name)

        return getattr(self._io, name)

    def _read_error(self) -> Callable:
        def read_error(*args, **kwargs):
            """Throw an error unless the argument is zero."""
            if args and args[0] == 0:
                if self._filesystem.is_windows_fs and self.raw_io:
                    return b"" if self._binary else ""
            self._raise("File is not open for reading.")

        return read_error

    def _write_error(self) -> Callable:
        def write_error(*args, **kwargs):
            """Throw an error."""
            if self.raw_io:
                if self._filesystem.is_windows_fs and args and len(args[0]) == 0:
                    return 0
            self._raise("File is not open for writing.")

        return write_error

    def _is_open(self) -> bool:
        if self.filedes is not None and self.filedes < len(self._filesystem.open_files):
            open_files = self._filesystem.open_files[self.filedes]
            if open_files is not None and self in open_files:
                return True
        return False

    def _check_open_file(self) -> None:
        if not self.is_stream and not self._is_open():
            raise ValueError("I/O operation on closed file")

    def __iter__(self) -> Union[Iterator[str], Iterator[bytes]]:
        if not self._read:
            self._raise("File is not open for reading")
        return self._io.__iter__()

    def __next__(self):
        if not self._read:
            self._raise("File is not open for reading")
        return next(self._io)


class StandardStreamWrapper:
    """Wrapper for a system standard stream to be used in open files list."""

    def __init__(self, stream_object: TextIO):
        self._stream_object = stream_object
        self.filedes: Optional[int] = None

    def get_object(self) -> TextIO:
        return self._stream_object

    def fileno(self) -> int:
        """Return the file descriptor of the wrapped standard stream."""
        if self.filedes is not None:
            return self.filedes
        raise OSError(errno.EBADF, "Invalid file descriptor")

    def read(self, n: int = -1) -> bytes:
        return cast(bytes, self._stream_object.read())

    def write(self, contents: bytes) -> int:
        self._stream_object.write(cast(str, contents))
        return len(contents)

    def close(self) -> None:
        """We do not support closing standard streams."""

    def close_fd(self, fd: Optional[int]) -> None:
        """We do not support closing standard streams."""

    def is_stream(self) -> bool:
        return True

    def __enter__(self) -> "StandardStreamWrapper":
        """To support usage of this standard stream with the 'with' statement."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """To support usage of this standard stream with the 'with' statement."""
        self.close()


class FakeDirWrapper:
    """Wrapper for a FakeDirectory object to be used in open files list."""

    def __init__(
        self,
        file_object: FakeDirectory,
        file_path: AnyString,
        filesystem: "FakeFilesystem",
    ):
        self.file_object = file_object
        self.file_path = file_path
        self._filesystem = filesystem
        self.filedes: Optional[int] = None

    def get_object(self) -> FakeDirectory:
        """Return the FakeFile object that is wrapped by the current
        instance."""
        return self.file_object

    def fileno(self) -> int:
        """Return the file descriptor of the file object."""
        if self.filedes is not None:
            return self.filedes
        raise OSError(errno.EBADF, "Invalid file descriptor")

    def close(self) -> None:
        """Close the directory."""
        self.close_fd(self.filedes)

    def close_fd(self, fd: Optional[int]) -> None:
        """Close the directory."""
        assert fd is not None
        self._filesystem.close_open_file(fd)

    def read(self, numBytes: int = -1) -> bytes:
        """Read from the directory."""
        return self.file_object.read(numBytes)

    def write(self, contents: bytes) -> int:
        """Write to the directory."""
        self.file_object.write(contents)
        return len(contents)

    def __enter__(self) -> "FakeDirWrapper":
        """To support usage of this fake directory with the 'with' statement."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """To support usage of this fake directory with the 'with' statement."""
        self.close()


class FakePipeWrapper:
    """Wrapper for a read or write descriptor of a real pipe object to be
    used in open files list.
    """

    def __init__(
        self,
        filesystem: "FakeFilesystem",
        fd: int,
        can_write: bool,
        mode: str = "",
    ):
        self._filesystem = filesystem
        self.fd = fd  # the real file descriptor
        self.can_write = can_write
        self.file_object = None
        self.filedes: Optional[int] = None
        self.real_file = None
        if mode:
            self.real_file = open(fd, mode)

    def __enter__(self) -> "FakePipeWrapper":
        """To support usage of this fake pipe with the 'with' statement."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """To support usage of this fake pipe with the 'with' statement."""
        self.close()

    def get_object(self) -> None:
        return self.file_object

    def fileno(self) -> int:
        """Return the fake file descriptor of the pipe object."""
        if self.filedes is not None:
            return self.filedes
        raise OSError(errno.EBADF, "Invalid file descriptor")

    def read(self, numBytes: int = -1) -> bytes:
        """Read from the real pipe."""
        if self.real_file:
            return self.real_file.read(numBytes)  # pytype: disable=bad-return-type
        return os.read(self.fd, numBytes)

    def flush(self) -> None:
        """Flush the real pipe?"""

    def write(self, contents: bytes) -> int:
        """Write to the real pipe."""
        if self.real_file:
            return self.real_file.write(contents)
        return os.write(self.fd, contents)

    def close(self) -> None:
        """Close the pipe descriptor."""
        self.close_fd(self.filedes)

    def close_fd(self, fd: Optional[int]) -> None:
        """Close the pipe descriptor with the given file descriptor."""
        assert fd is not None
        open_files = self._filesystem.open_files[fd]
        assert open_files is not None
        open_files.remove(self)
        if self.real_file:
            self.real_file.close()
        else:
            os.close(self.fd)

    def readable(self) -> bool:
        """The pipe end can either be readable or writable."""
        return not self.can_write

    def writable(self) -> bool:
        """The pipe end can either be readable or writable."""
        return self.can_write

    def seekable(self) -> bool:
        """A pipe is not seekable."""
        return False
