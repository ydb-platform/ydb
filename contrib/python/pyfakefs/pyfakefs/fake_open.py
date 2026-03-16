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

"""A fake open() function replacement. See ``fake_filesystem`` for usage."""

import errno
import io
import os
import sys
from stat import (
    S_ISDIR,
)
from typing import (
    Optional,
    Union,
    Any,
    Tuple,
    cast,
    AnyStr,
    TYPE_CHECKING,
    Callable,
    IO,
    List,
)

from pyfakefs import helpers
from pyfakefs.fake_file import (
    FakePipeWrapper,
    FakeFileWrapper,
    FakeFile,
    AnyFileWrapper,
)
from pyfakefs.helpers import (
    AnyString,
    is_called_from_skipped_module,
    is_root,
    PERM_READ,
    PERM_WRITE,
    _OpenModes,
)

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem


# Work around pyupgrade auto-rewriting `io.open()` to `open()`.
io_open = io.open

_OPEN_MODE_MAP = {
    # mode name:(file must exist, can read, can write,
    #            truncate, append, must not exist)
    "r": (True, True, False, False, False, False),
    "w": (False, False, True, True, False, False),
    "a": (False, False, True, False, True, False),
    "r+": (True, True, True, False, False, False),
    "w+": (False, True, True, True, False, False),
    "a+": (False, True, True, False, True, False),
    "x": (False, False, True, False, False, True),
    "x+": (False, True, True, False, False, True),
}


def fake_open(
    filesystem: "FakeFilesystem",
    skip_names: List[str],
    file: Union[AnyStr, int],
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    closefd: bool = True,
    opener: Optional[Callable] = None,
) -> Union[AnyFileWrapper, IO[Any]]:
    """Redirect the call to FakeFileOpen.
    See FakeFileOpen.call() for description.
    """
    if is_called_from_skipped_module(
        skip_names=skip_names,
        case_sensitive=filesystem.is_case_sensitive,
        check_open_code=sys.version_info >= (3, 12),
    ):
        return io_open(  # pytype: disable=wrong-arg-count
            file,
            mode,
            buffering,
            encoding,
            errors,
            newline,
            closefd,
            opener,
        )
    fake_file_open = FakeFileOpen(filesystem)
    return fake_file_open(
        file, mode, buffering, encoding, errors, newline, closefd, opener
    )


class FakeFileOpen:
    """Faked `file()` and `open()` function replacements.

    Returns FakeFile objects in a FakeFilesystem in place of the `file()`
    or `open()` function.
    """

    __name__ = "FakeFileOpen"

    def __init__(
        self,
        filesystem: "FakeFilesystem",
        delete_on_close: bool = False,
        raw_io: bool = False,
    ):
        """
        Args:
          filesystem:  FakeFilesystem used to provide file system information
          delete_on_close:  optional boolean, deletes file on close()
        """
        self.filesystem = filesystem
        self._delete_on_close = delete_on_close
        self.raw_io = raw_io

    def __call__(self, *args: Any, **kwargs: Any) -> AnyFileWrapper:
        """Redirects calls to file() or open() to appropriate method."""
        return self.call(*args, **kwargs)

    def call(
        self,
        file_: Union[AnyStr, int],
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        closefd: bool = True,
        opener: Any = None,
        open_modes: Optional[_OpenModes] = None,
    ) -> AnyFileWrapper:
        """Return a file-like object with the contents of the target
        file object.

        Args:
            file_: Path to target file or a file descriptor.
            mode: Additional file modes (all modes in `open()` are supported).
            buffering: the buffer size used for writing. Data will only be
                flushed if buffer size is exceeded. The default (-1) uses a
                system specific default buffer size. Text line mode (e.g.
                buffering=1 in text mode) is not supported.
            encoding: The encoding used to encode unicode strings / decode
                bytes.
            errors: (str) Defines how encoding errors are handled.
            newline: Controls universal newlines, passed to stream object.
            closefd: If a file descriptor rather than file name is passed,
                and this is set to `False`, then the file descriptor is kept
                open when file is closed.
            opener: an optional function object that will be called with
                `file_` and the open flags (derived from `mode`) and returns
                a file descriptor.
            open_modes: Modes for opening files if called from low-level API.

        Returns:
            A file-like object containing the contents of the target file.

        Raises:
            OSError depending on Python version / call mode:
                - if the target object is a directory
                - on an invalid path
                - if the file does not exist when it should
                - if the file exists but should not
                - if permission is denied
            ValueError: for an invalid mode or mode combination
        """
        binary = "b" in mode

        if binary and encoding:
            raise ValueError("binary mode doesn't take an encoding argument")

        newline, open_modes = self._handle_file_mode(mode, newline, open_modes)
        opened_as_fd = isinstance(file_, int)

        # the pathlib opener is defined in a Path instance that may not be
        # patched under some circumstances; as it just calls standard open(),
        # we may ignore it, as it would not change the behavior
        if opener is not None and opener.__module__ not in (
            "pathlib",
            "pathlib._local",
        ):
            # opener shall return a file descriptor, which will be handled
            # here as if directly passed
            file_ = opener(file_, self._open_flags_from_open_modes(open_modes))

        file_object, file_path, filedes, real_path, can_write = self._handle_file_arg(
            file_
        )
        if file_object is None and file_path is None:
            # file must be a fake pipe wrapper, find it...
            if (
                filedes is None
                or len(self.filesystem.open_files) <= filedes
                or not self.filesystem.open_files[filedes]
            ):
                raise OSError(errno.EBADF, "invalid pipe file descriptor")
            wrappers = self.filesystem.open_files[filedes]
            assert wrappers is not None
            existing_wrapper = wrappers[0]
            assert isinstance(existing_wrapper, FakePipeWrapper)
            wrapper = FakePipeWrapper(
                self.filesystem,
                existing_wrapper.fd,
                existing_wrapper.can_write,
                mode,
            )
            file_des = self.filesystem.add_open_file(wrapper)
            wrapper.filedes = file_des
            return wrapper

        assert file_path is not None
        if not filedes:
            closefd = True

        if (
            not opener
            and open_modes.must_not_exist
            and (
                file_object
                or self.filesystem.islink(file_path)
                and not self.filesystem.is_windows_fs
            )
        ):
            self.filesystem.raise_os_error(errno.EEXIST, file_path)

        assert real_path is not None
        file_object = self._init_file_object(
            file_object,
            file_path,
            open_modes,
            real_path,
            check_file_permission=not opened_as_fd,
        )

        if S_ISDIR(file_object.st_mode):
            if self.filesystem.is_windows_fs:
                self.filesystem.raise_os_error(errno.EACCES, file_path)
            else:
                self.filesystem.raise_os_error(errno.EISDIR, file_path)

        # If you print obj.name, the argument to open() must be printed.
        # Not the abspath, not the filename, but the actual argument.
        file_object.opened_as = file_path
        if open_modes.truncate:
            current_time = helpers.now()
            file_object.st_mtime = current_time
            if not self.filesystem.is_windows_fs:
                file_object.st_ctime = current_time

        fakefile = FakeFileWrapper(
            file_object,
            file_path,
            update=open_modes.can_write and can_write,
            read=open_modes.can_read,
            append=open_modes.append,
            delete_on_close=self._delete_on_close,
            filesystem=self.filesystem,
            newline=newline,
            binary=binary,
            closefd=closefd,
            encoding=encoding,
            errors=errors,
            buffering=buffering,
            raw_io=self.raw_io,
            opened_as_fd=opened_as_fd,
        )
        if filedes is not None:
            fakefile.filedes = filedes
            # replace the file wrapper
            open_files_list = self.filesystem.open_files[filedes]
            assert open_files_list is not None
            open_files_list.append(fakefile)
        else:
            fakefile.filedes = self.filesystem.add_open_file(fakefile)
        return fakefile

    @staticmethod
    def _open_flags_from_open_modes(open_modes: _OpenModes) -> int:
        flags = 0
        if open_modes.can_read and open_modes.can_write:
            flags |= os.O_RDWR
        elif open_modes.can_read:
            flags |= os.O_RDONLY
        elif open_modes.can_write:
            flags |= os.O_WRONLY

        if open_modes.append:
            flags |= os.O_APPEND
        if open_modes.truncate:
            flags |= os.O_TRUNC
        if not open_modes.must_exist and open_modes.can_write:
            flags |= os.O_CREAT
        if open_modes.must_not_exist and open_modes.can_write:
            flags |= os.O_EXCL
        return flags

    def _init_file_object(
        self,
        file_object: Optional[FakeFile],
        file_path: AnyStr,
        open_modes: _OpenModes,
        real_path: AnyString,
        check_file_permission: bool,
    ) -> FakeFile:
        if file_object:
            if (
                check_file_permission
                and not is_root()
                and (
                    (open_modes.can_read and not file_object.has_permission(PERM_READ))
                    or (
                        open_modes.can_write
                        and not file_object.has_permission(PERM_WRITE)
                    )
                )
            ):
                self.filesystem.raise_os_error(errno.EACCES, file_path)
            if open_modes.can_write:
                if open_modes.truncate:
                    file_object.set_contents("")
        else:
            if open_modes.must_exist:
                self.filesystem.raise_os_error(errno.ENOENT, file_path)
            if self.filesystem.islink(file_path):
                link_object = self.filesystem.resolve(file_path, follow_symlinks=False)
                assert link_object.contents is not None
                target_path = cast(
                    AnyStr, link_object.contents
                )  # pytype: disable=invalid-annotation
            else:
                target_path = file_path
            if self.filesystem.ends_with_path_separator(target_path):
                error = (
                    errno.EINVAL
                    if self.filesystem.is_windows_fs
                    else errno.ENOENT
                    if self.filesystem.is_macos
                    else errno.EISDIR
                )
                self.filesystem.raise_os_error(error, file_path)
            file_object = self.filesystem.create_file_internally(
                real_path, create_missing_dirs=False, apply_umask=True
            )
        return file_object

    def _handle_file_arg(
        self, file_: Union[AnyStr, int]
    ) -> Tuple[
        Optional[FakeFile], Optional[AnyStr], Optional[int], Optional[AnyStr], bool
    ]:
        file_object = None
        if isinstance(file_, int):
            # opening a file descriptor
            filedes: int = file_
            wrapper = self.filesystem.get_open_file(filedes)
            can_write = True
            if isinstance(wrapper, FakePipeWrapper):
                return None, None, filedes, None, can_write
            if isinstance(wrapper, FakeFileWrapper):
                self._delete_on_close = wrapper.delete_on_close
                can_write = wrapper.allow_update

            file_object = cast(
                FakeFile, self.filesystem.get_open_file(filedes).get_object()
            )
            assert file_object is not None
            path = file_object.name
            return (  # pytype: disable=bad-return-type
                file_object,
                cast(AnyStr, path),  # pytype: disable=invalid-annotation
                filedes,
                cast(AnyStr, path),  # pytype: disable=invalid-annotation
                can_write,
            )

        # open a file by path
        file_path = cast(AnyStr, file_)  # pytype: disable=invalid-annotation
        if file_path == self.filesystem.dev_null.name:
            file_object = self.filesystem.dev_null
            real_path = file_path
        else:
            real_path = self.filesystem.resolve_path(file_path)
            if self.filesystem.exists(file_path):
                file_object = self.filesystem.get_object_from_normpath(
                    real_path, check_read_perm=False
                )
        return file_object, file_path, None, real_path, True

    def _handle_file_mode(
        self,
        mode: str,
        newline: Optional[str],
        open_modes: Optional[_OpenModes],
    ) -> Tuple[Optional[str], _OpenModes]:
        orig_modes = mode  # Save original modes for error messages.
        # Normalize modes. Handle 't' and 'U'.
        if ("b" in mode and "t" in mode) or (
            sys.version_info > (3, 10) and "U" in mode
        ):
            raise ValueError("Invalid mode: " + mode)
        mode = mode.replace("t", "").replace("b", "")
        mode = mode.replace("rU", "r").replace("U", "r")
        if not self.raw_io:
            if mode not in _OPEN_MODE_MAP:
                raise ValueError("Invalid mode: %r" % orig_modes)
            open_modes = _OpenModes(*_OPEN_MODE_MAP[mode])
        assert open_modes is not None
        return newline, open_modes
