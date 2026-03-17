from __future__ import annotations

import base64
import datetime
import logging
import math
import os
import pathlib
import platform
import re
import shutil
import sys
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from queue import Empty, Full, Queue
from tempfile import mkstemp
from threading import Event, Thread
from types import TracebackType
from typing import (TYPE_CHECKING, AnyStr, BinaryIO, Callable, Generator,
                    Iterable, Optional, Type, Union)
from urllib import parse

import requests
import requests.adapters
from requests import RequestException

from .._base_client import _BaseClient, _RawResponse, _StreamingResponse
from .._property import _cached_property
from ..config import Config
from ..errors import AlreadyExists, InternalError, NotFound, PermissionDenied
from ..errors.mapper import _error_mapper
from ..retries import retried
from ..service import files
from ..service._internal import _escape_multi_segment_path_parameter
from ..service.files import DownloadResponse
from .files_utils import (CreateDownloadUrlResponse, _ConcatenatedInputStream,
                          _PresignedUrlDistributor)

if TYPE_CHECKING:
    from _typeshed import Self

_LOG = logging.getLogger(__name__)


class _DbfsIO(BinaryIO):
    MAX_CHUNK_SIZE = 1024 * 1024

    _status: files.FileInfo = None
    _created: files.CreateResponse = None
    _offset = 0
    _closed = False

    def __init__(
        self,
        api: files.DbfsAPI,
        path: str,
        *,
        read: bool = False,
        write: bool = False,
        overwrite: bool = False,
    ):
        self._api = api
        self._path = path
        if write and read:
            raise IOError(f"can open either for reading or writing")
        if read:
            self._status = api.get_status(path)
        elif write:
            self._created = api.create(path, overwrite=overwrite)
        else:
            raise IOError(f"need to open either for reading or writing")

    def __enter__(self) -> Self:
        return self

    @property
    def name(self) -> str:
        return self._path

    def writable(self) -> bool:
        """
        Return whether object was opened for writing.

        If False, write() will raise OSError.
        """
        return self._created is not None

    def write(self, buffer: bytes) -> int:
        """Write bytes to file.

        :return: Return the number of bytes written.
        """
        if not self.writable():
            raise IOError("file not open for writing")
        if type(buffer) is not bytes:
            # Python doesn't strictly enforce types. Even if they're specified.
            raise TypeError(f"a bytes-like object is required, not {type(buffer)}")
        total = 0
        while total < len(buffer):
            chunk = buffer[total:]
            if len(chunk) > self.MAX_CHUNK_SIZE:
                chunk = chunk[: self.MAX_CHUNK_SIZE]
            encoded = base64.b64encode(chunk).decode()
            self._api.add_block(self._created.handle, encoded)
            total += len(chunk)
        return total

    def close(self) -> None:
        """Disable all I/O operations."""
        if self.writable():
            self._api.close(self._created.handle)
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __exit__(
        self,
        __t: Type[BaseException] | None,
        __value: BaseException | None,
        __traceback: TracebackType | None,
    ):
        self.close()

    def readable(self) -> bool:
        return self._status is not None

    def read(self, size: int = ...) -> bytes:
        """Read at most size bytes, returned as a bytes object.

        :param size: If the size argument is negative, read until EOF is reached.
                     Return an empty bytes object at EOF.
        :return: bytes
        """
        if not self.readable():
            raise IOError("file not open for reading")

        # call __iter__() and read until EOF is reached
        if size is ... or size < 0:
            buffer = b""
            for chunk in self:
                buffer += chunk
            return buffer

        response = self._api.read(self._path, length=size, offset=self._offset)
        # The guard against offset >= size happens above, so this can only happen
        # if the file is modified or truncated while reading. If this happens,
        # the read contents will likely be corrupted, so we return an error.
        if response.bytes_read == 0:
            # as per Python interface convention, return an empty bytes object at EOF,
            # and not the EOFError as in other SDKs
            return b""

        raw = base64.b64decode(response.data)
        self._offset += response.bytes_read
        return raw

    def __iter__(self) -> Iterator[bytes]:
        while self._offset < self._status.file_size:
            yield self.__next__()

    def __next__(self) -> bytes:
        # TODO: verify semantics
        return self.read(self.MAX_CHUNK_SIZE)

    def fileno(self) -> int:
        return 0

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return False

    def readline(self, __limit: int = ...) -> AnyStr:
        raise NotImplementedError

    def readlines(self, __hint: int = ...) -> list[AnyStr]:
        raise NotImplementedError

    def seek(self, __offset: int, __whence: int = ...) -> int:
        raise NotImplementedError

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._offset

    def truncate(self, __size: int | None = ...) -> int:
        raise NotImplementedError

    def writelines(self, __lines: Iterable[AnyStr]) -> None:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<_DbfsIO {self._path} {'read' if self.readable() else 'write'}=True>"


class _VolumesIO(BinaryIO):

    def __init__(
        self,
        api: files.FilesAPI,
        path: str,
        *,
        read: bool,
        write: bool,
        overwrite: bool,
    ):
        self._buffer = []
        self._api = api
        self._path = path
        self._read = read
        self._write = write
        self._overwrite = overwrite
        self._closed = False
        self._read_handle = None
        self._offset = 0

    def __enter__(self):
        if self._read:
            self.__open_read()
        return self

    def close(self):
        if self._closed:
            return
        if self._write:
            to_write = b"".join(self._buffer)
            self._api.upload(
                self._path,
                contents=BytesIO(to_write),
                overwrite=self._overwrite,
            )
        elif self._read:
            self._read_handle.close()
        self._closed = True

    def fileno(self) -> int:
        return 0

    def flush(self):
        raise NotImplementedError()

    def isatty(self) -> bool:
        return False

    def __check_closed(self):
        if self._closed:
            raise ValueError("I/O operation on closed file")

    def __open_read(self):
        if self._read_handle is None:
            self._read_handle = self._api.download(self._path).contents

    def read(self, __n=...):
        self.__check_closed()
        self.__open_read()
        return self._read_handle.read(__n)

    def readable(self):
        return self._read

    def readline(self, __limit=...):
        raise NotImplementedError()

    def readlines(self, __hint=...):
        raise NotImplementedError()

    def seek(self, __offset, __whence=...):
        raise NotImplementedError()

    def seekable(self):
        return False

    def tell(self):
        if self._read_handle is not None:
            return self._read_handle.tell()
        return self._offset

    def truncate(self, __size=...):
        raise NotImplementedError()

    def writable(self):
        return self._write

    def write(self, __s):
        self.__check_closed()
        self._buffer.append(__s)

    def writelines(self, __lines):
        raise NotImplementedError()

    def __next__(self):
        self.__check_closed()
        return self._read_handle.__next__()

    def __iter__(self):
        self.__check_closed()
        return self._read_handle.__iter__()

    def __exit__(self, __t, __value, __traceback):
        self.close()

    def __repr__(self) -> str:
        return f"<_VolumesIO {self._path} {'read' if self.readable() else 'write'}=True>"


class _Path(ABC):

    @abstractmethod
    def __init__(self): ...

    @property
    def is_local(self) -> bool:
        return self._is_local()

    @abstractmethod
    def _is_local(self) -> bool: ...

    @property
    def is_dbfs(self) -> bool:
        return self._is_dbfs()

    @abstractmethod
    def _is_dbfs(self) -> bool: ...

    @abstractmethod
    def child(self, path: str) -> str: ...

    @_cached_property
    def is_dir(self) -> bool:
        return self._is_dir()

    @abstractmethod
    def _is_dir(self) -> bool: ...

    @abstractmethod
    def exists(self) -> bool: ...

    @abstractmethod
    def open(self, *, read=False, write=False, overwrite=False): ...

    def list(self, *, recursive=False) -> Generator[files.FileInfo, None, None]: ...

    @abstractmethod
    def mkdir(self): ...

    @abstractmethod
    def delete(self, *, recursive=False): ...

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def as_string(self) -> str:
        return str(self._path)


class _LocalPath(_Path):

    def __init__(self, path: str):
        if platform.system() == "Windows":
            self._path = pathlib.Path(str(path).replace("file:///", "").replace("file:", ""))
        else:
            self._path = pathlib.Path(str(path).replace("file:", ""))

    def _is_local(self) -> bool:
        return True

    def _is_dbfs(self) -> bool:
        return False

    def child(self, path: str) -> Self:
        return _LocalPath(str(self._path / path))

    def _is_dir(self) -> bool:
        return self._path.is_dir()

    def mkdir(self):
        self._path.mkdir(mode=0o755, parents=True, exist_ok=True)

    def exists(self) -> bool:
        return self._path.exists()

    def open(self, *, read=False, write=False, overwrite=False):
        # make local fs follow the similar semantics as DBFS
        self._path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        return self._path.open(mode="wb" if overwrite else "rb" if read else "xb")

    def list(self, recursive=False) -> Generator[files.FileInfo, None, None]:
        if not self.is_dir:
            st = self._path.stat()
            yield files.FileInfo(
                path="file:" + str(self._path.absolute()),
                is_dir=False,
                file_size=st.st_size,
                modification_time=int(st.st_mtime_ns / 1e6),
            )
            return
        queue = deque([self._path])
        while queue:
            path = queue.popleft()
            for leaf in path.iterdir():
                if leaf.is_dir():
                    if recursive:
                        queue.append(leaf)
                    continue
                info = leaf.stat()
                yield files.FileInfo(
                    path="file:" + str(leaf.absolute()),
                    is_dir=False,
                    file_size=info.st_size,
                    modification_time=int(info.st_mtime_ns / 1e6),
                )

    def delete(self, *, recursive=False):
        if self.is_dir:
            if recursive:
                for leaf in self.list(recursive=True):
                    _LocalPath(leaf.path).delete()
            self._path.rmdir()
        else:
            kw = {}
            if sys.version_info[:2] > (3, 7):
                kw["missing_ok"] = True
            self._path.unlink(**kw)

    def __repr__(self) -> str:
        return f"<_LocalPath {self._path}>"


class _VolumesPath(_Path):

    def __init__(self, api: files.FilesAPI, src: Union[str, pathlib.Path]):
        self._path = pathlib.PurePosixPath(str(src).replace("dbfs:", "").replace("file:", ""))
        self._api = api

    def _is_local(self) -> bool:
        return False

    def _is_dbfs(self) -> bool:
        return False

    def child(self, path: str) -> Self:
        return _VolumesPath(self._api, str(self._path / path))

    def _is_dir(self) -> bool:
        try:
            self._api.get_directory_metadata(self.as_string)
            return True
        except NotFound:
            return False

    def mkdir(self):
        self._api.create_directory(self.as_string)

    def exists(self) -> bool:
        try:
            self._api.get_metadata(self.as_string)
            return True
        except NotFound:
            return self.is_dir

    def open(self, *, read=False, write=False, overwrite=False) -> BinaryIO:
        return _VolumesIO(
            self._api,
            self.as_string,
            read=read,
            write=write,
            overwrite=overwrite,
        )

    def list(self, *, recursive=False) -> Generator[files.FileInfo, None, None]:
        if not self.is_dir:
            meta = self._api.get_metadata(self.as_string)
            yield files.FileInfo(
                path=self.as_string,
                is_dir=False,
                file_size=meta.content_length,
                modification_time=meta.last_modified,
            )
            return
        queue = deque([self])
        while queue:
            next_path = queue.popleft()
            for file in self._api.list_directory_contents(next_path.as_string):
                if recursive and file.is_directory:
                    queue.append(self.child(file.name))
                if not recursive or not file.is_directory:
                    yield files.FileInfo(
                        path=file.path,
                        is_dir=file.is_directory,
                        file_size=file.file_size,
                        modification_time=file.last_modified,
                    )

    def delete(self, *, recursive=False):
        if self.is_dir:
            for entry in self.list(recursive=False):
                _VolumesPath(self._api, entry.path).delete(recursive=True)
            self._api.delete_directory(self.as_string)
        else:
            self._api.delete(self.as_string)

    def __repr__(self) -> str:
        return f"<_VolumesPath {self._path}>"


class _DbfsPath(_Path):

    def __init__(self, api: files.DbfsAPI, src: str):
        self._path = pathlib.PurePosixPath(str(src).replace("dbfs:", "").replace("file:", ""))
        self._api = api

    def _is_local(self) -> bool:
        return False

    def _is_dbfs(self) -> bool:
        return True

    def child(self, path: str) -> Self:
        child = self._path / path
        return _DbfsPath(self._api, str(child))

    def _is_dir(self) -> bool:
        try:
            remote = self._api.get_status(self.as_string)
            return remote.is_dir
        except NotFound:
            return False

    def mkdir(self):
        self._api.mkdirs(self.as_string)

    def exists(self) -> bool:
        try:
            self._api.get_status(self.as_string)
            return True
        except NotFound:
            return False

    def open(self, *, read=False, write=False, overwrite=False) -> BinaryIO:
        return _DbfsIO(
            self._api,
            self.as_string,
            read=read,
            write=write,
            overwrite=overwrite,
        )

    def list(self, *, recursive=False) -> Generator[files.FileInfo, None, None]:
        if not self.is_dir:
            meta = self._api.get_status(self.as_string)
            yield files.FileInfo(
                path=self.as_string,
                is_dir=False,
                file_size=meta.file_size,
                modification_time=meta.modification_time,
            )
            return
        queue = deque([self])
        while queue:
            next_path = queue.popleft()
            for file in self._api.list(next_path.as_string):
                if recursive and file.is_dir:
                    queue.append(self.child(file.path))
                if not recursive or not file.is_dir:
                    yield file

    def delete(self, *, recursive=False):
        self._api.delete(self.as_string, recursive=recursive)

    def __repr__(self) -> str:
        return f"<_DbfsPath {self._path}>"


class _RetryableException(Exception):
    """Base class for retryable exceptions in DBFS operations."""

    def __init__(self, message: str, http_status_code: int):
        super().__init__()
        self.message = message
        self.http_status_code = http_status_code

    def __str__(self) -> str:
        return f"{self.message} (HTTP Status: {self.http_status_code})"

    @staticmethod
    def make_error(response: requests.Response) -> "_RetryableException":
        """Map the response to a retryable exception."""

        return _RetryableException(
            message=response.text,
            http_status_code=response.status_code,
        )


class DbfsExt(files.DbfsAPI):
    __doc__ = files.DbfsAPI.__doc__

    def __init__(self, api_client):
        super().__init__(api_client)
        self._files_api = files.FilesAPI(api_client)
        self._dbfs_api = files.DbfsAPI(api_client)

    def open(
        self,
        path: str,
        *,
        read: bool = False,
        write: bool = False,
        overwrite: bool = False,
    ) -> BinaryIO:
        return self._path(path).open(read=read, write=write, overwrite=overwrite)

    def upload(self, path: str, src: BinaryIO, *, overwrite: bool = False):
        """Upload file to DBFS"""
        with self.open(path, write=True, overwrite=overwrite) as dst:
            shutil.copyfileobj(src, dst, length=_DbfsIO.MAX_CHUNK_SIZE)

    def download(self, path: str) -> BinaryIO:
        """Download file from DBFS"""
        return self.open(path, read=True)

    def list(self, path: str, *, recursive=False) -> Iterator[files.FileInfo]:
        """List directory contents or file details.

        List the contents of a directory, or details of the file. If the file or directory does not exist,
        this call throws an exception with `RESOURCE_DOES_NOT_EXIST`.

        When calling list on a large directory, the list operation will time out after approximately 60
        seconds.

        :param path: the DBFS or UC Volume path to list
        :param recursive: traverse deep into directory tree
        :returns iterator of metadata for every file
        """
        p = self._path(path)
        yield from p.list(recursive=recursive)

    def mkdirs(self, path: str):
        """Create directory on DBFS"""
        p = self._path(path)
        p.mkdir()

    def exists(self, path: str) -> bool:
        """If file exists on DBFS"""
        p = self._path(path)
        return p.exists()

    __ALLOWED_SCHEMES = [None, "file", "dbfs"]

    def _path(self, src):
        src = parse.urlparse(str(src))
        if src.scheme and src.scheme not in self.__ALLOWED_SCHEMES:
            raise ValueError(
                f'unsupported scheme "{src.scheme}". DBUtils in the SDK only supports local, root DBFS, and '
                "UC Volumes paths, not external locations or DBFS mount points."
            )
        if src.scheme == "file":
            return _LocalPath(src.geturl())
        if src.path.startswith("/Volumes"):
            return _VolumesPath(self._files_api, src.geturl())
        return _DbfsPath(self._dbfs_api, src.geturl())

    def copy(self, src: str, dst: str, *, recursive=False, overwrite=False):
        """Copy files between DBFS and local filesystems"""
        src = self._path(src)
        dst = self._path(dst)
        if src.is_local and dst.is_local:
            raise IOError("both destinations are on local FS")
        if dst.exists() and dst.is_dir:
            # if target is a folder, make file with the same name there
            dst = dst.child(src.name)
        if src.is_dir:
            queue = [self._path(x.path) for x in src.list(recursive=recursive) if not x.is_dir]
        else:
            queue = [src]
        for child in queue:
            child_dst = dst.child(os.path.relpath(child.as_string, src.as_string))
            with child.open(read=True) as reader:
                with child_dst.open(write=True, overwrite=overwrite) as writer:
                    shutil.copyfileobj(reader, writer, length=_DbfsIO.MAX_CHUNK_SIZE)

    def move_(self, src: str, dst: str, *, recursive=False, overwrite=False):
        """Move files between local and DBFS systems"""
        source = self._path(src)
        target = self._path(dst)
        if source.is_dbfs and target.is_dbfs:
            # Moves a file from one location to another location within DBFS.
            # this operation is recursive by default.
            return self.move(source.as_string, target.as_string)
        if source.is_local and target.is_local:
            raise IOError("both destinations are on local FS")
        if source.is_dir and not recursive:
            src_type = "local" if source.is_local else "DBFS" if source.is_dbfs else "UC Volume"
            dst_type = "local" if target.is_local else "DBFS" if target.is_dbfs else "UC Volume"
            raise IOError(f"moving a directory from {src_type} to {dst_type} requires recursive flag")
        # do cross-fs moving
        self.copy(src, dst, recursive=recursive, overwrite=overwrite)
        self.delete(src, recursive=recursive)

    def delete(self, path: str, *, recursive=False):
        """Delete file or directory on DBFS"""
        p = self._path(path)
        if p.is_dir and not recursive:
            raise IOError("deleting directories requires recursive flag")
        p.delete(recursive=recursive)


class FallbackToUploadUsingFilesApi(Exception):
    """Custom exception that signals to fallback to FilesAPI for upload"""

    def __init__(self, buffer, message):
        super().__init__(message)
        self.buffer = buffer


class FallbackToDownloadUsingFilesApi(Exception):
    """Custom exception that signals to fallback to FilesAPI for download"""

    def __init__(self, message):
        super().__init__(message)


@dataclass
class UploadStreamResult:
    """Result of an upload from stream operation. Currently empty, but can be extended in the future."""


@dataclass
class UploadFileResult:
    """Result of an upload from file operation. Currently empty, but can be extended in the future."""


@dataclass
class DownloadFileResult:
    """Result of a download to file operation. Currently empty, but can be extended in the future."""


class FilesExt(files.FilesAPI):
    __doc__ = files.FilesAPI.__doc__

    # note that these error codes are retryable only for idempotent operations
    _RETRYABLE_STATUS_CODES: list[int] = [408, 429, 502, 503, 504]

    @dataclass(frozen=True)
    class _UploadContext:
        target_path: str
        """The absolute remote path of the target file, e.g. /Volumes/path/to/your/file."""
        overwrite: Optional[bool]
        """If true, an existing file will be overwritten. When unspecified, default behavior of the cloud storage provider is performed."""
        part_size: int
        """The size of each part in bytes for multipart upload."""
        batch_size: int
        """The number of urls to request in a single batch."""
        content_length: Optional[int] = None
        """The total size of the content being uploaded, if known."""
        source_file_path: Optional[str] = None
        """The local path of the file being uploaded, if applicable."""
        use_parallel: Optional[bool] = None
        """If true, the upload will be performed using multiple threads."""
        parallelism: Optional[int] = None
        """The number of threads to use for parallel upload, if applicable."""

    def __init__(self, api_client, config: Config):
        super().__init__(api_client)
        self._config = config.copy()
        self._multipart_upload_read_ahead_bytes = 1

    def download(
        self,
        file_path: str,
    ) -> DownloadResponse:
        """Download a file.

        Downloads a file as a stream into memory.

        Use this when you want to process the downloaded file in memory or pipe it into another system. Supports files of any size in SDK v0.72.0+. Earlier versions have a 5 GB file size limit.

        If the download is successful, the function returns the downloaded file result. If the download is unsuccessful, the function raises an exception.

        :param file_path: str
          The remote path of the file, e.g. /Volumes/path/to/your/file

        :returns: :class:`DownloadResponse`
        """
        if self._config.disable_experimental_files_api_client:
            _LOG.info("Disable experimental files API client, will use the original download method.")
            return super().download(file_path)

        initial_response: DownloadResponse = self._open_download_stream(
            file_path=file_path, start_byte_offset=0, if_unmodified_since_timestamp=None
        )

        wrapped_response = self._wrap_stream(file_path, initial_response)
        initial_response.contents._response = wrapped_response
        return initial_response

    def download_to(
        self,
        file_path: str,
        destination: str,
        *,
        overwrite: bool = True,
        use_parallel: bool = False,
        parallelism: Optional[int] = None,
    ) -> DownloadFileResult:
        """Downloads a file directly to a local file path.

        Use this when you want to write the file straight to disk instead of holding it in memory. Supports files of any size in SDK v0.72.0+. Earlier versions have a 5 GB file size limit.

        Supports parallel download (use_parallel=True), which may improve performance for large files. This is available on all operating systems except Windows.

        :param file_path: str
          The remote path of the file, e.g. /Volumes/path/to/your/file
        :param destination: str
          The local path where the file will be saved.
        :param overwrite: bool
          If true, an existing file will be overwritten. When not specified, defaults to True.
        :param use_parallel: bool
          If true, the download will be performed using multiple threads.
        :param parallelism: int
          The number of parallel threads to use for downloading. If not specified, defaults to the number of CPU cores.

        :returns: :class:`DownloadFileResult`
        """
        if self._config.disable_experimental_files_api_client:
            raise NotImplementedError(
                "Experimental files API features are disabled, download_to is not supported. Please use download instead."
            )

        # The existence of the target file is checked before starting the download. This is a best-effort check
        # to avoid overwriting an existing file. However, there is nothing preventing a file from being created
        # at the destination path after this check and before the file is written, and no way to prevent other
        # actor from writing to the destination path concurrently.
        if not overwrite and os.path.exists(destination):
            raise FileExistsError(destination)
        if use_parallel:
            # Parallel download is not supported for Windows due to the limit of only one open file handle
            # for writing. If parallel download is requested on Windows, fall back to sequential download with
            # a warning.
            if platform.system() == "Windows":
                _LOG.warning("Parallel download is not supported on Windows. Falling back to sequential download.")
                self._sequential_download_to_file(destination, remote_path=file_path)
                return DownloadFileResult()
            if parallelism is None:
                parallelism = self._config.files_ext_parallel_download_default_parallelism
            if parallelism < 1 or parallelism > 64:
                raise ValueError("parallelism must be between 1 and 64")
            self._parallel_download_with_fallback(file_path, destination, parallelism=parallelism)
        else:
            self._sequential_download_to_file(destination, remote_path=file_path)
        return DownloadFileResult()

    def _parallel_download_with_fallback(self, remote_path: str, destination: str, parallelism: int) -> None:
        """Download a file in parallel to a local path. There would be no responses returned if the download is successful.
        This method first tries to use the Presigned URL for parallel download. If it fails due to permission issues,
        it falls back to using Files API.

        :param remote_path: str
          The remote path of the file, e.g. /Volumes/path/to/your/file
        :param destination: str
          The local path where the file will be saved.
        :param parallelism: int
          The number of parallel threads to use for downloading.

        :returns: None
        """
        try:
            self._parallel_download_presigned_url(remote_path, destination, parallelism)
        except FallbackToDownloadUsingFilesApi as e:
            _LOG.info("Falling back to Files API download due to permission issues with Presigned URL: %s", e)
            self._parallel_download_files_api(remote_path, destination, parallelism)

    def _sequential_download_to_file(
        self, destination: str, remote_path: str, last_modified: Optional[str] = None
    ) -> None:
        with open(destination, "wb") as f:
            response = self._open_download_stream(
                file_path=remote_path,
                start_byte_offset=0,
                if_unmodified_since_timestamp=last_modified,
            )
            wrapped_response = self._wrap_stream(remote_path, response, 0)
            response.contents._response = wrapped_response
            shutil.copyfileobj(response.contents, f)

    def _do_parallel_download(
        self, remote_path: str, destination: str, parallelism: int, download_chunk: Callable
    ) -> None:

        file_info = self.get_metadata(remote_path)
        file_size = file_info.content_length
        last_modified = file_info.last_modified
        # If the file is smaller than the threshold, do not use parallel download.
        if file_size <= self._config.files_ext_parallel_download_min_file_size:
            self._sequential_download_to_file(destination, remote_path, last_modified)
            return
        part_size = self._config.files_ext_parallel_download_default_part_size
        part_count = int(math.ceil(file_size / part_size))

        fd, temp_file = mkstemp()
        # We are preallocate the file size to the same as the remote file to avoid seeking beyond the file size.
        os.truncate(temp_file, file_size)
        os.close(fd)
        try:
            aborted = Event()

            def wrapped_download_chunk(start: int, end: int, last_modified: Optional[str], temp_file: str) -> None:
                if aborted.is_set():
                    return
                additional_headers = {
                    "Range": f"bytes={start}-{end}",
                    "If-Unmodified-Since": last_modified,
                }
                try:
                    contents = download_chunk(additional_headers)
                    with open(temp_file, "r+b") as f:
                        f.seek(start)
                        shutil.copyfileobj(contents, f)
                except Exception as e:
                    aborted.set()
                    raise e

            with ThreadPoolExecutor(max_workers=parallelism) as executor:
                futures = []
                # Start the threads to download parts of the file.
                for i in range(part_count):
                    start = i * part_size
                    end = min(start + part_size - 1, file_size - 1)
                    futures.append(executor.submit(wrapped_download_chunk, start, end, last_modified, temp_file))

                # Wait for all threads to complete and check for exceptions.
                for future in as_completed(futures):
                    exception = future.exception()
                    if exception:
                        raise exception
            # Finally, move the temp file to the destination.
            shutil.move(temp_file, destination)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def _parallel_download_presigned_url(self, remote_path: str, destination: str, parallelism: int) -> None:
        """Download a file in parallel to a local path. There would be no responses returned if the download is successful.

        :param remote_path: str
          The remote path of the file, e.g. /Volumes/path/to/your/file
        :param destination: str
          The local path where the file will be saved.
        :param parallelism: int
          The number of parallel threads to use for downloading.

        :returns: None
        """

        cloud_session = self._create_cloud_provider_session()
        url_distributor = _PresignedUrlDistributor(lambda: self._create_download_url(remote_path))
        # An event to indicate if any download chunk has succeeded. If any chunk succeeds, we do not fall back to Files API.
        any_success = Event()

        def download_chunk(additional_headers: dict[str, str]) -> BinaryIO:
            retry_count = 0
            while retry_count < self._config.files_ext_parallel_download_max_retries:
                url_and_header, version = url_distributor.get_url()

                headers = {**url_and_header.headers, **additional_headers}

                def get_content() -> requests.Response:
                    return cloud_session.get(url_and_header.url, headers=headers)

                raw_resp = self._retry_cloud_idempotent_operation(get_content)

                if FilesExt._is_url_expired_response(raw_resp):
                    _LOG.info("Presigned URL expired, fetching a new one.")
                    url_distributor.invalidate_url(version)
                    retry_count += 1
                    continue
                elif raw_resp.status_code == 403 and not any_success.is_set():
                    raise FallbackToDownloadUsingFilesApi("Received 403 Forbidden from presigned URL")
                elif not any_success.is_set():
                    # For other errors, we raise a retryable exception to trigger retry logic.
                    raise FallbackToDownloadUsingFilesApi(f"Received {raw_resp.status_code} from presigned URL")

                raw_resp.raise_for_status()
                any_success.set()
                return BytesIO(raw_resp.content)
            raise ValueError("Exceeded maximum retries for downloading with presigned URL: URL expired too many times")

        self._do_parallel_download(remote_path, destination, parallelism, download_chunk)

    def _parallel_download_files_api(self, remote_path: str, destination: str, parallelism: int) -> None:
        """Download a file in parallel to a local path using FilesAPI. There would be no responses returned if the download is successful.

        :param remote_path: str
          The remote path of the file, e.g. /Volumes/path/to/your/file
        :param destination: str
          The local path where the file will be saved.
        :param parallelism: int
          The number of parallel threads to use for downloading.

        :returns: None
        """

        def download_chunk(additional_headers: dict[str, str]) -> BinaryIO:
            raw_response: dict = self._api.do(
                method="GET",
                path=f"/api/2.0/fs/files{remote_path}",
                headers=additional_headers,
                raw=True,
            )
            return raw_response["contents"]

        self._do_parallel_download(remote_path, destination, parallelism, download_chunk)

    def _get_optimized_performance_parameters_for_upload(
        self, content_length: Optional[int], part_size_overwrite: Optional[int]
    ) -> (int, int):
        """Get optimized part size and batch size for upload based on content length and provided part size.

        Returns tuple of (part_size, batch_size).
        """
        chosen_part_size = None

        # 1. decide on the part size
        if part_size_overwrite is not None:  # If a part size is provided, we use it directly after validation.
            if part_size_overwrite > self._config.files_ext_multipart_upload_max_part_size:
                raise ValueError(
                    f"Part size {part_size_overwrite} exceeds maximum allowed size {self._config.files_ext_multipart_upload_max_part_size} bytes."
                )
            chosen_part_size = part_size_overwrite
            _LOG.debug(f"Using provided part size: {chosen_part_size} bytes")
        else:  # If no part size is provided, we will optimize based on the content length.
            if content_length is not None:
                # Choosing the smallest part size that allows for a maximum of 100 parts.
                for part_size in self._config.files_ext_multipart_upload_part_size_options:
                    part_num = (content_length + part_size - 1) // part_size
                    if part_num <= 100:
                        chosen_part_size = part_size
                        _LOG.debug(
                            f"Optimized part size for upload: {chosen_part_size} bytes for content length {content_length} bytes"
                        )
                        break
                if chosen_part_size is None:  # If no part size was chosen, we default to the maximum allowed part size.
                    chosen_part_size = self._config.files_ext_multipart_upload_max_part_size

        # Use defaults if not determined yet
        if chosen_part_size is None:
            chosen_part_size = self._config.files_ext_multipart_upload_default_part_size

        # 2. decide on the batch size
        if content_length is not None and chosen_part_size is not None:
            part_num = (content_length + chosen_part_size - 1) // chosen_part_size
            chosen_batch_size = int(
                math.ceil(math.sqrt(part_num))
            )  # Using the square root of the number of parts as a heuristic for batch size.
        else:
            chosen_batch_size = self._config.files_ext_multipart_upload_batch_url_count

        return chosen_part_size, chosen_batch_size

    def upload(
        self,
        file_path: str,
        contents: BinaryIO,
        *,
        overwrite: Optional[bool] = None,
        part_size: Optional[int] = None,
        use_parallel: bool = True,
        parallelism: Optional[int] = None,
    ) -> UploadStreamResult:
        """
        Uploads a file from memory or a stream interface.

        Use this when you want to upload data already in memory or piped from another system. Supports files of any size in SDK v0.72.0+. Earlier versions have a 5 GB file size limit.

        Limitations: If the storage account is on Azure and has firewall enabled, the maximum file size is 5GB.

        :param file_path: str
            The absolute remote path of the target file, e.g. /Volumes/path/to/your/file
        :param contents: BinaryIO
            The contents of the file to upload. This must be a BinaryIO stream.
        :param overwrite: bool (optional)
            If true, an existing file will be overwritten. When not specified, defaults to True.
        :param part_size: int (optional)
            If set, multipart upload will use the value as its size per uploading part. If not set, an appropriate value will be automatically used.
        :param use_parallel: bool (optional)
            If true, the upload will be performed using multiple threads. Note that this will consume more memory
            because multiple parts will be buffered in memory before being uploaded. The amount of memory used is proportional
            to `parallelism * part_size`.
            If false, the upload will be performed in a single thread.
            Default is True.
        :param parallelism: int (optional)
            The number of threads to use for parallel uploads. This is only used if `use_parallel` is True.

        :returns: :class:`UploadStreamResult`
        """

        if self._config.disable_experimental_files_api_client:
            _LOG.info("Disable experimental files API client, will use the original upload method.")
            super().upload(file_path=file_path, contents=contents, overwrite=overwrite)
            return UploadStreamResult()

        _LOG.debug(f"Uploading file from BinaryIO stream")
        if parallelism is not None and not use_parallel:
            raise ValueError("parallelism can only be set if use_parallel is True")
        if parallelism is None and use_parallel:
            parallelism = self._config.files_ext_multipart_upload_default_parallelism

        # Determine content length if the stream is seekable
        content_length = None
        if contents.seekable():
            _LOG.debug(f"Uploading using seekable mode")
            # If the stream is seekable, we can read its size.
            contents.seek(0, os.SEEK_END)
            content_length = contents.tell()
            contents.seek(0)

        # Get optimized part size and batch size based on content length and provided part size
        optimized_part_size, optimized_batch_size = self._get_optimized_performance_parameters_for_upload(
            content_length, part_size
        )

        # Create context with all final parameters
        ctx = self._UploadContext(
            target_path=file_path,
            overwrite=overwrite,
            part_size=optimized_part_size,
            batch_size=optimized_batch_size,
            content_length=content_length,
            use_parallel=use_parallel,
            parallelism=parallelism,
        )

        _LOG.debug(
            f"Upload context: part_size={ctx.part_size}, batch_size={ctx.batch_size}, content_length={ctx.content_length}"
        )

        if ctx.use_parallel and (
            ctx.content_length is None or ctx.content_length >= self._config.files_ext_multipart_upload_min_stream_size
        ):
            self._parallel_upload_from_stream(ctx, contents)
            return UploadStreamResult()
        elif ctx.content_length is not None:
            self._upload_single_thread_with_known_size(ctx, contents)
            return UploadStreamResult()
        else:
            _LOG.debug(f"Uploading using non-seekable mode")
            # If the stream is not seekable, we cannot determine its size.
            # We will use a multipart upload.
            _LOG.debug(f"Using multipart upload for non-seekable input stream of unknown size for file {file_path}")
            self._single_thread_multipart_upload(ctx, contents)
            return UploadStreamResult()

    def upload_from(
        self,
        file_path: str,
        source_path: str,
        *,
        overwrite: Optional[bool] = None,
        part_size: Optional[int] = None,
        use_parallel: bool = True,
        parallelism: Optional[int] = None,
    ) -> UploadFileResult:
        """
        Uploads a file from a local file path.

        Use this when your data already exists on disk and you want to upload it directly without manually opening it yourself. Supports files of any size in SDK v0.72.0+. Earlier versions have a 5 GB file size limit.

        :param file_path: str
          The absolute remote path of the target file.
        :param source_path: str
          The local path of the file to upload. This must be a path to a local file.
        :param part_size: int (optional)
          If set, multipart upload will use the value as its size per uploading part. If not set, an appropriate default  value will be automatically used.
        :param overwrite: bool (optional)
          If true, an existing file will be overwritten. When not specified, defaults True.
        :param use_parallel: bool (optional)
          If true, the upload will be performed using multiple threads. Default is True.
        :param parallelism: int (optional)
          The number of threads to use for parallel uploads. This is only used if `use_parallel` is True.
          If not specified, the default parallelism will be set to config.multipart_upload_default_parallelism

        :returns: :class:`UploadFileResult`
        """
        if self._config.disable_experimental_files_api_client:
            raise NotImplementedError(
                "Experimental files API features are disabled, upload_from is not supported. Please use upload instead."
            )

        _LOG.debug(f"Uploading file from local path: {source_path}")

        if parallelism is not None and not use_parallel:
            raise ValueError("parallelism can only be set if use_parallel is True")
        if parallelism is None and use_parallel:
            parallelism = self._config.files_ext_multipart_upload_default_parallelism
        # Get the file size
        file_size = os.path.getsize(source_path)

        # Get optimized part size and batch size based on content length and provided part size
        optimized_part_size, optimized_batch_size = self._get_optimized_performance_parameters_for_upload(
            file_size, part_size
        )

        # Create context with all final parameters
        ctx = self._UploadContext(
            target_path=file_path,
            overwrite=overwrite,
            part_size=optimized_part_size,
            batch_size=optimized_batch_size,
            content_length=file_size,
            source_file_path=source_path,
            use_parallel=use_parallel,
            parallelism=parallelism,
        )
        if ctx.use_parallel and ctx.content_length >= self._config.files_ext_multipart_upload_min_stream_size:
            self._parallel_upload_from_file(ctx)
            return UploadFileResult()
        else:
            with open(source_path, "rb") as f:
                self._upload_single_thread_with_known_size(ctx, f)
                return UploadFileResult()

    def _upload_single_thread_with_known_size(self, ctx: _UploadContext, contents: BinaryIO) -> None:
        """Upload a file with a known size."""
        if ctx.content_length < self._config.files_ext_multipart_upload_min_stream_size:
            _LOG.debug(f"Using single-shot upload for input stream of size {ctx.content_length} bytes")
            return self._single_thread_single_shot_upload(ctx, contents)
        else:
            _LOG.debug(f"Using multipart upload for input stream of size {ctx.content_length} bytes")
            return self._single_thread_multipart_upload(ctx, contents)

    def _single_thread_single_shot_upload(self, ctx: _UploadContext, contents: BinaryIO) -> None:
        """Upload a file with a known size."""
        _LOG.debug(f"Using single-shot upload for input stream")
        return super().upload(file_path=ctx.target_path, contents=contents, overwrite=ctx.overwrite)

    def _initiate_multipart_upload(self, ctx: _UploadContext) -> dict:
        """Initiate a multipart upload and return the response."""
        query = {"action": "initiate-upload"}
        if ctx.overwrite is not None:
            query["overwrite"] = ctx.overwrite

        # Method _api.do() takes care of retrying and will raise an exception in case of failure.
        initiate_upload_response = self._api.do(
            "POST", f"/api/2.0/fs/files{_escape_multi_segment_path_parameter(ctx.target_path)}", query=query
        )
        return initiate_upload_response

    def _single_thread_multipart_upload(self, ctx: _UploadContext, contents: BinaryIO) -> None:

        # Upload empty and small files with one-shot upload.
        pre_read_buffer = contents.read(self._config.files_ext_multipart_upload_min_stream_size)
        if len(pre_read_buffer) < self._config.files_ext_multipart_upload_min_stream_size:
            _LOG.debug(
                f"Using one-shot upload for input stream of size {len(pre_read_buffer)} below {self._config.files_ext_multipart_upload_min_stream_size} bytes"
            )
            return self._single_thread_single_shot_upload(ctx, BytesIO(pre_read_buffer))

        # Initiate the multipart upload.
        initiate_upload_response = self._initiate_multipart_upload(ctx)

        if initiate_upload_response.get("multipart_upload"):
            cloud_provider_session = self._create_cloud_provider_session()
            session_token = initiate_upload_response["multipart_upload"].get("session_token")
            if not session_token:
                raise ValueError(f"Unexpected server response: {initiate_upload_response}")

            try:
                self._perform_multipart_upload(ctx, contents, session_token, pre_read_buffer, cloud_provider_session)
            except FallbackToUploadUsingFilesApi as e:
                try:
                    self._abort_multipart_upload(ctx, session_token, cloud_provider_session)
                except BaseException as ex:
                    # Ignore abort exceptions as it is a best-effort.
                    _LOG.warning(f"Failed to abort upload: {ex}")

                _LOG.info(f"Falling back to single-shot upload with Files API: {e}")
                # Concatenate the buffered part and the rest of the stream.
                full_stream = _ConcatenatedInputStream(BytesIO(e.buffer), contents)
                return self._single_thread_single_shot_upload(ctx, full_stream)

            except Exception as e:
                _LOG.info(f"Aborting multipart upload on error: {e}")
                try:
                    self._abort_multipart_upload(ctx, session_token, cloud_provider_session)
                except BaseException as ex:
                    # Ignore abort exceptions as it is a best-effort.
                    _LOG.warning(f"Failed to abort upload: {ex}")
                finally:
                    # Rethrow the original exception
                    raise e from None

        elif initiate_upload_response.get("resumable_upload"):
            cloud_provider_session = self._create_cloud_provider_session()
            session_token = initiate_upload_response["resumable_upload"]["session_token"]

            try:
                self._perform_resumable_upload(ctx, contents, session_token, pre_read_buffer, cloud_provider_session)
            except FallbackToUploadUsingFilesApi as e:
                _LOG.info(f"Falling back to single-shot upload with Files API: {e}")
                # Concatenate the buffered part and the rest of the stream.
                full_stream = _ConcatenatedInputStream(BytesIO(e.buffer), contents)
                return self._single_thread_single_shot_upload(ctx, full_stream)
        else:
            raise ValueError(f"Unexpected server response: {initiate_upload_response}")

    def _parallel_upload_from_stream(self, ctx: _UploadContext, contents: BinaryIO) -> None:
        """
        Upload a stream using multipart upload with multiple threads.
        This method is not implemented in this example, but it would typically
        involve creating multiple threads to upload different parts of the stream concurrently.
        """
        initiate_upload_response = self._initiate_multipart_upload(ctx)

        if initiate_upload_response.get("resumable_upload"):
            _LOG.warning("GCP does not support parallel resumable uploads, falling back to single-threaded upload")
            return self._single_thread_multipart_upload(ctx, contents)
        elif initiate_upload_response.get("multipart_upload"):
            session_token = initiate_upload_response["multipart_upload"].get("session_token")
            cloud_provider_session = self._create_cloud_provider_session()
            if not session_token:
                raise ValueError(f"Unexpected server response: {initiate_upload_response}")
            try:
                self._parallel_multipart_upload_from_stream(ctx, session_token, contents, cloud_provider_session)
            except FallbackToUploadUsingFilesApi as e:
                try:
                    self._abort_multipart_upload(ctx, session_token, cloud_provider_session)
                except Exception as abort_ex:
                    _LOG.warning(f"Failed to abort upload: {abort_ex}")
                _LOG.info(f"Falling back to single-shot upload with Files API: {e}")
                # Concatenate the buffered part and the rest of the stream.
                full_stream = _ConcatenatedInputStream(BytesIO(e.buffer), contents)
                return self._single_thread_single_shot_upload(ctx, full_stream)
            except Exception as e:
                _LOG.info(f"Aborting multipart upload on error: {e}")
                try:
                    self._abort_multipart_upload(ctx, session_token, cloud_provider_session)
                except Exception as abort_ex:
                    _LOG.warning(f"Failed to abort upload: {abort_ex}")
                finally:
                    # Rethrow the original exception.
                    raise e from None
        else:
            raise ValueError(f"Unexpected server response: {initiate_upload_response}")

    def _parallel_upload_from_file(
        self,
        ctx: _UploadContext,
    ) -> None:
        """
        Upload a file using multipart upload with multiple threads.
        This method is not implemented in this example, but it would typically
        involve creating multiple threads to upload different parts of the file concurrently.
        """

        initiate_upload_response = self._initiate_multipart_upload(ctx)

        if initiate_upload_response.get("multipart_upload"):
            cloud_provider_session = self._create_cloud_provider_session()
            session_token = initiate_upload_response["multipart_upload"].get("session_token")
            if not session_token:
                raise ValueError(f"Unexpected server response: {initiate_upload_response}")
            try:
                self._parallel_multipart_upload_from_file(ctx, session_token)
            except FallbackToUploadUsingFilesApi as e:
                try:
                    self._abort_multipart_upload(ctx, session_token, cloud_provider_session)
                except Exception as abort_ex:
                    _LOG.warning(f"Failed to abort upload: {abort_ex}")

                _LOG.info(f"Falling back to single-shot upload with Files API: {e}")
                # Concatenate the buffered part and the rest of the stream.
                with open(ctx.source_file_path, "rb") as f:
                    return self._single_thread_single_shot_upload(ctx, f)

            except Exception as e:
                _LOG.info(f"Aborting multipart upload on error: {e}")
                try:
                    self._abort_multipart_upload(ctx, session_token, cloud_provider_session)
                except Exception as abort_ex:
                    _LOG.warning(f"Failed to abort upload: {abort_ex}")
                finally:
                    # Rethrow the original exception.
                    raise e from None

        elif initiate_upload_response.get("resumable_upload"):
            _LOG.warning("GCP does not support parallel resumable uploads, falling back to single-threaded upload")
            with open(ctx.source_file_path, "rb") as f:
                return self._upload_single_thread_with_known_size(ctx, f)
        else:
            raise ValueError(f"Unexpected server response: {initiate_upload_response}")

    @dataclass
    class _MultipartUploadPart:
        ctx: FilesExt._UploadContext
        part_index: int
        part_offset: int
        part_size: int
        session_token: str

    def _parallel_multipart_upload_from_file(
        self,
        ctx: _UploadContext,
        session_token: str,
    ) -> None:
        # Calculate the number of parts.
        file_size = os.path.getsize(ctx.source_file_path)
        part_size = ctx.part_size
        num_parts = (file_size + part_size - 1) // part_size
        _LOG.debug(f"Uploading file of size {file_size} bytes in {num_parts} parts using {ctx.parallelism} threads")
        cloud_provider_session = self._create_cloud_provider_session()

        # Upload one part to verify the upload can proceed.
        with open(ctx.source_file_path, "rb") as f:
            f.seek(0)
            first_part_size = min(part_size, file_size)
            first_part_buffer = f.read(first_part_size)
            try:
                etag = self._do_upload_one_part(
                    ctx,
                    cloud_provider_session,
                    1,
                    0,
                    first_part_size,
                    session_token,
                    BytesIO(first_part_buffer),
                    is_first_part=True,
                )
            except FallbackToUploadUsingFilesApi as e:
                raise FallbackToUploadUsingFilesApi(None, "Falling back to single-shot upload with Files API") from e
        if num_parts == 1:
            self._complete_multipart_upload(ctx, {1: etag}, session_token)
            return

        # Create queues and worker threads.
        task_queue = Queue()
        etags_result_queue = Queue()
        etags_result_queue.put_nowait((1, etag))
        exception_queue = Queue()
        aborted = Event()
        workers = [
            Thread(
                target=self._upload_file_consumer,
                args=(cloud_provider_session, task_queue, etags_result_queue, exception_queue, aborted),
            )
            for _ in range(ctx.parallelism)
        ]
        _LOG.debug(f"Starting {len(workers)} worker threads for parallel upload")

        # Enqueue all parts. Since the task queue is populated before starting the workers, we don't need to signal completion.
        for part_index in range(2, num_parts + 1):
            part_offset = (part_index - 1) * part_size
            part_size = min(part_size, file_size - part_offset)
            part = self._MultipartUploadPart(ctx, part_index, part_offset, part_size, session_token)
            task_queue.put(part)

        # Start the worker threads for parallel upload.
        for worker in workers:
            worker.start()

        # Wait for all tasks to be processed.
        for worker in workers:
            worker.join()

        # Check for exceptions: if any worker encountered an exception, raise the first one.
        if not exception_queue.empty():
            first_exception = exception_queue.get()
            raise first_exception

        # Collect results from the etags queue.
        etags: dict = {}
        while not etags_result_queue.empty():
            part_number, etag = etags_result_queue.get()
            etags[part_number] = etag

        self._complete_multipart_upload(ctx, etags, session_token)

    def _parallel_multipart_upload_from_stream(
        self,
        ctx: _UploadContext,
        session_token: str,
        content: BinaryIO,
        cloud_provider_session: requests.Session,
    ) -> None:

        task_queue = Queue(maxsize=ctx.parallelism)  # Limit queue size to control memory usage
        etags_result_queue = Queue()
        exception_queue = Queue()
        all_produced = Event()
        aborted = Event()

        # Do the first part read ahead
        pre_read_buffer = content.read(ctx.part_size)
        if not pre_read_buffer:
            raise FallbackToUploadUsingFilesApi(
                b"", "Falling back to single-shot upload with Files API due to empty input stream"
            )
        try:
            etag = self._do_upload_one_part(
                ctx,
                cloud_provider_session,
                1,
                0,
                len(pre_read_buffer),
                session_token,
                BytesIO(pre_read_buffer),
                is_first_part=True,
            )
            etags_result_queue.put((1, etag))
        except FallbackToUploadUsingFilesApi as e:
            raise FallbackToUploadUsingFilesApi(
                pre_read_buffer, "Falling back to single-shot upload with Files API"
            ) from e

        if len(pre_read_buffer) < ctx.part_size:
            self._complete_multipart_upload(ctx, {1: etag}, session_token)
            return

        def producer() -> None:
            part_index = 2
            part_size = ctx.part_size
            while not aborted.is_set():
                part_content = content.read(part_size)
                if not part_content:
                    break
                part_offset = (part_index - 1) * part_size
                part = self._MultipartUploadPart(ctx, part_index, part_offset, len(part_content), session_token)
                while not aborted.is_set():
                    try:
                        task_queue.put((part, part_content), timeout=0.1)
                        break
                    except Full:
                        continue
                part_index += 1
            all_produced.set()

        producer_thread = Thread(target=producer)
        consumers = [
            Thread(
                target=self._upload_stream_consumer,
                args=(task_queue, etags_result_queue, exception_queue, all_produced, aborted),
            )
            for _ in range(ctx.parallelism)
        ]
        _LOG.debug(f"Starting {len(consumers)} worker threads for parallel upload")
        # Start producer and consumer threads
        producer_thread.start()
        for consumer in consumers:
            consumer.start()

        # Wait for producer to finish
        _LOG.debug(f"threads started, waiting for producer to finish")
        producer_thread.join()
        # Wait for all tasks to be processed
        _LOG.debug(f"producer finished, waiting for consumers to finish")
        # task_queue.join()
        for consumer in consumers:
            consumer.join()

        # Check for exceptions: if any worker encountered an exception, raise the first one.
        if not exception_queue.empty():
            first_exception = exception_queue.get()
            raise first_exception

        # Collect results from the etags queue
        etags: dict = {}
        while not etags_result_queue.empty():
            part_number, etag = etags_result_queue.get()
            etags[part_number] = etag

        self._complete_multipart_upload(ctx, etags, session_token)

    def _complete_multipart_upload(self, ctx, etags, session_token):
        query = {"action": "complete-upload", "upload_type": "multipart", "session_token": session_token}
        headers = {"Content-Type": "application/json"}
        body: dict = {}
        parts = []
        for part_number, etag in sorted(etags.items()):
            part = {"part_number": part_number, "etag": etag}
            parts.append(part)
        body["parts"] = parts
        self._api.do(
            "POST",
            f"/api/2.0/fs/files{_escape_multi_segment_path_parameter(ctx.target_path)}",
            query=query,
            headers=headers,
            body=body,
        )

    def _upload_file_consumer(
        self,
        cloud_provider_session: requests.Session,
        task_queue: Queue[FilesExt._MultipartUploadPart],
        etags_queue: Queue[tuple[int, str]],
        exception_queue: Queue[Exception],
        aborted: Event,
    ) -> None:
        while not aborted.is_set():
            try:
                part = task_queue.get(block=False)
            except Empty:
                # The task_queue was populated before the workers were started, so we can exit if it's empty.
                break

            try:
                with open(part.ctx.source_file_path, "rb") as f:
                    f.seek(part.part_offset, os.SEEK_SET)
                    part_content = BytesIO(f.read(part.part_size))
                    etag = self._do_upload_one_part(
                        part.ctx,
                        cloud_provider_session,
                        part.part_index,
                        part.part_offset,
                        part.part_size,
                        part.session_token,
                        part_content,
                    )
                etags_queue.put((part.part_index, etag))
            except Exception as e:
                aborted.set()
                exception_queue.put(e)
            finally:
                task_queue.task_done()

    def _upload_stream_consumer(
        self,
        task_queue: Queue[tuple[FilesExt._MultipartUploadPart, bytes]],
        etags_queue: Queue[tuple[int, str]],
        exception_queue: Queue[Exception],
        all_produced: Event,
        aborted: Event,
    ) -> None:
        cloud_provider_session = self._create_cloud_provider_session()
        while not aborted.is_set():
            try:
                part, content = task_queue.get(block=False, timeout=0.1)
            except Empty:
                if all_produced.is_set():
                    break  # No more parts will be produced and the queue is empty
                else:
                    continue
            try:
                etag = self._do_upload_one_part(
                    part.ctx,
                    cloud_provider_session,
                    part.part_index,
                    part.part_offset,
                    part.part_size,
                    part.session_token,
                    BytesIO(content),
                )
                etags_queue.put((part.part_index, etag))
            except Exception as e:
                aborted.set()
                exception_queue.put(e)
            finally:
                task_queue.task_done()

    def _do_upload_one_part(
        self,
        ctx: _UploadContext,
        cloud_provider_session: requests.Session,
        part_index: int,
        part_offset: int,
        part_size: int,
        session_token: str,
        part_content: BinaryIO,
        is_first_part: bool = False,
    ) -> str:
        retry_count = 0

        # Try to upload the part, retrying if the upload URL expires.
        while True:
            body: dict = {
                "path": ctx.target_path,
                "session_token": session_token,
                "start_part_number": part_index,
                "count": 1,
                "expire_time": self._get_upload_url_expire_time(),
            }

            headers = {"Content-Type": "application/json"}

            # Requesting URLs for the same set of parts is an idempotent operation and is safe to retry.
            try:
                # The _api.do() method handles retries and will raise an exception in case of failure.
                upload_part_urls_response = self._api.do(
                    "POST", "/api/2.0/fs/create-upload-part-urls", headers=headers, body=body
                )
            except Exception as e:
                if is_first_part:
                    raise FallbackToUploadUsingFilesApi(
                        None,
                        f"Failed to obtain upload URL for part {part_index}: {e}, falling back to single shot upload",
                    )
                else:
                    raise e

            upload_part_urls = upload_part_urls_response.get("upload_part_urls", [])
            if len(upload_part_urls) == 0:
                raise ValueError(f"Unexpected server response: {upload_part_urls_response}")
            upload_part_url = upload_part_urls[0]
            url = upload_part_url["url"]
            required_headers = upload_part_url.get("headers", [])
            assert part_index == upload_part_url["part_number"]

            headers: dict = {"Content-Type": "application/octet-stream"}
            for h in required_headers:
                headers[h["name"]] = h["value"]

            _LOG.debug(f"Uploading part {part_index}: [{part_offset}, {part_offset + part_size - 1}]")

            def rewind() -> None:
                part_content.seek(0, os.SEEK_SET)

            def perform_upload() -> requests.Response:
                return cloud_provider_session.request(
                    "PUT",
                    url,
                    headers=headers,
                    data=part_content,
                    timeout=self._config.files_ext_network_transfer_inactivity_timeout_seconds,
                )

            upload_response = self._retry_cloud_idempotent_operation(perform_upload, rewind)

            if upload_response.status_code in (200, 201):
                etag = upload_response.headers.get("ETag", "")
                return etag
            elif FilesExt._is_url_expired_response(upload_response):
                if retry_count < self._config.files_ext_multipart_upload_max_retries:
                    retry_count += 1
                    _LOG.debug("Upload URL expired, retrying...")
                    continue
                else:
                    raise ValueError(f"Unsuccessful chunk upload: upload URL expired after {retry_count} retries")
            elif upload_response.status_code == 403 and is_first_part:
                raise FallbackToUploadUsingFilesApi(None, f"Direct upload forbidden: {upload_response.content}")
            elif is_first_part:
                message = f"Unsuccessful chunk upload. Response status: {upload_response.status_code}, body: {upload_response.content}"
                raise FallbackToUploadUsingFilesApi(None, message)
            else:
                message = f"Unsuccessful chunk upload. Response status: {upload_response.status_code}, body: {upload_response.content}"
                _LOG.warning(message)
                mapped_error = _error_mapper(upload_response, {})
                raise mapped_error or ValueError(message)

    def _perform_multipart_upload(
        self,
        ctx: _UploadContext,
        input_stream: BinaryIO,
        session_token: str,
        pre_read_buffer: bytes,
        cloud_provider_session: requests.Session,
    ) -> None:
        """
        Performs multipart upload using presigned URLs on AWS and Azure:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
        """
        current_part_number = 1
        etags: dict = {}

        # Why are we buffering the current chunk?
        # AWS and Azure don't support traditional "Transfer-encoding: chunked", so we must
        # provide each chunk size up front. In case of a non-seekable input stream we need
        # to buffer a chunk before uploading to know its size. This also allows us to rewind
        # the stream before retrying on request failure.
        # AWS signed chunked upload: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
        # https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-tune-upload-download-python#buffering-during-uploads

        chunk_offset = 0

        # This buffer is expected to contain at least multipart_upload_chunk_size bytes.
        # Note that initially buffer can be bigger (from pre_read_buffer).
        buffer = pre_read_buffer

        retry_count = 0
        eof = False
        while not eof:
            # If needed, buffer the next chunk.
            buffer = FilesExt._fill_buffer(buffer, ctx.part_size, input_stream)
            if len(buffer) == 0:
                # End of stream, no need to request the next block of upload URLs.
                break

            _LOG.debug(
                f"Multipart upload: requesting next {ctx.batch_size} upload URLs starting from part {current_part_number}"
            )

            body: dict = {
                "path": ctx.target_path,
                "session_token": session_token,
                "start_part_number": current_part_number,
                "count": ctx.batch_size,
                "expire_time": self._get_upload_url_expire_time(),
            }

            headers = {"Content-Type": "application/json"}

            # Requesting URLs for the same set of parts is an idempotent operation, safe to retry.
            try:
                # Method _api.do() takes care of retrying and will raise an exception in case of failure.
                upload_part_urls_response = self._api.do(
                    "POST", "/api/2.0/fs/create-upload-part-urls", headers=headers, body=body
                )
            except Exception as e:
                if chunk_offset == 0:
                    raise FallbackToUploadUsingFilesApi(
                        buffer, f"Failed to obtain upload URLs: {e}, falling back to single shot upload"
                    ) from e
                else:
                    raise e

            upload_part_urls = upload_part_urls_response.get("upload_part_urls", [])
            if len(upload_part_urls) == 0:
                raise ValueError(f"Unexpected server response: {upload_part_urls_response}")

            for upload_part_url in upload_part_urls:
                buffer = FilesExt._fill_buffer(buffer, ctx.part_size, input_stream)
                actual_buffer_length = len(buffer)
                if actual_buffer_length == 0:
                    eof = True
                    break

                url = upload_part_url["url"]
                required_headers = upload_part_url.get("headers", [])
                assert current_part_number == upload_part_url["part_number"]

                headers: dict = {"Content-Type": "application/octet-stream"}
                for h in required_headers:
                    headers[h["name"]] = h["value"]

                actual_chunk_length = min(actual_buffer_length, ctx.part_size)
                _LOG.debug(
                    f"Uploading part {current_part_number}: [{chunk_offset}, {chunk_offset + actual_chunk_length - 1}]"
                )

                chunk = BytesIO(buffer[:actual_chunk_length])

                def rewind():
                    chunk.seek(0, os.SEEK_SET)

                def perform():
                    return cloud_provider_session.request(
                        "PUT",
                        url,
                        headers=headers,
                        data=chunk,
                        timeout=self._config.files_ext_network_transfer_inactivity_timeout_seconds,
                    )

                upload_response = self._retry_cloud_idempotent_operation(perform, rewind)

                if upload_response.status_code in (200, 201):
                    # Chunk upload successful

                    chunk_offset += actual_chunk_length

                    etag = upload_response.headers.get("ETag", "")
                    etags[current_part_number] = etag

                    # Discard uploaded bytes
                    buffer = buffer[actual_chunk_length:]

                    # Reset retry count when progressing along the stream
                    retry_count = 0

                elif FilesExt._is_url_expired_response(upload_response):
                    if retry_count < self._config.files_ext_multipart_upload_max_retries:
                        retry_count += 1
                        _LOG.debug("Upload URL expired")
                        # Preserve the buffer so we'll upload the current part again using next upload URL
                    else:
                        # don't confuse user with unrelated "Permission denied" error.
                        raise ValueError(f"Unsuccessful chunk upload: upload URL expired")

                elif upload_response.status_code == 403 and chunk_offset == 0:
                    # We got 403 failure when uploading the very first chunk (we can't tell if it is Azure for sure yet).
                    # This might happen due to Azure firewall enabled for the customer bucket.
                    # Let's fallback to using Files API which might be allowlisted to upload, passing
                    # currently buffered (but not yet uploaded) part of the stream.
                    raise FallbackToUploadUsingFilesApi(buffer, f"Direct upload forbidden: {upload_response.content}")
                elif chunk_offset == 0:
                    # We got an upload failure when uploading the very first chunk.
                    # Let's fallback to using Files API which might be more reliable in this case,
                    # passing currently buffered (but not yet uploaded) part of the stream.
                    raise FallbackToUploadUsingFilesApi(
                        buffer,
                        f"Unsuccessful chunk upload: {upload_response.status_code}, falling back to single shot upload",
                    )
                else:
                    message = f"Unsuccessful chunk upload. Response status: {upload_response.status_code}, body: {upload_response.content}"
                    _LOG.warning(message)
                    mapped_error = _error_mapper(upload_response, {})
                    raise mapped_error or ValueError(message)

                current_part_number += 1

        _LOG.debug(f"Completing multipart upload after uploading {len(etags)} parts of up to {ctx.part_size} bytes")
        self._complete_multipart_upload(ctx, etags, session_token)

    @staticmethod
    def _fill_buffer(buffer: bytes, desired_min_size: int, input_stream: BinaryIO) -> bytes:
        """
        Tries to fill given buffer to contain at least `desired_min_size` bytes by reading from input stream.
        """
        bytes_to_read = max(0, desired_min_size - len(buffer))
        if bytes_to_read > 0:
            next_buf = input_stream.read(bytes_to_read)
            new_buffer = buffer + next_buf
            return new_buffer
        else:
            # we have already buffered enough data
            return buffer

    @staticmethod
    def _is_url_expired_response(response: requests.Response) -> bool:
        """
        Checks if response matches one of the known "URL expired" responses from the cloud storage providers.
        """
        if response.status_code != 403:
            return False

        try:
            xml_root = ET.fromstring(response.content)
            if xml_root.tag != "Error":
                return False

            code = xml_root.find("Code")
            if code is None:
                return False

            if code.text == "AuthenticationFailed":
                # Azure
                details = xml_root.find("AuthenticationErrorDetail")
                if details is not None and "Signature not valid in the specified time frame" in details.text:
                    return True

            if code.text == "AccessDenied":
                # AWS
                message = xml_root.find("Message")
                if message is not None and message.text == "Request has expired":
                    return True

        except ET.ParseError:
            pass

        return False

    def _is_presigned_urls_disabled_error(self, e: PermissionDenied) -> bool:
        error_infos = e.get_error_info()
        for error_info in error_infos:
            if error_info.reason == "FILES_API_API_IS_NOT_ENABLED":
                return True
        return False

    def _is_presigned_urls_network_zone_error(self, e: InternalError) -> bool:
        error_infos = e.get_error_info()
        for error_info in error_infos:
            if error_info.reason == "FILES_API_REQUESTER_NETWORK_ZONE_UNKNOWN":
                return True
        return False

    def _perform_resumable_upload(
        self,
        ctx: _UploadContext,
        input_stream: BinaryIO,
        session_token: str,
        pre_read_buffer: bytes,
        cloud_provider_session: requests.Session,
    ) -> None:
        """
        Performs resumable upload on GCP: https://cloud.google.com/storage/docs/performing-resumable-uploads
        """

        # Session URI we're using expires after a week

        # Why are we buffering the current chunk?
        # When using resumable upload API we're uploading data in chunks. During chunk upload
        # server responds with the "received offset" confirming how much data it stored so far,
        # so we should continue uploading from that offset. (Note this is not a failure but an
        # expected behaviour as per the docs.) But, input stream might be consumed beyond that
        # offset, since server might have read more data than it confirmed received, or some data
        # might have been pre-cached by e.g. OS or a proxy. So, to continue upload, we must rewind
        # the input stream back to the byte next to "received offset". This is not possible
        # for non-seekable input stream, so we must buffer the whole last chunk and seek inside
        # the buffer. By always uploading from the buffer we fully support non-seekable streams.

        # Why are we doing read-ahead?
        # It's not possible to upload an empty chunk as "Content-Range" header format does not
        # support this. So if current chunk happens to finish exactly at the end of the stream,
        # we need to know that and mark the chunk as last (by passing real file size in the
        # "Content-Range" header) when uploading it. To detect if we're at the end of the stream
        # we're reading "ahead" an extra bytes but not uploading them immediately. If
        # nothing has been read ahead, it means we're at the end of the stream.
        # On the contrary, in multipart upload we can decide to complete upload *after*
        # last chunk has been sent.

        body: dict = {"path": ctx.target_path, "session_token": session_token}

        headers = {"Content-Type": "application/json"}

        try:
            # Method _api.do() takes care of retrying and will raise an exception in case of failure.
            resumable_upload_url_response = self._api.do(
                "POST", "/api/2.0/fs/create-resumable-upload-url", headers=headers, body=body
            )
        except Exception as e:
            raise FallbackToUploadUsingFilesApi(
                pre_read_buffer, f"Failed to obtain resumable upload URL: {e}, falling back to single shot upload"
            ) from e

        resumable_upload_url_node = resumable_upload_url_response.get("resumable_upload_url")
        if not resumable_upload_url_node:
            raise ValueError(f"Unexpected server response: {resumable_upload_url_response}")

        resumable_upload_url = resumable_upload_url_node.get("url")
        if not resumable_upload_url:
            raise ValueError(f"Unexpected server response: {resumable_upload_url_response}")

        required_headers = resumable_upload_url_node.get("headers", [])
        base_headers: dict = {}
        for h in required_headers:
            base_headers[h["name"]] = h["value"]

        try:
            # We will buffer this many bytes: one chunk + read-ahead block.
            # Note buffer may contain more data initially (from pre_read_buffer).
            min_buffer_size = ctx.part_size + self._multipart_upload_read_ahead_bytes

            buffer = pre_read_buffer

            # How many bytes in the buffer were confirmed to be received by the server.
            # All the remaining bytes in the buffer must be uploaded.
            uploaded_bytes_count = 0

            chunk_offset = 0

            retry_count = 0
            while True:
                # If needed, fill the buffer to contain at least min_buffer_size bytes
                # (unless end of stream), discarding already uploaded bytes.
                bytes_to_read = max(0, min_buffer_size - (len(buffer) - uploaded_bytes_count))
                next_buf = input_stream.read(bytes_to_read)
                buffer = buffer[uploaded_bytes_count:] + next_buf

                if len(next_buf) < bytes_to_read:
                    # This is the last chunk in the stream.
                    # Let's upload all the remaining bytes in one go.
                    actual_chunk_length = len(buffer)
                    file_size = chunk_offset + actual_chunk_length
                else:
                    # More chunks expected, let's upload current chunk (excluding read-ahead block).
                    actual_chunk_length = ctx.part_size
                    file_size = "*"

                headers: dict = {"Content-Type": "application/octet-stream", **base_headers}

                chunk_last_byte_offset = chunk_offset + actual_chunk_length - 1
                content_range_header = f"bytes {chunk_offset}-{chunk_last_byte_offset}/{file_size}"
                _LOG.debug(f"Uploading chunk: {content_range_header}")
                headers["Content-Range"] = content_range_header

                def retrieve_upload_status() -> Optional[requests.Response]:
                    def perform():
                        return cloud_provider_session.request(
                            "PUT",
                            resumable_upload_url,
                            headers={"Content-Range": "bytes */*"},
                            data=b"",
                            timeout=self._config.files_ext_network_transfer_inactivity_timeout_seconds,
                        )

                    try:
                        return self._retry_cloud_idempotent_operation(perform)
                    except RequestException:
                        _LOG.warning("Failed to retrieve upload status")
                        return None

                try:
                    upload_response = cloud_provider_session.request(
                        "PUT",
                        resumable_upload_url,
                        headers=headers,
                        data=BytesIO(buffer[:actual_chunk_length]),
                        timeout=self._config.files_ext_network_transfer_inactivity_timeout_seconds,
                    )

                    # https://cloud.google.com/storage/docs/performing-resumable-uploads#resume-upload
                    # If an upload request is terminated before receiving a response, or if you receive
                    # a 503 or 500 response, then you need to resume the interrupted upload from where it left off.

                    # Let's follow that for all potentially retryable status codes.
                    if upload_response.status_code in self._RETRYABLE_STATUS_CODES:
                        if retry_count < self._config.files_ext_multipart_upload_max_retries:
                            retry_count += 1
                            # let original upload_response be handled as an error
                            upload_response = retrieve_upload_status() or upload_response
                    else:
                        # we received non-retryable response, reset retry count
                        retry_count = 0

                except RequestException as e:
                    # Let's do the same for retryable network errors.
                    if (
                        _BaseClient._is_retryable(e)
                        and retry_count < self._config.files_ext_multipart_upload_max_retries
                    ):
                        retry_count += 1
                        upload_response = retrieve_upload_status()
                        if not upload_response:
                            # rethrow original exception
                            raise e from None
                    else:
                        # rethrow original exception
                        raise e from None

                if upload_response.status_code in (200, 201):
                    if file_size == "*":
                        raise ValueError(
                            f"Received unexpected status {upload_response.status_code} before reaching end of stream"
                        )

                    # upload complete
                    break

                elif upload_response.status_code == 308:
                    # chunk accepted (or check-status succeeded), let's determine received offset to resume from there
                    range_string = upload_response.headers.get("Range")
                    confirmed_offset = self._extract_range_offset(range_string)
                    _LOG.debug(f"Received confirmed offset: {confirmed_offset}")

                    if confirmed_offset:
                        if confirmed_offset < chunk_offset - 1 or confirmed_offset > chunk_last_byte_offset:
                            raise ValueError(
                                f"Unexpected received offset: {confirmed_offset} is outside of expected range, chunk offset: {chunk_offset}, chunk last byte offset: {chunk_last_byte_offset}"
                            )
                    else:
                        if chunk_offset > 0:
                            raise ValueError(
                                f"Unexpected received offset: {confirmed_offset} is outside of expected range, chunk offset: {chunk_offset}, chunk last byte offset: {chunk_last_byte_offset}"
                            )

                    # We have just uploaded a part of chunk starting from offset "chunk_offset" and ending
                    # at offset "confirmed_offset" (inclusive), so the next chunk will start at
                    # offset "confirmed_offset + 1"
                    if confirmed_offset:
                        next_chunk_offset = confirmed_offset + 1
                    else:
                        next_chunk_offset = chunk_offset
                    uploaded_bytes_count = next_chunk_offset - chunk_offset
                    chunk_offset = next_chunk_offset

                elif upload_response.status_code == 412 and not ctx.overwrite:
                    # Assuming this is only possible reason
                    # Full message in this case: "At least one of the pre-conditions you specified did not hold."
                    raise AlreadyExists("The file being created already exists.")

                else:
                    message = f"Unsuccessful chunk upload. Response status: {upload_response.status_code}, body: {upload_response.content}"
                    _LOG.warning(message)
                    mapped_error = _error_mapper(upload_response, {})
                    raise mapped_error or ValueError(message)

        except Exception as e:
            _LOG.info(f"Aborting resumable upload on error: {e}")
            try:
                self._abort_resumable_upload(resumable_upload_url, base_headers, cloud_provider_session)
            except BaseException as ex:
                _LOG.warning(f"Failed to abort upload: {ex}")
                # ignore, abort is a best-effort
            finally:
                # rethrow original exception
                raise e from None

    @staticmethod
    def _extract_range_offset(range_string: Optional[str]) -> Optional[int]:
        """Parses the response range header to extract the last byte."""
        if not range_string:
            return None  # server did not yet confirm any bytes

        if match := re.match("bytes=0-(\\d+)", range_string):
            return int(match.group(1))
        else:
            raise ValueError(f"Cannot parse response header: Range: {range_string}")

    def _get_rfc339_timestamp_with_future_offset(self, base_time: datetime.datetime, offset: timedelta) -> str:
        """Generates an offset timestamp in an RFC3339 format suitable for URL generation"""
        offset_timestamp = base_time + offset
        # From Google Protobuf doc:
        # In JSON format, the Timestamp type is encoded as a string in the
        #   * [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) format. That is, the
        #   * format is "{year}-{month}-{day}T{hour}:{min}:{sec}[.{frac_sec}]Z"
        return offset_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_upload_url_expire_time(self) -> str:
        """Generates expiration time in the required format."""
        current_time = datetime.datetime.now(datetime.timezone.utc)
        return self._get_rfc339_timestamp_with_future_offset(
            current_time, self._config.files_ext_multipart_upload_url_expiration_duration
        )

    def _get_download_url_expire_time(self) -> str:
        """Generates expiration time in the required format."""
        current_time = datetime.datetime.now(datetime.timezone.utc)
        return self._get_rfc339_timestamp_with_future_offset(
            current_time, self._config.files_ext_presigned_download_url_expiration_duration
        )

    def _abort_multipart_upload(
        self, ctx: _UploadContext, session_token: str, cloud_provider_session: requests.Session
    ) -> None:
        """Aborts ongoing multipart upload session to clean up incomplete file."""
        body: dict = {
            "path": ctx.target_path,
            "session_token": session_token,
            "expire_time": self._get_upload_url_expire_time(),
        }

        headers = {"Content-Type": "application/json"}

        # Method _api.do() takes care of retrying and will raise an exception in case of failure.
        abort_url_response = self._api.do("POST", "/api/2.0/fs/create-abort-upload-url", headers=headers, body=body)

        abort_upload_url_node = abort_url_response["abort_upload_url"]
        abort_url = abort_upload_url_node["url"]
        required_headers = abort_upload_url_node.get("headers", [])

        headers: dict = {"Content-Type": "application/octet-stream"}
        for h in required_headers:
            headers[h["name"]] = h["value"]

        def perform() -> requests.Response:
            return cloud_provider_session.request(
                "DELETE",
                abort_url,
                headers=headers,
                data=b"",
                timeout=self._config.files_ext_network_transfer_inactivity_timeout_seconds,
            )

        abort_response = self._retry_cloud_idempotent_operation(perform)

        if abort_response.status_code not in (200, 201):
            raise ValueError(abort_response)

    def _abort_resumable_upload(
        self, resumable_upload_url: str, headers: dict[str, str], cloud_provider_session: requests.Session
    ) -> None:
        """Aborts ongoing resumable upload session to clean up incomplete file."""

        def perform() -> requests.Response:
            return cloud_provider_session.request(
                "DELETE",
                resumable_upload_url,
                headers=headers,
                data=b"",
                timeout=self._config.files_ext_network_transfer_inactivity_timeout_seconds,
            )

        abort_response = self._retry_cloud_idempotent_operation(perform)

        if abort_response.status_code not in (200, 201):
            raise ValueError(abort_response)

    def _create_cloud_provider_session(self) -> requests.Session:
        """Creates a separate session which does not inherit auth headers from BaseClient session."""
        session = requests.Session()

        # following session config in _BaseClient
        http_adapter = requests.adapters.HTTPAdapter(
            self._config.max_connection_pools or 20, self._config.max_connections_per_pool or 20, pool_block=True
        )
        session.mount("https://", http_adapter)
        # presigned URL for storage proxy can use plain HTTP
        session.mount("http://", http_adapter)
        return session

    def _retry_cloud_idempotent_operation(
        self, operation: Callable[[], requests.Response], before_retry: Optional[Callable] = None
    ) -> requests.Response:
        """Perform given idempotent operation with necessary retries for requests to non Databricks APIs.
        For cloud APIs, we will retry on network errors and on server response codes.
        Since operation is idempotent it's safe to retry it for response codes where server state might have changed.
        """

        def delegate() -> requests.Response:
            response = operation()
            if response.status_code in self._RETRYABLE_STATUS_CODES:
                raise _RetryableException.make_error(response)
            else:
                return response

        def extended_is_retryable(e: BaseException) -> Optional[str]:
            retry_reason_from_base = _BaseClient._is_retryable(e)
            if retry_reason_from_base is not None:
                return retry_reason_from_base

            if isinstance(e, _RetryableException):
                # this is a retriable exception, but not a network error
                return f"retryable exception (status_code:{e.http_status_code})"
            return None

        # following _BaseClient timeout
        retry_timeout_seconds = self._config.retry_timeout_seconds or 300

        return retried(
            timeout=timedelta(seconds=retry_timeout_seconds),
            max_attempts=self._config.experimental_files_ext_cloud_api_max_retries,
            # also retry on network errors (connection error, connection timeout)
            # where we believe request didn't reach the server
            is_retryable=extended_is_retryable,
            before_retry=before_retry,
            clock=self._config.clock,
        )(delegate)()

    def _open_download_stream(
        self,
        file_path: str,
        start_byte_offset: int,
        if_unmodified_since_timestamp: Optional[str] = None,
    ) -> DownloadResponse:
        """Opens a download stream from given offset, performing necessary retries."""
        headers = {
            "Accept": "application/octet-stream",
        }

        if start_byte_offset and not if_unmodified_since_timestamp:
            raise Exception("if_unmodified_since_timestamp is required if start_byte_offset is specified")

        if start_byte_offset > 0:
            headers["Range"] = f"bytes={start_byte_offset}-"

        if if_unmodified_since_timestamp:
            headers["If-Unmodified-Since"] = if_unmodified_since_timestamp

        response_headers = [
            "content-length",
            "content-type",
            "last-modified",
        ]

        result = self._init_download_response_mode_csp_with_fallback(file_path, headers, response_headers)

        if not isinstance(result.contents, _StreamingResponse):
            raise Exception(
                "Internal error: response contents is of unexpected type: " + type(result.contents).__name__
            )

        return result

    def _init_download_response_files_api(
        self, file_path: str, headers: dict[str, str], response_headers: list[str]
    ) -> DownloadResponse:
        """
        Initiates a download response using the Files API.
        """

        # Method _api.do() takes care of retrying and will raise an exception in case of failure.
        res = self._api.do(
            "GET",
            f"/api/2.0/fs/files{_escape_multi_segment_path_parameter(file_path)}",
            headers=headers,
            response_headers=response_headers,
            raw=True,
        )
        return DownloadResponse.from_dict(res)

    def _create_download_url(self, file_path: str) -> CreateDownloadUrlResponse:
        """
        Creates a presigned download URL using the CSP presigned URL API.

        Wrapped in similar retry logic to the internal API.do call:
        1. Call _.api.do to obtain the presigned URL
        2. Return the presigned URL
        """

        # Method _api.do() takes care of retrying and will raise an exception in case of failure.
        try:
            raw_response = self._api.do(
                "POST",
                f"/api/2.0/fs/create-download-url",
                query={
                    "path": file_path,
                    "expire_time": self._get_download_url_expire_time(),
                },
            )

            return CreateDownloadUrlResponse.from_dict(raw_response)
        except Exception as e:
            raise FallbackToDownloadUsingFilesApi(f"Failed to create download URL: {e}") from e

    def _init_download_response_presigned_api(self, file_path: str, added_headers: dict[str, str]) -> DownloadResponse:
        """
        Initiates a download response using the CSP presigned URL API.

        Wrapped in similar retry logic to the internal API.do call:
        1. Call _.api.do to obtain the presigned URL
        2. Attempt to establish a streaming connection via the presigned URL
        3. Construct a StreamingResponse from the presigned URL
        """

        url_and_headers = self._create_download_url(file_path)
        cloud_provider_session = self._create_cloud_provider_session()

        header_overlap = added_headers.keys() & url_and_headers.headers.keys()
        if header_overlap:
            raise ValueError(
                f"Provided headers overlap with required headers from the CSP API bundle: {header_overlap}"
            )

        merged_headers = {**added_headers, **url_and_headers.headers}

        def perform() -> requests.Response:
            return cloud_provider_session.request(
                "GET",
                url_and_headers.url,
                headers=merged_headers,
                timeout=self._config.files_ext_network_transfer_inactivity_timeout_seconds,
                stream=True,
            )

        csp_response: _RawResponse = self._retry_cloud_idempotent_operation(perform)

        # Mapping the error if the response is not successful.
        if csp_response.status_code in (200, 201, 206):
            resp = DownloadResponse(
                content_length=int(csp_response.headers.get("content-length")),
                content_type=csp_response.headers.get("content-type"),
                last_modified=csp_response.headers.get("last-modified"),
                contents=_StreamingResponse(csp_response, self._config.files_ext_client_download_streaming_chunk_size),
            )
            return resp
        else:
            message = (
                f"Unsuccessful download. Response status: {csp_response.status_code}, body: {csp_response.content}"
            )
            raise FallbackToDownloadUsingFilesApi(message)

    def _init_download_response_mode_csp_with_fallback(
        self, file_path: str, headers: dict[str, str], response_headers: list[str]
    ) -> DownloadResponse:
        """
        Initiates a download response using the CSP presigned URL API or the Files API, depending on the configuration.
        If the CSP presigned download API is enabled, it will attempt to use that first.
        If the CSP API call fails, it will fall back to the Files API.
        If the CSP presigned download API is disabled, it will use the Files API directly.
        """

        try:
            _LOG.debug(f"Attempting download of {file_path} via CSP APIs")
            return self._init_download_response_presigned_api(file_path, headers)
        except FallbackToDownloadUsingFilesApi as e:
            _LOG.info(f"Falling back to download via Files API: {e}")
            _LOG.debug(f"Attempt via CSP APIs for {file_path} failed. Falling back to download via Files API")
            ret = self._init_download_response_files_api(file_path, headers, response_headers)
            return ret

    def _wrap_stream(
        self,
        file_path: str,
        download_response: DownloadResponse,
        start_byte_offset: int = 0,
    ) -> "_ResilientResponse":
        underlying_response = _ResilientIterator._extract_raw_response(download_response)
        return _ResilientResponse(
            self,
            file_path,
            download_response.last_modified,
            offset=start_byte_offset,
            underlying_response=underlying_response,
        )


class _ResilientResponse(_RawResponse):

    def __init__(
        self,
        api: FilesExt,
        file_path: str,
        file_last_modified: str,
        offset: int,
        underlying_response: _RawResponse,
    ) -> None:
        self.api = api
        self.file_path = file_path
        self.underlying_response = underlying_response
        self.offset = offset
        self.file_last_modified = file_last_modified

    def iter_content(self, chunk_size: int = 1, decode_unicode: bool = False) -> Iterator[bytes]:
        if decode_unicode:
            raise ValueError("Decode unicode is not supported")

        iterator = self.underlying_response.iter_content(chunk_size=chunk_size, decode_unicode=False)
        self.iterator = _ResilientIterator(
            iterator, self.file_path, self.file_last_modified, self.offset, self.api, chunk_size
        )
        return self.iterator

    def close(self) -> None:
        self.iterator.close()


class _ResilientIterator(Iterator):
    # This class tracks current offset (returned to the client code)
    # and recovers from failures by requesting download from the current offset.

    @staticmethod
    def _extract_raw_response(
        download_response: DownloadResponse,
    ) -> _RawResponse:
        streaming_response: _StreamingResponse = download_response.contents
        return streaming_response._response

    def __init__(
        self,
        underlying_iterator: Iterator[bytes],
        file_path: str,
        file_last_modified: str,
        offset: int,
        api: FilesExt,
        chunk_size: int,
    ) -> None:
        self._underlying_iterator = underlying_iterator
        self._api = api
        self._file_path = file_path

        # Absolute current offset (0-based), i.e. number of bytes from the beginning of the file
        # that were so far returned to the caller code.
        self._offset = offset
        self._file_last_modified = file_last_modified
        self._chunk_size = chunk_size

        self._total_recovers_count: int = 0
        self._recovers_without_progressing_count: int = 0
        self._closed: bool = False

    def _should_recover(self) -> bool:
        if self._total_recovers_count == self._api._config.files_ext_client_download_max_total_recovers:
            _LOG.debug("Total recovers limit exceeded")
            return False
        if (
            self._api._config.files_ext_client_download_max_total_recovers_without_progressing is not None
            and self._recovers_without_progressing_count
            >= self._api._config.files_ext_client_download_max_total_recovers_without_progressing
        ):
            _LOG.debug("No progression recovers limit exceeded")
            return False
        return True

    def _recover(self) -> bool:
        if not self._should_recover():
            return False  # recover suppressed, rethrow original exception

        self._total_recovers_count += 1
        self._recovers_without_progressing_count += 1

        try:
            self._underlying_iterator.close()

            _LOG.debug(f"Trying to recover from offset {self._offset}")

            # following call includes all the required network retries
            downloadResponse = self._api._open_download_stream(self._file_path, self._offset, self._file_last_modified)
            underlying_response = _ResilientIterator._extract_raw_response(downloadResponse)
            self._underlying_iterator = underlying_response.iter_content(
                chunk_size=self._chunk_size, decode_unicode=False
            )
            _LOG.debug("Recover succeeded")
            return True
        except:
            return False  # recover failed, rethrow original exception

    def __next__(self) -> bytes:
        if self._closed:
            # following _BaseClient
            raise ValueError("I/O operation on closed file")

        while True:
            try:
                returned_bytes = next(self._underlying_iterator)
                self._offset += len(returned_bytes)
                self._recovers_without_progressing_count = 0
                return returned_bytes

            except StopIteration:
                raise

            # https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions
            except RequestException:
                if not self._recover():
                    raise

    def close(self) -> None:
        self._underlying_iterator.close()
        self._closed = True
