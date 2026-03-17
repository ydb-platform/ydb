# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, BinaryIO, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service._internal import (
    _escape_multi_segment_path_parameter, _repeated_dict)

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AddBlockResponse:
    def as_dict(self) -> dict:
        """Serializes the AddBlockResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AddBlockResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AddBlockResponse:
        """Deserializes the AddBlockResponse from a dictionary."""
        return cls()


@dataclass
class CloseResponse:
    def as_dict(self) -> dict:
        """Serializes the CloseResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CloseResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CloseResponse:
        """Deserializes the CloseResponse from a dictionary."""
        return cls()


@dataclass
class CreateResponse:
    handle: Optional[int] = None
    """Handle which should subsequently be passed into the AddBlock and Close calls when writing to a
    file through a stream."""

    def as_dict(self) -> dict:
        """Serializes the CreateResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.handle is not None:
            body["handle"] = self.handle
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.handle is not None:
            body["handle"] = self.handle
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateResponse:
        """Deserializes the CreateResponse from a dictionary."""
        return cls(handle=d.get("handle", None))


@dataclass
class DeleteResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteResponse:
        """Deserializes the DeleteResponse from a dictionary."""
        return cls()


@dataclass
class DirectoryEntry:
    file_size: Optional[int] = None
    """The length of the file in bytes. This field is omitted for directories."""

    is_directory: Optional[bool] = None
    """True if the path is a directory."""

    last_modified: Optional[int] = None
    """Last modification time of given file in milliseconds since unix epoch."""

    name: Optional[str] = None
    """The name of the file or directory. This is the last component of the path."""

    path: Optional[str] = None
    """The absolute path of the file or directory."""

    def as_dict(self) -> dict:
        """Serializes the DirectoryEntry into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_size is not None:
            body["file_size"] = self.file_size
        if self.is_directory is not None:
            body["is_directory"] = self.is_directory
        if self.last_modified is not None:
            body["last_modified"] = self.last_modified
        if self.name is not None:
            body["name"] = self.name
        if self.path is not None:
            body["path"] = self.path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DirectoryEntry into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_size is not None:
            body["file_size"] = self.file_size
        if self.is_directory is not None:
            body["is_directory"] = self.is_directory
        if self.last_modified is not None:
            body["last_modified"] = self.last_modified
        if self.name is not None:
            body["name"] = self.name
        if self.path is not None:
            body["path"] = self.path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DirectoryEntry:
        """Deserializes the DirectoryEntry from a dictionary."""
        return cls(
            file_size=d.get("file_size", None),
            is_directory=d.get("is_directory", None),
            last_modified=d.get("last_modified", None),
            name=d.get("name", None),
            path=d.get("path", None),
        )


@dataclass
class DownloadResponse:
    content_length: Optional[int] = None
    """The length of the HTTP response body in bytes."""

    content_type: Optional[str] = None

    contents: Optional[BinaryIO] = None

    last_modified: Optional[str] = None
    """The last modified time of the file in HTTP-date (RFC 7231) format."""

    def as_dict(self) -> dict:
        """Serializes the DownloadResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.content_length is not None:
            body["content-length"] = self.content_length
        if self.content_type is not None:
            body["content-type"] = self.content_type
        if self.contents:
            body["contents"] = self.contents
        if self.last_modified is not None:
            body["last-modified"] = self.last_modified
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DownloadResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.content_length is not None:
            body["content-length"] = self.content_length
        if self.content_type is not None:
            body["content-type"] = self.content_type
        if self.contents:
            body["contents"] = self.contents
        if self.last_modified is not None:
            body["last-modified"] = self.last_modified
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DownloadResponse:
        """Deserializes the DownloadResponse from a dictionary."""
        return cls(
            content_length=int(d.get("content-length", None)),
            content_type=d.get("content-type", None),
            contents=d.get("contents", None),
            last_modified=d.get("last-modified", None),
        )


@dataclass
class FileInfo:
    file_size: Optional[int] = None
    """The length of the file in bytes. This field is omitted for directories."""

    is_dir: Optional[bool] = None
    """True if the path is a directory."""

    modification_time: Optional[int] = None
    """Last modification time of given file in milliseconds since epoch."""

    path: Optional[str] = None
    """The absolute path of the file or directory."""

    def as_dict(self) -> dict:
        """Serializes the FileInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_size is not None:
            body["file_size"] = self.file_size
        if self.is_dir is not None:
            body["is_dir"] = self.is_dir
        if self.modification_time is not None:
            body["modification_time"] = self.modification_time
        if self.path is not None:
            body["path"] = self.path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_size is not None:
            body["file_size"] = self.file_size
        if self.is_dir is not None:
            body["is_dir"] = self.is_dir
        if self.modification_time is not None:
            body["modification_time"] = self.modification_time
        if self.path is not None:
            body["path"] = self.path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileInfo:
        """Deserializes the FileInfo from a dictionary."""
        return cls(
            file_size=d.get("file_size", None),
            is_dir=d.get("is_dir", None),
            modification_time=d.get("modification_time", None),
            path=d.get("path", None),
        )


@dataclass
class GetMetadataResponse:
    content_length: Optional[int] = None
    """The length of the HTTP response body in bytes."""

    content_type: Optional[str] = None

    last_modified: Optional[str] = None
    """The last modified time of the file in HTTP-date (RFC 7231) format."""

    def as_dict(self) -> dict:
        """Serializes the GetMetadataResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.content_length is not None:
            body["content-length"] = self.content_length
        if self.content_type is not None:
            body["content-type"] = self.content_type
        if self.last_modified is not None:
            body["last-modified"] = self.last_modified
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetMetadataResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.content_length is not None:
            body["content-length"] = self.content_length
        if self.content_type is not None:
            body["content-type"] = self.content_type
        if self.last_modified is not None:
            body["last-modified"] = self.last_modified
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetMetadataResponse:
        """Deserializes the GetMetadataResponse from a dictionary."""
        return cls(
            content_length=int(d.get("content-length", None)),
            content_type=d.get("content-type", None),
            last_modified=d.get("last-modified", None),
        )


@dataclass
class ListDirectoryResponse:
    contents: Optional[List[DirectoryEntry]] = None
    """Array of DirectoryEntry."""

    next_page_token: Optional[str] = None
    """A token, which can be sent as `page_token` to retrieve the next page."""

    def as_dict(self) -> dict:
        """Serializes the ListDirectoryResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.contents:
            body["contents"] = [v.as_dict() for v in self.contents]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListDirectoryResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.contents:
            body["contents"] = self.contents
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListDirectoryResponse:
        """Deserializes the ListDirectoryResponse from a dictionary."""
        return cls(
            contents=_repeated_dict(d, "contents", DirectoryEntry), next_page_token=d.get("next_page_token", None)
        )


@dataclass
class ListStatusResponse:
    files: Optional[List[FileInfo]] = None
    """A list of FileInfo's that describe contents of directory or file. See example above."""

    def as_dict(self) -> dict:
        """Serializes the ListStatusResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.files:
            body["files"] = [v.as_dict() for v in self.files]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListStatusResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.files:
            body["files"] = self.files
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListStatusResponse:
        """Deserializes the ListStatusResponse from a dictionary."""
        return cls(files=_repeated_dict(d, "files", FileInfo))


@dataclass
class MkDirsResponse:
    def as_dict(self) -> dict:
        """Serializes the MkDirsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MkDirsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MkDirsResponse:
        """Deserializes the MkDirsResponse from a dictionary."""
        return cls()


@dataclass
class MoveResponse:
    def as_dict(self) -> dict:
        """Serializes the MoveResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the MoveResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MoveResponse:
        """Deserializes the MoveResponse from a dictionary."""
        return cls()


@dataclass
class PutResponse:
    def as_dict(self) -> dict:
        """Serializes the PutResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PutResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PutResponse:
        """Deserializes the PutResponse from a dictionary."""
        return cls()


@dataclass
class ReadResponse:
    bytes_read: Optional[int] = None
    """The number of bytes read (could be less than ``length`` if we hit end of file). This refers to
    number of bytes read in unencoded version (response data is base64-encoded)."""

    data: Optional[str] = None
    """The base64-encoded contents of the file read."""

    def as_dict(self) -> dict:
        """Serializes the ReadResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bytes_read is not None:
            body["bytes_read"] = self.bytes_read
        if self.data is not None:
            body["data"] = self.data
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ReadResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bytes_read is not None:
            body["bytes_read"] = self.bytes_read
        if self.data is not None:
            body["data"] = self.data
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ReadResponse:
        """Deserializes the ReadResponse from a dictionary."""
        return cls(bytes_read=d.get("bytes_read", None), data=d.get("data", None))


class DbfsAPI:
    """DBFS API makes it simple to interact with various data sources without having to include a users
    credentials every time to read a file."""

    def __init__(self, api_client):
        self._api = api_client

    def add_block(self, handle: int, data: str):
        """Appends a block of data to the stream specified by the input handle. If the handle does not exist,
        this call will throw an exception with ``RESOURCE_DOES_NOT_EXIST``.

        If the block of data exceeds 1 MB, this call will throw an exception with ``MAX_BLOCK_SIZE_EXCEEDED``.

        :param handle: int
          The handle on an open stream.
        :param data: str
          The base64-encoded data to append to the stream. This has a limit of 1 MB.


        """

        body = {}
        if data is not None:
            body["data"] = data
        if handle is not None:
            body["handle"] = handle
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/dbfs/add-block", body=body, headers=headers)

    def close(self, handle: int):
        """Closes the stream specified by the input handle. If the handle does not exist, this call throws an
        exception with ``RESOURCE_DOES_NOT_EXIST``.

        :param handle: int
          The handle on an open stream.


        """

        body = {}
        if handle is not None:
            body["handle"] = handle
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/dbfs/close", body=body, headers=headers)

    def create(self, path: str, *, overwrite: Optional[bool] = None) -> CreateResponse:
        """Opens a stream to write to a file and returns a handle to this stream. There is a 10 minute idle
        timeout on this handle. If a file or directory already exists on the given path and __overwrite__ is
        set to false, this call will throw an exception with ``RESOURCE_ALREADY_EXISTS``.

        A typical workflow for file upload would be:

        1. Issue a ``create`` call and get a handle. 2. Issue one or more ``add-block`` calls with the handle
        you have. 3. Issue a ``close`` call with the handle you have.

        :param path: str
          The path of the new file. The path should be the absolute DBFS path.
        :param overwrite: bool (optional)
          The flag that specifies whether to overwrite existing file/files.

        :returns: :class:`CreateResponse`
        """

        body = {}
        if overwrite is not None:
            body["overwrite"] = overwrite
        if path is not None:
            body["path"] = path
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/dbfs/create", body=body, headers=headers)
        return CreateResponse.from_dict(res)

    def delete(self, path: str, *, recursive: Optional[bool] = None):
        """Delete the file or directory (optionally recursively delete all files in the directory). This call
        throws an exception with `IO_ERROR` if the path is a non-empty directory and `recursive` is set to
        `false` or on other similar errors.

        When you delete a large number of files, the delete operation is done in increments. The call returns
        a response after approximately 45 seconds with an error message (503 Service Unavailable) asking you
        to re-invoke the delete operation until the directory structure is fully deleted.

        For operations that delete more than 10K files, we discourage using the DBFS REST API, but advise you
        to perform such operations in the context of a cluster, using the [File system utility
        (dbutils.fs)](/dev-tools/databricks-utils.html#dbutils-fs). `dbutils.fs` covers the functional scope
        of the DBFS REST API, but from notebooks. Running such operations using notebooks provides better
        control and manageability, such as selective deletes, and the possibility to automate periodic delete
        jobs.

        :param path: str
          The path of the file or directory to delete. The path should be the absolute DBFS path.
        :param recursive: bool (optional)
          Whether or not to recursively delete the directory's contents. Deleting empty directories can be
          done without providing the recursive flag.


        """

        body = {}
        if path is not None:
            body["path"] = path
        if recursive is not None:
            body["recursive"] = recursive
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/dbfs/delete", body=body, headers=headers)

    def get_status(self, path: str) -> FileInfo:
        """Gets the file information for a file or directory. If the file or directory does not exist, this call
        throws an exception with `RESOURCE_DOES_NOT_EXIST`.

        :param path: str
          The path of the file or directory. The path should be the absolute DBFS path.

        :returns: :class:`FileInfo`
        """

        query = {}
        if path is not None:
            query["path"] = path
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/dbfs/get-status", query=query, headers=headers)
        return FileInfo.from_dict(res)

    def list(self, path: str) -> Iterator[FileInfo]:
        """List the contents of a directory, or details of the file. If the file or directory does not exist,
        this call throws an exception with `RESOURCE_DOES_NOT_EXIST`.

        When calling list on a large directory, the list operation will time out after approximately 60
        seconds. We strongly recommend using list only on directories containing less than 10K files and
        discourage using the DBFS REST API for operations that list more than 10K files. Instead, we recommend
        that you perform such operations in the context of a cluster, using the [File system utility
        (dbutils.fs)](/dev-tools/databricks-utils.html#dbutils-fs), which provides the same functionality
        without timing out.

        :param path: str
          The path of the file or directory. The path should be the absolute DBFS path.

        :returns: Iterator over :class:`FileInfo`
        """

        query = {}
        if path is not None:
            query["path"] = path
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        json = self._api.do("GET", "/api/2.0/dbfs/list", query=query, headers=headers)
        parsed = ListStatusResponse.from_dict(json).files
        return parsed if parsed is not None else []

    def mkdirs(self, path: str):
        """Creates the given directory and necessary parent directories if they do not exist. If a file (not a
        directory) exists at any prefix of the input path, this call throws an exception with
        `RESOURCE_ALREADY_EXISTS`. **Note**: If this operation fails, it might have succeeded in creating some
        of the necessary parent directories.

        :param path: str
          The path of the new directory. The path should be the absolute DBFS path.


        """

        body = {}
        if path is not None:
            body["path"] = path
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/dbfs/mkdirs", body=body, headers=headers)

    def move(self, source_path: str, destination_path: str):
        """Moves a file from one location to another location within DBFS. If the source file does not exist,
        this call throws an exception with `RESOURCE_DOES_NOT_EXIST`. If a file already exists in the
        destination path, this call throws an exception with `RESOURCE_ALREADY_EXISTS`. If the given source
        path is a directory, this call always recursively moves all files.

        :param source_path: str
          The source path of the file or directory. The path should be the absolute DBFS path.
        :param destination_path: str
          The destination path of the file or directory. The path should be the absolute DBFS path.


        """

        body = {}
        if destination_path is not None:
            body["destination_path"] = destination_path
        if source_path is not None:
            body["source_path"] = source_path
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/dbfs/move", body=body, headers=headers)

    def put(self, path: str, *, contents: Optional[str] = None, overwrite: Optional[bool] = None):
        """Uploads a file through the use of multipart form post. It is mainly used for streaming uploads, but
        can also be used as a convenient single call for data upload.

        Alternatively you can pass contents as base64 string.

        The amount of data that can be passed (when not streaming) using the __contents__ parameter is limited
        to 1 MB. `MAX_BLOCK_SIZE_EXCEEDED` will be thrown if this limit is exceeded.

        If you want to upload large files, use the streaming upload. For details, see :method:dbfs/create,
        :method:dbfs/addBlock, :method:dbfs/close.

        :param path: str
          The path of the new file. The path should be the absolute DBFS path.
        :param contents: str (optional)
          This parameter might be absent, and instead a posted file will be used.
        :param overwrite: bool (optional)
          The flag that specifies whether to overwrite existing file/files.


        """

        body = {}
        if contents is not None:
            body["contents"] = contents
        if overwrite is not None:
            body["overwrite"] = overwrite
        if path is not None:
            body["path"] = path
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("POST", "/api/2.0/dbfs/put", body=body, headers=headers)

    def read(self, path: str, *, length: Optional[int] = None, offset: Optional[int] = None) -> ReadResponse:
        """Returns the contents of a file. If the file does not exist, this call throws an exception with
        `RESOURCE_DOES_NOT_EXIST`. If the path is a directory, the read length is negative, or if the offset
        is negative, this call throws an exception with `INVALID_PARAMETER_VALUE`. If the read length exceeds
        1 MB, this call throws an exception with `MAX_READ_SIZE_EXCEEDED`.

        If `offset + length` exceeds the number of bytes in a file, it reads the contents until the end of
        file.

        :param path: str
          The path of the file to read. The path should be the absolute DBFS path.
        :param length: int (optional)
          The number of bytes to read starting from the offset. This has a limit of 1 MB, and a default value
          of 0.5 MB.
        :param offset: int (optional)
          The offset to read from in bytes.

        :returns: :class:`ReadResponse`
        """

        query = {}
        if length is not None:
            query["length"] = length
        if offset is not None:
            query["offset"] = offset
        if path is not None:
            query["path"] = path
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/dbfs/read", query=query, headers=headers)
        return ReadResponse.from_dict(res)


class FilesAPI:
    """The Files API is a standard HTTP API that allows you to read, write, list, and delete files and
    directories by referring to their URI. The API makes working with file content as raw bytes easier and
    more efficient.

    The API supports [Unity Catalog volumes], where files and directories to operate on are specified using
    their volume URI path, which follows the format
    /Volumes/&lt;catalog_name&gt;/&lt;schema_name&gt;/&lt;volume_name&gt;/&lt;path_to_file&gt;.

    The Files API has two distinct endpoints, one for working with files (`/fs/files`) and another one for
    working with directories (`/fs/directories`). Both endpoints use the standard HTTP methods GET, HEAD, PUT,
    and DELETE to manage files and directories specified using their URI path. The path is always absolute.

    Some Files API client features are currently experimental. To enable them, set
    `enable_experimental_files_api_client = True` in your configuration profile or use the environment
    variable `DATABRICKS_ENABLE_EXPERIMENTAL_FILES_API_CLIENT=True`.

    Use of Files API may incur Databricks data transfer charges.

    [Unity Catalog volumes]: https://docs.databricks.com/en/connect/unity-catalog/volumes.html"""

    def __init__(self, api_client):
        self._api = api_client

    def create_directory(self, directory_path: str):
        """Creates an empty directory. If necessary, also creates any parent directories of the new, empty
        directory (like the shell command `mkdir -p`). If called on an existing directory, returns a success
        response; this method is idempotent (it will succeed if the directory already exists).

        :param directory_path: str
          The absolute path of a directory.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "PUT", f"/api/2.0/fs/directories{_escape_multi_segment_path_parameter(directory_path)}", headers=headers
        )

    def delete(self, file_path: str):
        """Deletes a file. If the request is successful, there is no response body.

        :param file_path: str
          The absolute path of the file.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/fs/files{_escape_multi_segment_path_parameter(file_path)}", headers=headers)

    def delete_directory(self, directory_path: str):
        """Deletes an empty directory.

        To delete a non-empty directory, first delete all of its contents. This can be done by listing the
        directory contents and deleting each file and subdirectory recursively.

        :param directory_path: str
          The absolute path of a directory.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE", f"/api/2.0/fs/directories{_escape_multi_segment_path_parameter(directory_path)}", headers=headers
        )

    def download(self, file_path: str) -> DownloadResponse:
        """Downloads a file. The file contents are the response body. This is a standard HTTP file download, not
        a JSON RPC. It supports the Range and If-Unmodified-Since HTTP headers.

        :param file_path: str
          The absolute path of the file.

        :returns: :class:`DownloadResponse`
        """

        headers = {
            "Accept": "application/octet-stream",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        response_headers = [
            "content-length",
            "content-type",
            "last-modified",
        ]
        res = self._api.do(
            "GET",
            f"/api/2.0/fs/files{_escape_multi_segment_path_parameter(file_path)}",
            headers=headers,
            response_headers=response_headers,
            raw=True,
        )
        return DownloadResponse.from_dict(res)

    def get_directory_metadata(self, directory_path: str):
        """Get the metadata of a directory. The response HTTP headers contain the metadata. There is no response
        body.

        This method is useful to check if a directory exists and the caller has access to it.

        If you wish to ensure the directory exists, you can instead use `PUT`, which will create the directory
        if it does not exist, and is idempotent (it will succeed if the directory already exists).

        :param directory_path: str
          The absolute path of a directory.


        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "HEAD", f"/api/2.0/fs/directories{_escape_multi_segment_path_parameter(directory_path)}", headers=headers
        )

    def get_metadata(self, file_path: str) -> GetMetadataResponse:
        """Get the metadata of a file. The response HTTP headers contain the metadata. There is no response body.

        :param file_path: str
          The absolute path of the file.

        :returns: :class:`GetMetadataResponse`
        """

        headers = {}

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        response_headers = [
            "content-length",
            "content-type",
            "last-modified",
        ]
        res = self._api.do(
            "HEAD",
            f"/api/2.0/fs/files{_escape_multi_segment_path_parameter(file_path)}",
            headers=headers,
            response_headers=response_headers,
        )
        return GetMetadataResponse.from_dict(res)

    def list_directory_contents(
        self, directory_path: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[DirectoryEntry]:
        """Returns the contents of a directory. If there is no directory at the specified path, the API returns a
        HTTP 404 error.

        :param directory_path: str
          The absolute path of a directory.
        :param page_size: int (optional)
          The maximum number of directory entries to return. The response may contain fewer entries. If the
          response contains a `next_page_token`, there may be more entries, even if fewer than `page_size`
          entries are in the response.

          We recommend not to set this value unless you are intentionally listing less than the complete
          directory contents.

          If unspecified, at most 1000 directory entries will be returned. The maximum value is 1000. Values
          above 1000 will be coerced to 1000.
        :param page_token: str (optional)
          An opaque page token which was the `next_page_token` in the response of the previous request to list
          the contents of this directory. Provide this token to retrieve the next page of directory entries.
          When providing a `page_token`, all other parameters provided to the request must match the previous
          request. To list all of the entries in a directory, it is necessary to continue requesting pages of
          entries until the response contains no `next_page_token`. Note that the number of entries returned
          must not be used to determine when the listing is complete.

        :returns: Iterator over :class:`DirectoryEntry`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET",
                f"/api/2.0/fs/directories{_escape_multi_segment_path_parameter(directory_path)}",
                query=query,
                headers=headers,
            )
            if "contents" in json:
                for v in json["contents"]:
                    yield DirectoryEntry.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def upload(self, file_path: str, contents: BinaryIO, *, overwrite: Optional[bool] = None):
        """Uploads a file of up to 5 GiB. The file contents should be sent as the request body as raw bytes (an
        octet stream); do not encode or otherwise modify the bytes before sending. The contents of the
        resulting file will be exactly the bytes sent in the request body. If the request is successful, there
        is no response body.

        :param file_path: str
          The absolute path of the file.
        :param contents: BinaryIO
        :param overwrite: bool (optional)
          If true or unspecified, an existing file will be overwritten. If false, an error will be returned if
          the path points to an existing file.


        """

        query = {}
        if overwrite is not None:
            query["overwrite"] = overwrite
        headers = {
            "Content-Type": "application/octet-stream",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "PUT",
            f"/api/2.0/fs/files{_escape_multi_segment_path_parameter(file_path)}",
            query=query,
            headers=headers,
            data=contents,
        )
