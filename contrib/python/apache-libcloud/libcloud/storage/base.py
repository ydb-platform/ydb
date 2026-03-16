# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Provides base classes for working with storage
"""

# Backward compatibility for Python 2.5

import errno
import hashlib
import os.path  # pylint: disable-msg=W0404
import warnings
from typing import Dict, List, Type, Iterator, Optional
from os.path import join as pjoin

import libcloud.utils.files
from libcloud.utils.py3 import b, httplib
from libcloud.common.base import BaseDriver, Connection, ConnectionUserAndKey
from libcloud.common.types import LibcloudError
from libcloud.storage.types import ObjectDoesNotExistError

__all__ = ["Object", "Container", "StorageDriver", "CHUNK_SIZE", "DEFAULT_CONTENT_TYPE"]

CHUNK_SIZE = 8096

# Default Content-Type which is sent when uploading an object if one is not
# supplied and can't be detected when using non-strict mode.
DEFAULT_CONTENT_TYPE = "application/octet-stream"


class Object:
    """
    Represents an object (BLOB).
    """

    def __init__(
        self,
        name,  # type: str
        size,  # type: int
        hash,  # type: str
        extra,  # type: dict
        meta_data,  # type: dict
        container,  # type: Container
        driver,  # type: StorageDriver
    ):
        """
        :param name: Object name (must be unique per container).
        :type  name: ``str``

        :param size: Object size in bytes.
        :type  size: ``int``

        :param hash: Object hash.
        :type  hash: ``str``

        :param container: Object container.
        :type  container: :class:`libcloud.storage.base.Container`

        :param extra: Extra attributes.
        :type  extra: ``dict``

        :param meta_data: Optional object meta data.
        :type  meta_data: ``dict``

        :param driver: StorageDriver instance.
        :type  driver: :class:`libcloud.storage.base.StorageDriver`
        """

        self.name = name
        self.size = size
        self.hash = hash
        self.container = container
        self.extra = extra or {}
        self.meta_data = meta_data or {}
        self.driver = driver

    def get_cdn_url(self):
        # type: () -> str
        return self.driver.get_object_cdn_url(obj=self)

    def enable_cdn(self):
        # type: () -> bool
        return self.driver.enable_object_cdn(obj=self)

    def download(self, destination_path, overwrite_existing=False, delete_on_failure=True):
        # type: (str, bool, bool) -> bool
        return self.driver.download_object(
            obj=self,
            destination_path=destination_path,
            overwrite_existing=overwrite_existing,
            delete_on_failure=delete_on_failure,
        )

    def as_stream(self, chunk_size=None):
        # type: (Optional[int]) -> Iterator[bytes]
        return self.driver.download_object_as_stream(obj=self, chunk_size=chunk_size)

    def download_range(
        self,
        destination_path,
        start_bytes,
        end_bytes=None,
        overwrite_existing=False,
        delete_on_failure=True,
    ):
        # type: (str, int, Optional[int], bool, bool) -> bool
        return self.driver.download_object_range(
            obj=self,
            destination_path=destination_path,
            start_bytes=start_bytes,
            end_bytes=end_bytes,
            overwrite_existing=overwrite_existing,
            delete_on_failure=delete_on_failure,
        )

    def range_as_stream(self, start_bytes, end_bytes=None, chunk_size=None):
        # type: (int, Optional[int], Optional[int]) -> Iterator[bytes]
        return self.driver.download_object_range_as_stream(
            obj=self,
            start_bytes=start_bytes,
            end_bytes=end_bytes,
            chunk_size=chunk_size,
        )

    def delete(self):
        # type: () -> bool
        return self.driver.delete_object(self)

    def __repr__(self):
        return "<Object: name={}, size={}, hash={}, provider={} ...>".format(
            self.name,
            self.size,
            self.hash,
            self.driver.name,
        )


class Container:
    """
    Represents a container (bucket) which can hold multiple objects.
    """

    def __init__(
        self,
        name,  # type: str
        extra,  # type: dict
        driver,  # type: StorageDriver
    ):
        """
        :param name: Container name (must be unique).
        :type name: ``str``

        :param extra: Extra attributes.
        :type extra: ``dict``

        :param driver: StorageDriver instance.
        :type driver: :class:`libcloud.storage.base.StorageDriver`
        """

        self.name = name
        self.extra = extra or {}
        self.driver = driver

    def iterate_objects(self, prefix=None, ex_prefix=None):
        # type: (Optional[str], Optional[str]) -> Iterator[Object]
        return self.driver.iterate_container_objects(
            container=self, prefix=prefix, ex_prefix=ex_prefix
        )

    def list_objects(self, prefix=None, ex_prefix=None):
        # type: (Optional[str], Optional[str]) -> List[Object]
        return self.driver.list_container_objects(
            container=self, prefix=prefix, ex_prefix=ex_prefix
        )

    def get_cdn_url(self):
        # type: () -> str
        return self.driver.get_container_cdn_url(container=self)

    def enable_cdn(self):
        # type: () -> bool
        return self.driver.enable_container_cdn(container=self)

    def get_object(self, object_name):
        # type: (str) -> Object
        return self.driver.get_object(container_name=self.name, object_name=object_name)

    def upload_object(self, file_path, object_name, extra=None, verify_hash=True, headers=None):
        # type: (str, str, Optional[dict], bool, Optional[Dict[str, str]]) -> Object  # noqa: E501
        return self.driver.upload_object(
            file_path,
            self,
            object_name,
            extra=extra,
            verify_hash=verify_hash,
            headers=headers,
        )

    def upload_object_via_stream(self, iterator, object_name, extra=None, headers=None):
        # type: (Iterator[bytes], str, Optional[dict], Optional[Dict[str, str]]) -> Object  # noqa: E501
        return self.driver.upload_object_via_stream(
            iterator, self, object_name, extra=extra, headers=headers
        )

    def download_object(
        self, obj, destination_path, overwrite_existing=False, delete_on_failure=True
    ):
        # type: (Object, str, bool, bool) -> bool
        return self.driver.download_object(
            obj,
            destination_path,
            overwrite_existing=overwrite_existing,
            delete_on_failure=delete_on_failure,
        )

    def download_object_as_stream(self, obj, chunk_size=None):
        # type: (Object, Optional[int]) -> Iterator[bytes]
        return self.driver.download_object_as_stream(obj, chunk_size)

    def download_object_range(
        self,
        obj,
        destination_path,
        start_bytes,
        end_bytes=None,
        overwrite_existing=False,
        delete_on_failure=True,
    ):
        # type: (Object, str, int, Optional[int], bool, bool) -> bool
        return self.driver.download_object_range(
            obj=obj,
            destination_path=destination_path,
            start_bytes=start_bytes,
            end_bytes=end_bytes,
            overwrite_existing=overwrite_existing,
            delete_on_failure=delete_on_failure,
        )

    def download_object_range_as_stream(self, obj, start_bytes, end_bytes=None, chunk_size=None):
        # type: (Object, int, Optional[int], Optional[int]) -> Iterator[bytes]
        return self.driver.download_object_range_as_stream(
            obj=obj, start_bytes=start_bytes, end_bytes=end_bytes, chunk_size=chunk_size
        )

    def delete_object(self, obj):
        # type: (Object) -> bool
        return self.driver.delete_object(obj)

    def delete(self):
        # type: () -> bool
        return self.driver.delete_container(self)

    def __repr__(self):
        return "<Container: name={}, provider={}>".format(self.name, self.driver.name)


class StorageDriver(BaseDriver):
    """
    A base StorageDriver to derive from.
    """

    connectionCls = ConnectionUserAndKey  # type: Type[Connection]
    name = None  # type: str
    hash_type = "md5"  # type: str
    supports_chunked_encoding = False  # type: bool

    # When strict mode is used, exception will be thrown if no content type is
    # provided and none can be detected when uploading an object
    strict_mode = False  # type: bool

    def iterate_containers(self):
        # type: () -> Iterator[Container]
        """
        Return a iterator of containers for the given account

        :return: A iterator of Container instances.
        :rtype: ``iterator`` of :class:`libcloud.storage.base.Container`
        """
        raise NotImplementedError("iterate_containers not implemented for this driver")

    def list_containers(self):
        # type: () -> List[Container]
        """
        Return a list of containers.

        :return: A list of Container instances.
        :rtype: ``list`` of :class:`Container`
        """
        return list(self.iterate_containers())

    def iterate_container_objects(self, container, prefix=None, ex_prefix=None):
        # type: (Container, Optional[str], Optional[str]) -> Iterator[Object]
        """
        Return a iterator of objects for the given container.

        :param container: Container instance
        :type container: :class:`libcloud.storage.base.Container`

        :param prefix: Filter objects starting with a prefix.
        :type  prefix: ``str``

        :param ex_prefix: (Deprecated.) Filter objects starting with a prefix.
        :type  ex_prefix: ``str``

        :return: A iterator of Object instances.
        :rtype: ``iterator`` of :class:`libcloud.storage.base.Object`
        """
        raise NotImplementedError("iterate_container_objects not implemented for this driver")

    def list_container_objects(self, container, prefix=None, ex_prefix=None):
        # type: (Container, Optional[str], Optional[str]) -> List[Object]
        """
        Return a list of objects for the given container.

        :param container: Container instance.
        :type container: :class:`libcloud.storage.base.Container`

        :param prefix: Filter objects starting with a prefix.
        :type  prefix: ``str``

        :param ex_prefix: (Deprecated.) Filter objects starting with a prefix.
        :type  ex_prefix: ``str``

        :return: A list of Object instances.
        :rtype: ``list`` of :class:`libcloud.storage.base.Object`
        """
        return list(self.iterate_container_objects(container, prefix=prefix, ex_prefix=ex_prefix))

    def _normalize_prefix_argument(self, prefix, ex_prefix):
        if ex_prefix:
            warnings.warn(
                "The ``ex_prefix`` argument is deprecated - "
                "please update code to use ``prefix``",
                DeprecationWarning,
            )
            return ex_prefix

        return prefix

    def _filter_listed_container_objects(self, objects, prefix):
        if prefix is not None:
            warnings.warn(
                "Driver %s does not implement native object "
                "filtering; falling back to filtering the full "
                "object stream." % self.__class__.__name__
            )

        for obj in objects:
            if prefix is None or obj.name.startswith(prefix):
                yield obj

    def get_container(self, container_name):
        # type: (str) -> Container
        """
        Return a container instance.

        :param container_name: Container name.
        :type container_name: ``str``

        :return: :class:`Container` instance.
        :rtype: :class:`libcloud.storage.base.Container`
        """
        raise NotImplementedError("get_object not implemented for this driver")

    def get_container_cdn_url(self, container):
        # type: (Container) -> str
        """
        Return a container CDN URL.

        :param container: Container instance
        :type  container: :class:`libcloud.storage.base.Container`

        :return: A CDN URL for this container.
        :rtype: ``str``
        """
        raise NotImplementedError("get_container_cdn_url not implemented for this driver")

    def get_object(self, container_name, object_name):
        # type: (str, str) -> Object
        """
        Return an object instance.

        :param container_name: Container name.
        :type  container_name: ``str``

        :param object_name: Object name.
        :type  object_name: ``str``

        :return: :class:`Object` instance.
        :rtype: :class:`libcloud.storage.base.Object`
        """
        raise NotImplementedError("get_object not implemented for this driver")

    def get_object_cdn_url(self, obj):
        # type: (Object) -> str
        """
        Return an object CDN URL.

        :param obj: Object instance
        :type  obj: :class:`libcloud.storage.base.Object`

        :return: A CDN URL for this object.
        :rtype: ``str``
        """
        raise NotImplementedError("get_object_cdn_url not implemented for this driver")

    def enable_container_cdn(self, container):
        # type: (Container) -> bool
        """
        Enable container CDN.

        :param container: Container instance
        :type  container: :class:`libcloud.storage.base.Container`

        :rtype: ``bool``
        """
        raise NotImplementedError("enable_container_cdn not implemented for this driver")

    def enable_object_cdn(self, obj):
        # type: (Object) -> bool
        """
        Enable object CDN.

        :param obj: Object instance
        :type  obj: :class:`libcloud.storage.base.Object`

        :rtype: ``bool``
        """
        raise NotImplementedError("enable_object_cdn not implemented for this driver")

    def download_object(
        self, obj, destination_path, overwrite_existing=False, delete_on_failure=True
    ):
        # type: (Object, str, bool, bool) -> bool
        """
        Download an object to the specified destination path.

        :param obj: Object instance.
        :type obj: :class:`libcloud.storage.base.Object`

        :param destination_path: Full path to a file or a directory where the
                                 incoming file will be saved.
        :type destination_path: ``str``

        :param overwrite_existing: True to overwrite an existing file,
                                   defaults to False.
        :type overwrite_existing: ``bool``

        :param delete_on_failure: True to delete a partially downloaded file if
                                   the download was not successful (hash
                                   mismatch / file size).
        :type delete_on_failure: ``bool``

        :return: True if an object has been successfully downloaded, False
                 otherwise.
        :rtype: ``bool``
        """
        raise NotImplementedError("download_object not implemented for this driver")

    def download_object_as_stream(self, obj, chunk_size=None):
        # type: (Object, Optional[int]) -> Iterator[bytes]
        """
        Return a iterator which yields object data.

        :param obj: Object instance
        :type obj: :class:`libcloud.storage.base.Object`

        :param chunk_size: Optional chunk size (in bytes).
        :type chunk_size: ``int``

        :rtype: ``iterator`` of ``bytes``
        """
        raise NotImplementedError("download_object_as_stream not implemented for this driver")

    def download_object_range(
        self,
        obj,
        destination_path,
        start_bytes,
        end_bytes=None,
        overwrite_existing=False,
        delete_on_failure=True,
    ):
        # type: (Object, str, int, Optional[int], bool, bool) -> bool
        """
        Download part of an object.

        :param obj: Object instance.
        :type obj: :class:`libcloud.storage.base.Object`

        :param destination_path: Full path to a file or a directory where the
                                 incoming file will be saved.
        :type destination_path: ``str``

        :param start_bytes: Start byte offset (inclusive) for the range
                            download. Offset is 0 index based so the first
                            byte in file file is "0".
        :type start_bytes: ``int``

        :param end_bytes: End byte offset (non-inclusive) for the range
                          download. If not provided, it will default to the
                          end of the file.
        :type end_bytes: ``int``

        :param overwrite_existing: True to overwrite an existing file,
                                   defaults to False.
        :type overwrite_existing: ``bool``

        :param delete_on_failure: True to delete a partially downloaded file if
                                   the download was not successful (hash
                                   mismatch / file size).
        :type delete_on_failure: ``bool``

        :return: True if an object has been successfully downloaded, False
                 otherwise.
        :rtype: ``bool``

        """
        raise NotImplementedError("download_object_range not implemented for this driver")

    def download_object_range_as_stream(self, obj, start_bytes, end_bytes=None, chunk_size=None):
        # type: (Object, int, Optional[int], Optional[int]) -> Iterator[bytes]
        """
        Return a iterator which yields range / part of the object data.

        :param obj: Object instance
        :type obj: :class:`libcloud.storage.base.Object`

        :param start_bytes: Start byte offset (inclusive) for the range
                            download. Offset is 0 index based so the first
                            byte in file file is "0".
        :type start_bytes: ``int``

        :param end_bytes: End byte offset (non-inclusive) for the range
                          download. If not provided, it will default to the
                          end of the file.
        :type end_bytes: ``int``

        :param chunk_size: Optional chunk size (in bytes).
        :type chunk_size: ``int``

        :rtype: ``iterator`` of ``bytes``
        """
        raise NotImplementedError("download_object_range_as_stream not implemented for this driver")

    def upload_object(
        self,
        file_path,
        container,
        object_name,
        extra=None,
        verify_hash=True,
        headers=None,
    ):
        # type: (str, Container, str, Optional[dict], bool, Optional[Dict[str, str]]) -> Object  # noqa: E501
        """
        Upload an object currently located on a disk.

        :param file_path: Path to the object on disk.
        :type file_path: ``str``

        :param container: Destination container.
        :type container: :class:`libcloud.storage.base.Container`

        :param object_name: Object name.
        :type object_name: ``str``

        :param verify_hash: Verify hash
        :type verify_hash: ``bool``

        :param extra: Extra attributes (driver specific). (optional)
        :type extra: ``dict``

        :param headers: (optional) Additional request headers,
            such as CORS headers. For example:
            headers = {'Access-Control-Allow-Origin': 'http://mozilla.com'}
        :type headers: ``dict``

        :rtype: :class:`libcloud.storage.base.Object`
        """
        raise NotImplementedError("upload_object not implemented for this driver")

    def upload_object_via_stream(self, iterator, container, object_name, extra=None, headers=None):
        # type: (Iterator[bytes], Container, str, Optional[dict], Optional[Dict[str, str]]) -> Object  # noqa: E501
        """
        Upload an object using an iterator.

        If a provider supports it, chunked transfer encoding is used and you
        don't need to know in advance the amount of data to be uploaded.

        Otherwise if a provider doesn't support it, iterator will be exhausted
        so a total size for data to be uploaded can be determined.

        Note: Exhausting the iterator means that the whole data must be
        buffered in memory which might result in memory exhausting when
        uploading a very large object.

        If a file is located on a disk you are advised to use upload_object
        function which uses fs.stat function to determine the file size and it
        doesn't need to buffer whole object in the memory.

        :param iterator: An object which implements the iterator interface.
        :type iterator: :class:`object`

        :param container: Destination container.
        :type container: :class:`libcloud.storage.base.Container`

        :param object_name: Object name.
        :type object_name: ``str``

        :param extra: (optional) Extra attributes (driver specific). Note:
            This dictionary must contain a 'content_type' key which represents
            a content type of the stored object.
        :type extra: ``dict``

        :param headers: (optional) Additional request headers,
            such as CORS headers. For example:
            headers = {'Access-Control-Allow-Origin': 'http://mozilla.com'}
        :type headers: ``dict``

        :rtype: ``libcloud.storage.base.Object``
        """
        raise NotImplementedError("upload_object_via_stream not implemented for this driver")

    def delete_object(self, obj):
        # type: (Object) -> bool
        """
        Delete an object.

        :param obj: Object instance.
        :type obj: :class:`libcloud.storage.base.Object`

        :return: ``bool`` True on success.
        :rtype: ``bool``
        """
        raise NotImplementedError("delete_object not implemented for this driver")

    def create_container(self, container_name):
        # type: (str) -> Container
        """
        Create a new container.

        :param container_name: Container name.
        :type container_name: ``str``

        :return: Container instance on success.
        :rtype: :class:`libcloud.storage.base.Container`
        """
        raise NotImplementedError("create_container not implemented for this driver")

    def delete_container(self, container):
        # type: (Container) -> bool
        """
        Delete a container.

        :param container: Container instance
        :type container: :class:`libcloud.storage.base.Container`

        :return: ``True`` on success, ``False`` otherwise.
        :rtype: ``bool``
        """
        raise NotImplementedError("delete_container not implemented for this driver")

    def _get_object(self, obj, callback, callback_kwargs, response, success_status_code=None):
        """
        Call passed callback and start transfer of the object'

        :param obj: Object instance.
        :type obj: :class:`Object`

        :param callback: Function which is called with the passed
            callback_kwargs
        :type callback: :class:`function`

        :param callback_kwargs: Keyword arguments which are passed to the
             callback.
        :type callback_kwargs: ``dict``

        :param response: Response instance.
        :type response: :class:`Response`

        :param success_status_code: Status code which represents a successful
                                    transfer (defaults to httplib.OK)
        :type success_status_code: ``int``

        :return: ``True`` on success, ``False`` otherwise.
        :rtype: ``bool``
        """
        success_status_code = success_status_code or httplib.OK

        if not isinstance(success_status_code, (list, tuple)):
            success_status_codes = [success_status_code]
        else:
            success_status_codes = success_status_code

        if response.status in success_status_codes:
            return callback(**callback_kwargs)
        elif response.status == httplib.NOT_FOUND:
            raise ObjectDoesNotExistError(object_name=obj.name, value="", driver=self)

        raise LibcloudError(value="Unexpected status code: %s" % (response.status), driver=self)

    def _save_object(
        self,
        response,
        obj,
        destination_path,
        overwrite_existing=False,
        delete_on_failure=True,
        chunk_size=None,
        partial_download=False,
    ):
        """
        Save object to the provided path.

        :param response: RawResponse instance.
        :type response: :class:`RawResponse`

        :param obj: Object instance.
        :type obj: :class:`Object`

        :param destination_path: Destination directory.
        :type destination_path: ``str``

        :param delete_on_failure: True to delete partially downloaded object if
                                  the download fails.
        :type delete_on_failure: ``bool``

        :param overwrite_existing: True to overwrite a local path if it already
                                   exists.
        :type overwrite_existing: ``bool``

        :param chunk_size: Optional chunk size
            (defaults to ``libcloud.storage.base.CHUNK_SIZE``, 8kb)
        :type chunk_size: ``int``

        :param partial_download: True if this is a range (partial) save,
                                 False otherwise.
        :type partial_download: ``bool``

        :return: ``True`` on success, ``False`` otherwise.
        :rtype: ``bool``
        """

        chunk_size = chunk_size or CHUNK_SIZE

        base_name = os.path.basename(destination_path)

        if not base_name and not os.path.exists(destination_path):
            raise LibcloudError(value="Path %s does not exist" % (destination_path), driver=self)

        if not base_name:
            file_path = pjoin(destination_path, obj.name)
        else:
            file_path = destination_path

        if os.path.exists(file_path) and not overwrite_existing:
            raise LibcloudError(
                value="File %s already exists, but " % (file_path) + "overwrite_existing=False",
                driver=self,
            )

        bytes_transferred = 0

        with open(file_path, "wb") as file_handle:
            for chunk in response._response.iter_content(chunk_size):
                file_handle.write(b(chunk))
                bytes_transferred += len(chunk)

        if not partial_download and int(obj.size) != int(bytes_transferred):
            # Transfer failed, support retry?
            # NOTE: We only perform this check if this is a regular and not a
            # partial / range download
            if delete_on_failure:
                try:
                    os.unlink(file_path)
                except Exception:
                    pass

            return False

        return True

    def _upload_object(
        self,
        object_name,
        content_type,
        request_path,
        request_method="PUT",
        headers=None,
        file_path=None,
        stream=None,
        chunked=False,
        multipart=False,
    ):
        """
        Helper function for setting common request headers and calling the
        passed in callback which uploads an object.
        """
        headers = headers or {}

        if file_path and not os.path.exists(file_path):
            raise OSError("File %s does not exist" % (file_path))

        if stream is not None and not hasattr(stream, "next") and not hasattr(stream, "__next__"):
            raise AttributeError("iterator object must implement next() " + "method.")

        headers["Content-Type"] = self._determine_content_type(
            content_type, object_name, file_path=file_path
        )

        if stream:
            response = self.connection.request(
                request_path,
                method=request_method,
                data=stream,
                headers=headers,
                raw=True,
            )
            stream_hash, stream_length = self._hash_buffered_stream(
                stream, self._get_hash_function()
            )
        else:
            with open(file_path, "rb") as file_stream:
                response = self.connection.request(
                    request_path,
                    method=request_method,
                    data=file_stream,
                    headers=headers,
                    raw=True,
                )
            with open(file_path, "rb") as file_stream:
                stream_hash, stream_length = self._hash_buffered_stream(
                    file_stream, self._get_hash_function()
                )

        return {
            "response": response,
            "bytes_transferred": stream_length,
            "data_hash": stream_hash,
        }

    def _determine_content_type(self, content_type, object_name, file_path=None):
        if content_type:
            return content_type

        name = file_path or object_name
        content_type, _ = libcloud.utils.files.guess_file_mime_type(name)

        if self.strict_mode and not content_type:
            raise AttributeError(
                "File content-type could not be guessed for "
                '"%s" and no content_type value is provided' % name
            )

        return content_type or DEFAULT_CONTENT_TYPE

    def _hash_buffered_stream(self, stream, hasher, blocksize=65536):
        total_len = 0

        if hasattr(stream, "__next__") or hasattr(stream, "next"):
            # Ensure we start from the beginning of a stream in case stream is
            # not at the beginning
            if hasattr(stream, "seek"):
                try:
                    stream.seek(0)
                except OSError as e:
                    if e.errno != errno.ESPIPE:
                        # This represents "OSError: [Errno 29] Illegal seek"
                        # error. This could either mean that the underlying
                        # handle doesn't support seek operation (e.g. pipe) or
                        # that the invalid seek position is provided. Sadly
                        # there is no good robust way to distinghuish that so
                        # we simply ignore all the "Illeal seek" errors so
                        # this function works correctly with pipes.
                        # See https://github.com/apache/libcloud/pull/1427 for
                        # details
                        raise e

            for chunk in libcloud.utils.files.read_in_chunks(iterator=stream):
                hasher.update(b(chunk))
                total_len += len(chunk)

            return (hasher.hexdigest(), total_len)

        if not hasattr(stream, "__exit__"):
            for s in stream:
                hasher.update(s)
                total_len = total_len + len(s)
            return (hasher.hexdigest(), total_len)

        with stream:
            buf = stream.read(blocksize)
            while len(buf) > 0:
                total_len = total_len + len(buf)
                hasher.update(buf)
                buf = stream.read(blocksize)

        return (hasher.hexdigest(), total_len)

    def _get_hash_function(self):
        """
        Return instantiated hash function for the hash type supported by
        the provider.
        """
        try:
            func = getattr(hashlib, self.hash_type)()
        except AttributeError:
            raise RuntimeError("Invalid or unsupported hash type: %s" % (self.hash_type))

        return func

    def _validate_start_and_end_bytes(self, start_bytes, end_bytes=None):
        # type: (int, Optional[int]) -> bool
        """
        Method which validates that start_bytes and end_bytes arguments contain
        valid values.
        """
        if start_bytes < 0:
            raise ValueError("start_bytes must be greater than 0")

        if end_bytes is not None:
            if start_bytes > end_bytes:
                raise ValueError("start_bytes must be smaller than end_bytes")
            elif start_bytes == end_bytes:
                raise ValueError(
                    "start_bytes and end_bytes can't be the " "same. end_bytes is non-inclusive"
                )

        return True

    def _get_standard_range_str(self, start_bytes, end_bytes=None, end_bytes_inclusive=False):
        # type: (int, Optional[int], bool) -> str
        """
        Return range string which is used as a Range header value for range
        requests for drivers which follow standard Range header notation

        This returns range string in the following format:
        bytes=<start_bytes>-<end bytes>.

        For example:

        bytes=1-10
        bytes=0-2
        bytes=5-
        bytes=100-5000

        :param end_bytes_inclusive: True if "end_bytes" offset should be
        inclusive (aka opposite from the Python indexing behavior where the end
        index is not inclusive).
        """
        range_str = "bytes=%s-" % (start_bytes)

        if end_bytes is not None:
            if end_bytes_inclusive:
                range_str += str(end_bytes)
            else:
                range_str += str(end_bytes - 1)

        return range_str
