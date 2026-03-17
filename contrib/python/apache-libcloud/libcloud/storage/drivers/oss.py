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

# pylint: disable=unexpected-keyword-arg

import os
import hmac
import time
import base64
import codecs
from hashlib import sha1

from libcloud.utils.py3 import ET, b, httplib, tostring, urlquote, urlencode
from libcloud.utils.xml import findtext, fixxpath
from libcloud.common.base import RawResponse, XmlResponse, ConnectionUserAndKey
from libcloud.utils.files import read_in_chunks
from libcloud.common.types import LibcloudError, InvalidCredsError, MalformedResponseError
from libcloud.storage.base import Object, Container, StorageDriver
from libcloud.storage.types import (
    ContainerError,
    ObjectDoesNotExistError,
    ObjectHashMismatchError,
    ContainerIsNotEmptyError,
    InvalidContainerNameError,
    ContainerDoesNotExistError,
)

try:
    from lxml.etree import Element, SubElement
except ImportError:
    from xml.etree.ElementTree import Element, SubElement


__all__ = [
    "OSSStorageDriver",
    "OSSMultipartUpload",
    "EXPIRATION_SECONDS",
    "CHUNK_SIZE",
    "MAX_UPLOADS_PER_RESPONSE",
]

GMT_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
EXPIRATION_SECONDS = 15 * 60

# OSS multi-part chunks must be great than 100KB except the last one
CHUNK_SIZE = 100 * 1024

# Desired number of items in each response inside a paginated request in
# ex_iterate_multipart_uploads.
MAX_UPLOADS_PER_RESPONSE = 1000


class OSSResponse(XmlResponse):
    namespace = None
    valid_response_codes = [httplib.NOT_FOUND, httplib.CONFLICT, httplib.BAD_REQUEST]

    def success(self):
        i = int(self.status)
        return 200 <= i <= 299 or i in self.valid_response_codes

    def parse_body(self):
        """
        OSSResponse body is in utf-8 encoding.
        """
        if len(self.body) == 0 and not self.parse_zero_length_body:
            return self.body

        try:
            parser = ET.XMLParser(encoding="utf-8")
            body = ET.XML(self.body.encode("utf-8"), parser=parser)
        except Exception:
            raise MalformedResponseError(
                "Failed to parse XML", body=self.body, driver=self.connection.driver
            )
        return body

    def parse_error(self):
        if self.status in [httplib.UNAUTHORIZED, httplib.FORBIDDEN]:
            raise InvalidCredsError(self.body)
        elif self.status == httplib.MOVED_PERMANENTLY:
            raise LibcloudError(
                "This bucket is located in a different " + "region. Please use the correct driver.",
                driver=OSSStorageDriver,
            )
        elif self.status == httplib.METHOD_NOT_ALLOWED:
            raise LibcloudError(
                "The method is not allowed. Status code: %d, "
                "headers: %s" % (self.status, self.headers)
            )
        raise LibcloudError(
            "Unknown error. Status code: %d, body: %s" % (self.status, self.body),
            driver=OSSStorageDriver,
        )


class OSSRawResponse(OSSResponse, RawResponse):
    pass


class OSSConnection(ConnectionUserAndKey):
    """
    Represents a single connection to the Aliyun OSS Endpoint
    """

    _domain = "aliyuncs.com"
    _default_location = "oss"
    responseCls = OSSResponse
    rawResponseCls = OSSRawResponse

    @staticmethod
    def _get_auth_signature(method, headers, params, expires, secret_key, path, vendor_prefix):
        """
        Signature = base64(hmac-sha1(AccessKeySecret,
          VERB + "\n"
          + CONTENT-MD5 + "\n"
          + CONTENT-TYPE + "\n"
          + EXPIRES + "\n"
          + CanonicalizedOSSHeaders
          + CanonicalizedResource))
        """
        special_headers = {"content-md5": "", "content-type": "", "expires": ""}
        vendor_headers = {}

        for key, value in list(headers.items()):
            key_lower = key.lower()
            if key_lower in special_headers:
                special_headers[key_lower] = value.strip()
            elif key_lower.startswith(vendor_prefix):
                vendor_headers[key_lower] = value.strip()

        if expires:
            special_headers["expires"] = str(expires)

        buf = [method]
        for _, value in sorted(special_headers.items()):
            buf.append(value)
        string_to_sign = "\n".join(buf)

        buf = []
        for key, value in sorted(vendor_headers.items()):
            buf.append("{}:{}".format(key, value))
        header_string = "\n".join(buf)

        values_to_sign = []
        for value in [string_to_sign, header_string, path]:
            if value:
                values_to_sign.append(value)

        string_to_sign = "\n".join(values_to_sign)
        b64_hmac = base64.b64encode(
            hmac.new(b(secret_key), b(string_to_sign), digestmod=sha1).digest()
        )
        return b64_hmac

    @staticmethod
    def _get_expires(params):
        """
        Get expires timeout seconds from parameters.
        """
        expires = None
        if "expires" in params:
            expires = params["expires"]
        elif "Expires" in params:
            expires = params["Expires"]
        if expires:
            try:
                return int(expires)
            except Exception:
                pass
        return int(time.time()) + EXPIRATION_SECONDS

    def add_default_params(self, params):
        expires_at = self._get_expires(params)
        expires = str(expires_at)
        params["OSSAccessKeyId"] = self.user_id
        params["Expires"] = expires
        return params

    def add_default_headers(self, headers):
        headers["Date"] = time.strftime(GMT_TIME_FORMAT, time.gmtime())
        return headers

    def pre_connect_hook(self, params, headers):
        if self._container:
            path = "/{}{}".format(self._container.name, self.action)
        else:
            path = self.action

        # pylint: disable=no-member
        params["Signature"] = self._get_auth_signature(
            method=self.method,
            headers=headers,
            params=params,
            expires=params["Expires"],
            secret_key=self.key,
            path=path,
            vendor_prefix=self.driver.http_vendor_prefix,
        )
        return params, headers

    def request(
        self,
        action,
        params=None,
        data=None,
        headers=None,
        method="GET",
        raw=False,
        container=None,
    ):
        self.host = "{}.{}".format(self._default_location, self._domain)
        self._container = container
        if container and container.name:
            if "location" in container.extra:
                self.host = "{}.{}.{}".format(
                    container.name,
                    container.extra["location"],
                    self._domain,
                )
            else:
                self.host = "{}.{}".format(container.name, self.host)
        return super().request(
            action=action,
            params=params,
            data=data,
            headers=headers,
            method=method,
            raw=raw,
        )


class OSSMultipartUpload:
    """
    Class representing an Aliyun OSS multipart upload
    """

    def __init__(self, key, id, initiated):
        """
        Class representing an Aliyun OSS multipart upload

        :param key: The object/key that was being uploaded
        :type key: ``str``

        :param id: The upload id assigned by Aliyun
        :type id: ``str``

        :param initiated: The date/time at which the upload was started
        :type created_at: ``str``
        """
        self.key = key
        self.id = id
        self.initiated = initiated

    def __repr__(self):
        return "<OSSMultipartUpload: key=%s>" % (self.key)


class OSSStorageDriver(StorageDriver):
    name = "Aliyun OSS"
    website = "http://www.aliyun.com/product/oss"
    connectionCls = OSSConnection
    hash_type = "md5"
    supports_chunked_encoding = False
    supports_multipart_upload = True
    namespace = None
    http_vendor_prefix = "x-oss-"

    def iterate_containers(self):
        response = self.connection.request("/")
        if response.status == httplib.OK:
            containers = self._to_containers(obj=response.object, xpath="Buckets/Bucket")
            return containers

        raise LibcloudError("Unexpected status code: %s" % (response.status), driver=self)

    def iterate_container_objects(self, container, prefix=None, ex_prefix=None):
        """
        Return a generator of objects for the given container.

        :param container: Container instance
        :type container: :class:`Container`

        :keyword prefix: Only return objects starting with prefix
        :type prefix: ``str``

        :keyword ex_prefix: (Deprecated.) Only return objects starting with
                            ex_prefix
        :type ex_prefix: ``str``

        :return: A generator of Object instances.
        :rtype: ``generator`` of :class:`Object`
        """
        prefix = self._normalize_prefix_argument(prefix, ex_prefix)

        params = {}

        if prefix:
            params["prefix"] = prefix

        last_key = None
        exhausted = False

        while not exhausted:
            if last_key:
                params["marker"] = last_key

            response = self.connection.request("/", params=params, container=container)

            if response.status != httplib.OK:
                raise LibcloudError("Unexpected status code: %s" % (response.status), driver=self)

            objects = self._to_objs(obj=response.object, xpath="Contents", container=container)
            is_truncated = response.object.findtext(
                fixxpath(xpath="IsTruncated", namespace=self.namespace)
            ).lower()
            exhausted = is_truncated == "false"

            last_key = None
            for obj in objects:
                last_key = obj.name
                yield obj

    def get_container(self, container_name):
        for container in self.iterate_containers():
            if container.name == container_name:
                return container
        raise ContainerDoesNotExistError(value=None, driver=self, container_name=container_name)

    def get_object(self, container_name, object_name):
        container = self.get_container(container_name=container_name)
        object_path = self._get_object_path(container, object_name)
        response = self.connection.request(object_path, method="HEAD", container=container)

        if response.status == httplib.OK:
            obj = self._headers_to_object(
                object_name=object_name, container=container, headers=response.headers
            )
            return obj

        raise ObjectDoesNotExistError(value=None, driver=self, object_name=object_name)

    def create_container(self, container_name, ex_location=None):
        """
        @inherits :class:`StorageDriver.create_container`

        :keyword ex_location: The desired location where to create container
        :type keyword: ``str``
        """
        extra = None
        if ex_location:
            root = Element("CreateBucketConfiguration")
            child = SubElement(root, "LocationConstraint")
            child.text = ex_location

            data = tostring(root)
            extra = {"location": ex_location}
        else:
            data = ""

        container = Container(name=container_name, extra=extra, driver=self)
        response = self.connection.request("/", data=data, method="PUT", container=container)

        if response.status == httplib.OK:
            return container
        elif response.status == httplib.CONFLICT:
            raise InvalidContainerNameError(
                value="Container with this name already exists. The name must "
                "be unique among all the containers in the system",
                container_name=container_name,
                driver=self,
            )
        elif response.status == httplib.BAD_REQUEST:
            raise ContainerError(
                value="Bad request when creating container: %s" % response.body,
                container_name=container_name,
                driver=self,
            )

        raise LibcloudError("Unexpected status code: %s" % (response.status), driver=self)

    def delete_container(self, container):
        # Note: All the objects in the container must be deleted first
        response = self.connection.request("/", method="DELETE", container=container)
        if response.status == httplib.NO_CONTENT:
            return True
        elif response.status == httplib.CONFLICT:
            raise ContainerIsNotEmptyError(
                value="Container must be empty before it can be deleted.",
                container_name=container.name,
                driver=self,
            )
        elif response.status == httplib.NOT_FOUND:
            raise ContainerDoesNotExistError(value=None, driver=self, container_name=container.name)

        return False

    def download_object(
        self, obj, destination_path, overwrite_existing=False, delete_on_failure=True
    ):
        obj_path = self._get_object_path(obj.container, obj.name)

        response = self.connection.request(
            obj_path, method="GET", raw=True, container=obj.container
        )

        return self._get_object(
            obj=obj,
            callback=self._save_object,
            response=response,
            callback_kwargs={
                "obj": obj,
                "response": response.response,
                "destination_path": destination_path,
                "overwrite_existing": overwrite_existing,
                "delete_on_failure": delete_on_failure,
            },
            success_status_code=httplib.OK,
        )

    def download_object_as_stream(self, obj, chunk_size=None):
        obj_path = self._get_object_path(obj.container, obj.name)
        response = self.connection.request(
            obj_path, method="GET", raw=True, container=obj.container
        )

        return self._get_object(
            obj=obj,
            callback=read_in_chunks,
            response=response,
            callback_kwargs={"iterator": response.response, "chunk_size": chunk_size},
            success_status_code=httplib.OK,
        )

    def upload_object(
        self,
        file_path,
        container,
        object_name,
        extra=None,
        verify_hash=True,
        headers=None,
    ):
        return self._put_object(
            container=container,
            object_name=object_name,
            extra=extra,
            file_path=file_path,
            verify_hash=verify_hash,
        )

    def upload_object_via_stream(self, iterator, container, object_name, extra=None, headers=None):
        method = "PUT"
        params = None

        if self.supports_multipart_upload:
            # @TODO: This needs implementing again from scratch.
            pass
        return self._put_object(
            container=container,
            object_name=object_name,
            extra=extra,
            method=method,
            query_args=params,
            stream=iterator,
            verify_hash=False,
            headers=headers,
        )

    def delete_object(self, obj):
        object_path = self._get_object_path(obj.container, obj.name)
        response = self.connection.request(object_path, method="DELETE", container=obj.container)
        if response.status == httplib.NO_CONTENT:
            return True
        elif response.status == httplib.NOT_FOUND:
            raise ObjectDoesNotExistError(value=None, driver=self, object_name=obj.name)

        return False

    def ex_iterate_multipart_uploads(
        self,
        container,
        prefix=None,
        delimiter=None,
        max_uploads=MAX_UPLOADS_PER_RESPONSE,
    ):
        """
        Extension method for listing all in-progress OSS multipart uploads.

        Each multipart upload which has not been committed or aborted is
        considered in-progress.

        :param container: The container holding the uploads
        :type container: :class:`Container`

        :keyword prefix: Print only uploads of objects with this prefix
        :type prefix: ``str``

        :keyword delimiter: The object/key names are grouped based on
            being split by this delimiter
        :type delimiter: ``str``

        :keyword max_uploads: The max upload items returned for one request
        :type max_uploads: ``int``

        :return: A generator of OSSMultipartUpload instances.
        :rtype: ``generator`` of :class:`OSSMultipartUpload`
        """

        if not self.supports_multipart_upload:
            raise LibcloudError("Feature not supported", driver=self)

        request_path = "/?uploads"
        params = {"max-uploads": max_uploads}

        if prefix:
            params["prefix"] = prefix

        if delimiter:
            params["delimiter"] = delimiter

        def finder(node, text):
            return node.findtext(fixxpath(xpath=text, namespace=self.namespace))

        while True:
            response = self.connection.request(request_path, params=params, container=container)

            if response.status != httplib.OK:
                raise LibcloudError(
                    "Error fetching multipart uploads. " "Got code: %s" % response.status,
                    driver=self,
                )

            body = response.parse_body()
            # pylint: disable=maybe-no-member
            for node in body.findall(fixxpath(xpath="Upload", namespace=self.namespace)):
                key = finder(node, "Key")
                upload_id = finder(node, "UploadId")
                initiated = finder(node, "Initiated")

                yield OSSMultipartUpload(key, upload_id, initiated)

            # Check if this is the last entry in the listing
            # pylint: disable=maybe-no-member
            is_truncated = body.findtext(fixxpath(xpath="IsTruncated", namespace=self.namespace))

            if is_truncated.lower() == "false":
                break

            # Provide params for the next request
            upload_marker = body.findtext(
                fixxpath(xpath="NextUploadIdMarker", namespace=self.namespace)
            )
            key_marker = body.findtext(fixxpath(xpath="NextKeyMarker", namespace=self.namespace))

            params["key-marker"] = key_marker
            params["upload-id-marker"] = upload_marker

    def ex_abort_all_multipart_uploads(self, container, prefix=None):
        """
        Extension method for removing all partially completed OSS multipart
        uploads.

        :param container: The container holding the uploads
        :type container: :class:`Container`

        :keyword prefix: Delete only uploads of objects with this prefix
        :type prefix: ``str``
        """

        # Iterate through the container and delete the upload ids
        for upload in self.ex_iterate_multipart_uploads(container, prefix, delimiter=None):
            object_path = self._get_object_path(container, upload.key)
            self._abort_multipart(object_path, upload.id, container=container)

    def _clean_object_name(self, name):
        name = urlquote(name)
        return name

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
        container=None,
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
                container=container,
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
                    container=container,
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

    def _put_object(
        self,
        container,
        object_name,
        method="PUT",
        query_args=None,
        extra=None,
        file_path=None,
        stream=None,
        verify_hash=False,
        headers=None,
    ):
        """
        Create an object and upload data using the given function.
        """
        headers = headers or {}
        extra = extra or {}

        content_type = extra.get("content_type", None)
        meta_data = extra.get("meta_data", None)
        acl = extra.get("acl", None)

        if meta_data:
            for key, value in list(meta_data.items()):
                key = self.http_vendor_prefix + "meta-%s" % (key)
                headers[key] = value

        if acl:
            if acl not in ["public-read", "private", "public-read-write"]:
                raise AttributeError("invalid acl value: %s" % acl)
            headers[self.http_vendor_prefix + "object-acl"] = acl

        request_path = self._get_object_path(container, object_name)

        if query_args:
            request_path = "?".join((request_path, query_args))

        result_dict = self._upload_object(
            object_name=object_name,
            content_type=content_type,
            request_path=request_path,
            request_method=method,
            headers=headers,
            file_path=file_path,
            stream=stream,
            container=container,
        )

        response = result_dict["response"]
        bytes_transferred = result_dict["bytes_transferred"]
        headers = response.headers

        server_hash = headers["etag"].replace('"', "")

        if verify_hash and result_dict["data_hash"].upper() != server_hash.upper():
            raise ObjectHashMismatchError(
                value="MD5 hash {} checksum does not match {}".format(
                    server_hash, result_dict["data_hash"]
                ),
                object_name=object_name,
                driver=self,
            )
        elif response.status == httplib.OK:
            obj = Object(
                name=object_name,
                size=bytes_transferred,
                hash=server_hash,
                extra={"acl": acl},
                meta_data=meta_data,
                container=container,
                driver=self,
            )

            return obj
        else:
            raise LibcloudError(
                "Unexpected status code, status_code=%s" % (response.status),
                driver=self,
            )

    def _upload_multipart(
        self, response, data, iterator, container, object_name, calculate_hash=True
    ):
        """
        Callback invoked for uploading data to OSS using Aliyun's
        multipart upload mechanism

        :param response: Response object from the initial POST request
        :type response: :class:`OSSRawResponse`

        :param data: Any data from the initial POST request
        :type data: ``str``

        :param iterator: The generator for fetching the upload data
        :type iterator: ``generator``

        :param container: The container owning the object to which data is
            being uploaded
        :type container: :class:`Container`

        :param object_name: The name of the object to which we are uploading
        :type object_name: ``str``

        :keyword calculate_hash: Indicates if we must calculate the data hash
        :type calculate_hash: ``bool``

        :return: A tuple of (status, checksum, bytes transferred)
        :rtype: ``tuple``
        """

        object_path = self._get_object_path(container, object_name)

        # Get the upload id from the response xml
        response.body = response.response.read()
        body = response.parse_body()
        upload_id = body.find(fixxpath(xpath="UploadId", namespace=self.namespace)).text

        try:
            # Upload the data through the iterator
            result = self._upload_from_iterator(
                iterator, object_path, upload_id, calculate_hash, container=container
            )
            (chunks, data_hash, bytes_transferred) = result

            # Commit the chunk info and complete the upload
            etag = self._commit_multipart(object_path, upload_id, chunks, container=container)
        except Exception as e:
            # Amazon provides a mechanism for aborting an upload.
            self._abort_multipart(object_path, upload_id, container=container)
            raise e

        # Modify the response header of the first request. This is used
        # by other functions once the callback is done
        response.headers["etag"] = etag

        return (True, data_hash, bytes_transferred)

    def _upload_from_iterator(
        self, iterator, object_path, upload_id, calculate_hash=True, container=None
    ):
        """
        Uploads data from an iterator in fixed sized chunks to OSS

        :param iterator: The generator for fetching the upload data
        :type iterator: ``generator``

        :param object_path: The path of the object to which we are uploading
        :type object_name: ``str``

        :param upload_id: The upload id allocated for this multipart upload
        :type upload_id: ``str``

        :keyword calculate_hash: Indicates if we must calculate the data hash
        :type calculate_hash: ``bool``

        :keyword container: the container object to upload object to
        :type container: :class:`Container`

        :return: A tuple of (chunk info, checksum, bytes transferred)
        :rtype: ``tuple``
        """

        data_hash = None
        if calculate_hash:
            data_hash = self._get_hash_function()

        bytes_transferred = 0
        count = 1
        chunks = []
        params = {"uploadId": upload_id}

        # Read the input data in chunk sizes suitable for AWS
        for data in read_in_chunks(
            iterator, chunk_size=CHUNK_SIZE, fill_size=True, yield_empty=True
        ):
            bytes_transferred += len(data)

            if calculate_hash:
                data_hash.update(data)

            chunk_hash = self._get_hash_function()
            chunk_hash.update(data)
            chunk_hash = base64.b64encode(chunk_hash.digest()).decode("utf-8")

            # OSS will calculate hash of the uploaded data and
            # check this header.
            headers = {"Content-MD5": chunk_hash}
            params["partNumber"] = count

            request_path = "?".join((object_path, urlencode(params)))

            resp = self.connection.request(
                request_path,
                method="PUT",
                data=data,
                headers=headers,
                container=container,
            )

            if resp.status != httplib.OK:
                raise LibcloudError("Error uploading chunk", driver=self)

            server_hash = resp.headers["etag"]

            # Keep this data for a later commit
            chunks.append((count, server_hash))
            count += 1

        if calculate_hash:
            data_hash = data_hash.hexdigest()

        return (chunks, data_hash, bytes_transferred)

    def _commit_multipart(self, object_path, upload_id, chunks, container=None):
        """
        Makes a final commit of the data.

        :param object_path: Server side object path.
        :type object_path: ``str``

        :param upload_id: ID of the multipart upload.
        :type upload_id: ``str``

        :param upload_id: A list of (chunk_number, chunk_hash) tuples.
        :type upload_id: ``list``

        :keyword container: The container owning the object to which data is
            being uploaded
        :type container: :class:`Container`
        """

        root = Element("CompleteMultipartUpload")

        for count, etag in chunks:
            part = SubElement(root, "Part")
            part_no = SubElement(part, "PartNumber")
            part_no.text = str(count)

            etag_id = SubElement(part, "ETag")
            etag_id.text = str(etag)

        data = tostring(root)

        params = {"uploadId": upload_id}
        request_path = "?".join((object_path, urlencode(params)))
        response = self.connection.request(
            request_path, data=data, method="POST", container=container
        )

        if response.status != httplib.OK:
            element = response.object
            # pylint: disable=maybe-no-member
            code, message = response._parse_error_details(element=element)
            msg = "Error in multipart commit: {} ({})".format(message, code)
            raise LibcloudError(msg, driver=self)

        # Get the server's etag to be passed back to the caller
        body = response.parse_body()
        server_hash = body.find(fixxpath(xpath="ETag", namespace=self.namespace)).text
        return server_hash

    def _abort_multipart(self, object_path, upload_id, container=None):
        """
        Aborts an already initiated multipart upload

        :param object_path: Server side object path.
        :type object_path: ``str``

        :param upload_id: ID of the multipart upload.
        :type upload_id: ``str``

        :keyword container: The container owning the object to which data is
            being uploaded
        :type container: :class:`Container`
        """

        params = {"uploadId": upload_id}
        request_path = "?".join((object_path, urlencode(params)))
        resp = self.connection.request(request_path, method="DELETE", container=container)

        if resp.status != httplib.NO_CONTENT:
            raise LibcloudError(
                "Error in multipart abort. status_code=%d" % (resp.status), driver=self
            )

    def _to_containers(self, obj, xpath):
        for element in obj.findall(fixxpath(xpath=xpath, namespace=self.namespace)):
            yield self._to_container(element)

    def _to_container(self, element):
        extra = {
            "creation_date": findtext(
                element=element, xpath="CreationDate", namespace=self.namespace
            ),
            "location": findtext(element=element, xpath="Location", namespace=self.namespace),
        }

        container = Container(
            name=findtext(element=element, xpath="Name", namespace=self.namespace),
            extra=extra,
            driver=self,
        )

        return container

    def _to_objs(self, obj, xpath, container):
        return [
            self._to_obj(element, container)
            for element in obj.findall(fixxpath(xpath=xpath, namespace=self.namespace))
        ]

    def _to_obj(self, element, container):
        owner_id = findtext(element=element, xpath="Owner/ID", namespace=self.namespace)
        owner_display_name = findtext(
            element=element, xpath="Owner/DisplayName", namespace=self.namespace
        )
        meta_data = {
            "owner": {
                "id": owner_id,
                "display_name": self._safe_decode(owner_display_name),
            }
        }
        last_modified = findtext(element=element, xpath="LastModified", namespace=self.namespace)
        extra = {"last_modified": last_modified}

        name = self._safe_decode(findtext(element=element, xpath="Key", namespace=self.namespace))
        obj = Object(
            name=name,
            size=int(findtext(element=element, xpath="Size", namespace=self.namespace)),
            hash=findtext(element=element, xpath="ETag", namespace=self.namespace).replace('"', ""),
            extra=extra,
            meta_data=meta_data,
            container=container,
            driver=self,
        )

        return obj

    def _safe_decode(self, encoded):
        """
        Decode it as an escaped string and then treat the content as
        UTF-8 encoded.
        """
        try:
            if encoded:
                unescaped, _ign = codecs.escape_decode(encoded)
                return unescaped.decode("utf-8")
            return encoded
        except Exception:
            return encoded

    def _get_container_path(self, container):
        """
        Return a container path

        :param container: Container instance
        :type  container: :class:`Container`

        :return: A path for this container.
        :rtype: ``str``
        """
        return "/%s" % (container.name)

    def _get_object_path(self, container, object_name):
        """
        Return an object's path.
        Aliyun OSS api puts the container name in the host,
        so ignore container here.

        :param container: Container instance
        :type  container: :class:`Container`

        :param object_name: Object name
        :type  object_name: :class:`str`

        :return: A  path for this object.
        :rtype: ``str``
        """
        object_name_cleaned = self._clean_object_name(object_name)
        object_path = "/%s" % object_name_cleaned
        return object_path

    def _headers_to_object(self, object_name, container, headers):
        hash = headers["etag"].replace('"', "")
        extra = {"content_type": headers["content-type"], "etag": headers["etag"]}
        meta_data = {}

        if "last-modified" in headers:
            extra["last_modified"] = headers["last-modified"]

        for key, value in headers.items():
            if not key.lower().startswith(self.http_vendor_prefix + "meta-"):
                continue

            key = key.replace(self.http_vendor_prefix + "meta-", "")
            meta_data[key] = value

        obj = Object(
            name=object_name,
            size=int(headers["content-length"]),
            hash=hash,
            extra=extra,
            meta_data=meta_data,
            container=container,
            driver=self,
        )
        return obj
