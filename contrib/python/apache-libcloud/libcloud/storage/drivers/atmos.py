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

import hmac
import time
import base64
import hashlib
from io import FileIO as file

from libcloud.utils.py3 import b, next, httplib, urlparse, urlquote, urlencode, urlunquote
from libcloud.common.base import XmlResponse, ConnectionUserAndKey
from libcloud.utils.files import read_in_chunks
from libcloud.common.types import LibcloudError
from libcloud.storage.base import CHUNK_SIZE, Object, Container, StorageDriver
from libcloud.storage.types import (
    ObjectDoesNotExistError,
    ContainerIsNotEmptyError,
    ContainerDoesNotExistError,
    ContainerAlreadyExistsError,
)


def collapse(s):
    return " ".join([x for x in s.split(" ") if x])


class AtmosError(LibcloudError):
    def __init__(self, code, message, driver=None):
        super().__init__(value=message, driver=driver)
        self.code = code


class AtmosResponse(XmlResponse):
    def success(self):
        return self.status in (
            httplib.OK,
            httplib.CREATED,
            httplib.NO_CONTENT,
            httplib.PARTIAL_CONTENT,
        )

    def parse_error(self):
        tree = self.parse_body()

        if tree is None:
            return None

        code = int(tree.find("Code").text)
        message = tree.find("Message").text
        raise AtmosError(code=code, message=message, driver=self.connection.driver)


class AtmosConnection(ConnectionUserAndKey):
    responseCls = AtmosResponse

    def add_default_headers(self, headers):
        headers["x-emc-uid"] = self.user_id
        headers["Date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        headers["x-emc-date"] = headers["Date"]

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/octet-stream"

        if "Accept" not in headers:
            headers["Accept"] = "*/*"

        return headers

    def pre_connect_hook(self, params, headers):
        headers["x-emc-signature"] = self._calculate_signature(params, headers)

        return params, headers

    def _calculate_signature(self, params, headers):
        pathstring = urlunquote(self.action)
        driver_path = self.driver.path  # pylint: disable=no-member

        if pathstring.startswith(driver_path):
            pathstring = pathstring[len(driver_path) :]

        if params:
            if type(params) is dict:
                params = list(params.items())
            pathstring += "?" + urlencode(params)
        pathstring = pathstring.lower()

        xhdrs = [(k, v) for k, v in list(headers.items()) if k.startswith("x-emc-")]
        xhdrs.sort(key=lambda x: x[0])

        signature = [
            self.method,
            headers.get("Content-Type", ""),
            headers.get("Range", ""),
            headers.get("Date", ""),
            pathstring,
        ]
        signature.extend([k + ":" + collapse(v) for k, v in xhdrs])
        signature = "\n".join(signature)
        key = base64.b64decode(self.key)
        signature = hmac.new(b(key), b(signature), hashlib.sha1).digest()

        return base64.b64encode(b(signature)).decode("utf-8")


class AtmosDriver(StorageDriver):
    connectionCls = AtmosConnection

    host = None  # type: str
    path = None  # type: str
    api_name = "atmos"
    supports_chunked_encoding = True
    website = "http://atmosonline.com/"
    name = "atmos"

    DEFAULT_CDN_TTL = 60 * 60 * 24 * 7  # 1 week

    def __init__(self, key, secret=None, secure=True, host=None, port=None):
        host = host or self.host
        super().__init__(key, secret, secure, host, port)

    def iterate_containers(self):
        result = self.connection.request(self._namespace_path(""))
        entries = self._list_objects(result.object, object_type="directory")

        for entry in entries:
            extra = {"object_id": entry["id"]}
            yield Container(entry["name"], extra, self)

    def get_container(self, container_name):
        path = self._namespace_path(container_name) + "/?metadata/system"
        try:
            result = self.connection.request(path)
        except AtmosError as e:
            if e.code != 1003:
                raise
            raise ContainerDoesNotExistError(e, self, container_name)
        meta = self._emc_meta(result)
        extra = {"object_id": meta["objectid"]}

        return Container(container_name, extra, self)

    def create_container(self, container_name):
        path = self._namespace_path(container_name) + "/"
        try:
            self.connection.request(path, method="POST")
        except AtmosError as e:
            if e.code != 1016:
                raise
            raise ContainerAlreadyExistsError(e, self, container_name)

        return self.get_container(container_name)

    def delete_container(self, container):
        try:
            self.connection.request(self._namespace_path(container.name) + "/", method="DELETE")
        except AtmosError as e:
            if e.code == 1003:
                raise ContainerDoesNotExistError(e, self, container.name)
            elif e.code == 1023:
                raise ContainerIsNotEmptyError(e, self, container.name)

        return True

    def get_object(self, container_name, object_name):
        container = self.get_container(container_name)
        object_name_cleaned = self._clean_object_name(object_name)
        path = self._namespace_path(container_name) + "/" + object_name_cleaned

        try:
            result = self.connection.request(path + "?metadata/system")
            system_meta = self._emc_meta(result)

            result = self.connection.request(path + "?metadata/user")
            user_meta = self._emc_meta(result)
        except AtmosError as e:
            if e.code != 1003:
                raise
            raise ObjectDoesNotExistError(e, self, object_name)

        last_modified = time.strptime(system_meta["mtime"], "%Y-%m-%dT%H:%M:%SZ")
        last_modified = time.strftime("%a, %d %b %Y %H:%M:%S GMT", last_modified)
        extra = {"object_id": system_meta["objectid"], "last_modified": last_modified}
        data_hash = user_meta.pop("md5", "")

        return Object(
            object_name,
            int(system_meta["size"]),
            data_hash,
            extra,
            user_meta,
            container,
            self,
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
        method = "PUT"

        extra = extra or {}
        object_name_cleaned = self._clean_object_name(object_name)
        request_path = self._namespace_path(container.name) + "/" + object_name_cleaned
        content_type = extra.get("content_type", None)

        try:
            self.connection.request(request_path + "?metadata/system")
        except AtmosError as e:
            if e.code != 1003:
                raise
            method = "POST"

        result_dict = self._upload_object(
            object_name=object_name,
            content_type=content_type,
            request_path=request_path,
            request_method=method,
            headers={},
            file_path=file_path,
        )

        bytes_transferred = result_dict["bytes_transferred"]

        if extra is None:
            meta_data = {}
        else:
            meta_data = extra.get("meta_data", {})
        meta_data["md5"] = result_dict["data_hash"]
        user_meta = ", ".join([k + "=" + str(v) for k, v in list(meta_data.items())])
        self.connection.request(
            request_path + "?metadata/user",
            method="POST",
            headers={"x-emc-meta": user_meta},
        )
        result = self.connection.request(request_path + "?metadata/system")
        meta = self._emc_meta(result)
        del meta_data["md5"]
        extra = {
            "object_id": meta["objectid"],
            "meta_data": meta_data,
        }

        return Object(
            object_name,
            bytes_transferred,
            result_dict["data_hash"],
            extra,
            meta_data,
            container,
            self,
        )

    def upload_object_via_stream(self, iterator, container, object_name, extra=None, headers=None):
        if isinstance(iterator, file):
            iterator = iter(iterator)

        extra_headers = headers or {}
        data_hash = hashlib.md5()  # nosec
        generator = read_in_chunks(iterator, CHUNK_SIZE, True)
        bytes_transferred = 0
        try:
            chunk = next(generator)
        except StopIteration:
            chunk = ""

        path = self._namespace_path(container.name + "/" + object_name)
        method = "PUT"

        if extra is not None:
            content_type = extra.get("content_type", None)
        else:
            content_type = None

        content_type = self._determine_content_type(content_type, object_name)

        try:
            self.connection.request(path + "?metadata/system")
        except AtmosError as e:
            if e.code != 1003:
                raise
            method = "POST"

        while True:
            end = bytes_transferred + len(chunk) - 1
            data_hash.update(b(chunk))
            headers = dict(extra_headers)

            headers.update(
                {
                    "x-emc-meta": "md5=" + data_hash.hexdigest(),  # nosec
                    "Content-Type": content_type,
                }
            )

            if len(chunk) > 0 and bytes_transferred > 0:
                headers["Range"] = "Bytes=%d-%d" % (bytes_transferred, end)
                method = "PUT"

            result = self.connection.request(path, method=method, data=chunk, headers=headers)
            bytes_transferred += len(chunk)

            try:
                chunk = next(generator)
            except StopIteration:
                break

            if len(chunk) == 0:
                break

        data_hash = data_hash.hexdigest()  # nosec

        if extra is None:
            meta_data = {}
        else:
            meta_data = extra.get("meta_data", {})
        meta_data["md5"] = data_hash
        user_meta = ", ".join([k + "=" + str(v) for k, v in list(meta_data.items())])
        self.connection.request(
            path + "?metadata/user", method="POST", headers={"x-emc-meta": user_meta}
        )

        result = self.connection.request(path + "?metadata/system")

        meta = self._emc_meta(result)
        extra = {
            "object_id": meta["objectid"],
            "meta_data": meta_data,
        }

        return Object(object_name, bytes_transferred, data_hash, extra, meta_data, container, self)

    def download_object(
        self, obj, destination_path, overwrite_existing=False, delete_on_failure=True
    ):
        path = self._namespace_path(obj.container.name + "/" + obj.name)
        response = self.connection.request(path, method="GET", raw=True)

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
        path = self._namespace_path(obj.container.name + "/" + obj.name)
        response = self.connection.request(path, method="GET", raw=True)

        return self._get_object(
            obj=obj,
            callback=read_in_chunks,
            response=response,
            callback_kwargs={"iterator": response.response, "chunk_size": chunk_size},
            success_status_code=httplib.OK,
        )

    def delete_object(self, obj):
        path = self._namespace_path(obj.container.name) + "/" + self._clean_object_name(obj.name)
        try:
            self.connection.request(path, method="DELETE")
        except AtmosError as e:
            if e.code != 1003:
                raise
            raise ObjectDoesNotExistError(e, self, obj.name)

        return True

    def enable_object_cdn(self, obj):
        return True

    def get_object_cdn_url(self, obj, expiry=None, use_object=False):
        """
        Return an object CDN URL.

        :param obj: Object instance
        :type  obj: :class:`Object`

        :param expiry: Expiry
        :type expiry: ``str``

        :param use_object: Use object
        :type use_object: ``bool``

        :rtype: ``str``
        """

        if use_object:
            path = "/rest/objects" + obj.meta_data["object_id"]
        else:
            path = "/rest/namespace/" + obj.container.name + "/" + obj.name

        if self.secure:
            protocol = "https"
        else:
            protocol = "http"

        expiry = str(expiry or int(time.time()) + self.DEFAULT_CDN_TTL)
        params = [
            ("uid", self.key),
            ("expires", expiry),
        ]
        params.append(("signature", self._cdn_signature(path, params, expiry)))

        params = urlencode(params)
        path = self.path + path

        return urlparse.urlunparse((protocol, self.host, path, "", params, ""))

    def _cdn_signature(self, path, params, expiry):
        key = base64.b64decode(self.secret)
        signature = "\n".join(["GET", path.lower(), self.key, expiry])
        signature = hmac.new(key, signature, hashlib.sha1).digest()

        return base64.b64encode(signature)

    def _list_objects(self, tree, object_type=None):
        listing = tree.find(self._emc_tag("DirectoryList"))
        entries = []

        for entry in listing.findall(self._emc_tag("DirectoryEntry")):
            file_type = entry.find(self._emc_tag("FileType")).text

            if object_type is not None and object_type != file_type:
                continue
            entries.append(
                {
                    "id": entry.find(self._emc_tag("ObjectID")).text,
                    "type": file_type,
                    "name": entry.find(self._emc_tag("Filename")).text,
                }
            )

        return entries

    def _clean_object_name(self, name):
        return urlquote(name.encode("ascii"))

    def _namespace_path(self, path):
        return self.path + "/rest/namespace/" + urlquote(path.encode("ascii"))

    def _object_path(self, object_id):
        return self.path + "/rest/objects/" + object_id.encode("ascii")

    @staticmethod
    def _emc_tag(tag):
        return "{http://www.emc.com/cos/}" + tag

    def _emc_meta(self, response):
        meta = response.headers.get("x-emc-meta", "")

        if len(meta) == 0:
            return {}
        meta = meta.split(", ")

        return dict([x.split("=", 1) for x in meta])

    def _entries_to_objects(self, container, entries):
        for entry in entries:
            metadata = {"object_id": entry["id"]}
            yield Object(entry["name"], 0, "", {}, metadata, container, self)

    def iterate_container_objects(self, container, prefix=None, ex_prefix=None):
        """
        Return a generator of objects for the given container.

        :param container: Container instance
        :type container: :class:`Container`

        :param prefix: Filter objects starting with a prefix.
                       Filtering is performed client-side.
        :type  prefix: ``str``

        :param ex_prefix: (Deprecated.) Filter objects starting with a prefix.
                          Filtering is performed client-side.
        :type  ex_prefix: ``str``

        :return: A generator of Object instances.
        :rtype: ``generator`` of :class:`Object`
        """
        prefix = self._normalize_prefix_argument(prefix, ex_prefix)

        headers = {"x-emc-include-meta": "1"}
        path = self._namespace_path(container.name) + "/"
        result = self.connection.request(path, headers=headers)
        entries = self._list_objects(result.object, object_type="regular")
        objects = self._entries_to_objects(container, entries)

        return self._filter_listed_container_objects(objects, prefix)
