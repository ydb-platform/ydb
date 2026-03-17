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


import os
import hmac
import base64
import hashlib
import binascii
from datetime import datetime, timedelta

from libcloud.utils.py3 import ET, b, httplib, tostring, urlquote, urlencode
from libcloud.utils.xml import fixxpath
from libcloud.utils.files import read_in_chunks
from libcloud.common.azure import AzureConnection, AzureActiveDirectoryConnection
from libcloud.common.types import LibcloudError
from libcloud.storage.base import Object, Container, StorageDriver
from libcloud.storage.types import (
    ObjectDoesNotExistError,
    ObjectHashMismatchError,
    ContainerIsNotEmptyError,
    InvalidContainerNameError,
    ContainerDoesNotExistError,
    ContainerAlreadyExistsError,
)

# Desired number of items in each response inside a paginated request
RESPONSES_PER_REQUEST = 100

# According to the Azure Docs:
# > The block must be less than or equal to 100 MB in size for version
# > 2016-05-31 and later (4 MB for older versions).
# However for performance reasons, using a lower upload chunk size
# usually leads to fewer dropped requests and retries.
AZURE_UPLOAD_CHUNK_SIZE = int(os.getenv("LIBCLOUD_AZURE_UPLOAD_CHUNK_SIZE_MB", "4")) * 1024 * 1024

AZURE_DOWNLOAD_CHUNK_SIZE = (
    int(os.getenv("LIBCLOUD_AZURE_DOWNLOAD_CHUNK_SIZE_MB", "4")) * 1024 * 1024
)

# The time period (in seconds) for which a lease must be obtained.
# If set as -1, we get an infinite lease, but that is a bad idea. If
# after getting an infinite lease, there was an issue in releasing the
# lease, the object will remain 'locked' forever, unless the lease is
# released using the lease_id (which is not exposed to the user)
AZURE_LEASE_PERIOD = int(os.getenv("LIBCLOUD_AZURE_LEASE_PERIOD_SECONDS", "60"))

AZURE_STORAGE_HOST_SUFFIX = "blob.core.windows.net"
AZURE_STORAGE_HOST_SUFFIX_CHINA = "blob.core.chinacloudapi.cn"
AZURE_STORAGE_HOST_SUFFIX_GOVERNMENT = "blob.core.usgovcloudapi.net"
AZURE_STORAGE_HOST_SUFFIX_PRIVATELINK = "privatelink.blob.core.windows.net"

AZURE_STORAGE_CDN_URL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

AZURE_STORAGE_CDN_URL_START_MINUTES = float(
    os.getenv("LIBCLOUD_AZURE_STORAGE_CDN_URL_START_MINUTES", "5")
)

AZURE_STORAGE_CDN_URL_EXPIRY_HOURS = float(
    os.getenv("LIBCLOUD_AZURE_STORAGE_CDN_URL_EXPIRY_HOURS", "24")
)


class AuthType:
    AZURE_AD = "azureAD"


class AzureBlobLease:
    """
    A class to help in leasing an azure blob and renewing the lease
    """

    def __init__(self, driver, object_path, use_lease):
        """
        :param driver: The Azure storage driver that is being used
        :type driver: :class:`AzureStorageDriver`

        :param object_path: The path of the object we need to lease
        :type object_path: ``str``

        :param use_lease: Indicates if we must take a lease or not
        :type use_lease: ``bool``
        """
        self.object_path = object_path
        self.driver = driver
        self.use_lease = use_lease
        self.lease_id = None
        self.params = {"comp": "lease"}

    def renew(self):
        """
        Renew the lease if it is older than a predefined time period
        """
        if self.lease_id is None:
            return

        headers = {
            "x-ms-lease-action": "renew",
            "x-ms-lease-id": self.lease_id,
            "x-ms-lease-duration": "60",
        }

        response = self.driver.connection.request(
            self.object_path, headers=headers, params=self.params, method="PUT"
        )

        if response.status != httplib.OK:
            raise LibcloudError("Unable to obtain lease", driver=self)

    def update_headers(self, headers):
        """
        Update the lease id in the headers
        """
        if self.lease_id:
            headers["x-ms-lease-id"] = self.lease_id

    def __enter__(self):
        if not self.use_lease:
            return self

        headers = {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "60"}

        response = self.driver.connection.request(
            self.object_path, headers=headers, params=self.params, method="PUT"
        )

        if response.status == httplib.NOT_FOUND:
            return self
        elif response.status != httplib.CREATED:
            raise LibcloudError("Unable to obtain lease", driver=self)

        self.lease_id = response.headers["x-ms-lease-id"]
        return self

    def __exit__(self, type, value, traceback):
        if self.lease_id is None:
            return

        headers = {"x-ms-lease-action": "release", "x-ms-lease-id": self.lease_id}
        response = self.driver.connection.request(
            self.object_path, headers=headers, params=self.params, method="PUT"
        )

        if response.status != httplib.OK:
            raise LibcloudError("Unable to release lease", driver=self)


class AzureBlobsConnection(AzureConnection):
    """
    Represents a single connection to Azure Blobs.

    The main Azure Blob Storage service uses a prefix in the hostname to
    distinguish between accounts, e.g. ``theaccount.blob.core.windows.net``.
    However, some custom deployments of the service, such as the Azurite
    emulator, instead use a URL prefix such as ``/theaccount``. To support
    these deployments, the parameter ``account_prefix`` must be set on the
    connection. This is done by instantiating the driver with arguments such
    as ``host='somewhere.tld'`` and ``key='theaccount'``. To specify a custom
    host without an account prefix, e.g. to connect to Azure Government or
    Azure China, the driver can be instantiated with the appropriate storage
    endpoint suffix, e.g. ``host='blob.core.usgovcloudapi.net'`` and
    ``key='theaccount'``.

    :param account_prefix: Optional prefix identifying the storage account.
                           Used when connecting to a custom deployment of the
                           storage service like Azurite or IoT Edge Storage.
    :type account_prefix: ``str``
    """

    def __init__(self, *args, **kwargs):
        self.account_prefix = kwargs.pop("account_prefix", None)
        super().__init__(*args, **kwargs)

    def morph_action_hook(self, action):
        action = super().morph_action_hook(action)

        if self.account_prefix is not None:
            action = "/{}{}".format(self.account_prefix, action)

        return action

    API_VERSION = "2018-11-09"


class AzureBlobsActiveDirectoryConnection(AzureActiveDirectoryConnection):
    """
    Represents a single connection to Azure Blobs.

    The main Azure Blob Storage service uses a prefix in the hostname to
    distinguish between accounts, e.g. ``theaccount.blob.core.windows.net``.
    However, some custom deployments of the service, such as the Azurite
    emulator, instead use a URL prefix such as ``/theaccount``. To support
    these deployments, the parameter ``account_prefix`` must be set on the
    connection. This is done by instantiating the driver with arguments such
    as ``host='somewhere.tld'`` and ``key='theaccount'``. To specify a custom
    host without an account prefix, e.g. to connect to Azure Government or
    Azure China, the driver can be instantiated with the appropriate storage
    endpoint suffix, e.g. ``host='blob.core.usgovcloudapi.net'`` and
    ``key='theaccount'``.

    This connection is similar to AzureBlobsConnection, but uses Azure Active
    Directory to authenticate

    :param account_prefix: Optional prefix identifying the storage account.
                           Used when connecting to a custom deployment of the
                           storage service like Azurite or IoT Edge Storage.
    :type account_prefix: ``str``
    """

    def __init__(self, *args, **kwargs):
        self.account_prefix = kwargs.pop("account_prefix", None)
        super().__init__(*args, **kwargs)

    def morph_action_hook(self, action):
        action = super().morph_action_hook(action)

        if self.account_prefix is not None:
            action = "/{}{}".format(self.account_prefix, action)

        return action

    API_VERSION = "2018-11-09"


class AzureBlobsStorageDriver(StorageDriver):
    name = "Microsoft Azure (blobs)"
    website = "http://windows.azure.com/"
    connectionCls = AzureBlobsConnection
    hash_type = "md5"
    supports_chunked_encoding = False

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        tenant_id=None,
        identity=None,
        auth_type=None,
        cloud_environment="default",
        **kwargs,
    ):
        self._host = host
        self._tenant_id = tenant_id
        self._identity = identity
        self._cloud_environment = cloud_environment
        self._auth_type = auth_type

        if self._auth_type == "azureAd":
            self.connectionCls = AzureBlobsActiveDirectoryConnection
        else:
            # B64decode() this key and keep it, so that we don't have to do
            # so for every request. Minor performance improvement
            secret = base64.b64decode(b(secret))

        super().__init__(key=key, secret=secret, secure=secure, host=host, port=port, **kwargs)

    def _ex_connection_class_kwargs(self):
        kwargs = {}
        # add tenant_id and identity if using azureAd auth
        if self._auth_type == "azureAd":
            kwargs["tenant_id"] = self._tenant_id
            kwargs["identity"] = self._identity
            kwargs["cloud_environment"] = self._cloud_environment

        # if the user didn't provide a custom host value, assume we're
        # targeting the default Azure Storage endpoints
        if self._host is None:
            kwargs["host"] = "{}.{}".format(self.key, AZURE_STORAGE_HOST_SUFFIX)
            return kwargs

        # connecting to a special storage region like Azure Government or
        # Azure China requires setting a custom storage endpoint but we
        # still use the same scheme to identify a specific account as for
        # the standard storage endpoint
        try:
            host_suffix = next(
                host_suffix
                for host_suffix in (
                    AZURE_STORAGE_HOST_SUFFIX_CHINA,
                    AZURE_STORAGE_HOST_SUFFIX_GOVERNMENT,
                    AZURE_STORAGE_HOST_SUFFIX_PRIVATELINK,
                )
                if self._host.endswith(host_suffix)
            )
        except StopIteration:
            pass
        else:
            kwargs["host"] = "{}.{}".format(self.key, host_suffix)
            return kwargs

        # if the host isn't targeting one of the special storage regions, it
        # must be pointing to Azurite or IoT Edge Storage so switch to prefix
        # identification
        kwargs["account_prefix"] = self.key

        return kwargs

    def _xml_to_container(self, node):
        """
        Converts a container XML node to a container instance

        :param node: XML info of the container
        :type node: :class:`xml.etree.ElementTree.Element`

        :return: A container instance
        :rtype: :class:`Container`
        """

        name = node.findtext(fixxpath(xpath="Name"))
        props = node.find(fixxpath(xpath="Properties"))
        metadata = node.find(fixxpath(xpath="Metadata"))

        extra = {
            "url": node.findtext(fixxpath(xpath="Url")),
            "last_modified": node.findtext(fixxpath(xpath="Last-Modified")),
            "etag": props.findtext(fixxpath(xpath="Etag")),
            "lease": {
                "status": props.findtext(fixxpath(xpath="LeaseStatus")),
                "state": props.findtext(fixxpath(xpath="LeaseState")),
                "duration": props.findtext(fixxpath(xpath="LeaseDuration")),
            },
            "meta_data": {},
        }

        if extra["etag"]:
            # Remove redundant double quotes around etag value
            # "0x8CFBAB7B5B82D8E" -> 0x8CFBAB7B5B82D8E
            extra["etag"] = extra["etag"].replace('"', "")

        if metadata is not None:
            for meta in list(metadata):
                extra["meta_data"][meta.tag] = meta.text

        return Container(name=name, extra=extra, driver=self)

    def _response_to_container(self, container_name, response):
        """
        Converts a HTTP response to a container instance

        :param container_name: Name of the container
        :type container_name: ``str``

        :param response: HTTP Response
        :type node: L{}

        :return: A container instance
        :rtype: :class:`Container`
        """

        headers = response.headers
        scheme = "https" if self.secure else "http"

        extra = {
            "url": "{}://{}{}".format(scheme, response.connection.host, response.connection.action),
            "etag": headers["etag"],
            "last_modified": headers["last-modified"],
            "lease": {
                "status": headers.get("x-ms-lease-status", None),
                "state": headers.get("x-ms-lease-state", None),
                "duration": headers.get("x-ms-lease-duration", None),
            },
            "meta_data": {},
        }

        for key, value in response.headers.items():
            if key.startswith("x-ms-meta-"):
                key = key.split("x-ms-meta-")[1]
                extra["meta_data"][key] = value

        return Container(name=container_name, extra=extra, driver=self)

    def _xml_to_object(self, container, blob):
        """
        Converts a BLOB XML node to an object instance

        :param container: Instance of the container holding the blob
        :type: :class:`Container`

        :param blob: XML info of the blob
        :type blob: L{}

        :return: An object instance
        :rtype: :class:`Object`
        """

        name = blob.findtext(fixxpath(xpath="Name"))
        props = blob.find(fixxpath(xpath="Properties"))
        metadata = blob.find(fixxpath(xpath="Metadata"))
        etag = props.findtext(fixxpath(xpath="Etag"))
        size = int(props.findtext(fixxpath(xpath="Content-Length")))

        extra = {
            "content_type": props.findtext(fixxpath(xpath="Content-Type")),
            "etag": etag,
            "md5_hash": props.findtext(fixxpath(xpath="Content-MD5")),
            "last_modified": props.findtext(fixxpath(xpath="Last-Modified")),
            "url": blob.findtext(fixxpath(xpath="Url")),
            "hash": props.findtext(fixxpath(xpath="Etag")),
            "lease": {
                "status": props.findtext(fixxpath(xpath="LeaseStatus")),
                "state": props.findtext(fixxpath(xpath="LeaseState")),
                "duration": props.findtext(fixxpath(xpath="LeaseDuration")),
            },
            "content_encoding": props.findtext(fixxpath(xpath="Content-Encoding")),
            "content_language": props.findtext(fixxpath(xpath="Content-Language")),
            "blob_type": props.findtext(fixxpath(xpath="BlobType")),
        }

        if extra["md5_hash"]:
            value = binascii.hexlify(base64.b64decode(b(extra["md5_hash"])))
            value = value.decode("ascii")
            extra["md5_hash"] = value

        meta_data = {}
        if metadata is not None:
            for meta in list(metadata):
                meta_data[meta.tag] = meta.text

        return Object(
            name=name,
            size=size,
            hash=etag,
            meta_data=meta_data,
            extra=extra,
            container=container,
            driver=self,
        )

    def _response_to_object(self, object_name, container, response):
        """
        Converts a HTTP response to an object (from headers)

        :param object_name: Name of the object
        :type object_name: ``str``

        :param container: Instance of the container holding the blob
        :type: :class:`Container`

        :param response: HTTP Response
        :type node: L{}

        :return: An object instance
        :rtype: :class:`Object`
        """

        headers = response.headers
        size = int(headers["content-length"])
        etag = headers["etag"]
        scheme = "https" if self.secure else "http"

        extra = {
            "url": "{}://{}{}".format(scheme, response.connection.host, response.connection.action),
            "etag": etag,
            "md5_hash": headers.get("content-md5", None),
            "content_type": headers.get("content-type", None),
            "content_language": headers.get("content-language", None),
            "content_encoding": headers.get("content-encoding", None),
            "last_modified": headers["last-modified"],
            "lease": {
                "status": headers.get("x-ms-lease-status", None),
                "state": headers.get("x-ms-lease-state", None),
                "duration": headers.get("x-ms-lease-duration", None),
            },
            "blob_type": headers["x-ms-blob-type"],
        }

        if extra["md5_hash"]:
            value = binascii.hexlify(base64.b64decode(b(extra["md5_hash"])))
            value = value.decode("ascii")
            extra["md5_hash"] = value

        meta_data = {}
        for key, value in response.headers.items():
            if key.startswith("x-ms-meta-"):
                key = key.split("x-ms-meta-")[1]
                meta_data[key] = value

        return Object(
            name=object_name,
            size=size,
            hash=etag,
            extra=extra,
            meta_data=meta_data,
            container=container,
            driver=self,
        )

    def iterate_containers(self):
        """
        @inherits: :class:`StorageDriver.iterate_containers`
        """
        params = {
            "comp": "list",
            "maxresults": RESPONSES_PER_REQUEST,
            "include": "metadata",
        }

        while True:
            response = self.connection.request("/", params)
            if response.status != httplib.OK:
                raise LibcloudError("Unexpected status code: %s" % (response.status), driver=self)

            body = response.parse_body()
            containers = body.find(fixxpath(xpath="Containers"))
            containers = containers.findall(fixxpath(xpath="Container"))

            for container in containers:
                yield self._xml_to_container(container)

            params["marker"] = body.findtext("NextMarker")
            if not params["marker"]:
                break

    def iterate_container_objects(self, container, prefix=None, ex_prefix=None):
        """
        @inherits: :class:`StorageDriver.iterate_container_objects`
        """
        prefix = self._normalize_prefix_argument(prefix, ex_prefix)

        params = {
            "restype": "container",
            "comp": "list",
            "maxresults": RESPONSES_PER_REQUEST,
            "include": "metadata",
        }

        if prefix:
            params["prefix"] = prefix

        container_path = self._get_container_path(container)

        while True:
            response = self.connection.request(container_path, params=params)

            if response.status == httplib.NOT_FOUND:
                raise ContainerDoesNotExistError(
                    value=None, driver=self, container_name=container.name
                )

            elif response.status != httplib.OK:
                raise LibcloudError("Unexpected status code: %s" % (response.status), driver=self)

            body = response.parse_body()
            blobs = body.find(fixxpath(xpath="Blobs"))
            blobs = blobs.findall(fixxpath(xpath="Blob"))

            for blob in blobs:
                yield self._xml_to_object(container, blob)

            params["marker"] = body.findtext("NextMarker")
            if not params["marker"]:
                break

    def get_container(self, container_name):
        """
        @inherits: :class:`StorageDriver.get_container`
        """
        params = {"restype": "container"}

        container_path = "/%s" % (container_name)

        response = self.connection.request(container_path, params=params, method="HEAD")

        if response.status == httplib.NOT_FOUND:
            raise ContainerDoesNotExistError(
                "Container %s does not exist" % (container_name),
                driver=self,
                container_name=container_name,
            )
        elif response.status != httplib.OK:
            raise LibcloudError("Unexpected status code: %s" % (response.status), driver=self)

        return self._response_to_container(container_name, response)

    def get_object(self, container_name, object_name):
        """
        @inherits: :class:`StorageDriver.get_object`
        """

        container = self.get_container(container_name=container_name)
        object_path = self._get_object_path(container, object_name)

        response = self.connection.request(object_path, method="HEAD")

        if response.status == httplib.OK:
            obj = self._response_to_object(object_name, container, response)
            return obj

        raise ObjectDoesNotExistError(value=None, driver=self, object_name=object_name)

    def get_object_cdn_url(self, obj, ex_expiry=AZURE_STORAGE_CDN_URL_EXPIRY_HOURS):
        """
        Return a SAS URL that enables reading the given object.

        :param obj: Object instance.
        :type  obj: :class:`Object`

        :param ex_expiry: The number of hours after which the URL expires.
                          Defaults to 24 hours.
        :type  ex_expiry: ``float``

        :return: A SAS URL for the object.
        :rtype: ``str``
        """
        object_path = self._get_object_path(obj.container, obj.name)

        now = datetime.utcnow()
        start = now - timedelta(minutes=AZURE_STORAGE_CDN_URL_START_MINUTES)
        expiry = now + timedelta(hours=ex_expiry)

        params = {
            "st": start.strftime(AZURE_STORAGE_CDN_URL_DATE_FORMAT),
            "se": expiry.strftime(AZURE_STORAGE_CDN_URL_DATE_FORMAT),
            "sp": "r",
            "spr": "https" if self.secure else "http,https",
            "sv": self.connectionCls.API_VERSION,
            "sr": "b",
        }

        string_to_sign = "\n".join(
            (
                params["sp"],
                params["st"],
                params["se"],
                "/blob/{}{}".format(self.key, object_path),
                "",  # signedIdentifier
                "",  # signedIP
                params["spr"],
                params["sv"],
                params["sr"],
                "",  # snapshot
                "",  # rscc
                "",  # rscd
                "",  # rsce
                "",  # rscl
                "",  # rsct
            )
        )

        params["sig"] = base64.b64encode(
            hmac.new(self.secret, string_to_sign.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")

        return "{scheme}://{host}:{port}{action}?{sas_token}".format(
            scheme="https" if self.secure else "http",
            host=self.connection.host,
            port=self.connection.port,
            action=self.connection.morph_action_hook(object_path),
            sas_token=urlencode(params),
        )

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
        Return an object's CDN path.

        :param container: Container instance
        :type  container: :class:`Container`

        :param object_name: Object name
        :type  object_name: :class:`str`

        :return: A  path for this object.
        :rtype: ``str``
        """
        container_url = self._get_container_path(container)
        object_name_cleaned = urlquote(object_name)
        object_path = "{}/{}".format(container_url, object_name_cleaned)
        return object_path

    def create_container(self, container_name):
        """
        @inherits: :class:`StorageDriver.create_container`
        """
        params = {"restype": "container"}

        container_path = "/%s" % (container_name)
        response = self.connection.request(container_path, params=params, method="PUT")

        if response.status == httplib.CREATED:
            return self._response_to_container(container_name, response)
        elif response.status == httplib.CONFLICT:
            raise ContainerAlreadyExistsError(
                value="Container with this name already exists. The name must "
                "be unique among all the containers in the system",
                container_name=container_name,
                driver=self,
            )
        elif response.status == httplib.BAD_REQUEST:
            raise InvalidContainerNameError(
                value="Container name contains " + "invalid characters.",
                container_name=container_name,
                driver=self,
            )

        raise LibcloudError("Unexpected status code: %s" % (response.status), driver=self)

    def delete_container(self, container):
        """
        @inherits: :class:`StorageDriver.delete_container`
        """
        # Azure does not check if the container is empty. So, we will do
        # a check to ensure that the behaviour is similar to other drivers
        for obj in container.iterate_objects():
            raise ContainerIsNotEmptyError(
                value="Container must be empty before it can be deleted.",
                container_name=container.name,
                driver=self,
            )

        params = {"restype": "container"}
        container_path = self._get_container_path(container)

        # Note: All the objects in the container must be deleted first
        response = self.connection.request(container_path, params=params, method="DELETE")

        if response.status == httplib.ACCEPTED:
            return True
        elif response.status == httplib.NOT_FOUND:
            raise ContainerDoesNotExistError(value=None, driver=self, container_name=container.name)

        return False

    def download_object(
        self, obj, destination_path, overwrite_existing=False, delete_on_failure=True
    ):
        """
        @inherits: :class:`StorageDriver.download_object`
        """
        obj_path = self._get_object_path(obj.container, obj.name)
        response = self.connection.request(obj_path, raw=True, data=None)

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
        """
        @inherits: :class:`StorageDriver.download_object_as_stream`
        """
        obj_path = self._get_object_path(obj.container, obj.name)
        response = self.connection.request(obj_path, method="GET", stream=True, raw=True)
        iterator = response.iter_content(AZURE_DOWNLOAD_CHUNK_SIZE)

        return self._get_object(
            obj=obj,
            callback=read_in_chunks,
            response=response,
            callback_kwargs={"iterator": iterator, "chunk_size": chunk_size},
            success_status_code=httplib.OK,
        )

    def download_object_range(
        self,
        obj,
        destination_path,
        start_bytes,
        end_bytes=None,
        overwrite_existing=False,
        delete_on_failure=True,
    ):
        self._validate_start_and_end_bytes(start_bytes=start_bytes, end_bytes=end_bytes)

        obj_path = self._get_object_path(obj.container, obj.name)
        headers = {"x-ms-range": self._get_standard_range_str(start_bytes, end_bytes)}
        response = self.connection.request(obj_path, headers=headers, raw=True, data=None)

        # NOTE: Some Azure Blobs implementation return 200 instead of 206
        # status code, see
        # https://github.com/c-w/libcloud-tests/pull/2#issuecomment-592765323
        # for details.
        success_status_codes = [httplib.OK, httplib.PARTIAL_CONTENT]

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
                "partial_download": True,
            },
            success_status_code=success_status_codes,
        )

    def download_object_range_as_stream(self, obj, start_bytes, end_bytes=None, chunk_size=None):
        self._validate_start_and_end_bytes(start_bytes=start_bytes, end_bytes=end_bytes)

        obj_path = self._get_object_path(obj.container, obj.name)

        headers = {"x-ms-range": self._get_standard_range_str(start_bytes, end_bytes)}
        response = self.connection.request(
            obj_path, method="GET", headers=headers, stream=True, raw=True
        )
        iterator = response.iter_content(AZURE_DOWNLOAD_CHUNK_SIZE)
        success_status_codes = [httplib.OK, httplib.PARTIAL_CONTENT]

        return self._get_object(
            obj=obj,
            callback=read_in_chunks,
            response=response,
            callback_kwargs={"iterator": iterator, "chunk_size": chunk_size},
            success_status_code=success_status_codes,
        )

    def _upload_in_chunks(
        self,
        stream,
        object_path,
        lease,
        meta_data,
        content_type,
        object_name,
        file_path,
        verify_hash,
        headers,
    ):
        """
        Uploads data from an iterator in fixed sized chunks to Azure Storage
        """

        data_hash = None
        if verify_hash:
            data_hash = self._get_hash_function()

        bytes_transferred = 0
        count = 1
        chunks = []
        headers = headers or {}

        lease.update_headers(headers)

        params = {"comp": "block"}

        # Read the input data in chunk sizes suitable for Azure
        for data in read_in_chunks(stream, AZURE_UPLOAD_CHUNK_SIZE, fill_size=True):
            data = b(data)
            content_length = len(data)
            bytes_transferred += content_length

            if verify_hash:
                data_hash.update(data)

            chunk_hash = self._get_hash_function()
            chunk_hash.update(data)
            chunk_hash = base64.b64encode(b(chunk_hash.digest()))

            headers["Content-MD5"] = chunk_hash.decode("utf-8")
            headers["Content-Length"] = str(content_length)

            # Block id can be any unique string that is base64 encoded
            # A 10 digit number can hold the max value of 50000 blocks
            # that are allowed for azure
            block_id = base64.b64encode(b("%10d" % (count)))
            block_id = block_id.decode("utf-8")
            params["blockid"] = block_id

            # Keep this data for a later commit
            chunks.append(block_id)

            # Renew lease before updating
            lease.renew()

            resp = self.connection.request(
                object_path, method="PUT", data=data, headers=headers, params=params
            )

            if resp.status != httplib.CREATED:
                resp.parse_error()
                raise LibcloudError(
                    "Error uploading chunk %d. Code: %d" % (count, resp.status),
                    driver=self,
                )

            count += 1

        if verify_hash:
            data_hash = base64.b64encode(b(data_hash.digest()))
            data_hash = data_hash.decode("utf-8")

        response = self._commit_blocks(
            object_path=object_path,
            chunks=chunks,
            lease=lease,
            headers=headers,
            meta_data=meta_data,
            content_type=content_type,
            data_hash=data_hash,
            object_name=object_name,
            file_path=file_path,
        )

        # According to the Azure docs:
        # > This header refers to the content of the request, meaning, in this
        # > case, the list of blocks, and not the content of the blob itself.
        # However, the validation code assumes that the content-md5 in the
        # server response refers to the object so we must discard the value
        response.headers["content-md5"] = None

        return {
            "response": response,
            "data_hash": data_hash,
            "bytes_transferred": bytes_transferred,
        }

    def _commit_blocks(
        self,
        object_path,
        chunks,
        lease,
        headers,
        meta_data,
        content_type,
        data_hash,
        object_name,
        file_path,
    ):
        """
        Makes a final commit of the data.
        """

        root = ET.Element("BlockList")

        for block_id in chunks:
            part = ET.SubElement(root, "Uncommitted")
            part.text = str(block_id)

        data = tostring(root)
        params = {"comp": "blocklist"}
        headers = headers or {}

        lease.update_headers(headers)
        lease.renew()

        headers["x-ms-blob-content-type"] = self._determine_content_type(
            content_type, object_name, file_path
        )

        if data_hash is not None:
            headers["x-ms-blob-content-md5"] = data_hash

        self._update_metadata(headers, meta_data)

        data_hash = self._get_hash_function()
        data_hash.update(data.encode("utf-8"))
        data_hash = base64.b64encode(b(data_hash.digest()))
        headers["Content-MD5"] = data_hash.decode("utf-8")

        headers["Content-Length"] = len(data)

        headers = self._fix_headers(headers)

        response = self.connection.request(
            object_path, data=data, params=params, headers=headers, method="PUT"
        )

        if response.status != httplib.CREATED:
            raise LibcloudError("Error in blocklist commit", driver=self)

        return response

    def upload_object(
        self,
        file_path,
        container,
        object_name,
        verify_hash=True,
        extra=None,
        headers=None,
        ex_use_lease=False,
        **deprecated_kwargs,
    ):
        """
        Upload an object currently located on a disk.

        @inherits: :class:`StorageDriver.upload_object`

        :param ex_use_lease: Indicates if we must take a lease before upload
        :type ex_use_lease: ``bool``
        """
        if deprecated_kwargs:
            raise ValueError(
                "Support for arguments was removed: %s" % ", ".join(deprecated_kwargs.keys())
            )

        blob_size = os.stat(file_path).st_size

        with open(file_path, "rb") as fobj:
            return self._put_object(
                container=container,
                object_name=object_name,
                extra=extra,
                verify_hash=verify_hash,
                use_lease=ex_use_lease,
                headers=headers,
                blob_size=blob_size,
                file_path=file_path,
                stream=fobj,
            )

    def upload_object_via_stream(
        self,
        iterator,
        container,
        object_name,
        verify_hash=True,
        extra=None,
        headers=None,
        ex_use_lease=False,
        **deprecated_kwargs,
    ):
        """
        @inherits: :class:`StorageDriver.upload_object_via_stream`

        :param ex_use_lease: Indicates if we must take a lease before upload
        :type ex_use_lease: ``bool``
        """
        if deprecated_kwargs:
            raise ValueError(
                "Support for arguments was removed: %s" % ", ".join(deprecated_kwargs.keys())
            )

        return self._put_object(
            container=container,
            object_name=object_name,
            extra=extra,
            verify_hash=verify_hash,
            use_lease=ex_use_lease,
            headers=headers,
            blob_size=None,
            stream=iterator,
        )

    def delete_object(self, obj):
        """
        @inherits: :class:`StorageDriver.delete_object`
        """
        object_path = self._get_object_path(obj.container, obj.name)
        response = self.connection.request(object_path, method="DELETE")

        if response.status == httplib.ACCEPTED:
            return True
        elif response.status == httplib.NOT_FOUND:
            raise ObjectDoesNotExistError(value=None, driver=self, object_name=obj.name)

        return False

    def _fix_headers(self, headers):
        """
        Update common HTTP headers to their equivalent in Azure Storage

        :param headers: The headers dictionary to be updated
        :type headers: ``dict``
        """
        to_fix = (
            "cache-control",
            "content-encoding",
            "content-language",
        )

        fixed = {}

        for key, value in headers.items():
            key_lower = key.lower()

            if key_lower in to_fix:
                fixed["x-ms-blob-%s" % key_lower] = value
            else:
                fixed[key] = value

        return fixed

    def _update_metadata(self, headers, meta_data):
        """
        Update the given metadata in the headers

        :param headers: The headers dictionary to be updated
        :type headers: ``dict``

        :param meta_data: Metadata key value pairs
        :type meta_data: ``dict``
        """
        for key, value in list(meta_data.items()):
            key = "x-ms-meta-%s" % (key)
            headers[key] = value

    def _put_object(
        self,
        container,
        object_name,
        stream,
        extra=None,
        verify_hash=True,
        headers=None,
        blob_size=None,
        file_path=None,
        use_lease=False,
    ):
        """
        Control function that does the real job of uploading data to a blob
        """
        extra = extra or {}
        content_type = extra.get("content_type", None)
        meta_data = extra.get("meta_data", {})

        object_path = self._get_object_path(container, object_name)

        # Get a lease if required and do the operations
        with AzureBlobLease(self, object_path, use_lease) as lease:
            if blob_size is not None and blob_size <= AZURE_UPLOAD_CHUNK_SIZE:
                result_dict = self._upload_directly(
                    stream=stream,
                    object_path=object_path,
                    lease=lease,
                    blob_size=blob_size,
                    meta_data=meta_data,
                    headers=headers,
                    content_type=content_type,
                    object_name=object_name,
                    file_path=file_path,
                )
            else:
                result_dict = self._upload_in_chunks(
                    stream=stream,
                    object_path=object_path,
                    lease=lease,
                    meta_data=meta_data,
                    headers=headers,
                    content_type=content_type,
                    object_name=object_name,
                    file_path=file_path,
                    verify_hash=verify_hash,
                )

            response = result_dict["response"]
            bytes_transferred = result_dict["bytes_transferred"]
            data_hash = result_dict["data_hash"]
            headers = response.headers

        if response.status != httplib.CREATED:
            raise LibcloudError(
                "Unexpected status code, status_code=%s" % (response.status),
                driver=self,
            )

        server_hash = headers.get("content-md5")

        if server_hash:
            server_hash = binascii.hexlify(base64.b64decode(b(server_hash)))
            server_hash = server_hash.decode("utf-8")
        else:
            # TODO: HACK - We could poll the object for a while and get
            # the hash
            pass

        if verify_hash and server_hash and data_hash != server_hash:
            raise ObjectHashMismatchError(
                value="MD5 hash checksum does not match",
                object_name=object_name,
                driver=self,
            )

        return Object(
            name=object_name,
            size=bytes_transferred,
            hash=headers["etag"],
            extra=None,
            meta_data=meta_data,
            container=container,
            driver=self,
        )

    def _upload_directly(
        self,
        stream,
        object_path,
        lease,
        blob_size,
        meta_data,
        content_type,
        object_name,
        file_path,
        headers,
    ):
        headers = headers or {}
        lease.update_headers(headers)

        self._update_metadata(headers, meta_data)

        headers["Content-Length"] = str(blob_size)
        headers["x-ms-blob-type"] = "BlockBlob"

        return self._upload_object(
            object_name=object_name,
            file_path=file_path,
            content_type=content_type,
            request_path=object_path,
            stream=stream,
            headers=headers,
        )

    def ex_set_object_metadata(self, obj, meta_data):
        """
        Set metadata for an object

        :param obj: The blob object
        :type obj: :class:`Object`

        :param meta_data: Metadata key value pairs
        :type meta_data: ``dict``
        """
        object_path = self._get_object_path(obj.container, obj.name)
        params = {"comp": "metadata"}
        headers = {}

        self._update_metadata(headers, meta_data)

        response = self.connection.request(
            object_path, method="PUT", params=params, headers=headers
        )

        if response.status != httplib.OK:
            # pylint: disable=too-many-function-args
            response.parse_error("Setting metadata")
