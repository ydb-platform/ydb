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

import copy
import json
import email.utils
from typing import Dict, Optional

from libcloud.utils.py3 import httplib, urlquote
from libcloud.common.base import ConnectionUserAndKey
from libcloud.common.types import ProviderError
from libcloud.common.google import GoogleAuthType, GoogleResponse, GoogleOAuth2Credential
from libcloud.storage.drivers.s3 import (
    S3Response,
    S3RawResponse,
    BaseS3Connection,
    BaseS3StorageDriver,
)

# Docs are a lie. Actual namespace returned is different that the one listed
# in the docs.
SIGNATURE_IDENTIFIER = "GOOG1"
API_VERSION = "2006-03-01"
NAMESPACE = "http://doc.s3.amazonaws.com/%s" % (API_VERSION)


def _clean_object_name(name):
    """
    Return the URL encoded name. name=None returns None. Useful for input
    checking without having to check for None first.

    :param name: The object name
    :type name: ``str`` or ``None``

    :return: The url-encoded object name or None if name=None.
    :rtype ``str`` or ``None``
    """
    return urlquote(name, safe="") if name else None


class ContainerPermissions:
    values = ["NONE", "READER", "WRITER", "OWNER"]
    NONE = 0
    READER = 1
    WRITER = 2
    OWNER = 3


class ObjectPermissions:
    values = ["NONE", "READER", "OWNER"]
    NONE = 0
    READER = 1
    OWNER = 2


class GoogleStorageConnection(ConnectionUserAndKey):
    """
    Represents a single connection to the Google storage API endpoint.

    This can either authenticate via the Google OAuth2 methods or via
    the S3 HMAC interoperability method.
    """

    host = "storage.googleapis.com"
    responseCls = S3Response
    rawResponseCls = S3RawResponse
    PROJECT_ID_HEADER = "x-goog-project-id"

    def __init__(self, user_id, key, secure=True, auth_type=None, credential_file=None, **kwargs):
        self.auth_type = auth_type or GoogleAuthType.guess_type(user_id)
        if GoogleAuthType.is_oauth2(self.auth_type):
            self.oauth2_credential = GoogleOAuth2Credential(
                user_id, key, self.auth_type, credential_file, **kwargs
            )
        else:
            self.oauth2_credential = None
        super().__init__(user_id, key, secure, **kwargs)

    def add_default_headers(self, headers):
        date = email.utils.formatdate(usegmt=True)
        headers["Date"] = date
        project = self.get_project()
        if project:
            headers[self.PROJECT_ID_HEADER] = project
        return headers

    def get_project(self):
        return getattr(self.driver, "project", None)

    def pre_connect_hook(self, params, headers):
        if self.auth_type == GoogleAuthType.GCS_S3:
            signature = self._get_s3_auth_signature(params, headers)
            headers["Authorization"] = "{} {}:{}".format(
                SIGNATURE_IDENTIFIER,
                self.user_id,
                signature,
            )
        else:
            headers["Authorization"] = "Bearer " + self.oauth2_credential.access_token
        return params, headers

    def _get_s3_auth_signature(self, params, headers):
        """Hacky wrapper to work with S3's get_auth_signature."""
        headers_copy = {}
        params_copy = copy.deepcopy(params)

        # Lowercase all headers except 'date' and Google header values
        for k, v in headers.items():
            k_lower = k.lower()
            # NOTE: It's important that the value of Content-Type header is
            # left as is and not lowercased
            if (
                k_lower in ["date", "content-type"]
                or k_lower.startswith(GoogleStorageDriver.http_vendor_prefix)
                or not isinstance(v, str)
            ):
                headers_copy[k_lower] = v
            else:
                headers_copy[k_lower] = v.lower()

        return BaseS3Connection.get_auth_signature(
            method=self.method,
            headers=headers_copy,
            params=params_copy,
            expires=None,
            secret_key=self.key,
            path=self.action,
            vendor_prefix=GoogleStorageDriver.http_vendor_prefix,
        )


class GCSResponse(GoogleResponse):
    pass


class GoogleStorageJSONConnection(GoogleStorageConnection):
    """
    Represents a single connection to the Google storage JSON API endpoint.

    This can either authenticate via the Google OAuth2 methods or via
    the S3 HMAC interoperability method.
    """

    host = "www.googleapis.com"
    responseCls = GCSResponse
    rawResponseCls = None

    def add_default_headers(self, headers):
        headers = super().add_default_headers(headers)
        headers["Content-Type"] = "application/json"
        return headers


class GoogleStorageDriver(BaseS3StorageDriver):
    """
    Driver for Google Cloud Storage.

    Can authenticate via standard Google Cloud methods (Service Accounts,
    Installed App credentials, and GCE instance service accounts)

    Examples:

    Service Accounts::

        driver = GoogleStorageDriver(key=client_email, secret=private_key, ...)

    Installed Application::

        driver = GoogleStorageDriver(key=client_id, secret=client_secret, ...)

    From GCE instance::

        driver = GoogleStorageDriver(key=foo, secret=bar, ...)

    Can also authenticate via Google Cloud Storage's S3 HMAC interoperability
    API. S3 user keys are 20 alphanumeric characters, starting with GOOG.

    Example::

        driver = GoogleStorageDriver(key='GOOG0123456789ABCXYZ',
                                     secret=key_secret)
    """

    name = "Google Cloud Storage"
    website = "http://cloud.google.com/storage"
    connectionCls = GoogleStorageConnection
    jsonConnectionCls = GoogleStorageJSONConnection
    hash_type = "md5"
    namespace = NAMESPACE
    supports_chunked_encoding = False
    supports_s3_multipart_upload = False
    http_vendor_prefix = "x-goog"

    def __init__(self, key, secret=None, project=None, **kwargs):
        super().__init__(key, secret, **kwargs)
        self.project = project

        self.json_connection = GoogleStorageJSONConnection(key, secret, **kwargs)

    def _get_container_permissions(self, container_name):
        """
        Return the container permissions for the current authenticated user.

        :param container_name:  The container name.
        :param container_name: ``str``

        :return: The permissions on the container.
        :rtype: ``int`` from ContainerPermissions
        """
        # Try OWNER permissions first: try listing the bucket ACL.
        # FORBIDDEN -> exists, but not an OWNER.
        # NOT_FOUND -> bucket DONE, return NONE.
        try:
            self.json_connection.request("/storage/v1/b/%s/acl" % container_name)
            return ContainerPermissions.OWNER
        except ProviderError as e:
            if e.http_code == httplib.FORBIDDEN:
                pass
            elif e.http_code == httplib.NOT_FOUND:
                return ContainerPermissions.NONE
            else:
                raise

        # Try WRITER permissions with a noop request: try delete with an
        # impossible precondition. Authorization is checked before file
        # existence or preconditions. So, if we get a NOT_FOUND or a
        # PRECONDITION_FAILED, then we must be authorized.
        try:
            self.json_connection.request(
                "/storage/v1/b/%s/o/writecheck" % container_name,
                headers={"x-goog-if-generation-match": "0"},
                method="DELETE",
            )
        except ProviderError as e:
            if e.http_code in [httplib.NOT_FOUND, httplib.PRECONDITION_FAILED]:
                return ContainerPermissions.WRITER
            elif e.http_code != httplib.FORBIDDEN:
                raise

        # Last, try READER permissions: try getting container metadata.
        try:
            self.json_connection.request("/storage/v1/b/%s" % container_name)
            return ContainerPermissions.READER
        except ProviderError as e:
            if e.http_code not in [httplib.FORBIDDEN, httplib.NOT_FOUND]:
                raise

        return ContainerPermissions.NONE

    def _get_user(self):
        """Gets this drivers' authenticated user, if any."""
        oauth2_creds = getattr(self.connection, "oauth2_credential")
        if oauth2_creds:
            return oauth2_creds.user_id
        else:
            return None

    def _get_object_permissions(self, container_name, object_name):
        """
        Return the object permissions for the current authenticated user.
        If the object does not exist, or no object_name is given, return the
        default object permissions.

        :param container_name: The container name.
        :type container_name: ``str``

        :param object_name: The object name.
        :type object_name: ``str``

        :return: The permissions on the object or default object permissions.
        :rtype: ``int`` from ObjectPermissions
        """
        # Try OWNER permissions first: try listing the object ACL.
        try:
            self.json_connection.request(
                "/storage/v1/b/{}/o/{}/acl".format(container_name, object_name)
            )
            return ObjectPermissions.OWNER
        except ProviderError as e:
            if e.http_code not in [httplib.FORBIDDEN, httplib.NOT_FOUND]:
                raise

        # Try READER permissions: try getting the object.
        try:
            self.json_connection.request(
                "/storage/v1/b/{}/o/{}".format(container_name, object_name)
            )
            return ObjectPermissions.READER
        except ProviderError as e:
            if e.http_code not in [httplib.FORBIDDEN, httplib.NOT_FOUND]:
                raise

        return ObjectPermissions.NONE

    def ex_delete_permissions(self, container_name, object_name=None, entity=None):
        """
        Delete permissions for an ACL entity on a container or object.

        :param container_name: The container name.
        :type container_name: ``str``

        :param object_name: The object name. Optional. Not providing an object
            will delete a container permission.
        :type object_name: ``str``

        :param entity: The entity to whose permission will be deleted.
            Optional. If not provided, the role will be applied to the
            authenticated user, if using an OAuth2 authentication scheme.
        :type entity: ``str`` or ``None``
        """
        object_name = _clean_object_name(object_name)
        if not entity:
            user_id = self._get_user()
            if not user_id:
                raise ValueError(
                    "Must provide an entity. Driver is not using an " "authenticated user."
                )
            else:
                entity = "user-%s" % user_id

        if object_name:
            url = "/storage/v1/b/{}/o/{}/acl/{}".format(container_name, object_name, entity)
        else:
            url = "/storage/v1/b/{}/acl/{}".format(container_name, entity)

        self.json_connection.request(url, method="DELETE")

    def ex_get_permissions(self, container_name, object_name=None):
        """
        Return the permissions for the currently authenticated user.

        :param container_name: The container name.
        :type container_name: ``str``

        :param object_name: The object name. Optional. Not providing an object
            will return only container permissions.
        :type object_name: ``str`` or ``None``

        :return: A tuple of container and object permissions.
        :rtype: ``tuple`` of (``int``, ``int`` or ``None``) from
            ContainerPermissions and ObjectPermissions, respectively.
        """
        object_name = _clean_object_name(object_name)
        obj_perms = (
            self._get_object_permissions(container_name, object_name) if object_name else None
        )
        return self._get_container_permissions(container_name), obj_perms

    def ex_set_permissions(self, container_name, object_name=None, entity=None, role=None):
        """
        Set the permissions for an ACL entity on a container or an object.

        :param container_name: The container name.
        :type container_name: ``str``

        :param object_name: The object name. Optional. Not providing an object
            will apply the acl to the container.
        :type object_name: ``str``

        :param entity: The entity to which apply the role. Optional. If not
            provided, the role will be applied to the authenticated user, if
            using an OAuth2 authentication scheme.
        :type entity: ``str``

        :param role: The permission/role to set on the entity.
        :type role: ``int`` from ContainerPermissions or ObjectPermissions
            or ``str``.

        :raises ValueError: If no entity was given, but was required. Or if
            the role isn't valid for the bucket or object.
        """
        object_name = _clean_object_name(object_name)
        if isinstance(role, int):
            perms = ObjectPermissions if object_name else ContainerPermissions
            try:
                role = perms.values[role]
            except IndexError:
                raise ValueError(
                    "%s is not a valid role level for container=%s object=%s"
                    % (role, container_name, object_name)
                )
        elif not isinstance(role, str):
            raise ValueError("%s is not a valid permission." % role)

        if not entity:
            user_id = self._get_user()
            if not user_id:
                raise ValueError(
                    "Must provide an entity. Driver is not using an " "authenticated user."
                )
            else:
                entity = "user-%s" % user_id

        if object_name:
            url = "/storage/v1/b/{}/o/{}/acl".format(container_name, object_name)
        else:
            url = "/storage/v1/b/%s/acl" % container_name

        self.json_connection.request(
            url, method="POST", data=json.dumps({"role": role, "entity": entity})
        )

    def _get_content_length_from_headers(self, headers: Dict[str, str]) -> Optional[int]:
        # We need to override this since Google storage doesn't always return
        # Content-Length header.
        # See https://github.com/apache/libcloud/issues/1544 for details.
        x_goog_content_length = headers.get("x-goog-stored-content-length", None)
        content_length = headers.get("content-length", x_goog_content_length)
        return content_length
