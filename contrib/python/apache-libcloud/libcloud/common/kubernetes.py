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
Module which contains common Kubernetes related code.
"""

import os
import base64
import warnings
from typing import Optional

from libcloud.utils.py3 import b, httplib
from libcloud.common.base import (
    JsonResponse,
    ConnectionKey,
    ConnectionUserAndKey,
    KeyCertificateConnection,
)
from libcloud.common.types import InvalidCredsError

__all__ = [
    "KubernetesException",
    "KubernetesBasicAuthConnection",
    "KubernetesTLSAuthConnection",
    "KubernetesTokenAuthConnection",
    "KubernetesDriverMixin",
    "VALID_RESPONSE_CODES",
]

VALID_RESPONSE_CODES = [
    httplib.OK,
    httplib.ACCEPTED,
    httplib.CREATED,
    httplib.NO_CONTENT,
]


class KubernetesException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "KubernetesException {} {}".format(self.code, self.message)


class KubernetesResponse(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_error(self):
        if self.status == 401:
            raise InvalidCredsError("Invalid credentials")
        return self.body

    def success(self):
        return self.status in self.valid_response_codes


class KubernetesTLSAuthConnection(KeyCertificateConnection):
    responseCls = KubernetesResponse
    timeout = 60

    def __init__(
        self,
        key,
        secure=True,
        host="localhost",
        port="6443",
        key_file=None,
        cert_file=None,
        **kwargs,
    ):
        super().__init__(
            key_file=key_file,
            cert_file=cert_file,
            secure=secure,
            host=host,
            port=port,
            url=None,
            proxy_url=None,
            timeout=None,
            backoff=None,
            retry_delay=None,
        )

        if key_file:
            keypath = os.path.expanduser(key_file)
            is_file_path = os.path.exists(keypath) and os.path.isfile(keypath)

            if not is_file_path:
                raise InvalidCredsError(
                    "You need an key PEM file to authenticate "
                    "via tls. For more info please visit:"
                    "https://kubernetes.io/docs/concepts/"
                    "cluster-administration/certificates/"
                )

            self.key_file = key_file

            certpath = os.path.expanduser(cert_file)
            is_file_path = os.path.exists(certpath) and os.path.isfile(certpath)

            if not is_file_path:
                raise InvalidCredsError(
                    "You need an certificate PEM file to authenticate "
                    "via tls. For more info please visit:"
                    "https://kubernetes.io/docs/concepts/"
                    "cluster-administration/certificates/"
                )

            self.cert_file = cert_file

    def add_default_headers(self, headers):
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"
        return headers


class KubernetesTokenAuthConnection(ConnectionKey):
    responseCls = KubernetesResponse
    timeout = 60

    def add_default_headers(self, headers):
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"
        if self.key:
            headers["Authorization"] = "Bearer " + self.key
        else:
            raise ValueError("Please provide a valid token in the key param")
        return headers


class KubernetesBasicAuthConnection(ConnectionUserAndKey):
    responseCls = KubernetesResponse
    timeout = 60

    def add_default_headers(self, headers):
        """
        Add parameters that are necessary for every request
        If user and password are specified, include a base http auth
        header
        """
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        if self.user_id and self.key:
            auth_string = b("{}:{}".format(self.user_id, self.key))
            user_b64 = base64.b64encode(auth_string)
            headers["Authorization"] = "Basic %s" % (user_b64.decode("utf-8"))
        return headers


class KubernetesDriverMixin:
    """
    Base driver class to be used with various Kubernetes drivers.

    NOTE: This base class can be used in different APIs such as container and
    compute one.
    """

    def __init__(
        self,
        key=None,
        secret=None,
        secure=False,
        host="localhost",
        port=4243,
        key_file=None,
        cert_file=None,
        ca_cert=None,
        ex_token_bearer_auth=False,
    ):
        """
        :param    key: API key or username to be used (required)
        :type     key: ``str``

        :param    secret: Secret password to be used (required)
        :type     secret: ``str``

        :param    secure: Whether to use HTTPS or HTTP. Note: Some providers
                          only support HTTPS, and it is on by default.
        :type     secure: ``bool``

        :param    host: Override hostname used for connections.
        :type     host: ``str``

        :param    port: Override port used for connections.
        :type     port: ``int``

        :param    key_file: Path to the key file used to authenticate (when
                            using key file auth).
        :type     key_file: ``str``

        :param    cert_file: Path to the cert file used to authenticate (when
                             using key file auth).
        :type     cert_file: ``str``

        :param    ex_token_bearer_auth: True to use token bearer auth.
        :type     ex_token_bearer_auth: ``bool``

        :return: ``None``
        """
        if ex_token_bearer_auth:
            self.connectionCls = KubernetesTokenAuthConnection
            if not key:
                msg = 'The token must be a string provided via "key" argument'
                raise ValueError(msg)
            secure = True

        if key_file or cert_file:
            # Certificate based auth is used
            if not (key_file and cert_file):
                raise ValueError("Both key and certificate files are needed")

        if key_file:
            self.connectionCls = KubernetesTLSAuthConnection
            self.key_file = key_file
            self.cert_file = cert_file
            secure = True

        if host and host.startswith("https://"):
            secure = True

        host = self._santize_host(host=host)

        super().__init__(
            key=key,
            secret=secret,
            secure=secure,
            host=host,
            port=port,
            key_file=key_file,
            cert_file=cert_file,
        )

        if ca_cert:
            self.connection.connection.ca_cert = ca_cert
        else:
            # do not verify SSL certificate
            warnings.warn(
                "Kubernetes has its own CA, since you didn't supply "
                "a CA certificate be aware that SSL verification "
                "will be disabled for this session."
            )
            self.connection.connection.ca_cert = False

        self.connection.secure = secure
        self.connection.host = host

        if port is not None:
            self.connection.port = port

    def _ex_connection_class_kwargs(self):
        kwargs = {}
        if hasattr(self, "key_file"):
            kwargs["key_file"] = self.key_file
        if hasattr(self, "cert_file"):
            kwargs["cert_file"] = self.cert_file
        return kwargs

    def _santize_host(self, host=None):
        # type: (Optional[str]) -> Optional[str]
        """
        Sanitize "host" argument any remove any protocol prefix (if specified).
        """
        if not host:
            return None

        prefixes = ["http://", "https://"]
        for prefix in prefixes:
            if host.startswith(prefix):
                host = host.lstrip(prefix)

        return host
