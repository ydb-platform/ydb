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
import hashlib
from datetime import datetime

from libcloud.utils.py3 import urlquote

__all__ = [
    "OSCRequestSignerAlgorithmV4",
]


class OSCRequestSigner:
    """
    Class which handles signing the outgoing AWS requests.
    """

    def __init__(self, access_key: str, access_secret: str, version: str, connection):
        """
        :param access_key: Access key.
        :type access_key: ``str``

        :param access_secret: Access secret.
        :type access_secret: ``str``

        :param version: API version.
        :type version: ``str``

        :param connection: Connection instance.
        :type connection: :class:`Connection`
        """
        self.access_key = access_key
        self.access_secret = access_secret
        self.version = version
        self.connection = connection


class OSCRequestSignerAlgorithmV4(OSCRequestSigner):
    @staticmethod
    def sign(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    @staticmethod
    def _get_signed_headers(headers: dict):
        return ";".join([k.lower() for k in sorted(headers.keys())])

    @staticmethod
    def _get_canonical_headers(headers: dict):
        return (
            "\n".join([":".join([k.lower(), str(v).strip()]) for k, v in sorted(headers.items())])
            + "\n"
        )

    @staticmethod
    def _get_request_params(params: dict):
        return "&".join(
            [
                "{}={}".format(urlquote(k, safe=""), urlquote(str(v), safe="~"))
                for k, v in sorted(params.items())
            ]
        )

    def get_request_headers(self, service_name: str, region: str, action: str, data: str):
        date = datetime.utcnow()
        host = "{}.{}.outscale.com".format(service_name, region)
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Osc-Date": date.strftime("%Y%m%dT%H%M%SZ"),
            "Host": host,
        }
        path = "/{}/{}/{}".format(self.connection.service_name, self.version, action)
        sig = self._get_authorization_v4_header(
            headers=headers, dt=date, method="POST", path=path, data=data
        )
        headers.update({"Authorization": sig})
        return headers

    def _get_authorization_v4_header(
        self,
        headers: dict,
        data: str,
        dt: datetime,
        method: str = "GET",
        path: str = "/",
    ):
        credentials_scope = self._get_credential_scope(dt=dt)
        signed_headers = self._get_signed_headers(headers=headers)
        signature = self._get_signature(headers=headers, dt=dt, method=method, path=path, data=data)
        return (
            "OSC4-HMAC-SHA256 Credential=%(u)s/%(c)s, "
            "SignedHeaders=%(sh)s, Signature=%(s)s"
            % {
                "u": self.access_key,
                "c": credentials_scope,
                "sh": signed_headers,
                "s": signature,
            }
        )

    def _get_signature(self, headers: dict, dt: datetime, method: str, path: str, data: str):
        string_to_sign = self._get_string_to_sign(
            headers=headers, dt=dt, method=method, path=path, data=data
        )
        signing_key = self._get_key_to_sign_with(self.access_secret, dt.strftime("%Y%m%d"))
        return hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    def _get_key_to_sign_with(self, key: str, dt: str):
        k_date = self.sign(("OSC4" + key).encode("utf-8"), dt)
        k_region = self.sign(k_date, self.connection.region_name)
        k_service = self.sign(k_region, self.connection.service_name)
        return self.sign(k_service, "osc4_request")

    def _get_string_to_sign(self, headers: dict, dt: datetime, method: str, path: str, data: str):
        canonical_request = self._get_canonical_request(
            headers=headers, method=method, path=path, data=data
        )
        return (
            "OSC4-HMAC-SHA256"
            + "\n"
            + dt.strftime("%Y%m%dT%H%M%SZ")
            + "\n"
            + self._get_credential_scope(dt)
            + "\n"
            + hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        )

    def _get_credential_scope(self, dt):
        return "/".join(
            [
                dt.strftime("%Y%m%d"),
                self.connection.region_name,
                self.connection.service_name,
                "osc4_request",
            ]
        )

    def _get_canonical_request(self, headers, method, path, data):
        return "\n".join(
            [
                method,
                path,
                self._get_request_params({}),
                self._get_canonical_headers(headers),
                self._get_signed_headers(headers),
                hashlib.sha256(data.encode("utf-8")).hexdigest(),
            ]
        )
