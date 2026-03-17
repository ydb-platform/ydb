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

import sys
import hmac
import time
import uuid
import base64
import hashlib

from libcloud.utils.py3 import ET, b, u, urlquote
from libcloud.utils.xml import findtext
from libcloud.common.base import XmlResponse, ConnectionUserAndKey
from libcloud.common.types import MalformedResponseError

__all__ = [
    "AliyunXmlResponse",
    "AliyunRequestSigner",
    "AliyunRequestSignerAlgorithmV1_0",
    "SignedAliyunConnection",
    "AliyunConnection",
    "SIGNATURE_VERSION_1_0",
    "DEFAULT_SIGNATURE_VERSION",
]

SIGNATURE_VERSION_1_0 = "1.0"
DEFAULT_SIGNATURE_VERSION = SIGNATURE_VERSION_1_0


class AliyunXmlResponse(XmlResponse):
    namespace = None

    def success(self):
        return 200 <= self.status < 300

    def parse_body(self):
        """
        Each response from Aliyun contains a request id and a host id.
        The response body is in utf-8 encoding.
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
        self.request_id = findtext(element=body, xpath="RequestId", namespace=self.namespace)
        self.host_id = findtext(element=body, xpath="HostId", namespace=self.namespace)
        return body

    def parse_error(self):
        """
        Parse error responses from Aliyun.
        """
        body = super().parse_error()
        code, message = self._parse_error_details(element=body)
        request_id = findtext(element=body, xpath="RequestId", namespace=self.namespace)
        host_id = findtext(element=body, xpath="HostId", namespace=self.namespace)
        error = {
            "code": code,
            "message": message,
            "request_id": request_id,
            "host_id": host_id,
        }
        return u(error)

    def _parse_error_details(self, element):
        """
        Parse error code and message from the provided error element.

        :return: ``tuple`` with two elements: (code, message)
        :rtype: ``tuple``
        """
        code = findtext(element=element, xpath="Code", namespace=self.namespace)
        message = findtext(element=element, xpath="Message", namespace=self.namespace)

        return (code, message)


class AliyunRequestSigner:
    """
    Class handles signing the outgoing Aliyun requests.
    """

    def __init__(self, access_key, access_secret, version):
        """
        :param access_key: Access key.
        :type access_key: ``str``

        :param access_secret: Access secret.
        :type access_secret: ``str``

        :param version: API version.
        :type version: ``str``
        """
        self.access_key = access_key
        self.access_secret = access_secret
        self.version = version

    def get_request_params(self, params, method="GET", path="/"):
        return params

    def get_request_headers(self, params, headers, method="GET", path="/"):
        return params, headers


class AliyunRequestSignerAlgorithmV1_0(AliyunRequestSigner):
    """Aliyun request signer using signature version 1.0."""

    def get_request_params(self, params, method="GET", path="/"):
        params["Format"] = "XML"
        params["Version"] = self.version
        params["AccessKeyId"] = self.access_key
        params["SignatureMethod"] = "HMAC-SHA1"
        params["SignatureVersion"] = SIGNATURE_VERSION_1_0
        params["SignatureNonce"] = _get_signature_nonce()
        # TODO: Support 'ResourceOwnerAccount'
        params["Timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        params["Signature"] = self._sign_request(params, method, path)
        return params

    def _sign_request(self, params, method, path):
        """
        Sign Aliyun requests parameters and get the signature.

        StringToSign = HTTPMethod + '&' +
                       percentEncode('/') + '&' +
                       percentEncode(CanonicalizedQueryString)
        """
        keys = list(params.keys())
        keys.sort()
        pairs = []
        for key in keys:
            pairs.append("{}={}".format(_percent_encode(key), _percent_encode(params[key])))
        qs = urlquote("&".join(pairs), safe="-_.~")
        string_to_sign = "&".join((method, urlquote(path, safe=""), qs))
        b64_hmac = base64.b64encode(
            hmac.new(
                b(self._get_access_secret()), b(string_to_sign), digestmod=hashlib.sha1
            ).digest()
        )

        return b64_hmac.decode("utf8")

    def _get_access_secret(self):
        return "%s&" % self.access_secret


class AliyunConnection(ConnectionUserAndKey):
    pass


class SignedAliyunConnection(AliyunConnection):
    api_version = None

    def __init__(
        self,
        user_id,
        key,
        secure=True,
        host=None,
        port=None,
        url=None,
        timeout=None,
        proxy_url=None,
        retry_delay=None,
        backoff=None,
        api_version=None,
        signature_version=DEFAULT_SIGNATURE_VERSION,
    ):
        super().__init__(
            user_id=user_id,
            key=key,
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            proxy_url=proxy_url,
            retry_delay=retry_delay,
            backoff=backoff,
        )

        self.signature_version = str(signature_version)

        if self.signature_version == "1.0":
            signer_cls = AliyunRequestSignerAlgorithmV1_0
        else:
            raise ValueError("Unsupported signature_version: %s" % signature_version)

        if api_version is not None:
            self.api_version = str(api_version)
        else:
            if self.api_version is None:
                raise ValueError("Unsupported null api_version")

        self.signer = signer_cls(
            access_key=self.user_id, access_secret=self.key, version=self.api_version
        )

    def add_default_params(self, params):
        params = self.signer.get_request_params(params=params, method=self.method, path=self.action)
        return params


def _percent_encode(encode_str):
    """
    Encode string to utf8, quote for url and replace '+' with %20,
    '*' with %2A and keep '~' not converted.

    :param src_str: ``str`` in the same encoding with sys.stdin,
                    default to encoding cp936.
    :return: ``str`` represents the encoded result
    :rtype: ``str``
    """
    encoding = sys.stdin.encoding or "cp936"
    decoded = str(encode_str)

    if isinstance(encode_str, bytes):
        decoded = encode_str.decode(encoding)

    res = urlquote(decoded.encode("utf8"), "")
    res = res.replace("+", "%20")
    res = res.replace("*", "%2A")
    res = res.replace("%7E", "~")
    return res


def _get_signature_nonce():
    return str(uuid.uuid4())
