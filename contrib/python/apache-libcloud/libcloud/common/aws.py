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
from typing import Dict, Type, Optional
from hashlib import sha256
from datetime import datetime

from libcloud.utils.py3 import ET, b, httplib, urlquote, basestring, _real_unicode
from libcloud.utils.xml import findall_ignore_namespace, findtext_ignore_namespace
from libcloud.common.base import BaseDriver, XmlResponse, JsonResponse, ConnectionUserAndKey
from libcloud.common.types import InvalidCredsError, MalformedResponseError

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore


__all__ = [
    "AWSBaseResponse",
    "AWSGenericResponse",
    "AWSTokenConnection",
    "SignedAWSConnection",
    "AWSRequestSignerAlgorithmV2",
    "AWSRequestSignerAlgorithmV4",
    "AWSDriver",
]

DEFAULT_SIGNATURE_VERSION = "2"
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"

PARAMS_NOT_STRING_ERROR_MSG = """
"params" dictionary contains an attribute "%s" which value (%s, %s) is not a
string.

Parameters are sent via query parameters and not via request body and as such,
all the values need to be of a simple type (string, int, bool).

For arrays and other complex types, you should use notation similar to this
one:

params['TagSpecification.1.Tag.Value'] = 'foo'
params['TagSpecification.2.Tag.Value'] = 'bar'

See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/Query-Requests.html
for details.
""".strip()


class AWSBaseResponse(XmlResponse):
    namespace = None

    def _parse_error_details(self, element):
        """
        Parse code and message from the provided error element.

        :return: ``tuple`` with two elements: (code, message)
        :rtype: ``tuple``
        """
        code = findtext_ignore_namespace(element=element, xpath="Code", namespace=self.namespace)
        message = findtext_ignore_namespace(
            element=element, xpath="Message", namespace=self.namespace
        )

        return code, message


class AWSGenericResponse(AWSBaseResponse):
    # There are multiple error messages in AWS, but they all have an Error node
    # with Code and Message child nodes. Xpath to select them
    # None if the root node *is* the Error node
    xpath = None

    # This dict maps <Error><Code>CodeName</Code></Error> to a specific
    # exception class that is raised immediately.
    # If a custom exception class is not defined, errors are accumulated and
    # returned from the parse_error method.
    exceptions = {}  # type: Dict[str, Type[Exception]]

    def success(self):
        return self.status in [httplib.OK, httplib.CREATED, httplib.ACCEPTED]

    def parse_error(self):
        context = self.connection.context
        status = int(self.status)

        # FIXME: Probably ditch this as the forbidden message will have
        # corresponding XML.
        if status == httplib.FORBIDDEN:
            if not self.body:
                raise InvalidCredsError(str(self.status) + ": " + self.error)
            else:
                raise InvalidCredsError(self.body)

        try:
            body = ET.XML(self.body)
        except Exception:
            raise MalformedResponseError(
                "Failed to parse XML", body=self.body, driver=self.connection.driver
            )

        if self.xpath:
            errs = findall_ignore_namespace(
                element=body, xpath=self.xpath, namespace=self.namespace
            )
        else:
            errs = [body]

        msgs = []
        for err in errs:
            code, message = self._parse_error_details(element=err)
            exceptionCls = self.exceptions.get(code, None)

            if exceptionCls is None:
                msgs.append("{}: {}".format(code, message))
                continue

            # Custom exception class is defined, immediately throw an exception
            params = {}
            if hasattr(exceptionCls, "kwargs"):
                for key in exceptionCls.kwargs:
                    if key in context:
                        params[key] = context[key]

            raise exceptionCls(value=message, driver=self.connection.driver, **params)

        return "\n".join(msgs)


class AWSTokenConnection(ConnectionUserAndKey):
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
        token=None,
        retry_delay=None,
        backoff=None,
    ):
        self.token = token
        super().__init__(
            user_id,
            key,
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            retry_delay=retry_delay,
            backoff=backoff,
            proxy_url=proxy_url,
        )

    def add_default_params(self, params):
        # Even though we are adding it to the headers, we need it here too
        # so that the token is added to the signature.
        if self.token:
            params["x-amz-security-token"] = self.token
        return super().add_default_params(params)

    def add_default_headers(self, headers):
        if self.token:
            headers["x-amz-security-token"] = self.token
        return super().add_default_headers(headers)


class AWSRequestSigner:
    """
    Class which handles signing the outgoing AWS requests.
    """

    def __init__(self, access_key, access_secret, version, connection):
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
        # TODO: Remove cycling dependency between connection and signer
        self.connection = connection

    def get_request_params(self, params, method="GET", path="/"):
        return params

    def get_request_headers(self, params, headers, method="GET", path="/", data=None):
        return params, headers


class AWSRequestSignerAlgorithmV2(AWSRequestSigner):
    def get_request_params(self, params, method="GET", path="/"):
        params["SignatureVersion"] = "2"
        params["SignatureMethod"] = "HmacSHA256"
        params["AWSAccessKeyId"] = self.access_key
        params["Version"] = self.version
        params["Timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        params["Signature"] = self._get_aws_auth_param(
            params=params, secret_key=self.access_secret, path=path
        )
        return params

    def _get_aws_auth_param(self, params, secret_key, path="/"):
        """
        Creates the signature required for AWS, per
        http://bit.ly/aR7GaQ [docs.amazonwebservices.com]:

        StringToSign = HTTPVerb + "\n" +
                       ValueOfHostHeaderInLowercase + "\n" +
                       HTTPRequestURI + "\n" +
                       CanonicalizedQueryString <from the preceding step>
        """
        connection = self.connection

        keys = list(params.keys())
        keys.sort()
        pairs = []
        for key in keys:
            value = str(params[key])
            pairs.append(urlquote(key, safe="") + "=" + urlquote(value, safe="-_~"))

        qs = "&".join(pairs)

        hostname = connection.host
        if (connection.secure and connection.port != 443) or (
            not connection.secure and connection.port != 80
        ):
            hostname += ":" + str(connection.port)

        string_to_sign = "\n".join(("GET", hostname, path, qs))

        b64_hmac = base64.b64encode(
            hmac.new(b(secret_key), b(string_to_sign), digestmod=sha256).digest()
        )

        return b64_hmac.decode("utf-8")


class AWSRequestSignerAlgorithmV4(AWSRequestSigner):
    def get_request_params(self, params, method="GET", path="/"):
        if method == "GET":
            params["Version"] = self.version
        return params

    def get_request_headers(self, params, headers, method="GET", path="/", data=None):
        now = datetime.utcnow()
        headers["X-AMZ-Date"] = now.strftime("%Y%m%dT%H%M%SZ")
        headers["X-AMZ-Content-SHA256"] = self._get_payload_hash(method, data)
        headers["Authorization"] = self._get_authorization_v4_header(
            params=params, headers=headers, dt=now, method=method, path=path, data=data
        )

        return params, headers

    def _get_authorization_v4_header(self, params, headers, dt, method="GET", path="/", data=None):
        credentials_scope = self._get_credential_scope(dt=dt)
        signed_headers = self._get_signed_headers(headers=headers)
        signature = self._get_signature(
            params=params, headers=headers, dt=dt, method=method, path=path, data=data
        )

        return (
            "AWS4-HMAC-SHA256 Credential=%(u)s/%(c)s, "
            "SignedHeaders=%(sh)s, Signature=%(s)s"
            % {
                "u": self.access_key,
                "c": credentials_scope,
                "sh": signed_headers,
                "s": signature,
            }
        )

    def _get_signature(self, params, headers, dt, method, path, data):
        key = self._get_key_to_sign_with(dt)
        string_to_sign = self._get_string_to_sign(
            params=params, headers=headers, dt=dt, method=method, path=path, data=data
        )
        return _sign(key=key, msg=string_to_sign, hex=True)

    def _get_key_to_sign_with(self, dt):
        return _sign(
            _sign(
                _sign(
                    _sign(("AWS4" + self.access_secret), dt.strftime("%Y%m%d")),
                    self.connection.driver.region_name,
                ),
                self.connection.service_name,
            ),
            "aws4_request",
        )

    def _get_string_to_sign(self, params, headers, dt, method, path, data):
        canonical_request = self._get_canonical_request(
            params=params, headers=headers, method=method, path=path, data=data
        )

        return "\n".join(
            [
                "AWS4-HMAC-SHA256",
                dt.strftime("%Y%m%dT%H%M%SZ"),
                self._get_credential_scope(dt),
                _hash(canonical_request),
            ]
        )

    def _get_credential_scope(self, dt):
        return "/".join(
            [
                dt.strftime("%Y%m%d"),
                self.connection.driver.region_name,
                self.connection.service_name,
                "aws4_request",
            ]
        )

    def _get_signed_headers(self, headers):
        return ";".join([k.lower() for k in sorted(headers.keys(), key=str.lower)])

    def _get_canonical_headers(self, headers):
        return (
            "\n".join(
                [
                    ":".join([k.lower(), str(v).strip()])
                    for k, v in sorted(headers.items(), key=lambda k: k[0].lower())
                ]
            )
            + "\n"
        )

    def _get_payload_hash(self, method, data=None):
        if data is UnsignedPayloadSentinel:
            return UNSIGNED_PAYLOAD
        if method in ("POST", "PUT"):
            if data:
                if hasattr(data, "next") or hasattr(data, "__next__"):
                    # File upload; don't try to read the entire payload
                    return UNSIGNED_PAYLOAD
                return _hash(data)
            else:
                return UNSIGNED_PAYLOAD
        else:
            return _hash("")

    def _get_request_params(self, params):
        # For self.method == GET
        return "&".join(
            [
                "{}={}".format(urlquote(k, safe=""), urlquote(str(v), safe="~"))
                for k, v in sorted(params.items())
            ]
        )

    def _get_canonical_request(self, params, headers, method, path, data):
        return "\n".join(
            [
                method,
                path,
                self._get_request_params(params),
                self._get_canonical_headers(headers),
                self._get_signed_headers(headers),
                self._get_payload_hash(method, data),
            ]
        )


class UnsignedPayloadSentinel:
    pass


class SignedAWSConnection(AWSTokenConnection):
    version = None  # type: Optional[str]

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
        token=None,
        retry_delay=None,
        backoff=None,
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
            token=token,
            retry_delay=retry_delay,
            backoff=backoff,
            proxy_url=proxy_url,
        )
        self.signature_version = str(signature_version)

        if self.signature_version == "2":
            signer_cls = AWSRequestSignerAlgorithmV2
        elif self.signature_version == "4":
            signer_cls = AWSRequestSignerAlgorithmV4
        else:
            raise ValueError("Unsupported signature_version: %s" % (signature_version))

        self.signer = signer_cls(
            access_key=self.user_id,
            access_secret=self.key,
            version=self.version,
            connection=self,
        )

    def add_default_params(self, params):
        params = self.signer.get_request_params(params=params, method=self.method, path=self.action)

        # Verify that params only contain simple types and no nested
        # dictionaries.
        # params are sent via query params so only strings are supported
        for key, value in params.items():
            if not isinstance(value, (_real_unicode, basestring, int, bool)):
                msg = PARAMS_NOT_STRING_ERROR_MSG % (key, value, type(value))
                raise ValueError(msg)

        return params

    def pre_connect_hook(self, params, headers):
        params, headers = self.signer.get_request_headers(
            params=params,
            headers=headers,
            method=self.method,
            path=self.action,
            data=self.data,
        )
        return params, headers


class AWSJsonResponse(JsonResponse):
    """
    Amazon ECS response class.
    ECS API uses JSON unlike the s3, elb drivers
    """

    def parse_error(self):
        response = json.loads(self.body)
        code = response["__type"]
        message = response.get("Message", response["message"])
        return "{}: {}".format(code, message)


def _sign(key, msg, hex=False):
    if hex:
        return hmac.new(b(key), b(msg), hashlib.sha256).hexdigest()
    else:
        return hmac.new(b(key), b(msg), hashlib.sha256).digest()


def _hash(msg):
    return hashlib.sha256(b(msg)).hexdigest()


class AWSDriver(BaseDriver):
    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=None,
        token=None,
        **kwargs,
    ):
        self.token = token
        super().__init__(
            key,
            secret=secret,
            secure=secure,
            host=host,
            port=port,
            api_version=api_version,
            region=region,
            token=token,
            **kwargs,
        )

    def _ex_connection_class_kwargs(self):
        kwargs = super()._ex_connection_class_kwargs()
        kwargs["token"] = self.token
        return kwargs
