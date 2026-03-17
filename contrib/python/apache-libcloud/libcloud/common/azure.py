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
import copy
import hmac
import time
import base64
from hashlib import sha256

from libcloud.http import LibcloudConnection
from libcloud.utils.py3 import ET, b, httplib, urlparse, urlencode, basestring
from libcloud.utils.xml import fixxpath
from libcloud.common.base import (
    BaseDriver,
    RawResponse,
    XmlResponse,
    ConnectionUserAndKey,
    CertificateConnection,
)
from libcloud.common.types import LibcloudError, InvalidCredsError, MalformedResponseError
from libcloud.common.azure_arm import AzureAuthJsonResponse, publicEnvironments

# The time format for headers in Azure requests
AZURE_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


class AzureRedirectException(Exception):
    def __init__(self, response):
        self.location = response.headers["location"]


class AzureResponse(XmlResponse):
    valid_response_codes = [
        httplib.NOT_FOUND,
        httplib.CONFLICT,
        httplib.BAD_REQUEST,
        # added TEMPORARY_REDIRECT as this can sometimes be
        # sent by azure instead of a success or fail response
        httplib.TEMPORARY_REDIRECT,
        # Used by Azure Blobs range downloads
        httplib.PARTIAL_CONTENT,
    ]

    def success(self):
        i = int(self.status)
        return 200 <= i <= 299 or i in self.valid_response_codes

    def parse_error(self, msg=None):
        error_msg = "Unknown error"

        try:
            # Azure does give some meaningful errors, but is inconsistent
            # Some APIs respond with an XML error. Others just dump HTML
            body = self.parse_body()

            # pylint: disable=no-member
            if type(body) == ET.Element:
                code = body.findtext(fixxpath(xpath="Code"))
                message = body.findtext(fixxpath(xpath="Message"))
                message = message.split("\n")[0]
                error_msg = "{}: {}".format(code, message)

        except MalformedResponseError:
            pass

        if msg:
            error_msg = "{} - {}".format(msg, error_msg)

        if self.status in [httplib.UNAUTHORIZED, httplib.FORBIDDEN]:
            raise InvalidCredsError(error_msg)

        raise LibcloudError("%s Status code: %d." % (error_msg, self.status), driver=self)

    def parse_body(self):
        is_redirect = int(self.status) == httplib.TEMPORARY_REDIRECT

        if is_redirect and self.connection.driver.follow_redirects:
            raise AzureRedirectException(self)
        else:
            return super().parse_body()


class AzureRawResponse(RawResponse):
    pass


class AzureBaseDriver(BaseDriver):
    name = "Microsoft Azure Service Management API"


class AzureActiveDirectoryConnection(ConnectionUserAndKey):
    """
    Represents a single connection to Azure using Azure AD for Blob
    """

    conn_class = LibcloudConnection
    driver = AzureBaseDriver
    name = "Azure AD Auth"
    responseCls = AzureResponse
    rawResponseCls = AzureRawResponse

    API_VERSION = "2017-11-09"

    def __init__(
        self,
        key,
        secret,
        secure=True,
        host=None,
        port=None,
        tenant_id=None,
        identity=None,
        cloud_environment="default",
        **kwargs,
    ):
        super().__init__(identity, secret, **kwargs)
        if isinstance(cloud_environment, basestring):
            cloud_environment = publicEnvironments[cloud_environment]
        if not isinstance(cloud_environment, dict):
            raise Exception(
                "cloud_environment must be one of '%s' or a dict "
                "containing keys 'resourceManagerEndpointUrl', "
                "'activeDirectoryEndpointUrl', "
                "'activeDirectoryResourceId', "
                "'storageEndpointSuffix'" % ("', '".join(publicEnvironments.keys()))
            )

        self.login_host = urlparse.urlparse(
            cloud_environment["activeDirectoryEndpointUrl"]
        ).hostname
        self.login_resource = cloud_environment["activeDirectoryResourceId"]
        self.host = host
        self.identity = identity
        self.tenant_id = tenant_id
        self.storage_account_id = key

    def add_default_headers(self, headers):
        headers["x-ms-date"] = time.strftime(AZURE_TIME_FORMAT, time.gmtime())
        headers["x-ms-version"] = self.API_VERSION
        headers["Content-Type"] = "application/xml"
        headers["Authorization"] = "Bearer %s" % self.access_token
        return headers

    def get_client_credentials(self):
        """
        Log in and get bearer token used to authorize API requests.
        """
        conn = self.conn_class(self.login_host, 443, timeout=self.timeout)
        conn.connect()
        params = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.user_id,
                "client_secret": self.key,
                "resource": "https://storage.azure.com/",
            }
        )
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        conn.request("POST", "/%s/oauth2/token" % self.tenant_id, params, headers)
        js = AzureAuthJsonResponse(conn.getresponse(), conn)
        self.access_token = js.object["access_token"]
        self.expires_on = js.object["expires_on"]

    def connect(self, **kwargs):
        self.get_client_credentials()
        return super().connect(**kwargs)

    def request(
        self,
        action,
        params=None,
        data=None,
        headers=None,
        method="GET",
        raw=False,
        stream=False,
        json=None,
        retry_failed=None,
        *kwargs,
    ):
        # Log in again if the token has expired or is going to expire soon
        # (next 5 minutes).
        if (time.time() + 300) >= int(self.expires_on):
            self.get_client_credentials()

        return super().request(
            action,
            params=params,
            data=data,
            headers=headers,
            method=method,
            raw=raw,
            stream=stream,
            json=json,
            retry_failed=retry_failed,
        )


class AzureConnection(ConnectionUserAndKey):
    """
    Represents a single connection to Azure
    """

    responseCls = AzureResponse
    rawResponseCls = AzureRawResponse

    API_VERSION = "2012-02-12"

    def add_default_params(self, params):
        return params

    def pre_connect_hook(self, params, headers):
        headers = copy.deepcopy(headers)

        # We have to add a date header in GMT
        headers["x-ms-date"] = time.strftime(AZURE_TIME_FORMAT, time.gmtime())
        headers["x-ms-version"] = self.API_VERSION

        # Add the authorization header
        headers["Authorization"] = self._get_azure_auth_signature(
            method=self.method,
            headers=headers,
            params=params,
            account=self.user_id,
            secret_key=self.key,
            path=self.action,
        )

        # Azure cribs about this in 'raw' connections
        headers.pop("Host", None)

        return params, headers

    def _get_azure_auth_signature(self, method, headers, params, account, secret_key, path="/"):
        """
        Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID,
                            UTF-8-Encoding-Of( StringToSign ) ) ) );

        StringToSign = HTTP-VERB + "\n" +
            Content-Encoding + "\n" +
            Content-Language + "\n" +
            Content-Length + "\n" +
            Content-MD5 + "\n" +
            Content-Type + "\n" +
            Date + "\n" +
            If-Modified-Since + "\n" +
            If-Match + "\n" +
            If-None-Match + "\n" +
            If-Unmodified-Since + "\n" +
            Range + "\n" +
            CanonicalizedHeaders +
            CanonicalizedResource;
        """
        xms_header_values = []
        param_list = []

        # Split the x-ms headers and normal headers and make everything
        # lower case
        headers_copy = {}
        for header, value in headers.items():
            header = header.lower()
            value = str(value).strip()
            if header.startswith("x-ms-"):
                xms_header_values.append((header, value))
            else:
                headers_copy[header] = value

        # Get the values for the headers in the specific order
        special_header_values = self._format_special_header_values(headers_copy, method)

        # Prepare the first section of the string to be signed
        values_to_sign = [method] + special_header_values
        # string_to_sign = '\n'.join([method] + special_header_values)

        # The x-ms-* headers have to be in lower case and sorted
        xms_header_values.sort()

        for header, value in xms_header_values:
            values_to_sign.append("{}:{}".format(header, value))

        # Add the canonicalized path
        values_to_sign.append("/{}{}".format(account, path))

        # URL query parameters (sorted and lower case)
        for key, value in params.items():
            param_list.append((key.lower(), str(value).strip()))

        param_list.sort()

        for key, value in param_list:
            values_to_sign.append("{}:{}".format(key, value))

        string_to_sign = b("\n".join(values_to_sign))
        secret_key = b(secret_key)
        b64_hmac = base64.b64encode(hmac.new(secret_key, string_to_sign, digestmod=sha256).digest())

        return "SharedKey {}:{}".format(self.user_id, b64_hmac.decode("utf-8"))

    def _format_special_header_values(self, headers, method):
        is_change = method not in ("GET", "HEAD")
        is_old_api = self.API_VERSION <= "2014-02-14"

        special_header_keys = [
            "content-encoding",
            "content-language",
            "content-length",
            "content-md5",
            "content-type",
            "date",
            "if-modified-since",
            "if-match",
            "if-none-match",
            "if-unmodified-since",
            "range",
        ]

        special_header_values = []

        for header in special_header_keys:
            header = header.lower()  # Just for safety
            if header in headers:
                special_header_values.append(headers[header])
            elif header == "content-length" and is_change and is_old_api:
                # For old API versions, the Content-Length header must be '0'
                # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#content-length-header-in-version-2014-02-14-and-earlier
                special_header_values.append("0")
            else:
                special_header_values.append("")

        return special_header_values


class AzureServiceManagementConnection(CertificateConnection):
    # This needs the following approach -
    # 1. Make request using LibcloudHTTPSConnection which is a overloaded
    # class which takes in a client certificate
    # 2. Depending on the type of operation use a PollingConnection
    # when the response id is returned
    # 3. The Response can be used in an AzureServiceManagementResponse

    """
    Authentication class for "Service Account" authentication.
    """

    driver = AzureBaseDriver
    responseCls = AzureResponse
    rawResponseCls = AzureRawResponse
    name = "Azure Service Management API Connection"
    host = "management.core.windows.net"
    keyfile = ""

    def __init__(self, subscription_id, key_file, *args, **kwargs):
        """
        Check to see if PyCrypto is available, and convert key file path into a
        key string if the key is in a file.

        :param  subscription_id: Azure subscription ID.
        :type   subscription_id: ``str``

        :param  key_file: The PEM file used to authenticate with the service.
        :type   key_file: ``str``
        """

        super().__init__(key_file, *args, **kwargs)

        self.subscription_id = subscription_id

        keypath = os.path.expanduser(key_file)
        self.keyfile = keypath
        is_file_path = os.path.exists(keypath) and os.path.isfile(keypath)
        if not is_file_path:
            raise InvalidCredsError(
                "You need an certificate PEM file to authenticate with "
                "Microsoft Azure. This can be found in the portal."
            )
        self.key_file = key_file

    def add_default_headers(self, headers):
        """
        @inherits: :class:`Connection.add_default_headers`
        TODO: move to constant..
        """
        headers["x-ms-version"] = "2014-05-01"
        headers["x-ms-date"] = time.strftime(AZURE_TIME_FORMAT, time.gmtime())
        #  headers['host'] = self.host
        return headers
