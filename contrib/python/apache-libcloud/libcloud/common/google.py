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
Module for Google Connection and Authentication classes.

Information about setting up your Google OAUTH2 credentials:

For libcloud, there are two basic methods for authenticating to Google using
OAUTH2: Service Accounts and Client IDs for Installed Applications.

Both are initially set up from the Cloud Console Console -
https://cloud.google.com/console

Setting up Service Account authentication (note that you need the cryptography
package installed to use this):

- Go to the Console
- Go to your project and then to "APIs & auth" on the left
- Click on "Credentials"
- Click on "Create New Client ID..."
- Select "Service account" and click on "Create Client ID"
- Download the Private Key (should happen automatically).  The key you download
  is in JSON format.
- Move the .json file to a safe location.
- Optionally, you may choose to Generate a PKCS12 key from the Console.
  It needs to be converted to the PEM format.  Please note, the PKCS12 format
  is deprecated and may be removed in a future release.
  - Convert the key using OpenSSL (the default password is 'notasecret').
  - Move the .pem file to a safe location.
- To Authenticate, you will need to pass the Service Account's "Email
  address" in as the user_id and the path to the .pem file as the key.

Setting up Installed Application authentication:

- Go to the Console
- Go to your project and then to "APIs & auth" on the left
- Click on "Credentials"
- Select "Installed application" and "Other" then click on
  "Create Client ID"
- To Authenticate, pass in the "Client ID" as the user_id and the "Client
  secret" as the key
- The first time that you do this, the libcloud will give you a URL to
  visit.  Copy and paste the URL into a browser.
- When you go to the URL it will ask you to log in (if you aren't already)
  and ask you if you want to allow the project access to your account.
- Click on Accept and you will be given a code.
- Paste that code at the prompt given to you by the Google libcloud
  connection.
- At that point, a token & refresh token will be stored in your home
  directory and will be used for authentication.

Please remember to secure your keys and access tokens.
"""


import os
import sys
import time
import errno
import base64
import logging
import datetime
import urllib.parse
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

from libcloud.utils.py3 import b, httplib, urlparse, urlencode
from libcloud.common.base import BaseDriver, JsonResponse, PollingConnection, ConnectionUserAndKey
from libcloud.common.types import LibcloudError, ProviderError
from libcloud.utils.connection import get_response_object

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore


try:
    from cryptography import exceptions
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.hashes import SHA256
    from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
except ImportError:
    # The cryptography library is unavailable
    SHA256 = None  # type: ignore

UTC_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

LOG = logging.getLogger(__name__)


def _utcnow():
    """
    Mocked in libcloud.test.common.google.GoogleTestCase.
    """
    return datetime.datetime.utcnow()


def _utc_timestamp(datetime_obj):
    """
    Return string of datetime_obj in the UTC Timestamp Format
    """
    return datetime_obj.strftime(UTC_TIMESTAMP_FORMAT)


def _from_utc_timestamp(timestamp):
    """
    Return datetime obj where date and time are pulled from timestamp string.
    """
    return datetime.datetime.strptime(timestamp, UTC_TIMESTAMP_FORMAT)


def _get_gce_metadata(path="", retry_failed: Optional[bool] = None):
    try:
        url = "http://metadata/computeMetadata/v1/" + path.lstrip("/")
        headers = {"Metadata-Flavor": "Google"}
        response = get_response_object(url, headers=headers, retry_failed=retry_failed)
        return response.status, "", response.body
    except Exception as e:
        return -1, str(e), None


class GoogleAuthError(LibcloudError):
    """Generic Error class for various authentication errors."""

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return repr(self.value)


class GoogleBaseError(ProviderError):
    def __init__(self, value, http_code, code, driver=None):
        self.code = code
        super().__init__(value, http_code, driver)


class InvalidRequestError(GoogleBaseError):
    pass


class JsonParseError(GoogleBaseError):
    pass


class ResourceNotFoundError(GoogleBaseError):
    def __init__(self, value, http_code, code, driver=None):
        self.code = code
        if (
            isinstance(value, dict)
            and "message" in value
            and value["message"].count("/") == 1
            and value["message"].count("projects/") == 1
        ):
            value["message"] = (
                value["message"] + ". A missing project "
                "error may be an authentication issue. "
                "Please  ensure your auth credentials match "
                "your project. "
            )
        super().__init__(value, http_code, driver)


class QuotaExceededError(GoogleBaseError):
    pass


class ResourceExistsError(GoogleBaseError):
    pass


class ResourceInUseError(GoogleBaseError):
    pass


class GoogleResponse(JsonResponse):
    """
    Google Base Response class.
    """

    def success(self):
        """
        Determine if the request was successful.

        For the Google response class, tag all responses as successful and
        raise appropriate Exceptions from parse_body.

        :return: C{True}
        """
        return True

    def _get_error(self, body):
        """
        Get the error code and message from a JSON response.

        Return just the first error if there are multiple errors.

        :param  body: The body of the JSON response dictionary
        :type   body: ``dict``

        :return:  Tuple containing error code and message
        :rtype:   ``tuple`` of ``str`` or ``int``
        """
        if "errors" in body["error"]:
            err = body["error"]["errors"][0]
        else:
            err = body["error"]

        if "code" in err:
            code = err.get("code")
            message = err.get("message")
        else:
            code = None
            if "reason" in err:
                code = err.get("reason")
            message = body.get("error_description", err)

        return (code, message)

    def parse_body(self):
        """
        Parse the JSON response body, or raise exceptions as appropriate.

        :return:  JSON dictionary
        :rtype:   ``dict``
        """
        if len(self.body) == 0 and not self.parse_zero_length_body:
            return self.body

        json_error = False
        try:
            body = json.loads(self.body)
        except Exception:
            # If there is both a JSON parsing error and an unsuccessful http
            # response (like a 404), we want to raise the http error and not
            # the JSON one, so don't raise JsonParseError here.
            body = self.body
            json_error = True

        valid_http_codes = [
            httplib.OK,
            httplib.CREATED,
            httplib.ACCEPTED,
            httplib.CONFLICT,
        ]
        if self.status in valid_http_codes:
            if json_error:
                raise JsonParseError(body, self.status, None)
            elif "error" in body:
                (code, message) = self._get_error(body)
                if code == "QUOTA_EXCEEDED":
                    raise QuotaExceededError(message, self.status, code)
                elif code == "RESOURCE_ALREADY_EXISTS":
                    raise ResourceExistsError(message, self.status, code)
                elif code == "alreadyExists":
                    raise ResourceExistsError(message, self.status, code)
                elif code.startswith("RESOURCE_IN_USE"):
                    raise ResourceInUseError(message, self.status, code)
                else:
                    raise GoogleBaseError(message, self.status, code)
            else:
                return body

        elif self.status == httplib.NOT_FOUND:
            if (not json_error) and ("error" in body):
                (code, message) = self._get_error(body)
            else:
                message = body
                code = None
            raise ResourceNotFoundError(message, self.status, code)

        elif self.status == httplib.BAD_REQUEST:
            if (not json_error) and ("error" in body):
                (code, message) = self._get_error(body)
            else:
                message = body
                code = None
            raise InvalidRequestError(message, self.status, code)

        else:
            if (not json_error) and ("error" in body):
                (code, message) = self._get_error(body)
            else:
                message = body
                code = None
            raise GoogleBaseError(message, self.status, code)


class GoogleBaseDriver(BaseDriver):
    name = "Google API"


class GoogleBaseAuthConnection(ConnectionUserAndKey):
    """
    Base class for Google Authentication.  Should be subclassed for specific
    types of authentication.
    """

    driver = GoogleBaseDriver
    responseCls = GoogleResponse
    name = "Google Auth"
    host = "accounts.google.com"
    auth_path = "/o/oauth2/auth"
    redirect_uri_port = 8087

    def __init__(
        self,
        user_id,
        key=None,
        scopes=None,
        redirect_uri="http://127.0.0.1",
        login_hint=None,
        **kwargs,
    ):
        """
        :param  user_id: The email address (for service accounts) or Client ID
                         (for installed apps) to be used for authentication.
        :type   user_id: ``str``

        :param  key: The RSA Key (for service accounts) or file path containing
                     key or Client Secret (for installed apps) to be used for
                     authentication.
        :type   key: ``str``

        :param  scopes: A list of urls defining the scope of authentication
                       to grant.
        :type   scopes: ``list``

        :keyword  redirect_uri: The Redirect URI for the authentication
                                request.  See Google OAUTH2 documentation for
                                more info.
        :type     redirect_uri: ``str``

        :keyword  login_hint: Login hint for authentication request.  Useful
                              for Installed Application authentication.
        :type     login_hint: ``str``
        """
        scopes = scopes or []

        self.scopes = " ".join(scopes)
        self.redirect_uri = redirect_uri
        self.login_hint = login_hint

        super().__init__(user_id, key, **kwargs)

    def add_default_headers(self, headers):
        """
        Add defaults for 'Content-Type' and 'Host' headers.
        """
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Host"] = self.host
        return headers

    def _token_request(self, request_body):
        """
        Return an updated token from a token request body.

        :param  request_body: A dictionary of values to send in the body of the
                              token request.
        :type   request_body: ``dict``

        :return:  A dictionary with updated token information
        :rtype:   ``dict``
        """
        data = urlencode(request_body)
        try:
            response = self.request("/o/oauth2/token", method="POST", data=data)
        except AttributeError:
            raise GoogleAuthError(
                "Invalid authorization response, please " "check your credentials and time drift."
            )
        token_info = response.object
        if "expires_in" in token_info:
            expire_time = _utcnow() + datetime.timedelta(seconds=token_info["expires_in"])
            token_info["expire_time"] = _utc_timestamp(expire_time)
        return token_info

    def refresh_token(self, token_info):
        """
        Refresh the current token.

        Fetch an updated refresh token from internal metadata service.

        :param  token_info: Dictionary containing token information.
                            (Not used, but here for compatibility)
        :type   token_info: ``dict``

        :return:  A dictionary containing updated token information.
        :rtype:   ``dict``
        """
        # pylint: disable=no-member
        return self.get_new_token()


class GoogleInstalledAppAuthConnection(GoogleBaseAuthConnection):
    """Authentication connection for "Installed Application" authentication."""

    _state = "Libcloud Request"

    def get_code(self):
        """
        Give the user a URL that they can visit to authenticate.

        Mocked in libcloud.test.common.google.GoogleTestCase.

        :return:  Code supplied by the user after authenticating
        :rtype:   ``str``
        """
        auth_params = {
            "response_type": "code",
            "client_id": self.user_id,
            "redirect_uri": self._redirect_uri_with_port,
            "scope": self.scopes,
            "state": self._state,
        }
        if self.login_hint:
            auth_params["login_hint"] = self.login_hint

        data = urlencode(auth_params)

        url = "https://{}{}?{}".format(self.host, self.auth_path, data)
        print("\nPlease Go to the following URL and sign in:")
        print(url)
        code = self._receive_code_through_local_loopback()
        return code

    def get_new_token(self):
        """
        Get a new token. Generally used when no previous token exists or there
        is no refresh token

        :return:  Dictionary containing token information
        :rtype:   ``dict``
        """
        code = self.get_code()

        token_request = {
            "code": code,
            "client_id": self.user_id,
            "client_secret": self.key,
            "redirect_uri": self._redirect_uri_with_port,
            "grant_type": "authorization_code",
        }

        return self._token_request(token_request)

    def refresh_token(self, token_info):
        """
        Use the refresh token supplied in the token info to get a new token.

        :param  token_info: Dictionary containing current token information
        :type   token_info: ``dict``

        :return:  A dictionary containing updated token information.
        :rtype:   ``dict``
        """
        if "refresh_token" not in token_info:
            return self.get_new_token()
        refresh_request = {
            "refresh_token": token_info["refresh_token"],
            "client_id": self.user_id,
            "client_secret": self.key,
            "grant_type": "refresh_token",
        }

        new_token = self._token_request(refresh_request)
        if "refresh_token" not in new_token:
            new_token["refresh_token"] = token_info["refresh_token"]
        return new_token

    @property
    def _redirect_uri_with_port(self):
        return self.redirect_uri + ":" + str(self.redirect_uri_port)

    def _receive_code_through_local_loopback(self):
        """
        Start a local HTTP server that listens to a single GET request that is expected to be made
        by the loopback in the sign-in process and stops again afterwards.
        See https://developers.google.com/identity/protocols/oauth2/native-app#redirect-uri_loopback

        :return: The access code that was extracted from the local loopback GET request
        :rtype: ``str``
        """
        access_code = None

        class AccessCodeReceiver(BaseHTTPRequestHandler):
            # noinspection PyMethodParameters,PyPep8Naming
            def do_GET(self_):  # pylint: disable=no-self-argument
                query = urlparse.urlparse(self_.path).query
                query_components = dict(qc.split("=") for qc in query.split("&"))
                if "state" in query_components and query_components["state"] != urllib.parse.quote(
                    self._state
                ):
                    raise ValueError(
                        "States do not match: {} != {}, can't trust authentication".format(
                            self._state, query_components["state"]
                        )
                    )
                nonlocal access_code
                access_code = query_components["code"]
                self_.send_response(200)
                self_.send_header("Content-type", "text/html")
                self_.end_headers()
                self_.wfile.write(b"<html><head><title>Libcloud Sign-In</title></head>")
                self_.wfile.write(b"<body><p>You can now close this tab</p>")

        if (
            "127.0.0.1" in self.redirect_uri
            or "[::1]" in self.redirect_uri
            or "localhost" in self.redirect_uri
        ):
            # HTTPServer does not understand localhost unless you explicitly call it so
            server_address = "localhost", self.redirect_uri_port
        else:
            server_address = self.redirect_uri, self.redirect_uri_port

        server = HTTPServer(server_address=server_address, RequestHandlerClass=AccessCodeReceiver)
        # Only waits for a single request and stops afterwards
        server.handle_request()
        if access_code is None:
            raise RuntimeError(
                "Could not receive OAuth2 code: could not extract code though loopback"
            )
        return access_code


class GoogleServiceAcctAuthConnection(GoogleBaseAuthConnection):
    """Authentication class for "Service Account" authentication."""

    def __init__(self, user_id, key, *args, **kwargs):
        """
        Check to see if cryptography is available, and convert PEM key file
        into a key string, or extract the key from JSON object, string or
        file.

        :param  user_id: Email address to be used for Service Account
                authentication.
        :type   user_id: ``str``

        :param  key: The path to a PEM/JSON file containing the private RSA
        key, or a str/dict containing the PEM/JSON.
        :type   key: ``str`` or ``dict``

        """
        if SHA256 is None:
            raise GoogleAuthError(
                "cryptography library required for " "Service Account Authentication."
            )

        if isinstance(key, dict):
            # if it's a dict, assume it's containing parsed JSON
            key_content = key
            key = None
        else:
            key_path = os.path.expanduser(key)
            if os.path.exists(key_path) and os.path.isfile(key_path):
                # Assume it's a file and read it
                try:
                    with open(key_path) as f:
                        key_content = f.read()
                except OSError:
                    raise GoogleAuthError("Missing (or unreadable) key " "file: '%s'" % key)
            else:
                # assume it's a PEM str or serialized JSON str
                key_content = key
                key = None

        try:
            key_content = json.loads(key_content)
            # check if serialized JSON given
        except TypeError:
            # already a dict
            pass
        except ValueError:
            # it's been a PEM string all along
            pass
        finally:
            if "private_key" in key_content:
                key = key_content["private_key"]
            else:
                key = key_content

        try:
            # check if the key is actually a PEM encoded private key
            serialization.load_pem_private_key(b(key), password=None, backend=default_backend())
        except ValueError as e:
            raise GoogleAuthError("Unable to decode provided PEM key: %s" % e)
        except TypeError as e:
            raise GoogleAuthError("Unable to decode provided PEM key: %s" % e)
        except exceptions.UnsupportedAlgorithm as e:
            raise GoogleAuthError("Unable to decode provided PEM key: %s" % e)

        super().__init__(user_id, key, *args, **kwargs)

    def get_new_token(self):
        """
        Get a new token using the email address and RSA Key.

        :return:  Dictionary containing token information
        :rtype:   ``dict``
        """
        # The header is always the same
        header = {"alg": "RS256", "typ": "JWT"}
        header_enc = base64.urlsafe_b64encode(b(json.dumps(header)))

        # Construct a claim set
        claim_set = {
            "iss": self.user_id,
            "scope": self.scopes,
            "aud": "https://accounts.google.com/o/oauth2/token",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        claim_set_enc = base64.urlsafe_b64encode(b(json.dumps(claim_set)))

        # The message contains both the header and claim set
        message = b".".join((header_enc, claim_set_enc))
        # Then the message is signed using the key supplied
        key = serialization.load_pem_private_key(
            b(self.key), password=None, backend=default_backend()
        )
        signature = key.sign(data=b(message), padding=PKCS1v15(), algorithm=SHA256())
        signature = base64.urlsafe_b64encode(signature)

        # Finally the message and signature are sent to get a token
        jwt = b".".join((message, signature))
        request = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt,
        }

        return self._token_request(request)


class GoogleGCEServiceAcctAuthConnection(GoogleBaseAuthConnection):
    """Authentication class for self-authentication when used with a GCE
    instance that supports serviceAccounts.
    """

    def get_new_token(self):
        """
        Get a new token from the internal metadata service.

        :return:  Dictionary containing token information
        :rtype:   ``dict``
        """
        path = "/instance/service-accounts/default/token"
        http_code, http_reason, token_info = _get_gce_metadata(path)
        if http_code == httplib.NOT_FOUND:
            raise ValueError("Service Accounts are not enabled for this " "GCE instance.")
        if http_code != httplib.OK:
            raise ValueError("Internal GCE Authorization failed: " "'%s'" % str(http_reason))
        token_info = json.loads(token_info)
        if "expires_in" in token_info:
            expire_time = _utcnow() + datetime.timedelta(seconds=token_info["expires_in"])
            token_info["expire_time"] = _utc_timestamp(expire_time)
        return token_info


class GoogleAuthType:
    """
    SA (Service Account),
    IA (Installed Application),
    GCE (Auth from a GCE instance with service account enabled)
    GCS_S3 (Cloud Storage S3 interoperability authentication)
    """

    SA = "SA"
    IA = "IA"
    GCE = "GCE"
    GCS_S3 = "GCS_S3"

    ALL_TYPES = [SA, IA, GCE, GCS_S3]
    OAUTH2_TYPES = [SA, IA, GCE]

    @classmethod
    def guess_type(cls, user_id):
        if cls._is_sa(user_id):
            return cls.SA
        elif cls._is_gcs_s3(user_id):
            return cls.GCS_S3
        elif cls._is_installed_application(user_id):
            # NOTE: This should be before "_is_gce()" call so we avoid
            # querying GCE metadata service if that's not necessary
            return cls.IA
        elif cls._is_gce():
            return cls.GCE
        else:
            # TODO: It's probably safe to throw here, but we return cls.IA
            # for backward compatibility reasons
            return cls.IA

    @classmethod
    def is_oauth2(cls, auth_type):
        return auth_type in cls.OAUTH2_TYPES

    @staticmethod
    def _is_installed_application(user_id):
        return user_id.endswith("apps.googleusercontent.com")

    @staticmethod
    def _is_gce():
        """
        Checks if we can access the GCE metadata server.
        Mocked in libcloud.test.common.google.GoogleTestCase.
        """
        # When using oAuth credentials we check for metadata server first, so
        # if server is unavailable and we retry many times before timing out,
        # this will slow down the driver instantiation when retrying failed
        # requests is enabled globally.
        http_code, http_reason, body = _get_gce_metadata(retry_failed=False)
        if http_code == httplib.OK and body:
            return True
        return False

    @staticmethod
    def _is_gcs_s3(user_id):
        """
        Checks S3 key format: alphanumeric chars starting with GOOG.
        """
        return user_id.startswith("GOOG")

    @staticmethod
    def _is_sa(user_id):
        return user_id.endswith(".gserviceaccount.com")


class GoogleOAuth2Credential:
    default_credential_file = "~/.google_libcloud_auth"

    def __init__(self, user_id, key, auth_type=None, credential_file=None, scopes=None, **kwargs):
        self.auth_type = auth_type or GoogleAuthType.guess_type(user_id)
        if self.auth_type not in GoogleAuthType.ALL_TYPES:
            raise GoogleAuthError("Invalid auth type: %s" % self.auth_type)
        if not GoogleAuthType.is_oauth2(self.auth_type):
            raise GoogleAuthError("Auth type %s cannot be used with OAuth2" % self.auth_type)
        self.user_id = user_id
        self.key = key

        default_credential_file = ".".join([self.default_credential_file, user_id])
        self.credential_file = credential_file or default_credential_file
        # Default scopes to read/write for compute, storage, and dns.
        self.scopes = scopes or [
            "https://www.googleapis.com/auth/compute",
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/ndev.clouddns.readwrite",
        ]

        self.token = self._get_token_from_file()

        if self.auth_type == GoogleAuthType.GCE:
            self.oauth2_conn = GoogleGCEServiceAcctAuthConnection(
                self.user_id, self.scopes, **kwargs
            )
        elif self.auth_type == GoogleAuthType.SA:
            self.oauth2_conn = GoogleServiceAcctAuthConnection(
                self.user_id, self.key, self.scopes, **kwargs
            )
        elif self.auth_type == GoogleAuthType.IA:
            self.oauth2_conn = GoogleInstalledAppAuthConnection(
                self.user_id, self.key, self.scopes, **kwargs
            )
        else:
            raise GoogleAuthError("Invalid auth_type: %s" % str(self.auth_type))

        if self.token is None:
            self.token = self.oauth2_conn.get_new_token()
            self._write_token_to_file()

    @property
    def access_token(self):
        if self.token_expire_utc_datetime < _utcnow():
            self._refresh_token()
        return self.token["access_token"]

    @property
    def token_expire_utc_datetime(self):
        return _from_utc_timestamp(self.token["expire_time"])

    def _refresh_token(self):
        self.token = self.oauth2_conn.refresh_token(self.token)
        self._write_token_to_file()

    def _get_token_from_file(self):
        """
        Read credential file and return token information.
        Mocked in libcloud.test.common.google.GoogleTestCase.

        :return:  Token information dictionary, or None
        :rtype:   ``dict`` or ``None``
        """
        token = None
        filename = os.path.realpath(os.path.expanduser(self.credential_file))

        try:
            with open(filename) as f:
                data = f.read()
            token = json.loads(data)
        except (OSError, ValueError) as e:
            # Note: File related errors (IOError) and errors related to json
            # parsing of the data (ValueError) are not fatal.
            LOG.info('Failed to read cached auth token from file "%s": %s', filename, str(e))

        return token

    def _write_token_to_file(self):
        """
        Write token to credential file.
        Mocked in libcloud.test.common.google.GoogleTestCase.
        """
        filename = os.path.expanduser(self.credential_file)
        filename = os.path.realpath(filename)

        try:
            data = json.dumps(self.token)
            write_flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
            with os.fdopen(os.open(filename, write_flags, int("600", 8)), "w") as f:
                f.write(data)
        except Exception as e:
            # Note: Failure to write (cache) token in a file is not fatal. It
            # simply means degraded performance since we will need to acquire a
            # new token each time script runs.
            LOG.info('Failed to write auth token to file "%s": %s', filename, str(e))


class GoogleBaseConnection(ConnectionUserAndKey, PollingConnection):
    """Base connection class for interacting with Google APIs."""

    driver = GoogleBaseDriver
    responseCls = GoogleResponse
    host = "www.googleapis.com"
    poll_interval = 2.0
    timeout = 180

    def __init__(
        self,
        user_id,
        key=None,
        auth_type=None,
        credential_file=None,
        scopes=None,
        **kwargs,
    ):
        """
        Determine authentication type, set up appropriate authentication
        connection and get initial authentication information.

        :param  user_id: The email address (for service accounts) or Client ID
                         (for installed apps) to be used for authentication.
        :type   user_id: ``str``

        :param  key: The RSA Key (for service accounts) or file path containing
                     key or Client Secret (for installed apps) to be used for
                     authentication.
        :type   key: ``str``

        :keyword  auth_type: See GoogleAuthType class for list and description
                             of accepted values.
                             If not supplied, auth_type will be guessed based
                             on value of user_id or if the code is running
                             on a GCE instance.
        :type     auth_type: ``str``

        :keyword  credential_file: Path to file for caching authentication
                                   information.
        :type     credential_file: ``str``

        :keyword  scopes: List of OAuth2 scope URLs. The empty default sets
                          read/write access to Compute, Storage, and DNS.
        :type     scopes: ``list``
        """
        super().__init__(user_id, key, **kwargs)

        self.oauth2_credential = GoogleOAuth2Credential(
            user_id, key, auth_type, credential_file, scopes, **kwargs
        )

        python_ver = "{}.{}.{}".format(
            sys.version_info[0],
            sys.version_info[1],
            sys.version_info[2],
        )
        ver_platform = "Python {}/{}".format(python_ver, sys.platform)
        self.user_agent_append(ver_platform)

    def add_default_headers(self, headers):
        """
        @inherits: :class:`Connection.add_default_headers`
        """
        headers["Content-Type"] = "application/json"
        headers["Host"] = self.host
        return headers

    def pre_connect_hook(self, params, headers):
        """
        Check to make sure that token hasn't expired.  If it has, get an
        updated token.  Also, add the token to the headers.

        @inherits: :class:`Connection.pre_connect_hook`
        """
        headers["Authorization"] = "Bearer " + self.oauth2_credential.access_token
        return params, headers

    def encode_data(self, data):
        """Encode data to JSON"""
        return json.dumps(data)

    def request(self, *args, **kwargs):
        """
        @inherits: :class:`Connection.request`
        """
        # Adds some retry logic for the occasional
        # "Connection Reset by peer" error.
        retries = 4
        tries = 0
        while tries < (retries - 1):
            try:
                return super().request(*args, **kwargs)
            except OSError as e:
                if e.errno == errno.ECONNRESET:
                    tries = tries + 1
                else:
                    raise e
        # One more time, then give up.
        return super().request(*args, **kwargs)

    def has_completed(self, response):
        """
        Determine if operation has completed based on response.

        :param  response: JSON response
        :type   response: I{responseCls}

        :return:  True if complete, False otherwise
        :rtype:   ``bool``
        """
        if response.object["status"] == "DONE":
            return True
        else:
            return False

    def get_poll_request_kwargs(self, response, context, request_kwargs):
        """
        @inherits: :class:`PollingConnection.get_poll_request_kwargs`
        """
        return {"action": response.object["selfLink"]}

    def morph_action_hook(self, action):
        """
        Update action to correct request path.

        In many places, the Google API returns a full URL to a resource.
        This will strip the scheme and host off of the path and just return
        the request.  Otherwise, it will prepend the base request_path to
        the action.

        :param  action: The action to be called in the http request
        :type   action: ``str``

        :return:  The modified request based on the action
        :rtype:   ``str``
        """
        if action.startswith("https://"):
            u = urlparse.urlsplit(action)
            request = urlparse.urlunsplit(("", "", u[2], u[3], u[4]))
        else:
            request = self.request_path + action
        return request
