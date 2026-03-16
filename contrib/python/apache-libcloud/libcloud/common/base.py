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
import ssl
import copy
import json
import time
import socket
import binascii
from typing import Any, Dict, Type, Union, Optional

import libcloud
from libcloud.http import LibcloudConnection, HttpLibResponseProxy
from libcloud.utils.py3 import ET, httplib, urlparse, urlencode
from libcloud.utils.misc import lowercase_keys
from libcloud.utils.retry import Retry
from libcloud.common.types import LibcloudError, MalformedResponseError
from libcloud.common.exceptions import exception_from_message

__all__ = [
    "RETRY_FAILED_HTTP_REQUESTS",
    "BaseDriver",
    "Connection",
    "PollingConnection",
    "ConnectionKey",
    "ConnectionUserAndKey",
    "CertificateConnection",
    "Response",
    "HTTPResponse",
    "JsonResponse",
    "XmlResponse",
    "RawResponse",
]

# Module level variable indicates if the failed HTTP requests should be retried
RETRY_FAILED_HTTP_REQUESTS = False

# Set to True to allow double slashes in the URL path. This way
# morph_action_hook() won't strip potentially double slashes in the URLs.
# This is to support scenarios such as this one -
# https://github.com/apache/libcloud/issues/1529.
# We default it to False for backward compatibility reasons.
ALLOW_PATH_DOUBLE_SLASHES = False


class LazyObject:
    """An object that doesn't get initialized until accessed."""

    @classmethod
    def _proxy(cls, *lazy_init_args, **lazy_init_kwargs):
        class Proxy(cls):
            _lazy_obj = None

            def __init__(self):
                # Must override the lazy_cls __init__
                pass

            def __getattribute__(self, attr):
                lazy_obj = object.__getattribute__(self, "_get_lazy_obj")()
                return getattr(lazy_obj, attr)

            def __setattr__(self, attr, value):
                lazy_obj = object.__getattribute__(self, "_get_lazy_obj")()
                setattr(lazy_obj, attr, value)

            def _get_lazy_obj(self):
                lazy_obj = object.__getattribute__(self, "_lazy_obj")
                if lazy_obj is None:
                    lazy_obj = cls(*lazy_init_args, **lazy_init_kwargs)
                    object.__setattr__(self, "_lazy_obj", lazy_obj)
                return lazy_obj

        return Proxy()

    @classmethod
    def lazy(cls, *lazy_init_args, **lazy_init_kwargs):
        """Create a lazily instantiated instance of the subclass, cls."""
        return cls._proxy(*lazy_init_args, **lazy_init_kwargs)


class HTTPResponse(httplib.HTTPResponse):
    # On python 2.6 some calls can hang because HEAD isn't quite properly
    # supported.
    # In particular this happens on S3 when calls are made to get_object to
    # objects that don't exist.
    # This applies the behaviour from 2.7, fixing the hangs.
    def read(self, amt=None):
        if self.fp is None:
            return ""

        if self._method == "HEAD":
            self.close()
            return ""

        return httplib.HTTPResponse.read(self, amt)


class Response:
    """
    A base Response class to derive from.
    """

    # Response status code
    status = httplib.OK  # type: int
    # Response headers
    headers = {}  # type: dict

    # Raw response body
    body = None

    # Parsed response body
    object = None

    error = None  # Reason returned by the server.
    connection = None  # Parent connection class
    parse_zero_length_body = False

    def __init__(self, response, connection):
        """
        :param response: HTTP response object. (optional)
        :type response: :class:`httplib.HTTPResponse`

        :param connection: Parent connection object.
        :type connection: :class:`.Connection`
        """
        self.connection = connection

        # http.client In Python 3 doesn't automatically lowercase the header
        # names
        self.headers = lowercase_keys(dict(response.headers))
        self.error = response.reason
        self.status = response.status_code
        self.request = response.request
        self.iter_content = response.iter_content

        self.body = (
            response.text.strip()
            if response.text is not None and hasattr(response.text, "strip")
            else ""
        )

        if not self.success():
            raise exception_from_message(
                code=self.status, message=self.parse_error(), headers=self.headers
            )

        self.object = self.parse_body()

    def parse_body(self):
        """
        Parse response body.

        Override in a provider's subclass.

        :return: Parsed body.
        :rtype: ``str``
        """
        return self.body if self.body is not None else ""

    def parse_error(self):
        """
        Parse the error messages.

        Override in a provider's subclass.

        :return: Parsed error.
        :rtype: ``str``
        """
        return self.body

    def success(self):
        """
        Determine if our request was successful.

        The meaning of this can be arbitrary; did we receive OK status? Did
        the node get created? Were we authenticated?

        :rtype: ``bool``
        :return: ``True`` or ``False``
        """
        # pylint: disable=E1101
        import requests

        return self.status in [
            requests.codes.ok,
            requests.codes.created,
            httplib.OK,
            httplib.CREATED,
            httplib.ACCEPTED,
        ]


class JsonResponse(Response):
    """
    A Base JSON Response class to derive from.
    """

    def parse_body(self):
        if len(self.body) == 0 and not self.parse_zero_length_body:
            return self.body

        try:
            body = json.loads(self.body)
        except Exception:
            raise MalformedResponseError(
                "Failed to parse JSON", body=self.body, driver=self.connection.driver
            )
        return body

    parse_error = parse_body


class XmlResponse(Response):
    """
    A Base XML Response class to derive from.
    """

    def parse_body(self):
        if len(self.body) == 0 and not self.parse_zero_length_body:
            return self.body

        try:
            try:
                body = ET.XML(self.body)
            except ValueError:
                # lxml wants a bytes and tests are basically hard-coded to str
                body = ET.XML(self.body.encode("utf-8"))
        except Exception:
            raise MalformedResponseError(
                "Failed to parse XML", body=self.body, driver=self.connection.driver
            )
        return body

    parse_error = parse_body


class RawResponse(Response):
    def __init__(self, connection, response=None):
        """
        :param connection: Parent connection object.
        :type connection: :class:`.Connection`
        """
        self._status = None
        self._response = None
        self._headers = {}
        self._error = None
        self._reason = None
        self.connection = connection
        if response is not None:
            self.headers = lowercase_keys(dict(response.headers))
            self.error = response.reason
            self.status = response.status_code
            self.request = response.request
            self.iter_content = response.iter_content

        if not self.success():
            self.parse_error()

    def success(self):
        """
        Determine if our request was successful.

        The meaning of this can be arbitrary; did we receive OK status? Did
        the node get created? Were we authenticated?

        :rtype: ``bool``
        :return: ``True`` or ``False``
        """
        # pylint: disable=E1101
        import requests

        return self.status in [
            requests.codes.ok,
            requests.codes.created,
            httplib.OK,
            httplib.CREATED,
            httplib.ACCEPTED,
        ]

    @property
    def response(self):
        if not self._response:
            response = self.connection.connection.getresponse()
            self._response = HttpLibResponseProxy(response)
            if not self.success():
                self.parse_error()
        return self._response

    @property
    def body(self):
        # Note: We use property to avoid saving whole response body into RAM
        # See https://github.com/apache/libcloud/pull/1132 for details
        return self.response.body

    @property
    def reason(self):
        if not self._reason:
            self._reason = self.response.reason
        return self._reason


class Connection:
    """
    A Base Connection class to derive from.
    """

    conn_class = LibcloudConnection

    responseCls = Response
    rawResponseCls = RawResponse
    retryCls = Retry
    connection = None
    host = "127.0.0.1"  # type: str
    port = 443
    timeout = None  # type: Optional[Union[int, float]]
    secure = 1
    driver = None  # type:  Type[BaseDriver]
    action = None
    cache_busting = False
    backoff = None
    retry_delay = None

    allow_insecure = True

    def __init__(
        self,
        secure=True,
        host=None,
        port=None,
        url=None,
        timeout=None,
        proxy_url=None,
        retry_delay=None,
        backoff=None,
    ):
        self.secure = secure and 1 or 0
        self.ua = []
        self.context = {}

        if not self.allow_insecure and not secure:
            # TODO: We should eventually switch to whitelist instead of
            # blacklist approach
            raise ValueError("Non https connections are not allowed (use " "secure=True)")

        self.request_path = ""

        if host:
            self.host = host

        if port is not None:
            self.port = port
        else:
            if self.secure == 1:
                self.port = 443
            else:
                self.port = 80

        if url:
            (
                self.host,
                self.port,
                self.secure,
                self.request_path,
            ) = self._tuple_from_url(url)

        self.timeout = timeout or self.timeout
        self.retry_delay = retry_delay
        self.backoff = backoff
        self.proxy_url = proxy_url

    def set_http_proxy(self, proxy_url):
        """
        Set a HTTP / HTTPS proxy which will be used with this connection.

        :param proxy_url: Proxy URL (e.g. http://<hostname>:<port> without
                          authentication and
                          <scheme>://<username>:<password>@<hostname>:<port>
                          for basic auth authentication information.
        :type proxy_url: ``str``
        """
        self.proxy_url = proxy_url

        # NOTE: Because of the way connection instantion works, we need to call
        # self.connection.set_http_proxy() here. Just setting "self.proxy_url"
        # won't work.
        self.connection.set_http_proxy(proxy_url=proxy_url)

    def set_context(self, context):
        if not isinstance(context, dict):
            raise TypeError("context needs to be a dictionary")

        self.context = context

    def reset_context(self):
        self.context = {}

    def _tuple_from_url(self, url):
        secure = 1
        port = None
        (scheme, netloc, request_path, param, query, fragment) = urlparse.urlparse(url)

        if scheme not in ["http", "https"]:
            raise LibcloudError("Invalid scheme: {} in url {}".format(scheme, url))

        if scheme == "http":
            secure = 0

        if ":" in netloc:
            netloc, port = netloc.rsplit(":")
            port = int(port)

        if not port:
            if scheme == "http":
                port = 80
            else:
                port = 443

        host = netloc
        port = int(port)

        return (host, port, secure, request_path)

    def connect(self, host=None, port=None, base_url=None, **kwargs):
        """
        Establish a connection with the API server.

        :type host: ``str``
        :param host: Optional host to override our default

        :type port: ``int``
        :param port: Optional port to override our default

        :returns: A connection
        """
        # prefer the attribute base_url if its set or sent
        connection = None
        secure = self.secure

        if getattr(self, "base_url", None) and base_url is None:
            (host, port, secure, request_path) = self._tuple_from_url(getattr(self, "base_url"))
        elif base_url is not None:
            (host, port, secure, request_path) = self._tuple_from_url(base_url)
        else:
            host = host or self.host
            port = port or self.port

        # Make sure port is an int
        port = int(port)

        if not hasattr(kwargs, "host"):
            kwargs.update({"host": host})

        if not hasattr(kwargs, "port"):
            kwargs.update({"port": port})

        if not hasattr(kwargs, "secure"):
            kwargs.update({"secure": self.secure})

        if not hasattr(kwargs, "key_file") and hasattr(self, "key_file"):
            kwargs.update({"key_file": getattr(self, "key_file")})

        if not hasattr(kwargs, "cert_file") and hasattr(self, "cert_file"):
            kwargs.update({"cert_file": getattr(self, "cert_file")})

        if self.timeout:
            kwargs.update({"timeout": self.timeout})

        if self.proxy_url:
            kwargs.update({"proxy_url": self.proxy_url})

        connection = self.conn_class(**kwargs)
        # You can uncomment this line, if you setup a reverse proxy server
        # which proxies to your endpoint, and lets you easily capture
        # connections in cleartext when you setup the proxy to do SSL
        # for you
        # connection = self.conn_class("127.0.0.1", 8080)

        self.connection = connection

    def _user_agent(self):
        user_agent_suffix = " ".join(["(%s)" % x for x in self.ua])

        if self.driver:
            user_agent = "libcloud/{} ({}) {}".format(
                libcloud.__version__,
                self.driver.name,
                user_agent_suffix,
            )
        else:
            user_agent = "libcloud/{} {}".format(libcloud.__version__, user_agent_suffix)

        return user_agent

    def user_agent_append(self, token):
        """
        Append a token to a user agent string.

        Users of the library should call this to uniquely identify their
        requests to a provider.

        :type token: ``str``
        :param token: Token to add to the user agent.
        """
        self.ua.append(token)

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
    ):
        """
        Request a given `action`.

        Basically a wrapper around the connection
        object's `request` that does some helpful pre-processing.

        :type action: ``str``
        :param action: A path. This can include arguments. If included,
            any extra parameters are appended to the existing ones.

        :type params: ``dict``
        :param params: Optional mapping of additional parameters to send. If
            None, leave as an empty ``dict``.

        :type data: ``unicode``
        :param data: A body of data to send with the request.

        :type headers: ``dict``
        :param headers: Extra headers to add to the request
            None, leave as an empty ``dict``.

        :type method: ``str``
        :param method: An HTTP method such as "GET" or "POST".

        :type raw: ``bool``
        :param raw: True to perform a "raw" request aka only send the headers
                     and use the rawResponseCls class. This is used with
                     storage API when uploading a file.

        :type stream: ``bool``
        :param stream: True to return an iterator in Response.iter_content
                    and allow streaming of the response data
                    (for downloading large files)

        :param retry_failed: True if failed requests should be retried. This
                              argument can override module level constant and
                              environment variable value on per-request basis.

        :return: An :class:`Response` instance.
        :rtype: :class:`Response` instance

        """
        if params is None:
            params = {}
        else:
            params = copy.copy(params)

        if headers is None:
            headers = {}
        else:
            headers = copy.copy(headers)

        retry_enabled = (
            os.environ.get("LIBCLOUD_RETRY_FAILED_HTTP_REQUESTS", False)
            or RETRY_FAILED_HTTP_REQUESTS
        )

        # Method level argument has precedence over module level constant and
        # environment variable
        if retry_failed is not None:
            retry_enabled = retry_failed

        action = self.morph_action_hook(action)
        self.action = action
        self.method = method
        self.data = data

        # Extend default parameters
        params = self.add_default_params(params)

        # Add cache busting parameters (if enabled)
        if self.cache_busting and method == "GET":
            params = self._add_cache_busting_to_params(params=params)

        # Extend default headers
        headers = self.add_default_headers(headers)

        # We always send a user-agent header
        headers.update({"User-Agent": self._user_agent()})

        # Indicate that we support gzip and deflate compression
        headers.update({"Accept-Encoding": "gzip,deflate"})

        port = int(self.port)

        if port not in (80, 443):
            headers.update({"Host": "%s:%d" % (self.host, port)})
        else:
            headers.update({"Host": self.host})

        if data:
            data = self.encode_data(data)

        params, headers = self.pre_connect_hook(params, headers)

        if params:
            if "?" in action:
                url = "&".join((action, urlencode(params, doseq=True)))
            else:
                url = "?".join((action, urlencode(params, doseq=True)))
        else:
            url = action

        # IF connection has not yet been established
        if self.connection is None:
            self.connect()

        request_to_be_executed = self._retryable_request

        if retry_enabled:
            retry_request = self.retryCls(
                retry_delay=self.retry_delay, timeout=self.timeout, backoff=self.backoff
            )
            request_to_be_executed = retry_request(self._retryable_request)

        return request_to_be_executed(
            url=url, method=method, raw=raw, stream=stream, headers=headers, data=data
        )

    def _retryable_request(
        self,
        url: str,
        data: bytes,
        headers: Dict[str, Any],
        method: str,
        raw: bool,
        stream: bool,
    ) -> Union[RawResponse, Response]:
        try:
            # @TODO: Should we just pass File object as body to request method
            # instead of dealing with splitting and sending the file ourselves?
            assert self.connection is not None

            if raw:
                self.connection.prepared_request(
                    method=method,
                    url=url,
                    body=data,
                    headers=headers,
                    raw=raw,
                    stream=stream,
                )
            else:
                self.connection.request(
                    method=method, url=url, body=data, headers=headers, stream=stream
                )

        except socket.gaierror as e:
            message = str(e)
            errno = getattr(e, "errno", None)

            if errno == -5:
                # Throw a more-friendly exception on "no address associated
                # with hostname" error. This error could simply indicate that
                # "host" Connection class attribute is set to an incorrect
                # value
                class_name = self.__class__.__name__
                msg = (
                    '%s. Perhaps "host" Connection class attribute '
                    "(%s.connection) is set to an invalid, non-hostname "
                    "value (%s)?" % (message, class_name, self.host)
                )
                raise socket.gaierror(msg)  # type: ignore
            self.reset_context()
            raise e
        except ssl.SSLError as e:
            self.reset_context()
            raise ssl.SSLError(str(e))

        if raw:
            responseCls = self.rawResponseCls
            kwargs = {"connection": self, "response": self.connection.getresponse()}
        else:
            responseCls = self.responseCls
            kwargs = {"connection": self, "response": self.connection.getresponse()}

        try:
            response = responseCls(**kwargs)
        finally:
            # Always reset the context after the request has completed
            self.reset_context()

        return response

    def morph_action_hook(self, action):
        """
        Here we strip any duplicated leading or trailing slashes to
        prevent typos and other issues where some APIs don't correctly
        handle double slashes.

        Keep in mind that in some situations, "/" is a valid path name
        so we have a module flag which disables this behavior
        (https://github.com/apache/libcloud/issues/1529).
        """
        if ALLOW_PATH_DOUBLE_SLASHES:
            # Special case to support scenarios where double slashes are
            # valid - e.g. for S3 paths - /bucket//path1/path2.txt
            return self.request_path + action

        url = urlparse.urljoin(self.request_path.lstrip("/").rstrip("/") + "/", action.lstrip("/"))

        if not url.startswith("/"):
            return "/" + url
        else:
            return url

    def add_default_params(self, params):
        """
        Adds default parameters (such as API key, version, etc.)
        to the passed `params`

        Should return a dictionary.
        """
        return params

    def add_default_headers(self, headers):
        """
        Adds default headers (such as Authorization, X-Foo-Bar)
        to the passed `headers`

        Should return a dictionary.
        """
        return headers

    def pre_connect_hook(self, params, headers):
        """
        A hook which is called before connecting to the remote server.
        This hook can perform a final manipulation on the params, headers and
        url parameters.

        :type params: ``dict``
        :param params: Request parameters.

        :type headers: ``dict``
        :param headers: Request headers.
        """
        return params, headers

    def encode_data(self, data):
        """
        Encode body data.

        Override in a provider's subclass.
        """
        return data

    def _add_cache_busting_to_params(self, params):
        """
        Add cache busting parameter to the query parameters of a GET request.

        Parameters are only added if "cache_busting" class attribute is set to
        True.

        Note: This should only be used with *naughty* providers which use
        excessive caching of responses.
        """
        cache_busting_value = binascii.hexlify(os.urandom(8)).decode("ascii")

        if isinstance(params, dict):
            params["cache-busting"] = cache_busting_value
        else:
            params.append(("cache-busting", cache_busting_value))

        return params


class PollingConnection(Connection):
    """
    Connection class which can also work with the async APIs.

    After initial requests, this class periodically polls for jobs status and
    waits until the job has finished.
    If job doesn't finish in timeout seconds, an Exception thrown.
    """

    poll_interval = 0.5
    timeout = 200
    request_method = "request"

    def async_request(
        self, action, params=None, data=None, headers=None, method="GET", context=None
    ):
        """
        Perform an 'async' request to the specified path. Keep in mind that
        this function is *blocking* and 'async' in this case means that the
        hit URL only returns a job ID which is the periodically polled until
        the job has completed.

        This function works like this:

        - Perform a request to the specified path. Response should contain a
          'job_id'.

        - Returned 'job_id' is then used to construct a URL which is used for
          retrieving job status. Constructed URL is then periodically polled
          until the response indicates that the job has completed or the
          timeout of 'self.timeout' seconds has been reached.

        :type action: ``str``
        :param action: A path

        :type params: ``dict``
        :param params: Optional mapping of additional parameters to send. If
            None, leave as an empty ``dict``.

        :type data: ``unicode``
        :param data: A body of data to send with the request.

        :type headers: ``dict``
        :param headers: Extra headers to add to the request
            None, leave as an empty ``dict``.

        :type method: ``str``
        :param method: An HTTP method such as "GET" or "POST".

        :type context: ``dict``
        :param context: Context dictionary which is passed to the functions
                        which construct initial and poll URL.

        :return: An :class:`Response` instance.
        :rtype: :class:`Response` instance
        """

        request = getattr(self, self.request_method)
        kwargs = self.get_request_kwargs(
            action=action,
            params=params,
            data=data,
            headers=headers,
            method=method,
            context=context,
        )
        response = request(**kwargs)
        kwargs = self.get_poll_request_kwargs(
            response=response, context=context, request_kwargs=kwargs
        )

        end = time.time() + self.timeout
        completed = False
        while time.time() < end and not completed:
            response = request(**kwargs)
            completed = self.has_completed(response=response)
            if not completed:
                time.sleep(self.poll_interval)

        if not completed:
            raise LibcloudError("Job did not complete in %s seconds" % (self.timeout))

        return response

    def get_request_kwargs(
        self, action, params=None, data=None, headers=None, method="GET", context=None
    ):
        """
        Arguments which are passed to the initial request() call inside
        async_request.
        """
        kwargs = {
            "action": action,
            "params": params,
            "data": data,
            "headers": headers,
            "method": method,
        }
        return kwargs

    def get_poll_request_kwargs(self, response, context, request_kwargs):
        """
        Return keyword arguments which are passed to the request() method when
        polling for the job status.

        :param response: Response object returned by poll request.
        :type response: :class:`HTTPResponse`

        :param request_kwargs: Kwargs previously used to initiate the
                                  poll request.
        :type response: ``dict``

        :return ``dict`` Keyword arguments
        """
        raise NotImplementedError("get_poll_request_kwargs not implemented")

    def has_completed(self, response):
        """
        Return job completion status.

        :param response: Response object returned by poll request.
        :type response: :class:`HTTPResponse`

        :return ``bool`` True if the job has completed, False otherwise.
        """
        raise NotImplementedError("has_completed not implemented")


class ConnectionKey(Connection):
    """
    Base connection class which accepts a single ``key`` argument.
    """

    def __init__(
        self,
        key,
        secure=True,
        host=None,
        port=None,
        url=None,
        timeout=None,
        proxy_url=None,
        backoff=None,
        retry_delay=None,
    ):
        """
        Initialize `user_id` and `key`; set `secure` to an ``int`` based on
        passed value.
        """
        super().__init__(
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            proxy_url=proxy_url,
            backoff=backoff,
            retry_delay=retry_delay,
        )
        self.key = key


class CertificateConnection(Connection):
    """
    Base connection class which accepts a single ``cert_file`` argument.
    """

    def __init__(
        self,
        cert_file,
        secure=True,
        host=None,
        port=None,
        url=None,
        proxy_url=None,
        timeout=None,
        backoff=None,
        retry_delay=None,
    ):
        """
        Initialize `cert_file`; set `secure` to an ``int`` based on
        passed value.
        """
        super().__init__(
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            backoff=backoff,
            retry_delay=retry_delay,
            proxy_url=proxy_url,
        )

        self.cert_file = cert_file


class KeyCertificateConnection(CertificateConnection):
    """
    Base connection class which accepts both ``key_file`` and ``cert_file``
    argument.
    """

    key_file = None

    def __init__(
        self,
        key_file,
        cert_file,
        secure=True,
        host=None,
        port=None,
        url=None,
        proxy_url=None,
        timeout=None,
        backoff=None,
        retry_delay=None,
        ca_cert=None,
    ):
        """
        Initialize `cert_file`; set `secure` to an ``int`` based on
        passed value.
        """
        super().__init__(
            cert_file,
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            backoff=backoff,
            retry_delay=retry_delay,
            proxy_url=proxy_url,
        )

        self.key_file = key_file


class ConnectionUserAndKey(ConnectionKey):
    """
    Base connection class which accepts a ``user_id`` and ``key`` argument.
    """

    user_id = None  # type: int

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
        backoff=None,
        retry_delay=None,
    ):
        super().__init__(
            key,
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            backoff=backoff,
            retry_delay=retry_delay,
            proxy_url=proxy_url,
        )
        self.user_id = user_id


class BaseDriver:
    """
    Base driver class from which other classes can inherit from.
    """

    connectionCls = ConnectionKey  # type: Type[Connection]

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=None,
        **kwargs,
    ):
        """
        :param    key:    API key or username to be used (required)
        :type     key:    ``str``

        :param    secret: Secret password to be used (required)
        :type     secret: ``str``

        :param    secure: Whether to use HTTPS or HTTP. Note: Some providers
                            only support HTTPS, and it is on by default.
        :type     secure: ``bool``

        :param    host: Override hostname used for connections.
        :type     host: ``str``

        :param    port: Override port used for connections.
        :type     port: ``int``

        :param    api_version: Optional API version. Only used by drivers
                                 which support multiple API versions.
        :type     api_version: ``str``

        :param region: Optional driver region. Only used by drivers which
                       support multiple regions.
        :type region: ``str``

        :rtype: ``None``
        """

        self.key = key
        self.secret = secret
        self.secure = secure
        self.api_version = api_version
        self.region = region

        conn_kwargs = self._ex_connection_class_kwargs()
        conn_kwargs.update(
            {
                "timeout": kwargs.pop("timeout", None),
                "retry_delay": kwargs.pop("retry_delay", None),
                "backoff": kwargs.pop("backoff", None),
                "proxy_url": kwargs.pop("proxy_url", None),
            }
        )

        args = [self.key]

        if self.secret is not None:
            args.append(self.secret)

        args.append(secure)

        host = conn_kwargs.pop("host", None) or host

        if host is not None:
            args.append(host)

        if port is not None:
            args.append(port)

        self.connection = self.connectionCls(*args, **conn_kwargs)
        self.connection.driver = self
        self.connection.connect()

    def _ex_connection_class_kwargs(self):
        """
        Return extra connection keyword arguments which are passed to the
        Connection class constructor.
        """
        return {}
