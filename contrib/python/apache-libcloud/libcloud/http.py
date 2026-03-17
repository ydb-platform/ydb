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
Subclass for httplib.HTTPSConnection with optional certificate name
verification, depending on libcloud.security settings.
"""

import os
import warnings

import requests
from requests.adapters import HTTPAdapter

import libcloud.security
from libcloud.utils.py3 import urlparse

try:
    # requests no longer vendors urllib3 in newer versions
    # https://github.com/python/typeshed/issues/6893#issuecomment-1012511758
    from urllib3.poolmanager import PoolManager
except ImportError:
    from requests.packages.urllib3.poolmanager import PoolManager  # type: ignore


__all__ = ["LibcloudBaseConnection", "LibcloudConnection"]

ALLOW_REDIRECTS = 1

# Default timeout for HTTP requests in seconds
DEFAULT_REQUEST_TIMEOUT = 60

HTTP_PROXY_ENV_VARIABLE_NAME = "http_proxy"
HTTPS_PROXY_ENV_VARIABLE_NAME = "https_proxy"


class SignedHTTPSAdapter(HTTPAdapter):
    def __init__(self, cert_file, key_file):
        self.cert_file = cert_file
        self.key_file = key_file
        super().__init__()

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            cert_file=self.cert_file,
            key_file=self.key_file,
        )


class LibcloudBaseConnection:
    """
    Base connection class to inherit from.

    Note: This class should not be instantiated directly.
    """

    session = None

    proxy_scheme = None
    proxy_host = None
    proxy_port = None

    proxy_username = None
    proxy_password = None

    http_proxy_used = False

    ca_cert = None

    def __init__(self):
        self.session = requests.Session()

    def set_http_proxy(self, proxy_url):
        """
        Set a HTTP proxy which will be used with this connection.

        :param proxy_url: Proxy URL (e.g. http://<hostname>:<port> without
                          authentication and
                          http://<username>:<password>@<hostname>:<port> for
                          basic auth authentication information.
        :type proxy_url: ``str``
        """
        result = self._parse_proxy_url(proxy_url=proxy_url)

        scheme = result[0]
        host = result[1]
        port = result[2]
        username = result[3]
        password = result[4]

        self.proxy_scheme = scheme
        self.proxy_host = host
        self.proxy_port = port
        self.proxy_username = username
        self.proxy_password = password
        self.http_proxy_used = True

        self.session.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }

    def _parse_proxy_url(self, proxy_url):
        """
        Parse and validate a proxy URL.

        :param proxy_url: Proxy URL (e.g. http://hostname:3128)
        :type proxy_url: ``str``

        :rtype: ``tuple`` (``scheme``, ``hostname``, ``port``)
        """
        parsed = urlparse.urlparse(proxy_url)

        if parsed.scheme not in ("http", "https"):
            raise ValueError("Only http and https proxies are supported")

        if not parsed.hostname or not parsed.port:
            raise ValueError(
                "proxy_url must be in the following format: " "<scheme>://<proxy host>:<proxy port>"
            )

        proxy_scheme = parsed.scheme
        proxy_host, proxy_port = parsed.hostname, parsed.port

        netloc = parsed.netloc

        if "@" in netloc:
            username_password = netloc.split("@", 1)[0]
            split = username_password.split(":", 1)

            if len(split) < 2:
                raise ValueError("URL is in an invalid format")

            proxy_username, proxy_password = split[0], split[1]
        else:
            proxy_username = None
            proxy_password = None

        return (proxy_scheme, proxy_host, proxy_port, proxy_username, proxy_password)

    def _setup_verify(self):
        self.verify = libcloud.security.VERIFY_SSL_CERT

    def _setup_ca_cert(self, **kwargs):
        # simulating keyword-only argument in Python 2
        ca_certs_path = kwargs.get("ca_cert", libcloud.security.CA_CERTS_PATH)

        if self.verify is False:
            pass
        else:
            if isinstance(ca_certs_path, list):
                msg = (
                    "Providing a list of CA trusts is no longer supported "
                    "since libcloud 2.0. Using the first element in the list. "
                    "See http://libcloud.readthedocs.io/en/latest/other/"
                    "changes_in_2_0.html#providing-a-list-of-ca-trusts-is-no-"
                    "longer-supported"
                )
                warnings.warn(msg, DeprecationWarning)
                self.ca_cert = ca_certs_path[0]
            else:
                self.ca_cert = ca_certs_path

    def _setup_signing(self, cert_file=None, key_file=None):
        """
        Setup request signing by mounting a signing
        adapter to the session
        """
        self.session.mount("https://", SignedHTTPSAdapter(cert_file, key_file))


class LibcloudConnection(LibcloudBaseConnection):
    timeout = None
    host = None
    response = None

    def __init__(self, host, port, secure=None, **kwargs):
        scheme = "https" if secure is not None and secure else "http"
        self.host = "{}://{}{}".format(
            "https" if port == 443 else scheme,
            host,
            ":{}".format(port) if port not in (80, 443) else "",
        )

        # Support for HTTP(s) proxy
        # NOTE: We always only use a single proxy (either HTTP or HTTPS)
        https_proxy_url_env = os.environ.get(HTTPS_PROXY_ENV_VARIABLE_NAME, None)
        http_proxy_url_env = os.environ.get(HTTP_PROXY_ENV_VARIABLE_NAME, https_proxy_url_env)

        # Connection argument has precedence over environment variables
        proxy_url = kwargs.pop("proxy_url", http_proxy_url_env)

        self._setup_verify()
        self._setup_ca_cert()

        LibcloudBaseConnection.__init__(self)

        self.session.timeout = kwargs.pop("timeout", DEFAULT_REQUEST_TIMEOUT)

        if "cert_file" in kwargs or "key_file" in kwargs:
            self._setup_signing(**kwargs)

        if proxy_url:
            self.set_http_proxy(proxy_url=proxy_url)

    @property
    def verification(self):
        """
        The option for SSL verification given to underlying requests
        """
        return self.ca_cert if self.ca_cert is not None else self.verify

    def request(self, method, url, body=None, headers=None, raw=False, stream=False, hooks=None):
        url = urlparse.urljoin(self.host, url)
        headers = self._normalize_headers(headers=headers)

        self.response = self.session.request(
            method=method.lower(),
            url=url,
            data=body,
            headers=headers,
            allow_redirects=ALLOW_REDIRECTS,
            stream=stream,
            verify=self.verification,
            timeout=self.session.timeout,
            hooks=hooks,
        )

    def prepared_request(self, method, url, body=None, headers=None, raw=False, stream=False):
        headers = self._normalize_headers(headers=headers)

        req = requests.Request(method, "".join([self.host, url]), data=body, headers=headers)

        prepped = self.session.prepare_request(req)

        self.response = self.session.send(
            prepped,
            stream=stream,
            verify=self.ca_cert if self.ca_cert is not None else self.verify,
        )

    def getresponse(self):
        return self.response

    def getheaders(self):
        # urlib decoded response body, libcloud has a bug
        # and will not check if content is gzipped, so let's
        # remove headers indicating compressed content.
        if "content-encoding" in self.response.headers:
            del self.response.headers["content-encoding"]
        return self.response.headers

    @property
    def status(self):
        return self.response.status_code

    @property
    def reason(self):
        return None if self.response.status_code > 400 else self.response.text

    def connect(self):  # pragma: no cover
        pass

    def read(self):
        return self.response.content

    def close(self):  # pragma: no cover
        # return connection back to pool
        self.response.close()

    def _normalize_headers(self, headers):
        headers = headers or {}

        # all headers should be strings
        for key, value in headers.items():
            if isinstance(value, (int, float)):
                headers[key] = str(value)

        return headers


class HttpLibResponseProxy:
    """
    Provides a proxy pattern around the :class:`requests.Response`
    object to a :class:`httplib.HTTPResponse` object
    """

    def __init__(self, response):
        self._response = response

    def read(self, amt=None):
        return self._response.text

    def getheader(self, name, default=None):
        """
        Get the contents of the header name, or default
        if there is no matching header.
        """
        if name in self._response.headers.keys():
            return self._response.headers[name]
        else:
            return default

    def getheaders(self):
        """
        Return a list of (header, value) tuples.
        """
        return list(self._response.headers.items())

    @property
    def status(self):
        return self._response.status_code

    @property
    def reason(self):
        return self._response.reason

    @property
    def version(self):
        # requests doesn't expose this
        return "11"

    @property
    def body(self):
        # NOTE: We use property to avoid saving whole response body into RAM
        # See https://github.com/apache/libcloud/pull/1132 for details
        return self._response.content
