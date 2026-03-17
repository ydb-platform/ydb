# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import ssl
import time
import warnings
from typing import Any, Callable, Collection, Mapping, Optional, Union

import urllib3
from urllib3.exceptions import ReadTimeoutError
from urllib3.exceptions import SSLError as UrllibSSLError
from urllib3.util.retry import Retry

from opensearchpy.metrics import Metrics, MetricsNone

from ..compat import reraise_exceptions, urlencode
from ..exceptions import (
    ConnectionError,
    ConnectionTimeout,
    ImproperlyConfigured,
    SSLError,
)
from .base import Connection

# sentinel value for `verify_certs` and `ssl_show_warn`.
# This is used to detect if a user is passing in a value
# for SSL kwargs if also using an SSLContext.
VERIFY_CERTS_DEFAULT = object()
SSL_SHOW_WARN_DEFAULT = object()


def create_ssl_context(**kwargs: Any) -> Any:
    """
    A helper function around creating an SSL context

    https://docs.python.org/3/library/ssl.html#context-creation

    Accepts kwargs in the same manner as `create_default_context`.
    """
    ctx = ssl.create_default_context(**kwargs)
    return ctx


class Urllib3HttpConnection(Connection):
    """
    Default connection class using the `urllib3` library and the http protocol.

    :arg host: hostname of the node (default: localhost)
    :arg port: port to use (integer, default: 9200)
    :arg url_prefix: optional url prefix for opensearch
    :arg timeout: default timeout in seconds (float, default: 10)
    :arg http_auth: optional http auth information as either ':' separated
        string or a tuple
    :arg use_ssl: use ssl for the connection if `True`
    :arg verify_certs: whether to verify SSL certificates
    :arg ssl_show_warn: show warning when verify certs is disabled
    :arg ca_certs: optional path to CA bundle.
        See https://urllib3.readthedocs.io/en/latest/security.html#using-certifi-with-urllib3
        for instructions how to get default set
    :arg client_cert: path to the file containing the private key and the
        certificate, or cert only if using client_key
    :arg client_key: path to the file containing the private key if using
        separate cert and key files (client_cert will contain only the cert)
    :arg ssl_version: version of the SSL protocol to use. Choices are:
        SSLv23 (default) SSLv2 SSLv3 TLSv1 (see ``PROTOCOL_*`` constants in the
        ``ssl`` module for exact options for your environment).
    :arg ssl_assert_hostname: use hostname verification if not `False`
    :arg ssl_assert_fingerprint: verify the supplied certificate fingerprint if not `None`
    :arg pool_maxsize: the number of connections which will be kept open to this
        host. See https://urllib3.readthedocs.io/en/1.4/pools.html#api for more
        information.
    :arg headers: any custom http headers to be add to requests
    :arg http_compress: Use gzip compression
    :arg opaque_id: Send this value in the 'X-Opaque-Id' HTTP header
        For tracing all requests made by this transport.
    :arg metrics: metrics is an instance of a subclass of the
        :class:`~opensearchpy.Metrics` class, used for collecting
        and reporting metrics related to the client's operations;
    """

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        http_auth: Any = None,
        use_ssl: bool = False,
        verify_certs: Any = VERIFY_CERTS_DEFAULT,
        ssl_show_warn: Any = SSL_SHOW_WARN_DEFAULT,
        ca_certs: Any = None,
        client_cert: Any = None,
        client_key: Any = None,
        ssl_version: Any = None,
        ssl_assert_hostname: Any = None,
        ssl_assert_fingerprint: Any = None,
        pool_maxsize: Any = None,
        headers: Any = None,
        ssl_context: Any = None,
        http_compress: Any = None,
        opaque_id: Any = None,
        metrics: Metrics = MetricsNone(),
        **kwargs: Any,
    ) -> None:
        self.metrics = metrics
        # Initialize headers before calling super().__init__().
        self.headers = urllib3.make_headers(keep_alive=True)

        super().__init__(
            host=host,
            port=port,
            use_ssl=use_ssl,
            headers=headers,
            http_compress=http_compress,
            opaque_id=opaque_id,
            **kwargs,
        )

        self.http_auth = http_auth
        if self.http_auth is not None:
            if isinstance(self.http_auth, Callable):  # type: ignore
                pass
            elif isinstance(self.http_auth, (tuple, list)):
                self.headers.update(
                    urllib3.make_headers(basic_auth=":".join(http_auth))
                )
            else:
                self.headers.update(urllib3.make_headers(basic_auth=http_auth))

        pool_class: Any = urllib3.HTTPConnectionPool
        kw = {}

        # if providing an SSL context, raise error if any other SSL related flag is used
        if ssl_context and (
            (verify_certs is not VERIFY_CERTS_DEFAULT)
            or (ssl_show_warn is not SSL_SHOW_WARN_DEFAULT)
            or ca_certs
            or client_cert
            or client_key
            or ssl_version
        ):
            warnings.warn(
                "When using `ssl_context`, all other SSL related kwargs are ignored"
            )

        # if ssl_context provided use SSL by default
        if ssl_context and self.use_ssl:
            pool_class = urllib3.HTTPSConnectionPool
            kw.update(
                {
                    "assert_fingerprint": ssl_assert_fingerprint,
                    "ssl_context": ssl_context,
                }
            )

        elif self.use_ssl:
            pool_class = urllib3.HTTPSConnectionPool
            kw.update(
                {
                    "ssl_version": ssl_version,
                    "assert_hostname": ssl_assert_hostname,
                    "assert_fingerprint": ssl_assert_fingerprint,
                }
            )

            # Convert all sentinel values to their actual default
            # values if not using an SSLContext.
            if verify_certs is VERIFY_CERTS_DEFAULT:
                verify_certs = True
            if ssl_show_warn is SSL_SHOW_WARN_DEFAULT:
                ssl_show_warn = True

            ca_certs = self.default_ca_certs() if ca_certs is None else ca_certs
            if verify_certs:
                if not ca_certs:
                    raise ImproperlyConfigured(
                        "Root certificates are missing for certificate "
                        "validation. Either pass them in using the ca_certs parameter or "
                        "install certifi to use it automatically."
                    )

                kw.update(
                    {
                        "cert_reqs": "CERT_REQUIRED",
                        "ca_certs": ca_certs,
                        "cert_file": client_cert,
                        "key_file": client_key,
                    }
                )
            else:
                kw["cert_reqs"] = "CERT_NONE"
                if ssl_show_warn:
                    warnings.warn(
                        "Connecting to %s using SSL with verify_certs=False is insecure."
                        % self.host
                    )
                if not ssl_show_warn:
                    urllib3.disable_warnings()

        if pool_maxsize and isinstance(pool_maxsize, int):
            kw["maxsize"] = pool_maxsize

        self._urllib3_pool_factory = lambda: pool_class(
            self.hostname, port=self.port, timeout=self.timeout, **kw
        )
        self._create_urllib3_pool()

    def _create_urllib3_pool(self) -> None:
        self.pool = self._urllib3_pool_factory()  # type: ignore

    def perform_request(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        body: Optional[bytes] = None,
        timeout: Optional[Union[int, float]] = None,
        ignore: Collection[int] = (),
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        if self.pool is None:
            self._create_urllib3_pool()
        assert self.pool is not None

        url = self.url_prefix + url
        if params:
            url = f"{url}?{urlencode(params)}"

        full_url = self.host + url

        start = time.time()
        orig_body = body
        try:
            kw = {}
            if timeout:
                kw["timeout"] = timeout

            # in python2 we need to make sure the url and method are not
            # unicode. Otherwise the body will be decoded into unicode too and
            # that will fail (#133, #201).
            if not isinstance(url, str):
                url = url.encode("utf-8")
            if not isinstance(method, str):
                method = method.encode("utf-8")

            request_headers = self.headers.copy()
            request_headers.update(headers or ())

            if self.http_compress and body:
                body = self._gzip_compress(body)
                request_headers["content-encoding"] = "gzip"

            if self.http_auth is not None:
                if isinstance(self.http_auth, Callable):  # type: ignore
                    request_headers.update(self.http_auth(method, full_url, body))

            self.metrics.request_start()

            response = self.pool.urlopen(
                method, url, body, retries=Retry(False), headers=request_headers, **kw
            )
            duration = time.time() - start
            raw_data = response.data.decode("utf-8", "surrogatepass")
        except reraise_exceptions:
            raise
        except Exception as e:
            self.log_request_fail(
                method, full_url, url, orig_body, time.time() - start, exception=e
            )
            if isinstance(e, UrllibSSLError):
                raise SSLError("N/A", str(e), e)
            if isinstance(e, ReadTimeoutError):
                raise ConnectionTimeout("TIMEOUT", str(e), e)
            raise ConnectionError("N/A", str(e), e)
        finally:
            self.metrics.request_end()

        # raise warnings if any from the 'Warnings' header.
        warning_headers = response.headers.get_all("warning", ())
        self._raise_warnings(warning_headers)

        # raise errors based on http status codes, let the client handle those if needed
        if not (200 <= response.status < 300) and response.status not in ignore:
            self.log_request_fail(
                method, full_url, url, orig_body, duration, response.status, raw_data
            )
            self._raise_error(
                response.status,
                raw_data,
                self.get_response_headers(response).get("content-type"),
            )

        self.log_request_success(
            method, full_url, url, orig_body, response.status, raw_data, duration
        )

        return response.status, response.headers, raw_data

    def get_response_headers(self, response: Any) -> Any:
        return {header.lower(): value for header, value in response.headers.items()}

    def close(self) -> None:
        """
        Explicitly closes connection
        """
        if self.pool:
            self.pool.close()
            self.pool = None
