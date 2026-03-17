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


import time
import warnings
from typing import Any, Collection, Mapping, MutableMapping, Optional, Union

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from opensearchpy.metrics import Metrics, MetricsNone

from ..compat import reraise_exceptions, string_types, urlencode
from ..exceptions import (
    ConnectionError,
    ConnectionTimeout,
    ImproperlyConfigured,
    SSLError,
)
from .base import Connection


class RequestsHttpConnection(Connection):
    """
    Connection using the `requests` library.

    :arg http_auth: optional http auth information as either ':' separated
        string or a tuple. Any value will be passed into requests as `auth`.
    :arg use_ssl: use ssl for the connection if `True`
    :arg verify_certs: whether to verify SSL certificates
    :arg ssl_show_warn: show warning when verify certs is disabled
    :arg ca_certs: optional path to CA bundle. Defaults to configured OpenSSL
        bundles from environment variables and then certifi before falling
        back to the standard requests bundle to improve consistency with
        other Connection implementations
    :arg client_cert: path to the file containing the private key and the
        certificate, or cert only if using client_key
    :arg client_key: path to the file containing the private key if using
        separate cert and key files (client_cert will contain only the cert)
    :arg headers: any custom http headers to be add to requests
    :arg http_compress: Use gzip compression
    :arg opaque_id: Send this value in the 'X-Opaque-Id' HTTP header
        For tracing all requests made by this transport.
    :arg pool_maxsize: Maximum connection pool size used by pool-manager
        For custom connection-pooling on current session
    :arg metrics: metrics is an instance of a subclass of the
        :class:`~opensearchpy.Metrics` class, used for collecting
        and reporting metrics related to the client's operations;
    :arg proxies: optional dictionary mapping protocol to the URL of the proxy.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        http_auth: Any = None,
        use_ssl: bool = False,
        verify_certs: bool = True,
        ssl_show_warn: bool = True,
        ca_certs: Any = None,
        client_cert: Any = None,
        client_key: Any = None,
        headers: Any = None,
        http_compress: Any = None,
        opaque_id: Any = None,
        pool_maxsize: Any = None,
        metrics: Metrics = MetricsNone(),
        proxies: Optional[MutableMapping[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        self.metrics = metrics
        if not REQUESTS_AVAILABLE:
            raise ImproperlyConfigured(
                "Please install requests to use RequestsHttpConnection."
            )

        # Initialize Session so .headers works before calling super().__init__().
        self.session = requests.Session()
        for key in list(self.session.headers):
            self.session.headers.pop(key)

        # Mount http-adapter with custom connection-pool size. Default=10
        if pool_maxsize and isinstance(pool_maxsize, int):
            pool_adapter = requests.adapters.HTTPAdapter(pool_maxsize=pool_maxsize)
            self.session.mount("http://", pool_adapter)
            self.session.mount("https://", pool_adapter)

        super().__init__(
            host=host,
            port=port,
            use_ssl=use_ssl,
            headers=headers,
            http_compress=http_compress,
            opaque_id=opaque_id,
            **kwargs,
        )

        if not self.http_compress:
            # Need to set this to 'None' otherwise Requests adds its own.
            self.session.headers["accept-encoding"] = None  # type: ignore

        if http_auth is not None:
            if isinstance(http_auth, (tuple, list)):
                http_auth = tuple(http_auth)
            elif isinstance(http_auth, string_types):
                http_auth = tuple(http_auth.split(":", 1))  # type: ignore
            self.session.auth = http_auth

        self.session.proxies = proxies or {}

        self.base_url = f"{self.host}{self.url_prefix}"
        self.session.verify = verify_certs
        if not client_key:
            self.session.cert = client_cert
        elif client_cert:
            # cert is a tuple of (certfile, keyfile)
            self.session.cert = (client_cert, client_key)
        if ca_certs:
            if not verify_certs:
                raise ImproperlyConfigured(
                    "You cannot pass CA certificates when verify SSL is off."
                )
            self.session.verify = ca_certs
        elif verify_certs:
            ca_certs = self.default_ca_certs()
            if ca_certs:
                self.session.verify = ca_certs

        if not ssl_show_warn:
            requests.packages.urllib3.disable_warnings()  # type: ignore

        if self.use_ssl and not verify_certs and ssl_show_warn:
            warnings.warn(
                "Connecting to %s using SSL with verify_certs=False is insecure."
                % self.host
            )

    def perform_request(  # type: ignore
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        body: Optional[bytes] = None,
        timeout: Optional[Union[int, float]] = None,
        allow_redirects: Optional[bool] = True,
        ignore: Collection[int] = (),
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        url = self.base_url + url
        headers = headers or {}
        if params:
            url = f"{url}?{urlencode(params or {})}"

        orig_body = body
        if self.http_compress and body:
            body = self._gzip_compress(body)
            headers["content-encoding"] = "gzip"  # type: ignore

        start = time.time()
        request = requests.Request(method=method, headers=headers, url=url, data=body)
        prepared_request = self.session.prepare_request(request)
        settings = self.session.merge_environment_settings(
            prepared_request.url, {}, None, None, None
        )
        send_kwargs: Any = {
            "timeout": timeout or self.timeout,
            "allow_redirects": allow_redirects,
        }
        send_kwargs.update(settings)
        try:
            self.metrics.request_start()
            response = self.session.send(prepared_request, **send_kwargs)
            duration = time.time() - start
            raw_data = response.content.decode("utf-8", "surrogatepass")
        except reraise_exceptions:
            raise
        except Exception as e:
            self.log_request_fail(
                method,
                url,
                prepared_request.path_url,
                orig_body,
                time.time() - start,
                exception=e,
            )
            if isinstance(e, requests.exceptions.SSLError):
                raise SSLError("N/A", str(e), e)
            if isinstance(e, requests.Timeout):
                raise ConnectionTimeout("TIMEOUT", str(e), e)
            raise ConnectionError("N/A", str(e), e)
        finally:
            self.metrics.request_end()

        # raise warnings if any from the 'Warnings' header.
        warnings_headers = (
            (response.headers["warning"],) if "warning" in response.headers else ()
        )
        self._raise_warnings(warnings_headers)

        # raise errors based on http status codes, let the client handle those if needed
        if (
            not (200 <= response.status_code < 300)
            and response.status_code not in ignore
        ):
            self.log_request_fail(
                method,
                url,
                response.request.path_url,
                orig_body,
                duration,
                response.status_code,
                raw_data,
            )
            self._raise_error(
                response.status_code,
                raw_data,
                response.headers.get("Content-Type"),
            )

        self.log_request_success(
            method,
            url,
            response.request.path_url,
            orig_body,
            response.status_code,
            raw_data,
            duration,
        )

        return response.status_code, response.headers, raw_data

    @property
    def headers(self) -> Any:  # type: ignore
        return self.session.headers

    def close(self) -> None:
        """
        Explicitly closes connections
        """
        self.session.close()
