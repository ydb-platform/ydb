# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from .. import _typing as t
from .._async_compat.concurrency import Lock
from .._conf import (
    Config,
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)


if t.TYPE_CHECKING:
    import ssl

    from .._auth_management import ClientCertificate


class PoolConfig(Config):
    """Connection pool configuration."""

    #: Max Connection Lifetime
    max_connection_lifetime = 3600  # seconds
    # The maximum duration the driver will keep a connection for before being
    # removed from the pool.

    #: Timeout after which idle connections will be checked for liveness
    #: before returned from the pool.
    liveness_check_timeout = None

    #: Max Connection Pool Size
    max_connection_pool_size = 100
    # The maximum total number of connections allowed, per host
    # (i.e. cluster nodes), to be managed by the connection pool.

    #: Connection Timeout
    connection_timeout = 30.0  # seconds
    # The maximum amount of time to wait for a TCP connection to be
    # established.

    #: Connection Write Timeout
    connection_write_timeout = 30.0  # seconds
    # The maximum amount of time to wait for I/O write operations to complete.

    #: Custom Resolver
    resolver = None
    # Custom resolver function, returning list of resolved addresses.

    #: Encrypted
    encrypted = False
    # Specify whether to use an encrypted connection between the driver and
    # server.

    #: SSL Certificates to Trust
    trusted_certificates = TrustSystemCAs()
    # Specify how to determine the authenticity of encryption certificates
    # provided by the Neo4j instance on connection.
    # * ``neo4j.TrustSystemCAs()``: Use system trust store. (default)
    # * ``neo4j.TrustAll()``: Trust any certificate.
    # * ``neo4j.TrustCustomCAs("<path>", ...)``:
    #       Trust the specified certificate(s).

    #: Certificate to use for mTLS as 2nd authentication factor.
    client_certificate = None

    #: Custom SSL context to use for wrapping sockets
    ssl_context = None
    # Use any custom SSL context to wrap sockets.
    # Overwrites ``trusted_certificates`` and ``encrypted``.
    # The use of this option is strongly discouraged.

    #: User Agent (Python Driver Specific)
    user_agent = None
    # Specify the client agent name.

    #: Socket Keep Alive (Python and .NET Driver Specific)
    keep_alive = True
    # Specify whether TCP keep-alive should be enabled.

    #: Authentication provider
    auth = None

    #: Lowest notification severity for the server to return
    notifications_min_severity = None

    #: List of notification classifications/categories for the server to ignore
    notifications_disabled_classifications = None

    #: Opt-Out of telemetry collection
    telemetry_disabled = False

    _ssl_context_cache: ssl.SSLContext | None
    _ssl_context_cache_lock: Lock

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ssl_context_cache = None
        self._ssl_context_cache_lock = Lock()

    def get_ssl_context(self) -> ssl.SSLContext | None:
        if self.ssl_context is not None:
            return self.ssl_context

        if not self.encrypted:
            return None

        client_cert: ClientCertificate | None = None

        # try to serve the cached ssl context
        with self._ssl_context_cache_lock:
            if self._ssl_context_cache is not None:
                if self.client_certificate is None:
                    return self._ssl_context_cache
                client_cert = self.client_certificate.get_certificate()
                if client_cert is None:
                    return self._ssl_context_cache
            elif self.client_certificate is not None:
                client_cert = self.client_certificate.get_certificate()

            import ssl

            # SSL stands for Secure Sockets Layer and was originally created by
            # Netscape.
            # SSLv2 and SSLv3 are the 2 versions of this protocol (SSLv1 was
            # never publicly released).
            # After SSLv3, SSL was renamed to TLS.
            # TLS stands for Transport Layer Security and started with TLSv1.0
            # which is an upgraded version of SSLv3.
            # SSLv2 - (Disabled)
            # SSLv3 - (Disabled)
            # TLS 1.0 - Released in 1999, published as RFC 2246. (Disabled)
            # TLS 1.1 - Released in 2006, published as RFC 4346. (Disabled)
            # TLS 1.2 - Released in 2008, published as RFC 5246.
            # https://docs.python.org/3.7/library/ssl.html#ssl.PROTOCOL_TLS_CLIENT
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

            # For recommended security options see
            # https://docs.python.org/3.10/library/ssl.html#protocol-versions
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

            if isinstance(self.trusted_certificates, TrustAll):
                # trust any certificate
                ssl_context.check_hostname = False
                # https://docs.python.org/3.7/library/ssl.html#ssl.CERT_NONE
                ssl_context.verify_mode = ssl.CERT_NONE
            elif isinstance(self.trusted_certificates, TrustCustomCAs):
                # trust the specified certificate(s)
                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                for cert in self.trusted_certificates.certs:
                    ssl_context.load_verify_locations(cert)
            else:
                # default
                # trust system CA certificates
                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                # Must be load_default_certs, not set_default_verify_paths to
                # work on Windows with system CAs.
                ssl_context.load_default_certs()

            if client_cert is not None:
                ssl_context.load_cert_chain(
                    client_cert.certfile,
                    keyfile=client_cert.keyfile,
                    password=client_cert.password,
                )

            self._ssl_context_cache = ssl_context
            return ssl_context
