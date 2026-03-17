import ssl
from typing import Any

import pytest

try:
    import trustme

    TRUSTME = True
except ImportError:
    TRUSTME = False


@pytest.fixture
def tls_certificate_authority() -> Any:
    if not TRUSTME:
        pytest.xfail("trustme fails on 32bit Linux")
    return trustme.CA()


@pytest.fixture
def tls_certificate(tls_certificate_authority: Any) -> Any:
    return tls_certificate_authority.issue_server_cert(
        "localhost",
        "www.cloudflare.com",
        "127.0.0.1",
        "::1",
    )


@pytest.fixture
def ssl_ctx(tls_certificate: Any) -> ssl.SSLContext:
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls_certificate.configure_cert(ssl_ctx)
    return ssl_ctx


@pytest.fixture
def client_ssl_ctx(tls_certificate_authority: Any) -> ssl.SSLContext:
    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    tls_certificate_authority.configure_trust(ssl_ctx)
    return ssl_ctx
