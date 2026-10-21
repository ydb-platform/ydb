# Copyright (c) The pyOpenSSL developers
# See LICENSE for details.

import pathlib
from tempfile import mktemp

import pytest


def pytest_report_header(config: pytest.Config) -> str:
    import cryptography

    import OpenSSL.SSL

    return (
        f"OpenSSL: "
        f"{OpenSSL.SSL.SSLeay_version(OpenSSL.SSL.SSLEAY_VERSION)!r}\n"
        f"cryptography: {cryptography.__version__}"
    )


@pytest.fixture
def tmpfile(tmp_path: pathlib.Path) -> bytes:
    """
    Return UTF-8-encoded bytes of a path to a tmp file.

    The file will be cleaned up after the test run.
    """
    return mktemp(dir=tmp_path).encode("utf-8")
