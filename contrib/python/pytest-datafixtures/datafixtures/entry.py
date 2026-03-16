from io import StringIO, BytesIO
from pathlib import Path

import pytest
import yatest.common

if False:  # pragma nocover
    from _pytest.fixtures import SubRequest  # noqa


@pytest.fixture
def datafix_dir(request: 'SubRequest') -> Path:
    """Returns data fixtures directory (as Path object) for test."""
    subdir = request.module.__package__.removeprefix('__tests__').strip('.').replace('.', '/')
    return Path(yatest.common.test_source_path(subdir)) / 'datafixtures'


@pytest.fixture
def datafix(datafix_dir: Path, request: 'SubRequest') -> Path:
    """Returns data fixture Path object for current test."""
    return datafix_dir / request.node.name


@pytest.fixture
def datafix_dump(datafix_dir: Path, request: 'SubRequest'):
    """Allows dumping data to a datafixtures directory"""

    testname = request.node.name

    def datafix_dump_(
        data: str | bytes,
        fname: str = '',
        *,
        encoding: str = None,
    ) -> Path:

        fname = fname or testname
        target = (datafix_dir / fname)

        if isinstance(data, str):
            target.write_text(data, encoding=encoding)

        else:
            target.write_bytes(data)

        return target

    return datafix_dump_


@pytest.fixture
def datafix_read(datafix_dir: Path, request: 'SubRequest'):
    """Returns text from the data fixture by its name."""

    testname = request.node.name

    def datafix_read_(
        fname: str = None,
        *,
        encoding: str | None = None,
        io: bool = False

    ) -> str | StringIO:

        fname = fname or testname
        data = (datafix_dir / fname).read_text(encoding=encoding)
        if io:
            return StringIO(data)
        return data

    return datafix_read_


@pytest.fixture
def datafix_readbin(datafix_dir: Path, request: 'SubRequest'):
    """Returns binary from the data fixture by its name."""

    testname = request.node.name

    def datafix_readbin(
        fname: str = None,
        *,
        io: bool = False

    ) -> bytes | BytesIO:

        fname = fname or testname
        data = (datafix_dir / fname).read_bytes()
        if io:
            return BytesIO(data)
        return data

    return datafix_readbin
