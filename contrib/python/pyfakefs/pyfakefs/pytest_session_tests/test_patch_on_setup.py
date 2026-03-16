from pathlib import Path

import pytest

from pyfakefs.fake_filesystem import FakeFilesystem


def pytest_generate_tests(metafunc):
    handlers = [a, b, c]
    if "handler_class" in metafunc.fixturenames:
        metafunc.parametrize("handler_class", handlers)


def a():
    pass


def b():
    pass


def c():
    pass


@pytest.fixture
def class_a():
    pass


@pytest.fixture
def class_b():
    pass


@pytest.fixture
def class_c():
    pass


@pytest.fixture
def make_handler(request):
    def _make_handler(cls):
        return request.getfixturevalue(f"class_{cls.__name__}")

    yield _make_handler


@pytest.fixture
def handler_and_check(handler_class, make_handler):
    assert Path("/foo/bar").exists()
    yield


def test_handler_and_check_in_fixture(handler_and_check):
    assert Path("/foo/bar").exists()


@pytest.fixture(scope="session", autouse=True)
def config(fs_session: FakeFilesystem):
    fs_session.create_file("/foo/bar")
