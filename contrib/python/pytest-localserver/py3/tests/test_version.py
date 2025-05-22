import pytest_localserver


def test_version():
    assert hasattr(pytest_localserver, "VERSION")
    assert isinstance(pytest_localserver.VERSION, str)
