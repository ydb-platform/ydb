import os
import pytest


@pytest.fixture
def os_environ(monkeypatch):
    mock_environ = dict(os.environ)
    monkeypatch.setattr(os, 'environ', mock_environ)
    return mock_environ


def pytest_generate_tests(metafunc):
    if hasattr(metafunc.function, "pytestmark"):
        for mark in metafunc.function.pytestmark:
            if mark.name == "all_locales":
                from babel.localedata import locale_identifiers
                metafunc.parametrize("locale", list(locale_identifiers()))
                break
