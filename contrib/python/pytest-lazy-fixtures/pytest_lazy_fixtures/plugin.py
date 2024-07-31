import pytest

from .lazy_fixture import LazyFixtureWrapper, lf
from .lazy_fixture_callable import lfc
from .loader import load_lazy_fixtures
from .normalizer import normalize_metafunc_calls


def pytest_configure():
    pytest.lazy_fixtures = lf
    pytest.lazy_fixtures_callable = lfc


@pytest.hookimpl(tryfirst=True)
def pytest_fixture_setup(fixturedef, request):
    val = getattr(request, "param", None)
    if val is not None:
        request.param = load_lazy_fixtures(val, request)


def pytest_make_parametrize_id(config, val, argname):
    if isinstance(val, LazyFixtureWrapper):
        return val.name


@pytest.hookimpl(hookwrapper=True)
def pytest_generate_tests(metafunc):
    yield

    normalize_metafunc_calls(metafunc)
