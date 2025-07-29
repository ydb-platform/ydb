import pytest

from .lazy_fixture import LazyFixtureWrapper
from .loader import load_lazy_fixtures
from .normalizer import normalize_metafunc_calls


@pytest.hookimpl(tryfirst=True)
def pytest_fixture_setup(fixturedef, request):  # noqa: ARG001
    val = getattr(request, "param", None)
    if val is not None:
        request.param = load_lazy_fixtures(val, request)


def pytest_make_parametrize_id(config, val, argname):  # noqa: ARG001
    if isinstance(val, LazyFixtureWrapper):
        return val.name
    return None


@pytest.hookimpl(hookwrapper=True)
def pytest_generate_tests(metafunc):
    yield

    normalize_metafunc_calls(metafunc)
