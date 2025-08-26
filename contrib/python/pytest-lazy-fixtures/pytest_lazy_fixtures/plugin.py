from __future__ import annotations

import pytest

from .fixture_collector import collect_fixtures
from .lazy_fixture import LazyFixtureWrapper
from .loader import load_lazy_fixtures
from .normalizer import normalize_metafunc_calls


@pytest.hookimpl(tryfirst=True)
def pytest_fixture_setup(fixturedef: pytest.FixtureDef, request: pytest.FixtureRequest):  # noqa: ARG001
    val = getattr(request, "param", None)
    if val is not None:
        request.param = load_lazy_fixtures(val, request)


def pytest_make_parametrize_id(config: pytest.Config, val, argname):  # noqa: ARG001
    if isinstance(val, LazyFixtureWrapper):
        return val.name
    return None


@pytest.hookimpl(hookwrapper=True)
def pytest_generate_tests(metafunc: pytest.Metafunc):
    yield

    normalize_metafunc_calls(metafunc)


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]):  # noqa: ARG001
    if not getattr(config.option, "deadfixtures", False):
        return
    collect_fixtures(config, items)
