import pytest

from .lazy_fixture import LazyFixtureWrapper
from .lazy_fixture_callable import LazyFixtureCallableWrapper


def load_lazy_fixtures(value, request: pytest.FixtureRequest):
    if isinstance(value, LazyFixtureCallableWrapper):
        return value.get_func(request)(
            *load_lazy_fixtures(value.args, request),
            **load_lazy_fixtures(value.kwargs, request),
        )
    if isinstance(value, LazyFixtureWrapper):
        return value.load_fixture(request)
    # we need to check exact type
    if type(value) is dict:  # noqa: E721
        return {key: load_lazy_fixtures(value, request) for key, value in value.items()}
    # we need to check exact type
    elif type(value) in {list, tuple, set}:
        return type(value)([load_lazy_fixtures(value, request) for value in value])
    return value
