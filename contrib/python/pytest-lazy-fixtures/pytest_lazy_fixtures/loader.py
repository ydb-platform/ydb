import pytest

from .lazy_fixture import LazyFixtureWrapper
from .lazy_fixture_callable import LazyFixtureCallableWrapper


class LazyFixtureLoader:
    def __init__(self, request: pytest.FixtureRequest):
        self.request = request
        self.lazy_fixture_loaded = False

    def load_lazy_fixtures(self, value):
        if isinstance(value, LazyFixtureCallableWrapper):
            self.lazy_fixture_loaded = True
            return value.get_func(self.request)(
                *self.load_lazy_fixtures(value.args),
                **self.load_lazy_fixtures(value.kwargs),
            )
        if isinstance(value, LazyFixtureWrapper):
            self.lazy_fixture_loaded = True
            return value.load_fixture(self.request)
        if type(value) is dict:
            new_value = {self.load_lazy_fixtures(key): self.load_lazy_fixtures(val) for key, val in value.items()}
            return new_value if self.lazy_fixture_loaded else value
        if type(value) in (list, set, tuple):
            new_value = type(value)(self.load_lazy_fixtures(val) for val in value)
            return new_value if self.lazy_fixture_loaded else value
        return value
