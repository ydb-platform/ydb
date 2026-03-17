import enum
import pathlib

import ijson

import pytest


def _get_available_backends():
    backends = []
    for backend in ijson.ALL_BACKENDS:
        try:
            backends.append(ijson.get_backend(backend))
        except ImportError:
            pass
    return backends


_available_backends = _get_available_backends()


class InputType(enum.Enum):
    ASYNC_FILE = enum.auto()
    ASYNC_TYPES_COROUTINES_FILE = enum.auto()
    ASYNC_ITERABLE = enum.auto()
    FILE = enum.auto()
    ITERABLE = enum.auto()
    SENDABLE = enum.auto()


class BackendAdaptor:
    """
    Ties a backend together with an input type to provide easy access to
    calling the backend's methods and retrieving all results in a single call.
    """
    def __init__(self, backend, input_type, suffix, get_all):
        self.backend = backend
        self._input_type = input_type
        self._suffix = suffix
        self._get_all = get_all

    @property
    def pytest_parameter_id(self):
        return f"{self.backend.backend_name}-{self._input_type.name.lower()}"

    def __getattr__(self, name):
        routine = getattr(self.backend, name + self._suffix)
        def get_all_for_name(*args, **kwargs):
            return self._get_all(routine, *args, **kwargs)
        return get_all_for_name


from .support.async_ import get_all as get_all_async
from .support.async_types_coroutines import get_all as get_all_async_types_coroutines
from .support.aiterators import get_all as get_all_async_iterable
from .support.coroutines import get_all as get_all_coro
from .support.generators import get_all as get_all_gen
from .support.iterators import get_all as get_all_iterable

_pull_backend_adaptors = [
    backend_adaptor
    for backend in _available_backends
    for backend_adaptor in [
        BackendAdaptor(backend, InputType.ASYNC_FILE, "_async", get_all_async),
        BackendAdaptor(backend, InputType.ASYNC_TYPES_COROUTINES_FILE, "_async", get_all_async_types_coroutines),
        BackendAdaptor(backend, InputType.ASYNC_ITERABLE, "_async", get_all_async_iterable),
        BackendAdaptor(backend, InputType.FILE, "_gen", get_all_gen),
        BackendAdaptor(backend, InputType.ITERABLE, "_gen", get_all_iterable),
    ]
]

_push_backend_adaptors = [
    backend_adaptor
    for backend in _available_backends
    for backend_adaptor in [
        BackendAdaptor(backend, InputType.SENDABLE, "_coro", get_all_coro),
    ]
]

_all_backend_adaptors = _pull_backend_adaptors + _push_backend_adaptors

BACKEND_PARAM_NAME = "backend"
ADAPTOR_PARAM_NAME = "adaptor"

def pytest_generate_tests(metafunc):
    requires_backend = BACKEND_PARAM_NAME in metafunc.fixturenames
    requires_adaptor = ADAPTOR_PARAM_NAME in metafunc.fixturenames
    assert not (requires_backend and requires_adaptor)

    names = []

    # if both are required we need to match backend and adaptors correctly
    if requires_backend:
        names = BACKEND_PARAM_NAME
        values = _available_backends
        ids = [backend.backend_name for backend in _available_backends]
    elif requires_adaptor:
        pull_only = bool(list(metafunc.definition.iter_markers('pull_only')))
        adaptors = _pull_backend_adaptors if pull_only else _all_backend_adaptors
        names = ADAPTOR_PARAM_NAME
        values = adaptors
        ids = [adaptor.pytest_parameter_id for adaptor in adaptors]

    if names:
        metafunc.parametrize(names, values, ids=ids)

def pytest_addoption(parser):
    group = parser.getgroup("Memory leak tests")
    group.addoption("--memleaks", action="store_true", help="include memory leak tests")
    group.addoption("--memleaks-only", action="store_true", help="run ONLY memory leak tests")

def pytest_collection_modifyitems(config, items):
    if config.option.memleaks_only:
        skip_mark = pytest.mark.skip(reason="running only memleak tests")
        for item in items:
            if pathlib.Path(item.fspath).name != "test_memleaks.py":
                item.add_marker(skip_mark)
    elif not config.option.memleaks:
        skip_mark = pytest.mark.skip(reason="run with --memleaks or --memleaks-only option")
        for item in items:
            if pathlib.Path(item.fspath).name == "test_memleaks.py":
                item.add_marker(skip_mark)
