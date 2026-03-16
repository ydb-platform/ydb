import ijson
import io

import pytest


def _memusage():
    """memory usage in MB. getrusage defaults to KB on Linux."""
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _exhaust(it):
    sum(1 for _ in it)


def _repeat(n, f, *args, exhaust=False, **kwargs):
    for _ in range(n):
        obj = f(*args, **kwargs)
        if exhaust:
            _exhaust(obj)


@pytest.fixture(autouse=True)
def _memory_leak_detection():
    THRESHOLD = 0.1
    memusage_start = _memusage()
    yield
    memusage_end = _memusage()
    assert (memusage_end - memusage_start) / memusage_start <= THRESHOLD



@pytest.mark.parametrize("exhaust", (True, False))
@pytest.mark.parametrize("n", (100000,))
@pytest.mark.parametrize("function",
    (
        pytest.param(construction, id=name) for name, construction in
        {
            "basic_parse": lambda backend: backend.basic_parse_gen(io.BytesIO(b'[1, 2, 3, 4, 5]')),
            "parse": lambda backend: backend.parse_gen(io.BytesIO(b'[1, 2, 3, 4, 5]')),
            "items": lambda backend: backend.items_gen(io.BytesIO(b'[1, 2, 3, 4, 5]'), 'item'),
            "kvitems": lambda backend: backend.kvitems_gen(io.BytesIO(b'{"a": 0, "b": 1, "c": 2}'), ''),
        }.items()
    )
)
def test_generator(backend, function, n, exhaust):
    """Tests that running parse doesn't leak"""
    _repeat(n, function, backend, exhaust=exhaust)


@pytest.mark.parametrize("n", (100000,))
@pytest.mark.parametrize("invalid_construction",
    (
        pytest.param(construction, id=name) for name, construction in
        {
            "basic_parse": lambda backend: next(backend.basic_parse_gen(io.BytesIO(b'[1, 2, 3, 4, 5]'), not_a_kwarg=0)),
            "parse": lambda backend: next(backend.parse_gen(io.BytesIO(b'[1, 2, 3, 4, 5]'), not_a_kwarg=0)),
            "items": lambda backend: next(backend.items_gen(io.BytesIO(b'[1, 2, 3, 4, 5]'), 'item', not_a_kwarg=0)),
            "kvitems": lambda backend: next(backend.kvitems_gen(io.BytesIO(b'{"a": 0, "b": 1, "c": 2}'), '', not_a_kwarg=0)),
        }.items()
    )
)
def test_erroneous_generator_construction(backend, invalid_construction, n):
    """Tests that running parse doesn't leak"""
    def erroneous_generator_construction():
        with pytest.raises(TypeError):
            invalid_construction(backend)
    _repeat(n, erroneous_generator_construction)


@pytest.mark.parametrize("n", (100000,))
@pytest.mark.parametrize("invalid_construction",
    (
        pytest.param(construction, id=name) for name, construction in
        {
            "basic_parse": lambda backend: backend.basic_parse_async(b'[1, 2, 3, 4, 5]', not_a_kwarg=0),
            "parse": lambda backend: backend.parse_async(b'[1, 2, 3, 4, 5]', not_a_kwarg=0),
            "items": lambda backend: backend.items_async(b'[1, 2, 3, 4, 5]', 'item', not_a_kwarg=0),
            "kvitems": lambda backend: backend.kvitems_async(b'{"a": 0, "b": 1, "c": 2}', '', not_a_kwarg=0),
        }.items()
    )
)
def test_erroneous_async_construction(backend, n ,invalid_construction):
    def erroneous_async_construction():
        with pytest.raises(TypeError):
            invalid_construction(backend)
    _repeat(n, erroneous_async_construction)
