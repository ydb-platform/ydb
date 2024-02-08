import pytest

from multidict._compat import USE_EXTENSIONS
from multidict._multidict_py import MultiDict as PyMultiDict  # noqa: E402

if USE_EXTENSIONS:
    from multidict._multidict import MultiDict  # type: ignore


@pytest.fixture(
    params=([MultiDict] if USE_EXTENSIONS else []) + [PyMultiDict],
    ids=(["MultiDict"] if USE_EXTENSIONS else []) + ["PyMultiDict"],
)
def cls(request):
    return request.param


def test_guard_items(cls):
    md = cls({"a": "b"})
    it = iter(md.items())
    md["a"] = "c"
    with pytest.raises(RuntimeError):
        next(it)


def test_guard_keys(cls):
    md = cls({"a": "b"})
    it = iter(md.keys())
    md["a"] = "c"
    with pytest.raises(RuntimeError):
        next(it)


def test_guard_values(cls):
    md = cls({"a": "b"})
    it = iter(md.values())
    md["a"] = "c"
    with pytest.raises(RuntimeError):
        next(it)
