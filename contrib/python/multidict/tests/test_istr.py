import gc
import sys
from typing import Type

import pytest

from multidict._compat import USE_EXTENSIONS
from multidict._multidict_py import istr as _istr  # noqa: E402

if USE_EXTENSIONS:
    from multidict._multidict import istr  # type: ignore
else:
    from multidict import istr


IMPLEMENTATION = getattr(sys, "implementation")  # to suppress mypy error


class IStrMixin:

    cls = Type[istr]

    def test_ctor(self):
        s = self.cls()
        assert "" == s

    def test_ctor_str(self):
        s = self.cls("aBcD")
        assert "aBcD" == s

    def test_ctor_istr(self):
        s = self.cls("A")
        s2 = self.cls(s)
        assert "A" == s
        assert s == s2

    def test_ctor_buffer(self):
        s = self.cls(b"aBc")
        assert "b'aBc'" == s

    def test_ctor_repr(self):
        s = self.cls(None)
        assert "None" == s

    def test_str(self):
        s = self.cls("aBcD")
        s1 = str(s)
        assert s1 == "aBcD"
        assert type(s1) is str

    def test_eq(self):
        s1 = "Abc"
        s2 = self.cls(s1)
        assert s1 == s2


class TestPyIStr(IStrMixin):
    cls = _istr

    @staticmethod
    def _create_strs():
        _istr("foobarbaz")
        istr2 = _istr()
        _istr(istr2)

    @pytest.mark.skipif(
        IMPLEMENTATION.name != "cpython", reason="PyPy has different GC implementation"
    )
    def test_leak(self):
        gc.collect()
        cnt = len(gc.get_objects())
        for _ in range(10000):
            self._create_strs()

        gc.collect()
        cnt2 = len(gc.get_objects())
        assert abs(cnt - cnt2) < 10  # on PyPy these numbers are not equal


if USE_EXTENSIONS:

    class TestIStr(IStrMixin):
        cls = istr
