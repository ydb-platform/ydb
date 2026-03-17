import pytest

from w3lib.util import to_bytes, to_unicode


class TestToBytes:
    def test_type_error(self):
        with pytest.raises(TypeError):
            to_bytes(True)  # type: ignore[arg-type]


class TestToUnicode:
    def test_type_error(self):
        with pytest.raises(TypeError):
            to_unicode(True)  # type: ignore[arg-type]
