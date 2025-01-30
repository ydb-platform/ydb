import pytest

from itsdangerous._compat import _constant_time_compare


@pytest.mark.parametrize(
    ("a", "b", "expect"),
    ((b"a", b"a", True), (b"a", b"b", False), (b"a", b"aa", False)),
)
def test_python_constant_time_compare(a, b, expect):
    assert _constant_time_compare(a, b) == expect
