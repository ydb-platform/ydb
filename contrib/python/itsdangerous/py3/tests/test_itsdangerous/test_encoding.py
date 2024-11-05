import pytest

from itsdangerous.encoding import base64_decode
from itsdangerous.encoding import base64_encode
from itsdangerous.encoding import bytes_to_int
from itsdangerous.encoding import int_to_bytes
from itsdangerous.encoding import want_bytes
from itsdangerous.exc import BadData


@pytest.mark.parametrize("value", ("mañana", b"tomorrow"))
def test_want_bytes(value):
    out = want_bytes(value)
    assert isinstance(out, bytes)


@pytest.mark.parametrize("value", ("無限", b"infinite"))
def test_base64(value):
    enc = base64_encode(value)
    assert isinstance(enc, bytes)
    dec = base64_decode(enc)
    assert dec == want_bytes(value)


def test_base64_bad():
    with pytest.raises(BadData):
        base64_decode("12345")


@pytest.mark.parametrize(
    ("value", "expect"), ((0, b""), (192, b"\xc0"), (18446744073709551615, b"\xff" * 8))
)
def test_int_bytes(value, expect):
    enc = int_to_bytes(value)
    assert enc == expect
    dec = bytes_to_int(enc)
    assert dec == value
