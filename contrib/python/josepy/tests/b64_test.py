"""Tests for josepy.b64."""

import sys
from typing import Union

import pytest

# https://en.wikipedia.org/wiki/Base64#Examples
B64_PADDING_EXAMPLES = {
    b"any carnal pleasure.": (b"YW55IGNhcm5hbCBwbGVhc3VyZS4", b"="),
    b"any carnal pleasure": (b"YW55IGNhcm5hbCBwbGVhc3VyZQ", b"=="),
    b"any carnal pleasur": (b"YW55IGNhcm5hbCBwbGVhc3Vy", b""),
    b"any carnal pleasu": (b"YW55IGNhcm5hbCBwbGVhc3U", b"="),
    b"any carnal pleas": (b"YW55IGNhcm5hbCBwbGVhcw", b"=="),
}


B64_URL_UNSAFE_EXAMPLES = {
    bytes((251, 239)): b"--8",
    bytes((255,)) * 2: b"__8",
}


class B64EncodeTest:
    """Tests for josepy.b64.b64encode."""

    @classmethod
    def _call(cls, data: bytes) -> bytes:
        from josepy.b64 import b64encode

        return b64encode(data)

    def test_empty(self) -> None:
        assert self._call(b"") == b""

    def test_unsafe_url(self) -> None:
        for text, b64 in B64_URL_UNSAFE_EXAMPLES.items():
            assert self._call(text) == b64

    def test_different_paddings(self) -> None:
        for text, (b64, _) in B64_PADDING_EXAMPLES.items():
            assert self._call(text) == b64

    def test_unicode_fails_with_type_error(self) -> None:
        with pytest.raises(TypeError):
            # We're purposefully testing with the incorrect type here.
            self._call("some unicode")  # type: ignore


class B64DecodeTest:
    """Tests for josepy.b64.b64decode."""

    @classmethod
    def _call(cls, data: Union[bytes, str]) -> bytes:
        from josepy.b64 import b64decode

        return b64decode(data)

    def test_unsafe_url(self) -> None:
        for text, b64 in B64_URL_UNSAFE_EXAMPLES.items():
            assert self._call(b64) == text

    def test_input_without_padding(self) -> None:
        for text, (b64, _) in B64_PADDING_EXAMPLES.items():
            assert self._call(b64) == text

    def test_input_with_padding(self) -> None:
        for text, (b64, pad) in B64_PADDING_EXAMPLES.items():
            assert self._call(b64 + pad) == text

    def test_unicode_with_ascii(self) -> None:
        assert self._call("YQ") == b"a"

    def test_non_ascii_unicode_fails(self) -> None:
        with pytest.raises(ValueError):
            self._call("\u0105")

    def test_type_error_no_unicode_or_bytes(self) -> None:
        with pytest.raises(TypeError):
            # We're purposefully testing with the incorrect type here.
            self._call(object())  # type: ignore


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
