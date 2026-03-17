import pytest
import sys

import semver


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


def ensure_binary(s, encoding="utf-8", errors="strict"):
    """Coerce **s** to six.binary_type.

    For Python 2:
      - `unicode` -> encoded to `str`
      - `str` -> `str`

    For Python 3:
      - `str` -> encoded to `bytes`
      - `bytes` -> `bytes`
    """
    if isinstance(s, semver.text_type):
        return s.encode(encoding, errors)
    elif isinstance(s, semver.binary_type):
        return s
    else:
        raise TypeError("not expecting type '%s'" % type(s))


def test_should_work_with_string_and_unicode():
    result = semver.compare(semver.u("1.1.0"), semver.b("1.2.2"))
    assert result == -1
    result = semver.compare(semver.b("1.1.0"), semver.u("1.2.2"))
    assert result == -1


class TestEnsure:
    # From six project
    # grinning face emoji
    UNICODE_EMOJI = semver.u("\U0001F600")
    BINARY_EMOJI = b"\xf0\x9f\x98\x80"

    def test_ensure_binary_raise_type_error(self):
        with pytest.raises(TypeError):
            semver.ensure_str(8)

    def test_errors_and_encoding(self):
        ensure_binary(self.UNICODE_EMOJI, encoding="latin-1", errors="ignore")
        with pytest.raises(UnicodeEncodeError):
            ensure_binary(self.UNICODE_EMOJI, encoding="latin-1", errors="strict")

    def test_ensure_binary_raise(self):
        converted_unicode = ensure_binary(
            self.UNICODE_EMOJI, encoding="utf-8", errors="strict"
        )
        converted_binary = ensure_binary(
            self.BINARY_EMOJI, encoding="utf-8", errors="strict"
        )
        if semver.PY2:
            # PY2: unicode -> str
            assert converted_unicode == self.BINARY_EMOJI and isinstance(
                converted_unicode, str
            )
            # PY2: str -> str
            assert converted_binary == self.BINARY_EMOJI and isinstance(
                converted_binary, str
            )
        else:
            # PY3: str -> bytes
            assert converted_unicode == self.BINARY_EMOJI and isinstance(
                converted_unicode, bytes
            )
            # PY3: bytes -> bytes
            assert converted_binary == self.BINARY_EMOJI and isinstance(
                converted_binary, bytes
            )

    def test_ensure_str(self):
        converted_unicode = semver.ensure_str(
            self.UNICODE_EMOJI, encoding="utf-8", errors="strict"
        )
        converted_binary = semver.ensure_str(
            self.BINARY_EMOJI, encoding="utf-8", errors="strict"
        )
        if PY2:
            # PY2: unicode -> str
            assert converted_unicode == self.BINARY_EMOJI and isinstance(
                converted_unicode, str
            )
            # PY2: str -> str
            assert converted_binary == self.BINARY_EMOJI and isinstance(
                converted_binary, str
            )
        else:
            # PY3: str -> str
            assert converted_unicode == self.UNICODE_EMOJI and isinstance(
                converted_unicode, str
            )
            # PY3: bytes -> str
            assert converted_binary == self.UNICODE_EMOJI and isinstance(
                converted_unicode, str
            )
