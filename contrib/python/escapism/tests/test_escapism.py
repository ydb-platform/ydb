import pytest

from escapism import SAFE, escape, unescape

text = str

test_strings = [
    "asdf",
    "sposmål",
    "godtbrød",
    "≠¡™£¢∞§¶¨•d",
    "_\\-+",
]


def test_escape_default():
    for s in test_strings:
        e = escape(s)
        assert isinstance(e, text)
        u = unescape(e)
        assert isinstance(u, text)
        assert u == s


def test_escape_custom_char():
    for escape_char in r"\-%+_":
        for s in test_strings:
            e = escape(s, escape_char=escape_char)
            assert isinstance(e, text)
            u = unescape(e, escape_char=escape_char)
            assert isinstance(u, text)
            assert u == s


def test_escape_custom_safe():
    safe = "ABCDEFabcdef0123456789"
    escape_char = "\\"
    safe_set = set(safe + "\\")
    for s in test_strings:
        e = escape(s, safe=safe, escape_char=escape_char)
        assert all(c in safe_set for c in e)
        u = unescape(e, escape_char=escape_char)
        assert u == s


def test_safe_escape_char():
    escape_char = "-"
    safe = SAFE.union({escape_char})
    with pytest.warns(RuntimeWarning):
        e = escape(escape_char, safe=safe, escape_char=escape_char)
    assert e == f"{escape_char}{ord(escape_char):02X}"
    u = unescape(e, escape_char=escape_char)
    assert u == escape_char


def test_allow_collisions():
    escaped = escape("foo-bar ", escape_char="-", allow_collisions=True)
    assert escaped == "foo-bar-20"
