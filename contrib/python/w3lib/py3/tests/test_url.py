from __future__ import annotations

import os
import sys
from inspect import isclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pytest

from w3lib._infra import (
    _ASCII_ALPHA,
    _ASCII_ALPHANUMERIC,
    _ASCII_TAB_OR_NEWLINE,
    _C0_CONTROL_OR_SPACE,
)
from w3lib._url import _SPECIAL_SCHEMES
from w3lib.url import (
    add_or_replace_parameter,
    add_or_replace_parameters,
    any_to_uri,
    canonicalize_url,
    file_uri_to_path,
    is_url,
    parse_data_uri,
    parse_url,
    path_to_file_uri,
    safe_download_url,
    safe_url_string,
    url_query_cleaner,
    url_query_parameter,
)

if TYPE_CHECKING:
    from collections.abc import Callable

# Test cases for URL-to-safe-URL conversions with a URL and an encoding as
# input parameters.
#
# (encoding, input URL, output URL or exception)
SAFE_URL_ENCODING_CASES: list[tuple[str | None, str | bytes, str | type[Exception]]] = [
    (None, "", ValueError),
    (None, "https://example.com", "https://example.com"),
    (None, "https://example.com/©", "https://example.com/%C2%A9"),
    # Paths are always UTF-8-encoded.
    ("iso-8859-1", "https://example.com/©", "https://example.com/%C2%A9"),
    # Queries are UTF-8-encoded if the scheme is not special, ws or wss.
    ("iso-8859-1", "a://example.com?©", "a://example.com?%C2%A9"),
    *(
        ("iso-8859-1", f"{scheme}://example.com?©", f"{scheme}://example.com?%C2%A9")
        for scheme in ("ws", "wss")
    ),
    *(
        ("iso-8859-1", f"{scheme}://example.com?©", f"{scheme}://example.com?%A9")
        for scheme in _SPECIAL_SCHEMES
        if scheme not in {"ws", "wss"}
    ),
    # Fragments are always UTF-8-encoded.
    ("iso-8859-1", "https://example.com#©", "https://example.com#%C2%A9"),
]

INVALID_SCHEME_FOLLOW_UPS = "".join(
    chr(value)
    for value in range(0x81)
    if (
        chr(value) not in _ASCII_ALPHANUMERIC
        and chr(value) not in "+-."
        and chr(value) not in _C0_CONTROL_OR_SPACE  # stripped
        and chr(value) != ":"  # separator
    )
)

SAFE_URL_URL_INVALID_SCHEME_CASES = tuple(
    (f"{scheme}://example.com", ValueError)
    for scheme in (
        # A scheme is required.
        "",
        # The first scheme letter must be an ASCII alpha.
        # Note: 0x80 is included below to also test non-ASCII example.
        *(
            chr(value)
            for value in range(0x81)
            if (
                chr(value) not in _ASCII_ALPHA
                and chr(value) not in _C0_CONTROL_OR_SPACE  # stripped
                and chr(value) != ":"  # separator
            )
        ),
        # The follow-up scheme letters can also be ASCII numbers, plus, hyphen,
        # or period.
        f"a{INVALID_SCHEME_FOLLOW_UPS}",
    )
)

SCHEME_NON_FIRST = _ASCII_ALPHANUMERIC + "+-."

# Username and password characters that do not need escaping.
# Removed for RFC 2396 and RFC 3986: %
# Removed for the URL living standard: :;=
USERINFO_SAFE = _ASCII_ALPHANUMERIC + "-_.!~*'()" + "&+$,"
USERNAME_TO_ENCODE = "".join(
    chr(value)
    for value in range(0x80)
    if (
        chr(value) not in _C0_CONTROL_OR_SPACE
        and chr(value) not in USERINFO_SAFE
        and chr(value) not in ":/?#\\[]"
    )
)
USERNAME_ENCODED = "".join(f"%{ord(char):02X}" for char in USERNAME_TO_ENCODE)
PASSWORD_TO_ENCODE = USERNAME_TO_ENCODE + ":"
PASSWORD_ENCODED = "".join(f"%{ord(char):02X}" for char in PASSWORD_TO_ENCODE)

# Path characters that do not need escaping.
# Removed for RFC 2396 and RFC 3986: %[\]^|
PATH_SAFE = _ASCII_ALPHANUMERIC + "-_.!~*'()" + ":@&=+$," + "/" + ";"
PATH_TO_ENCODE = "".join(
    chr(value)
    for value in range(0x80)
    if (
        chr(value) not in _C0_CONTROL_OR_SPACE
        and chr(value) not in PATH_SAFE
        and chr(value) not in "?#\\"
    )
)
PATH_ENCODED = "".join(f"%{ord(char):02X}" for char in PATH_TO_ENCODE)

# Query characters that do not need escaping.
# Removed for RFC 2396 and RFC 3986: %[\]^`{|}
# Removed for the URL living standard: ' (special)
QUERY_SAFE = _ASCII_ALPHANUMERIC + "-_.!~*'()" + ":@&=+$," + "/" + ";" + "?"
QUERY_TO_ENCODE = "".join(
    chr(value)
    for value in range(0x80)
    if (
        chr(value) not in _C0_CONTROL_OR_SPACE
        and chr(value) not in QUERY_SAFE
        and chr(value) not in "#"
    )
)
QUERY_ENCODED = "".join(f"%{ord(char):02X}" for char in QUERY_TO_ENCODE)
SPECIAL_QUERY_SAFE = QUERY_SAFE.replace("'", "")
SPECIAL_QUERY_TO_ENCODE = "".join(
    chr(value)
    for value in range(0x80)
    if (
        chr(value) not in _C0_CONTROL_OR_SPACE
        and chr(value) not in SPECIAL_QUERY_SAFE
        and chr(value) not in "#"
    )
)
SPECIAL_QUERY_ENCODED = "".join(f"%{ord(char):02X}" for char in SPECIAL_QUERY_TO_ENCODE)

# Fragment characters that do not need escaping.
# Removed for RFC 2396 and RFC 3986: #%[\\]^{|}
FRAGMENT_SAFE = _ASCII_ALPHANUMERIC + "-_.!~*'()" + ":@&=+$," + "/" + ";" + "?"
FRAGMENT_TO_ENCODE = "".join(
    chr(value)
    for value in range(0x80)
    if (chr(value) not in _C0_CONTROL_OR_SPACE and chr(value) not in FRAGMENT_SAFE)
)
FRAGMENT_ENCODED = "".join(f"%{ord(char):02X}" for char in FRAGMENT_TO_ENCODE)


# Test cases for URL-to-safe-URL conversions with only a URL as input parameter
# (i.e. no encoding or base URL).
#
# (input URL, output URL or exception)
SAFE_URL_URL_CASES = (
    # Invalid input type
    (1, Exception),
    (object(), Exception),
    # Empty string
    ("", ValueError),
    # Remove any leading and trailing C0 control or space from input.
    *(
        (f"{char}https://example.com{char}", "https://example.com")
        for char in _C0_CONTROL_OR_SPACE
        if char not in _ASCII_TAB_OR_NEWLINE
    ),
    # Remove all ASCII tab or newline from input.
    (
        (
            f"{_ASCII_TAB_OR_NEWLINE}h{_ASCII_TAB_OR_NEWLINE}ttps"
            f"{_ASCII_TAB_OR_NEWLINE}:{_ASCII_TAB_OR_NEWLINE}/"
            f"{_ASCII_TAB_OR_NEWLINE}/{_ASCII_TAB_OR_NEWLINE}a"
            f"{_ASCII_TAB_OR_NEWLINE}b{_ASCII_TAB_OR_NEWLINE}:"
            f"{_ASCII_TAB_OR_NEWLINE}a{_ASCII_TAB_OR_NEWLINE}b"
            f"{_ASCII_TAB_OR_NEWLINE}@{_ASCII_TAB_OR_NEWLINE}exam"
            f"{_ASCII_TAB_OR_NEWLINE}ple.com{_ASCII_TAB_OR_NEWLINE}:"
            f"{_ASCII_TAB_OR_NEWLINE}1{_ASCII_TAB_OR_NEWLINE}2"
            f"{_ASCII_TAB_OR_NEWLINE}/{_ASCII_TAB_OR_NEWLINE}a"
            f"{_ASCII_TAB_OR_NEWLINE}b{_ASCII_TAB_OR_NEWLINE}?"
            f"{_ASCII_TAB_OR_NEWLINE}a{_ASCII_TAB_OR_NEWLINE}b"
            f"{_ASCII_TAB_OR_NEWLINE}#{_ASCII_TAB_OR_NEWLINE}a"
            f"{_ASCII_TAB_OR_NEWLINE}b{_ASCII_TAB_OR_NEWLINE}"
        ),
        "https://ab:ab@example.com:12/ab?ab#ab",
    ),
    # Scheme
    (f"{_ASCII_ALPHA}://example.com", f"{_ASCII_ALPHA.lower()}://example.com"),
    (
        f"a{SCHEME_NON_FIRST}://example.com",
        f"a{SCHEME_NON_FIRST.lower()}://example.com",
    ),
    *SAFE_URL_URL_INVALID_SCHEME_CASES,
    # Authority
    ("https://a@example.com", "https://a@example.com"),
    ("https://a:@example.com", "https://a:@example.com"),
    ("https://a:a@example.com", "https://a:a@example.com"),
    ("https://a%3A@example.com", "https://a%3A@example.com"),
    (
        f"https://{USERINFO_SAFE}:{USERINFO_SAFE}@example.com",
        f"https://{USERINFO_SAFE}:{USERINFO_SAFE}@example.com",
    ),
    (
        f"https://{USERNAME_TO_ENCODE}:{PASSWORD_TO_ENCODE}@example.com",
        f"https://{USERNAME_ENCODED}:{PASSWORD_ENCODED}@example.com",
    ),
    ("https://@\\example.com", ValueError),
    ("https://\x80:\x80@example.com", "https://%C2%80:%C2%80@example.com"),
    # Host
    ("https://example.com", "https://example.com"),
    ("https://.example", "https://.example"),
    ("https://\x80.example", ValueError),
    ("https://%80.example", ValueError),
    # The 4 cases below test before and after crossing DNS length limits on
    # domain name labels (63 characters) and the domain name as a whole (253
    # characters). However, all cases are expected to pass because the URL
    # living standard does not require domain names to be within these limits.
    (f"https://{'a' * 63}.example", f"https://{'a' * 63}.example"),
    (f"https://{'a' * 64}.example", f"https://{'a' * 64}.example"),
    (
        f"https://{'a' * 63}.{'a' * 63}.{'a' * 63}.{'a' * 53}.example",
        f"https://{'a' * 63}.{'a' * 63}.{'a' * 63}.{'a' * 53}.example",
    ),
    (
        f"https://{'a' * 63}.{'a' * 63}.{'a' * 63}.{'a' * 54}.example",
        f"https://{'a' * 63}.{'a' * 63}.{'a' * 63}.{'a' * 54}.example",
    ),
    ("https://ñ.example", "https://xn--ida.example"),
    ("http://192.168.0.0", "http://192.168.0.0"),
    ("http://192.168.0.256", ValueError),
    ("http://192.168.0.0.0", ValueError),
    ("http://[2a01:5cc0:1:2::4]", "http://[2a01:5cc0:1:2::4]"),
    ("http://[2a01:5cc0:1:2:3:4]", ValueError),
    # Port
    ("https://example.com:", "https://example.com:"),
    ("https://example.com:1", "https://example.com:1"),
    ("https://example.com:443", "https://example.com:443"),
    # Path
    ("https://example.com/", "https://example.com/"),
    ("https://example.com/a", "https://example.com/a"),
    ("https://example.com\\a", "https://example.com/a"),
    ("https://example.com/a\\b", "https://example.com/a/b"),
    (
        f"https://example.com/{PATH_SAFE}",
        f"https://example.com/{PATH_SAFE}",
    ),
    (
        f"https://example.com/{PATH_TO_ENCODE}",
        f"https://example.com/{PATH_ENCODED}",
    ),
    ("https://example.com/ñ", "https://example.com/%C3%B1"),
    ("https://example.com/ñ%C3%B1", "https://example.com/%C3%B1%C3%B1"),
    # Query
    ("https://example.com?", "https://example.com?"),
    ("https://example.com/?", "https://example.com/?"),
    ("https://example.com?a", "https://example.com?a"),
    ("https://example.com?a=", "https://example.com?a="),
    ("https://example.com?a=b", "https://example.com?a=b"),
    (
        f"a://example.com?{QUERY_SAFE}",
        f"a://example.com?{QUERY_SAFE}",
    ),
    (
        f"a://example.com?{QUERY_TO_ENCODE}",
        f"a://example.com?{QUERY_ENCODED}",
    ),
    *(
        (
            f"{scheme}://example.com?{SPECIAL_QUERY_SAFE}",
            f"{scheme}://example.com?{SPECIAL_QUERY_SAFE}",
        )
        for scheme in _SPECIAL_SCHEMES
    ),
    *(
        (
            f"{scheme}://example.com?{SPECIAL_QUERY_TO_ENCODE}",
            f"{scheme}://example.com?{SPECIAL_QUERY_ENCODED}",
        )
        for scheme in _SPECIAL_SCHEMES
    ),
    ("https://example.com?ñ", "https://example.com?%C3%B1"),
    ("https://example.com?ñ%C3%B1", "https://example.com?%C3%B1%C3%B1"),
    # Fragment
    ("https://example.com#", "https://example.com#"),
    ("https://example.com/#", "https://example.com/#"),
    ("https://example.com?#", "https://example.com?#"),
    ("https://example.com/?#", "https://example.com/?#"),
    ("https://example.com#a", "https://example.com#a"),
    (
        f"a://example.com#{FRAGMENT_SAFE}",
        f"a://example.com#{FRAGMENT_SAFE}",
    ),
    (
        f"a://example.com#{FRAGMENT_TO_ENCODE}",
        f"a://example.com#{FRAGMENT_ENCODED}",
    ),
    ("https://example.com#ñ", "https://example.com#%C3%B1"),
    ("https://example.com#ñ%C3%B1", "https://example.com#%C3%B1%C3%B1"),
    # All fields, UTF-8 wherever possible.
    (
        "https://ñ:ñ@ñ.example:1/ñ?ñ#ñ",
        "https://%C3%B1:%C3%B1@xn--ida.example:1/%C3%B1?%C3%B1#%C3%B1",
    ),
)


def _test_safe_url_func(
    url: str | bytes,
    *,
    encoding: str | None = None,
    output: str | type[Exception],
    func: Callable[..., str],
) -> None:
    kwargs = {}
    if encoding is not None:
        kwargs["encoding"] = encoding
    if isclass(output) and issubclass(output, Exception):
        with pytest.raises(output):
            func(url, **kwargs)
        return
    actual = func(url, **kwargs)
    assert actual == output
    assert func(actual, **kwargs) == output  # Idempotency


def _test_safe_url_string(
    url: str | bytes,
    *,
    encoding: str | None = None,
    output: str | type[Exception],
) -> None:
    return _test_safe_url_func(
        url,
        encoding=encoding,
        output=output,
        func=safe_url_string,
    )


KNOWN_SAFE_URL_STRING_ENCODING_ISSUES = {
    (None, ""),  # Invalid URL
    # UTF-8 encoding is not enforced in non-special URLs, or in URLs with the
    # ws or wss schemas.
    ("iso-8859-1", "a://example.com?\xa9"),
    ("iso-8859-1", "ws://example.com?\xa9"),
    ("iso-8859-1", "wss://example.com?\xa9"),
    # UTF-8 encoding is not enforced on the fragment.
    ("iso-8859-1", "https://example.com#\xa9"),
}


@pytest.mark.parametrize(
    ("encoding", "url", "output"),
    [
        (
            case
            if case[:2] not in KNOWN_SAFE_URL_STRING_ENCODING_ISSUES
            else pytest.param(*case, marks=pytest.mark.xfail(strict=True))
        )
        for case in SAFE_URL_ENCODING_CASES
    ],
)
def test_safe_url_string_encoding(
    encoding: str | None, url: str | bytes, output: str | type[Exception]
) -> None:
    _test_safe_url_string(url, encoding=encoding, output=output)


KNOWN_SAFE_URL_STRING_URL_ISSUES = {
    "",  # Invalid URL
    *(case[0] for case in SAFE_URL_URL_INVALID_SCHEME_CASES),
    # Userinfo characters that the URL living standard requires escaping (:;=)
    # are not escaped.
    "https://@\\example.com",  # Invalid URL
    "https://\x80.example",  # Invalid domain name (non-visible character)
    "https://%80.example",  # Invalid domain name (non-visible character)
    "http://192.168.0.256",  # Invalid IP address
    "http://192.168.0.0.0",  # Invalid IP address / domain name
    "http://[2a01:5cc0:1:2::4]",  # https://github.com/scrapy/w3lib/issues/193
    "https://example.com:",  # Removes the :
    # Does not convert \ to /
    "https://example.com\\a",
    "https://example.com\\a\\b",
    # Encodes \ and / after the first one in the path
    "https://example.com/a/b",
    "https://example.com/a\\b",
    # Some path characters that RFC 2396 and RFC 3986 require escaping (%)
    # are not escaped.
    f"https://example.com/{PATH_TO_ENCODE}",
    # ? is removed
    "https://example.com?",
    "https://example.com/?",
    # Some query characters that RFC 2396 and RFC 3986 require escaping (%)
    # are not escaped.
    f"a://example.com?{QUERY_TO_ENCODE}",
    # Some special query characters that RFC 2396 and RFC 3986 require escaping
    # (%) are not escaped.
    *(
        f"{scheme}://example.com?{SPECIAL_QUERY_TO_ENCODE}"
        for scheme in _SPECIAL_SCHEMES
    ),
    # ? and # are removed
    "https://example.com#",
    "https://example.com/#",
    "https://example.com?#",
    "https://example.com/?#",
    # Some fragment characters that RFC 2396 and RFC 3986 require escaping
    # (%) are not escaped.
    f"a://example.com#{FRAGMENT_TO_ENCODE}",
}
if (
    sys.version_info < (3, 9, 21)
    or (sys.version_info[:2] == (3, 10) and sys.version_info < (3, 10, 16))
    or (sys.version_info[:2] == (3, 11) and sys.version_info < (3, 11, 4))
):
    KNOWN_SAFE_URL_STRING_URL_ISSUES.add("http://[2a01:5cc0:1:2:3:4]")  # Invalid IPv6


@pytest.mark.parametrize(
    ("url", "output"),
    [
        (
            case
            if case[0] not in KNOWN_SAFE_URL_STRING_URL_ISSUES
            else pytest.param(*case, marks=pytest.mark.xfail(strict=True))
        )
        for case in SAFE_URL_URL_CASES
    ],
)
def test_safe_url_string_url(url: str | bytes, output: str | type[Exception]) -> None:
    _test_safe_url_string(url, output=output)


class TestUrl:
    def test_safe_url_string(self):
        # Motoko Kusanagi (Cyborg from Ghost in the Shell)
        motoko = "\u8349\u8599 \u7d20\u5b50"
        # note the %20 for space
        assert safe_url_string(motoko) == "%E8%8D%89%E8%96%99%20%E7%B4%A0%E5%AD%90"
        assert safe_url_string(motoko) == safe_url_string(safe_url_string(motoko))
        # copyright symbol
        assert safe_url_string("©") == "%C2%A9"
        # page-encoding does not affect URL path
        assert safe_url_string("©", "iso-8859-1") == "%C2%A9"
        # path_encoding does
        assert safe_url_string("©", path_encoding="iso-8859-1") == "%A9"
        assert safe_url_string("http://www.example.org/") == "http://www.example.org/"

        alessi = "/ecommerce/oggetto/Te \xf2/tea-strainer/1273"

        assert (
            safe_url_string(alessi)
            == "/ecommerce/oggetto/Te%20%C3%B2/tea-strainer/1273"
        )

        assert (
            safe_url_string(
                "http://www.example.com/test?p(29)url(http://www.another.net/page)"
            )
            == "http://www.example.com/test?p(29)url(http://www.another.net/page)"
        )
        assert (
            safe_url_string(
                "http://www.example.com/Brochures_&_Paint_Cards&PageSize=200"
            )
            == "http://www.example.com/Brochures_&_Paint_Cards&PageSize=200"
        )

        # page-encoding does not affect URL path
        # we still end up UTF-8 encoding characters before percent-escaping
        safeurl = safe_url_string("http://www.example.com/£")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3"

        safeurl = safe_url_string("http://www.example.com/£", encoding="utf-8")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3"

        safeurl = safe_url_string("http://www.example.com/£", encoding="latin-1")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3"

        safeurl = safe_url_string("http://www.example.com/£", path_encoding="latin-1")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%A3"

        assert isinstance(safe_url_string(b"http://example.com/"), str)

    def test_safe_url_string_remove_ascii_tab_and_newlines(self):
        assert (
            safe_url_string("http://example.com/test\n.html")
            == "http://example.com/test.html"
        )
        assert (
            safe_url_string("http://example.com/test\t.html")
            == "http://example.com/test.html"
        )
        assert (
            safe_url_string("http://example.com/test\r.html")
            == "http://example.com/test.html"
        )
        assert (
            safe_url_string("http://example.com/test\r.html\n")
            == "http://example.com/test.html"
        )
        assert (
            safe_url_string("http://example.com/test\r\n.html\t")
            == "http://example.com/test.html"
        )
        assert (
            safe_url_string("http://example.com/test\a\n.html")
            == "http://example.com/test%07.html"
        )

    def test_safe_url_string_quote_path(self):
        safeurl = safe_url_string('http://google.com/"hello"', quote_path=True)
        assert safeurl == "http://google.com/%22hello%22"

        safeurl = safe_url_string('http://google.com/"hello"', quote_path=False)
        assert safeurl == 'http://google.com/"hello"'

        safeurl = safe_url_string('http://google.com/"hello"')
        assert safeurl == "http://google.com/%22hello%22"

    def test_safe_url_string_with_query(self):
        safeurl = safe_url_string("http://www.example.com/£?unit=µ")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3?unit=%C2%B5"

        safeurl = safe_url_string("http://www.example.com/£?unit=µ", encoding="utf-8")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3?unit=%C2%B5"

        safeurl = safe_url_string("http://www.example.com/£?unit=µ", encoding="latin-1")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3?unit=%B5"

        safeurl = safe_url_string(
            "http://www.example.com/£?unit=µ", path_encoding="latin-1"
        )
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%A3?unit=%C2%B5"

        safeurl = safe_url_string(
            "http://www.example.com/£?unit=µ",
            encoding="latin-1",
            path_encoding="latin-1",
        )
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%A3?unit=%B5"

    def test_safe_url_string_misc(self):
        # mixing Unicode and percent-escaped sequences
        safeurl = safe_url_string("http://www.example.com/£?unit=%C2%B5")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3?unit=%C2%B5"

        safeurl = safe_url_string("http://www.example.com/%C2%A3?unit=µ")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3?unit=%C2%B5"

    def test_safe_url_string_bytes_input(self):
        safeurl = safe_url_string(b"http://www.example.com/")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/"

        # bytes input is assumed to be UTF-8
        safeurl = safe_url_string(b"http://www.example.com/\xc2\xb5")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%B5"

        # page-encoding encoded bytes still end up as UTF-8 sequences in path
        safeurl = safe_url_string(b"http://www.example.com/\xb5", encoding="latin1")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%B5"

        safeurl = safe_url_string(
            b"http://www.example.com/\xa3?unit=\xb5", encoding="latin1"
        )
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%C2%A3?unit=%B5"

    def test_safe_url_string_bytes_input_nonutf8(self):
        # latin1
        safeurl = safe_url_string(b"http://www.example.com/\xa3?unit=\xb5")
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/%A3?unit=%B5"

        # cp1251
        # >>> 'Россия'.encode('cp1251')
        # '\xd0\xee\xf1\xf1\xe8\xff'
        safeurl = safe_url_string(
            b"http://www.example.com/country/\xd0\xee\xf1\xf1\xe8\xff"
        )
        assert isinstance(safeurl, str)
        assert safeurl == "http://www.example.com/country/%D0%EE%F1%F1%E8%FF"

    def test_safe_url_idna(self):
        # adapted from:
        # https://ssl.icu-project.org/icu-bin/idnbrowser
        # http://unicode.org/faq/idn.html
        # + various others
        websites = (
            (
                "http://www.färgbolaget.nu/färgbolaget",
                "http://www.xn--frgbolaget-q5a.nu/f%C3%A4rgbolaget",
            ),
            (
                "http://www.räksmörgås.se/?räksmörgås=yes",
                "http://www.xn--rksmrgs-5wao1o.se/?r%C3%A4ksm%C3%B6rg%C3%A5s=yes",
            ),
            (
                "http://www.brændendekærlighed.com/brændende/kærlighed",
                "http://www.xn--brndendekrlighed-vobh.com/br%C3%A6ndende/k%C3%A6rlighed",
            ),
            ("http://www.예비교사.com", "http://www.xn--9d0bm53a3xbzui.com"),
            ("http://理容ナカムラ.com", "http://xn--lck1c3crb1723bpq4a.com"),
            ("http://あーるいん.com", "http://xn--l8je6s7a45b.com"),
            # --- real websites ---
            # in practice, this redirect (301) to http://www.buecher.de/?q=b%C3%BCcher
            (
                "http://www.bücher.de/?q=bücher",
                "http://www.xn--bcher-kva.de/?q=b%C3%BCcher",
            ),
            # Japanese
            (
                "http://はじめよう.みんな/?query=サ&maxResults=5",
                "http://xn--p8j9a0d9c9a.xn--q9jyb4c/?query=%E3%82%B5&maxResults=5",
            ),
            # Russian
            ("http://кто.рф/", "http://xn--j1ail.xn--p1ai/"),
            (
                "http://кто.рф/index.php?domain=Что",
                "http://xn--j1ail.xn--p1ai/index.php?domain=%D0%A7%D1%82%D0%BE",
            ),
            # Korean
            ("http://내도메인.한국/", "http://xn--220b31d95hq8o.xn--3e0b707e/"),
            (
                "http://맨체스터시티축구단.한국/",
                "http://xn--2e0b17htvgtvj9haj53ccob62ni8d.xn--3e0b707e/",
            ),
            # Arabic
            ("http://nic.شبكة", "http://nic.xn--ngbc5azd"),
            # Chinese
            ("https://www.贷款.在线", "https://www.xn--0kwr83e.xn--3ds443g"),
            ("https://www2.xn--0kwr83e.在线", "https://www2.xn--0kwr83e.xn--3ds443g"),
            ("https://www3.贷款.xn--3ds443g", "https://www3.xn--0kwr83e.xn--3ds443g"),
        )
        for idn_input, safe_result in websites:
            safeurl = safe_url_string(idn_input)
            assert safeurl == safe_result

        # make sure the safe URL is unchanged when made safe a 2nd time
        for _, safe_result in websites:
            safeurl = safe_url_string(safe_result)
            assert safeurl == safe_result

    def test_safe_url_idna_encoding_failure(self):
        # missing DNS label
        assert (
            safe_url_string("http://.example.com/résumé?q=résumé")
            == "http://.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )

        # DNS label too long
        assert (
            safe_url_string(f"http://www.{'example' * 11}.com/résumé?q=résumé")
            == f"http://www.{'example' * 11}.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )

    def test_safe_url_port_number(self):
        assert (
            safe_url_string("http://www.example.com:80/résumé?q=résumé")
            == "http://www.example.com:80/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )
        assert (
            safe_url_string("http://www.example.com:/résumé?q=résumé")
            == "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )

    def test_safe_url_string_preserve_nonfragment_hash(self):
        # don't decode `%23` to `#`
        assert (
            safe_url_string("http://www.example.com/path/to/%23/foo/bar")
            == "http://www.example.com/path/to/%23/foo/bar"
        )
        assert (
            safe_url_string("http://www.example.com/path/to/%23/foo/bar#frag")
            == "http://www.example.com/path/to/%23/foo/bar#frag"
        )
        assert (
            safe_url_string(
                "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2Fpath%2Fto%2F%23%2Fbar%2Ffoo"
            )
            == "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2Fpath%2Fto%2F%23%2Fbar%2Ffoo"
        )
        assert (
            safe_url_string(
                "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2F%2Fpath%2Fto%2F%23%2Fbar%2Ffoo#frag"
            )
            == "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2F%2Fpath%2Fto%2F%23%2Fbar%2Ffoo#frag"
        )

    def test_safe_url_string_encode_idna_domain_with_port(self):
        assert (
            safe_url_string("http://新华网.中国:80")
            == "http://xn--xkrr14bows.xn--fiqs8s:80"
        )

    def test_safe_url_string_encode_idna_domain_with_username_password_and_port_number(
        self,
    ):
        assert (
            safe_url_string("ftp://admin:admin@新华网.中国:21")
            == "ftp://admin:admin@xn--xkrr14bows.xn--fiqs8s:21"
        )
        assert (
            safe_url_string("http://Åsa:abc123@➡.ws:81/admin")
            == "http://%C3%85sa:abc123@xn--hgi.ws:81/admin"
        )
        assert (
            safe_url_string("http://japão:não@️i❤️.ws:8000/")
            == "http://jap%C3%A3o:n%C3%A3o@xn--i-7iq.ws:8000/"
        )

    def test_safe_url_string_encode_idna_domain_with_username_and_empty_password_and_port_number(
        self,
    ):
        assert (
            safe_url_string("ftp://admin:@新华网.中国:21")
            == "ftp://admin:@xn--xkrr14bows.xn--fiqs8s:21"
        )
        assert (
            safe_url_string("ftp://admin@新华网.中国:21")
            == "ftp://admin@xn--xkrr14bows.xn--fiqs8s:21"
        )

    def test_safe_url_string_userinfo_unsafe_chars(
        self,
    ):
        assert (
            safe_url_string("ftp://admin:|%@example.com")
            == "ftp://admin:%7C%25@example.com"
        )

    def test_safe_url_string_user_and_pass_percentage_encoded(self):
        assert (
            safe_url_string("http://%25user:%25pass@host")
            == "http://%25user:%25pass@host"
        )

        assert (
            safe_url_string("http://%user:%pass@host") == "http://%25user:%25pass@host"
        )

        assert (
            safe_url_string("http://%26user:%26pass@host") == "http://&user:&pass@host"
        )

        assert (
            safe_url_string("http://%2525user:%2525pass@host")
            == "http://%2525user:%2525pass@host"
        )

        assert (
            safe_url_string("http://%2526user:%2526pass@host")
            == "http://%2526user:%2526pass@host"
        )

        assert (
            safe_url_string("http://%25%26user:%25%26pass@host")
            == "http://%25&user:%25&pass@host"
        )

    def test_safe_download_url(self):
        assert safe_download_url("http://www.example.org") == "http://www.example.org/"
        assert (
            safe_download_url("http://www.example.org/../") == "http://www.example.org/"
        )
        assert (
            safe_download_url("http://www.example.org/../../images/../image")
            == "http://www.example.org/image"
        )
        assert (
            safe_download_url("http://www.example.org/dir/")
            == "http://www.example.org/dir/"
        )
        assert (
            safe_download_url(b"http://www.example.org/dir/")
            == "http://www.example.org/dir/"
        )

        # Encoding related tests
        assert (
            safe_download_url(
                b"http://www.example.org?\xa3",
                encoding="latin-1",
                path_encoding="latin-1",
            )
            == "http://www.example.org/?%A3"
        )
        assert (
            safe_download_url(
                b"http://www.example.org?\xc2\xa3",
                encoding="utf-8",
                path_encoding="utf-8",
            )
            == "http://www.example.org/?%C2%A3"
        )
        assert (
            safe_download_url(
                b"http://www.example.org/\xc2\xa3?\xc2\xa3",
                encoding="utf-8",
                path_encoding="latin-1",
            )
            == "http://www.example.org/%A3?%C2%A3"
        )

    def test_is_url(self):
        assert is_url("http://www.example.org")
        assert is_url("https://www.example.org")
        assert is_url("file:///some/path")
        assert not is_url("foo://bar")
        assert not is_url("foo--bar")

    def test_url_query_parameter(self):
        assert url_query_parameter("product.html?id=200&foo=bar", "id") == "200"
        assert (
            url_query_parameter("product.html?id=200&foo=bar", "notthere", "mydefault")
            == "mydefault"
        )
        assert url_query_parameter("product.html?id=", "id") is None
        assert url_query_parameter("product.html?id=", "id", keep_blank_values=1) == ""

    @pytest.mark.xfail
    def test_url_query_parameter_2(self):
        """
        This problem was seen several times in the feeds. Sometime affiliate URLs contains
        nested encoded affiliate URL with direct URL as parameters. For example:
        aff_url1 = 'http://www.tkqlhce.com/click-2590032-10294381?url=http%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FArgosCreateReferral%3FstoreId%3D10001%26langId%3D-1%26referrer%3DCOJUN%26params%3Dadref%253DGarden+and+DIY-%3EGarden+furniture-%3EChildren%26%2339%3Bs+garden+furniture%26referredURL%3Dhttp%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FProductDisplay%253FstoreId%253D10001%2526catalogId%253D1500001501%2526productId%253D1500357023%2526langId%253D-1'
        the typical code to extract needed URL from it is:
        aff_url2 = url_query_parameter(aff_url1, 'url')
        after this aff2_url is:
        'http://www.argos.co.uk/webapp/wcs/stores/servlet/ArgosCreateReferral?storeId=10001&langId=-1&referrer=COJUN&params=adref%3DGarden and DIY->Garden furniture->Children&#39;s gardenfurniture&referredURL=http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay%3FstoreId%3D10001%26catalogId%3D1500001501%26productId%3D1500357023%26langId%3D-1'
        the direct URL extraction is
        url = url_query_parameter(aff_url2, 'referredURL')
        but this will not work, because aff_url2 contains &#39; (comma sign encoded in the feed)
        and the URL extraction will fail, current workaround was made in the spider,
        just a replace for &#39; to %27
        """
        # correct case
        aff_url1 = "http://www.anrdoezrs.net/click-2590032-10294381?url=http%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FArgosCreateReferral%3FstoreId%3D10001%26langId%3D-1%26referrer%3DCOJUN%26params%3Dadref%253DGarden+and+DIY-%3EGarden+furniture-%3EGarden+table+and+chair+sets%26referredURL%3Dhttp%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FProductDisplay%253FstoreId%253D10001%2526catalogId%253D1500001501%2526productId%253D1500357199%2526langId%253D-1"
        aff_url2 = url_query_parameter(aff_url1, "url")
        assert (
            aff_url2
            == "http://www.argos.co.uk/webapp/wcs/stores/servlet/ArgosCreateReferral?storeId=10001&langId=-1&referrer=COJUN&params=adref%3DGarden and DIY->Garden furniture->Garden table and chair sets&referredURL=http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay%3FstoreId%3D10001%26catalogId%3D1500001501%26productId%3D1500357199%26langId%3D-1"
        )
        assert aff_url2 is not None
        prod_url = url_query_parameter(aff_url2, "referredURL")
        assert (
            prod_url
            == "http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay?storeId=10001&catalogId=1500001501&productId=1500357199&langId=-1"
        )
        # weird case
        aff_url1 = "http://www.tkqlhce.com/click-2590032-10294381?url=http%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FArgosCreateReferral%3FstoreId%3D10001%26langId%3D-1%26referrer%3DCOJUN%26params%3Dadref%253DGarden+and+DIY-%3EGarden+furniture-%3EChildren%26%2339%3Bs+garden+furniture%26referredURL%3Dhttp%3A%2F%2Fwww.argos.co.uk%2Fwebapp%2Fwcs%2Fstores%2Fservlet%2FProductDisplay%253FstoreId%253D10001%2526catalogId%253D1500001501%2526productId%253D1500357023%2526langId%253D-1"
        aff_url2 = url_query_parameter(aff_url1, "url")
        assert (
            aff_url2
            == "http://www.argos.co.uk/webapp/wcs/stores/servlet/ArgosCreateReferral?storeId=10001&langId=-1&referrer=COJUN&params=adref%3DGarden and DIY->Garden furniture->Children&#39;s garden furniture&referredURL=http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay%3FstoreId%3D10001%26catalogId%3D1500001501%26productId%3D1500357023%26langId%3D-1"
        )
        assert aff_url2 is not None
        prod_url = url_query_parameter(aff_url2, "referredURL")
        # fails, prod_url is None now
        assert (
            prod_url
            == "http://www.argos.co.uk/webapp/wcs/stores/servlet/ProductDisplay?storeId=10001&catalogId=1500001501&productId=1500357023&langId=-1"
        )

    def test_add_or_replace_parameter(self):
        url = "http://domain/test"
        assert add_or_replace_parameter(url, "arg", "v") == "http://domain/test?arg=v"
        url = "http://domain/test?arg1=v1&arg2=v2&arg3=v3"
        assert (
            add_or_replace_parameter(url, "arg4", "v4")
            == "http://domain/test?arg1=v1&arg2=v2&arg3=v3&arg4=v4"
        )
        assert (
            add_or_replace_parameter(url, "arg3", "nv3")
            == "http://domain/test?arg1=v1&arg2=v2&arg3=nv3"
        )

        assert (
            add_or_replace_parameter(
                "http://domain/moreInfo.asp?prodID=", "prodID", "20"
            )
            == "http://domain/moreInfo.asp?prodID=20"
        )
        url = "http://rmc-offers.co.uk/productlist.asp?BCat=2%2C60&CatID=60"
        assert (
            add_or_replace_parameter(url, "BCat", "newvalue")
            == "http://rmc-offers.co.uk/productlist.asp?BCat=newvalue&CatID=60"
        )
        url = "http://rmc-offers.co.uk/productlist.asp?BCat=2,60&CatID=60"
        assert (
            add_or_replace_parameter(url, "BCat", "newvalue")
            == "http://rmc-offers.co.uk/productlist.asp?BCat=newvalue&CatID=60"
        )
        url = "http://rmc-offers.co.uk/productlist.asp?"
        assert (
            add_or_replace_parameter(url, "BCat", "newvalue")
            == "http://rmc-offers.co.uk/productlist.asp?BCat=newvalue"
        )

        url = "http://example.com/?version=1&pageurl=http%3A%2F%2Fwww.example.com%2Ftest%2F%23fragment%3Dy&param2=value2"
        assert (
            add_or_replace_parameter(url, "version", "2")
            == "http://example.com/?version=2&pageurl=http%3A%2F%2Fwww.example.com%2Ftest%2F%23fragment%3Dy&param2=value2"
        )
        assert (
            add_or_replace_parameter(url, "pageurl", "test")
            == "http://example.com/?version=1&pageurl=test&param2=value2"
        )

        url = "http://domain/test?arg1=v1&arg2=v2&arg1=v3"
        assert (
            add_or_replace_parameter(url, "arg4", "v4")
            == "http://domain/test?arg1=v1&arg2=v2&arg1=v3&arg4=v4"
        )
        assert (
            add_or_replace_parameter(url, "arg1", "v3")
            == "http://domain/test?arg1=v3&arg2=v2"
        )

    @pytest.mark.xfail(reason="https://github.com/scrapy/w3lib/issues/164")
    def test_add_or_replace_parameter_fail(self):
        assert (
            add_or_replace_parameter("http://domain/test?arg1=v1;arg2=v2", "arg1", "v3")
            == "http://domain/test?arg1=v3&arg2=v2"
        )

    def test_add_or_replace_parameters(self):
        url = "http://domain/test"
        assert (
            add_or_replace_parameters(url, {"arg": "v"}) == "http://domain/test?arg=v"
        )
        url = "http://domain/test?arg1=v1&arg2=v2&arg3=v3"
        assert (
            add_or_replace_parameters(url, {"arg4": "v4"})
            == "http://domain/test?arg1=v1&arg2=v2&arg3=v3&arg4=v4"
        )
        assert (
            add_or_replace_parameters(url, {"arg4": "v4", "arg3": "v3new"})
            == "http://domain/test?arg1=v1&arg2=v2&arg3=v3new&arg4=v4"
        )
        url = "http://domain/test?arg1=v1&arg2=v2&arg1=v3"
        assert (
            add_or_replace_parameters(url, {"arg4": "v4"})
            == "http://domain/test?arg1=v1&arg2=v2&arg1=v3&arg4=v4"
        )
        assert (
            add_or_replace_parameters(url, {"arg1": "v3"})
            == "http://domain/test?arg1=v3&arg2=v2"
        )

    def test_add_or_replace_parameters_does_not_change_input_param(self):
        url = "http://domain/test?arg=original"
        input_param = {"arg": "value"}
        add_or_replace_parameters(url, input_param)
        assert input_param == {"arg": "value"}

    def test_url_query_cleaner(self):
        assert url_query_cleaner("product.html?") == "product.html"
        assert url_query_cleaner("product.html?&") == "product.html"
        assert (
            url_query_cleaner("product.html?id=200&foo=bar&name=wired", ["id"])
            == "product.html?id=200"
        )
        assert (
            url_query_cleaner("product.html?&id=200&&foo=bar&name=wired", ["id"])
            == "product.html?id=200"
        )
        assert (
            url_query_cleaner("product.html?foo=bar&name=wired", ["id"])
            == "product.html"
        )
        assert (
            url_query_cleaner("product.html?id=200&foo=bar&name=wired", ["id", "name"])
            == "product.html?id=200&name=wired"
        )
        assert (
            url_query_cleaner("product.html?id&other=3&novalue=", ["id"])
            == "product.html?id"
        )
        # default is to remove duplicate keys
        assert (
            url_query_cleaner("product.html?d=1&e=b&d=2&d=3&other=other", ["d"])
            == "product.html?d=1"
        )
        # unique=False disables duplicate keys filtering
        assert (
            url_query_cleaner(
                "product.html?d=1&e=b&d=2&d=3&other=other", ["d"], unique=False
            )
            == "product.html?d=1&d=2&d=3"
        )
        assert (
            url_query_cleaner(
                "product.html?id=200&foo=bar&name=wired#id20", ["id", "foo"]
            )
            == "product.html?id=200&foo=bar"
        )
        assert (
            url_query_cleaner(
                "product.html?id=200&foo=bar&name=wired", ["id"], remove=True
            )
            == "product.html?foo=bar&name=wired"
        )
        assert (
            url_query_cleaner(
                "product.html?id=2&foo=bar&name=wired", ["id", "foo"], remove=True
            )
            == "product.html?name=wired"
        )
        assert (
            url_query_cleaner(
                "product.html?id=2&foo=bar&name=wired", ["id", "footo"], remove=True
            )
            == "product.html?foo=bar&name=wired"
        )
        assert url_query_cleaner("product.html", ["id"], remove=True) == "product.html"
        assert (
            url_query_cleaner("product.html?&", ["id"], remove=True) == "product.html"
        )
        assert (
            url_query_cleaner("product.html?foo=bar&name=wired", "foo")
            == "product.html?foo=bar"
        )
        assert (
            url_query_cleaner("product.html?foo=bar&foobar=wired", "foobar")
            == "product.html?foobar=wired"
        )

    def test_url_query_cleaner_keep_fragments(self):
        assert (
            url_query_cleaner(
                "product.html?id=200&foo=bar&name=wired#foo",
                ["id"],
                keep_fragments=True,
            )
            == "product.html?id=200#foo"
        )
        assert (
            url_query_cleaner(
                "product.html?id=200&foo=bar&name=wired", ["id"], keep_fragments=True
            )
            == "product.html?id=200"
        )

    def test_path_to_file_uri(self):
        if os.name == "nt":
            assert (
                path_to_file_uri(r"C:\windows\clock.avi")
                == "file:///C:/windows/clock.avi"
            )
        else:
            assert path_to_file_uri("/some/path.txt") == "file:///some/path.txt"

        fn = "test.txt"
        x = path_to_file_uri(fn)
        assert x.startswith("file:///")
        assert file_uri_to_path(x).lower() == str(Path(fn).absolute()).lower()

    def test_file_uri_to_path(self):
        if os.name == "nt":
            assert (
                file_uri_to_path("file:///C:/windows/clock.avi")
                == r"C:\windows\clock.avi"
            )
            uri = "file:///C:/windows/clock.avi"
            uri2 = path_to_file_uri(file_uri_to_path(uri))
            assert uri == uri2
        else:
            assert file_uri_to_path("file:///path/to/test.txt") == "/path/to/test.txt"
            assert file_uri_to_path("/path/to/test.txt") == "/path/to/test.txt"
            uri = "file:///path/to/test.txt"
            uri2 = path_to_file_uri(file_uri_to_path(uri))
            assert uri == uri2

        assert file_uri_to_path("test.txt") == "test.txt"

    def test_any_to_uri(self):
        if os.name == "nt":
            assert any_to_uri(r"C:\windows\clock.avi") == "file:///C:/windows/clock.avi"
        else:
            assert any_to_uri("/some/path.txt") == "file:///some/path.txt"
        assert any_to_uri("file:///some/path.txt") == "file:///some/path.txt"
        assert (
            any_to_uri("http://www.example.com/some/path.txt")
            == "http://www.example.com/some/path.txt"
        )


class TestCanonicalizeUrl:
    def test_canonicalize_url(self):
        # simplest case
        assert canonicalize_url("http://www.example.com/") == "http://www.example.com/"

    def test_return_str(self):
        assert isinstance(canonicalize_url("http://www.example.com"), str)
        assert isinstance(canonicalize_url(b"http://www.example.com"), str)

    def test_append_missing_path(self):
        assert canonicalize_url("http://www.example.com") == "http://www.example.com/"

    def test_typical_usage(self):
        assert (
            canonicalize_url("http://www.example.com/do?a=1&b=2&c=3")
            == "http://www.example.com/do?a=1&b=2&c=3"
        )
        assert (
            canonicalize_url("http://www.example.com/do?c=1&b=2&a=3")
            == "http://www.example.com/do?a=3&b=2&c=1"
        )
        assert (
            canonicalize_url("http://www.example.com/do?&a=1")
            == "http://www.example.com/do?a=1"
        )

    def test_port_number(self):
        assert (
            canonicalize_url("http://www.example.com:8888/do?a=1&b=2&c=3")
            == "http://www.example.com:8888/do?a=1&b=2&c=3"
        )
        # trailing empty ports are removed
        assert (
            canonicalize_url("http://www.example.com:/do?a=1&b=2&c=3")
            == "http://www.example.com/do?a=1&b=2&c=3"
        )

    def test_sorting(self):
        assert (
            canonicalize_url("http://www.example.com/do?c=3&b=5&b=2&a=50")
            == "http://www.example.com/do?a=50&b=2&b=5&c=3"
        )

    def test_keep_blank_values(self):
        assert (
            canonicalize_url(
                "http://www.example.com/do?b=&a=2", keep_blank_values=False
            )
            == "http://www.example.com/do?a=2"
        )
        assert (
            canonicalize_url("http://www.example.com/do?b=&a=2")
            == "http://www.example.com/do?a=2&b="
        )
        assert (
            canonicalize_url(
                "http://www.example.com/do?b=&c&a=2", keep_blank_values=False
            )
            == "http://www.example.com/do?a=2"
        )
        assert (
            canonicalize_url("http://www.example.com/do?b=&c&a=2")
            == "http://www.example.com/do?a=2&b=&c="
        )

        assert (
            canonicalize_url("http://www.example.com/do?1750,4")
            == "http://www.example.com/do?1750%2C4="
        )

    def test_spaces(self):
        assert (
            canonicalize_url("http://www.example.com/do?q=a space&a=1")
            == "http://www.example.com/do?a=1&q=a+space"
        )
        assert (
            canonicalize_url("http://www.example.com/do?q=a+space&a=1")
            == "http://www.example.com/do?a=1&q=a+space"
        )
        assert (
            canonicalize_url("http://www.example.com/do?q=a%20space&a=1")
            == "http://www.example.com/do?a=1&q=a+space"
        )

    def test_canonicalize_url_unicode_path(self):
        assert (
            canonicalize_url("http://www.example.com/résumé")
            == "http://www.example.com/r%C3%A9sum%C3%A9"
        )

    def test_canonicalize_url_unicode_query_string(self):
        # default encoding for path and query is UTF-8
        assert (
            canonicalize_url("http://www.example.com/résumé?q=résumé")
            == "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )

        # passed encoding will affect query string
        assert (
            canonicalize_url(
                "http://www.example.com/résumé?q=résumé", encoding="latin1"
            )
            == "http://www.example.com/r%C3%A9sum%C3%A9?q=r%E9sum%E9"
        )

        assert (
            canonicalize_url(
                "http://www.example.com/résumé?country=Россия", encoding="cp1251"
            )
            == "http://www.example.com/r%C3%A9sum%C3%A9?country=%D0%EE%F1%F1%E8%FF"
        )

    def test_canonicalize_url_unicode_query_string_wrong_encoding(self):
        # trying to encode with wrong encoding
        # fallback to UTF-8
        assert (
            canonicalize_url(
                "http://www.example.com/résumé?currency=€", encoding="latin1"
            )
            == "http://www.example.com/r%C3%A9sum%C3%A9?currency=%E2%82%AC"
        )

        assert (
            canonicalize_url(
                "http://www.example.com/résumé?country=Россия", encoding="latin1"
            )
            == "http://www.example.com/r%C3%A9sum%C3%A9?country=%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F"
        )

    def test_normalize_percent_encoding_in_paths(self):
        assert (
            canonicalize_url("http://www.example.com/r%c3%a9sum%c3%a9")
            == "http://www.example.com/r%C3%A9sum%C3%A9"
        )

        # non-UTF8 encoded sequences: they should be kept untouched, only upper-cased
        # 'latin1'-encoded sequence in path
        assert (
            canonicalize_url("http://www.example.com/a%a3do")
            == "http://www.example.com/a%A3do"
        )

        # 'latin1'-encoded path, UTF-8 encoded query string
        assert (
            canonicalize_url("http://www.example.com/a%a3do?q=r%c3%a9sum%c3%a9")
            == "http://www.example.com/a%A3do?q=r%C3%A9sum%C3%A9"
        )

        # 'latin1'-encoded path and query string
        assert (
            canonicalize_url("http://www.example.com/a%a3do?q=r%e9sum%e9")
            == "http://www.example.com/a%A3do?q=r%E9sum%E9"
        )

        url = "https://example.com/a%23b%2cc#bash"
        canonical = canonicalize_url(url)
        # %23 is not accidentally interpreted as a URL fragment separator
        assert canonical == "https://example.com/a%23b,c"
        assert canonical == canonicalize_url(canonical)

    def test_normalize_percent_encoding_in_query_arguments(self):
        assert (
            canonicalize_url("http://www.example.com/do?k=b%a3")
            == "http://www.example.com/do?k=b%A3"
        )

        assert (
            canonicalize_url("http://www.example.com/do?k=r%c3%a9sum%c3%a9")
            == "http://www.example.com/do?k=r%C3%A9sum%C3%A9"
        )

    def test_non_ascii_percent_encoding_in_paths(self):
        assert (
            canonicalize_url("http://www.example.com/a do?a=1")
            == "http://www.example.com/a%20do?a=1"
        )

        assert (
            canonicalize_url("http://www.example.com/a %20do?a=1")
            == "http://www.example.com/a%20%20do?a=1"
        )

        assert (
            canonicalize_url("http://www.example.com/a do£.html?a=1")
            == "http://www.example.com/a%20do%C2%A3.html?a=1"
        )

        assert (
            canonicalize_url(b"http://www.example.com/a do\xc2\xa3.html?a=1")
            == "http://www.example.com/a%20do%C2%A3.html?a=1"
        )

    def test_non_ascii_percent_encoding_in_query_arguments(self):
        assert (
            canonicalize_url("http://www.example.com/do?price=£500&a=5&z=3")
            == "http://www.example.com/do?a=5&price=%C2%A3500&z=3"
        )
        assert (
            canonicalize_url(b"http://www.example.com/do?price=\xc2\xa3500&a=5&z=3")
            == "http://www.example.com/do?a=5&price=%C2%A3500&z=3"
        )
        assert (
            canonicalize_url(b"http://www.example.com/do?price(\xc2\xa3)=500&a=1")
            == "http://www.example.com/do?a=1&price%28%C2%A3%29=500"
        )

    def test_urls_with_auth_and_ports(self):
        assert (
            canonicalize_url("http://user:pass@www.example.com:81/do?now=1")
            == "http://user:pass@www.example.com:81/do?now=1"
        )

    def test_remove_fragments(self):
        assert (
            canonicalize_url("http://user:pass@www.example.com/do?a=1#frag")
            == "http://user:pass@www.example.com/do?a=1"
        )
        assert (
            canonicalize_url(
                "http://user:pass@www.example.com/do?a=1#frag", keep_fragments=True
            )
            == "http://user:pass@www.example.com/do?a=1#frag"
        )

    def test_dont_convert_safe_characters(self):
        # dont convert safe characters to percent encoding representation
        assert (
            canonicalize_url(
                "http://www.simplybedrooms.com/White-Bedroom-Furniture/Bedroom-Mirror:-Josephine-Cheval-Mirror.html"
            )
            == "http://www.simplybedrooms.com/White-Bedroom-Furniture/Bedroom-Mirror:-Josephine-Cheval-Mirror.html"
        )

    def test_safe_characters_unicode(self):
        # urllib.quote uses a mapping cache of encoded characters. when parsing
        # an already percent-encoded url, it will fail if that url was not
        # percent-encoded as utf-8, that's why canonicalize_url must always
        # convert the urls to string. the following test asserts that
        # functionality.
        assert (
            canonicalize_url("http://www.example.com/caf%E9-con-leche.htm")
            == "http://www.example.com/caf%E9-con-leche.htm"
        )

    def test_domains_are_case_insensitive(self):
        assert canonicalize_url("http://www.EXAMPLE.com/") == "http://www.example.com/"

    def test_userinfo_is_case_sensitive(self):
        assert (
            canonicalize_url("sftp://UsEr:PaSsWoRd@www.EXAMPLE.com/")
            == "sftp://UsEr:PaSsWoRd@www.example.com/"
        )

    def test_canonicalize_idns(self):
        assert (
            canonicalize_url("http://www.bücher.de?q=bücher")
            == "http://www.xn--bcher-kva.de/?q=b%C3%BCcher"
        )
        # Japanese (+ reordering query parameters)
        assert (
            canonicalize_url("http://はじめよう.みんな/?query=サ&maxResults=5")
            == "http://xn--p8j9a0d9c9a.xn--q9jyb4c/?maxResults=5&query=%E3%82%B5"
        )

    def test_quoted_slash_and_question_sign(self):
        assert (
            canonicalize_url("http://foo.com/AC%2FDC+rocks%3f/?yeah=1")
            == "http://foo.com/AC%2FDC+rocks%3F/?yeah=1"
        )
        assert canonicalize_url("http://foo.com/AC%2FDC/") == "http://foo.com/AC%2FDC/"

    def test_canonicalize_urlparsed(self):
        # canonicalize_url() can be passed an already urlparse'd URL
        assert (
            canonicalize_url(urlparse("http://www.example.com/résumé?q=résumé"))
            == "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )
        assert (
            canonicalize_url(urlparse("http://www.example.com/caf%e9-con-leche.htm"))
            == "http://www.example.com/caf%E9-con-leche.htm"
        )
        assert (
            canonicalize_url(
                urlparse("http://www.example.com/a%a3do?q=r%c3%a9sum%c3%a9")
            )
            == "http://www.example.com/a%A3do?q=r%C3%A9sum%C3%A9"
        )

    def test_canonicalize_parse_url(self):
        # parse_url() wraps urlparse and is used in link extractors
        assert (
            canonicalize_url(parse_url("http://www.example.com/résumé?q=résumé"))
            == "http://www.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )
        assert (
            canonicalize_url(parse_url("http://www.example.com/caf%e9-con-leche.htm"))
            == "http://www.example.com/caf%E9-con-leche.htm"
        )
        assert (
            canonicalize_url(
                parse_url("http://www.example.com/a%a3do?q=r%c3%a9sum%c3%a9")
            )
            == "http://www.example.com/a%A3do?q=r%C3%A9sum%C3%A9"
        )

    def test_canonicalize_url_idempotence(self):
        for url, enc in [
            ("http://www.bücher.de/résumé?q=résumé", "utf8"),
            ("http://www.example.com/résumé?q=résumé", "latin1"),
            ("http://www.example.com/résumé?country=Россия", "cp1251"),
            ("http://はじめよう.みんな/?query=サ&maxResults=5", "iso2022jp"),
        ]:
            canonicalized = canonicalize_url(url, encoding=enc)

            # if we canonicalize again, we ge the same result
            assert canonicalize_url(canonicalized, encoding=enc) == canonicalized

            # without encoding, already canonicalized URL is canonicalized identically
            assert canonicalize_url(canonicalized) == canonicalized

    def test_canonicalize_url_idna_exceptions(self):
        # missing DNS label
        assert (
            canonicalize_url("http://.example.com/résumé?q=résumé")
            == "http://.example.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )

        # DNS label too long
        assert (
            canonicalize_url(f"http://www.{'example' * 11}.com/résumé?q=résumé")
            == f"http://www.{'example' * 11}.com/r%C3%A9sum%C3%A9?q=r%C3%A9sum%C3%A9"
        )

    def test_preserve_nonfragment_hash(self):
        # don't decode `%23` to `#`
        assert (
            canonicalize_url("http://www.example.com/path/to/%23/foo/bar")
            == "http://www.example.com/path/to/%23/foo/bar"
        )
        assert (
            canonicalize_url("http://www.example.com/path/to/%23/foo/bar#frag")
            == "http://www.example.com/path/to/%23/foo/bar"
        )
        assert (
            canonicalize_url(
                "http://www.example.com/path/to/%23/foo/bar#frag", keep_fragments=True
            )
            == "http://www.example.com/path/to/%23/foo/bar#frag"
        )
        assert (
            canonicalize_url(
                "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2Fpath%2Fto%2F%23%2Fbar%2Ffoo"
            )
            == "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2Fpath%2Fto%2F%23%2Fbar%2Ffoo"
        )
        assert (
            canonicalize_url(
                "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2F%2Fpath%2Fto%2F%23%2Fbar%2Ffoo#frag"
            )
            == "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2F%2Fpath%2Fto%2F%23%2Fbar%2Ffoo"
        )
        assert (
            canonicalize_url(
                "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2F%2Fpath%2Fto%2F%23%2Fbar%2Ffoo#frag",
                keep_fragments=True,
            )
            == "http://www.example.com/path/to/%23/foo/bar?url=http%3A%2F%2Fwww.example.com%2F%2Fpath%2Fto%2F%23%2Fbar%2Ffoo#frag"
        )

    def test_strip_spaces(self):
        assert canonicalize_url(" https://example.com") == "https://example.com/"
        assert canonicalize_url("https://example.com ") == "https://example.com/"
        assert canonicalize_url(" https://example.com ") == "https://example.com/"


class TestDataURI:
    def test_default_mediatype_charset(self):
        result = parse_data_uri("data:,A%20brief%20note")
        assert result.media_type == "text/plain"
        assert result.media_type_parameters == {"charset": "US-ASCII"}
        assert result.data == b"A brief note"

    def test_text_uri(self):
        result = parse_data_uri("data:,A%20brief%20note")
        assert result.data == b"A brief note"

    def test_bytes_uri(self):
        result = parse_data_uri(b"data:,A%20brief%20note")
        assert result.data == b"A brief note"

    def test_unicode_uri(self):
        result = parse_data_uri("data:,é")
        assert result.data == "é".encode()

    def test_default_mediatype(self):
        result = parse_data_uri("data:;charset=iso-8859-7,%be%d3%be")
        assert result.media_type == "text/plain"
        assert result.media_type_parameters == {"charset": "iso-8859-7"}
        assert result.data == b"\xbe\xd3\xbe"

    def test_text_charset(self):
        result = parse_data_uri("data:text/plain;charset=iso-8859-7,%be%d3%be")
        assert result.media_type == "text/plain"
        assert result.media_type_parameters == {"charset": "iso-8859-7"}
        assert result.data == b"\xbe\xd3\xbe"

    def test_mediatype_parameters(self):
        result = parse_data_uri(
            "data:text/plain;"
            "foo=%22foo;bar%5C%22%22;"
            "charset=utf-8;"
            "bar=%22foo;%5C%22foo%20;/%20,%22,"
            "%CE%8E%CE%A3%CE%8E"
        )

        assert result.media_type == "text/plain"
        assert result.media_type_parameters == {
            "charset": "utf-8",
            "foo": 'foo;bar"',
            "bar": 'foo;"foo ;/ ,',
        }
        assert result.data == b"\xce\x8e\xce\xa3\xce\x8e"

    def test_base64(self):
        result = parse_data_uri("data:text/plain;base64,SGVsbG8sIHdvcmxkLg%3D%3D")
        assert result.media_type == "text/plain"
        assert result.data == b"Hello, world."

    def test_base64_spaces(self):
        result = parse_data_uri(
            "data:text/plain;base64,SGVsb%20G8sIH%0A%20%20dvcm%20%20%20xk%20Lg%3D%0A%3D"
        )
        assert result.media_type == "text/plain"
        assert result.data == b"Hello, world."

        result = parse_data_uri(
            "data:text/plain;base64,SGVsb G8sIH\n  dvcm   xk Lg%3D\n%3D"
        )
        assert result.media_type == "text/plain"
        assert result.data == b"Hello, world."

    def test_wrong_base64_param(self):
        with pytest.raises(ValueError, match="invalid data URI"):
            parse_data_uri("data:text/plain;baes64,SGVsbG8sIHdvcmxkLg%3D%3D")

    def test_missing_comma(self):
        with pytest.raises(ValueError, match="invalid data URI"):
            parse_data_uri("data:A%20brief%20note")

    def test_missing_scheme(self):
        with pytest.raises(ValueError, match="invalid URI"):
            parse_data_uri("text/plain,A%20brief%20note")

    def test_wrong_scheme(self):
        with pytest.raises(ValueError, match="not a data URI"):
            parse_data_uri("http://example.com/")

    def test_scheme_case_insensitive(self):
        result = parse_data_uri("DATA:,A%20brief%20note")
        assert result.data == b"A brief note"
        result = parse_data_uri("DaTa:,A%20brief%20note")
        assert result.data == b"A brief note"
