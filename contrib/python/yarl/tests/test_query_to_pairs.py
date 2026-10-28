from urllib.parse import parse_qsl, quote, quote_plus

import pytest

from yarl import URL, query_to_pairs
from yarl._quoting import NO_EXTENSIONS
from yarl._quoting_py import _Unquoter as _PyUnquoter

if NO_EXTENSIONS:
    unquoters = [_PyUnquoter]
    unquoter_ids = ["PyUnquoter"]
else:
    from yarl._quoting_c import (  # type: ignore[import-not-found]
        _Unquoter as _CUnquoter,
    )

    unquoters = [_PyUnquoter, _CUnquoter]
    unquoter_ids = ["PyUnquoter", "CUnquoter"]


@pytest.fixture(params=unquoters, ids=unquoter_ids)
def unquoter(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    unquote = request.param(plus=True, replace_invalid=True)
    monkeypatch.setattr("yarl._parse.UNQUOTER_PLUS", unquote)


QUERY_STRINGS = [
    "",
    "a",
    "a=1&b=2",
    "a=1&a=2&a=3",
    "x&",
    "&&",
    "&a=1&&b=2&",
    "=",
    "==",
    "a=b=c",
    "a=1;b=2",
    "a+b=c+d",
    "a+b=c%20d",
    "%2B=%2b&%26=%3D",
    "%25=%2525",
    "na%C3%AFve=%E2%82%AC",
    "%F0%9F%98%80=%E6%97%A5%E6%9C%AC",
    "%E9=%ff",
    "a=%E2%82",
    "a=%E2%82b",
    "a=%ED%A0%80",
    "a=%C0%AF",
    "a=%zz&b=%4&c=%",
    "a=%%41",
    "a=é&%C3é=%C3%A9é",
    "a=\x00&b=%00",
    "name=" + "a" * 5000 + "%20" + "b" * 5000,
    "&".join(f"f{i}=v+%C3%A9+{i}" for i in range(100)),
]


@pytest.mark.usefixtures("unquoter")
@pytest.mark.parametrize("query_string", QUERY_STRINGS)
def test_matches_parse_qsl(query_string: str) -> None:
    assert query_to_pairs(query_string) == parse_qsl(
        query_string, keep_blank_values=True
    )


@pytest.mark.parametrize("encoding", ["UTF8", "utf_8", "latin-1", "cp1252", "utf-16"])
@pytest.mark.parametrize("query_string", QUERY_STRINGS)
def test_matches_parse_qsl_encoding(query_string: str, encoding: str) -> None:
    assert query_to_pairs(query_string, encoding=encoding) == parse_qsl(
        query_string, keep_blank_values=True, encoding=encoding
    )


def test_real_world_form_body() -> None:
    body = "&".join(
        (
            "csrfmiddlewaretoken=" + quote_plus("AAECAwQF+/=="),
            "username=" + quote_plus("jane.doe+test@example.com"),
            "message=" + quote_plus("Hi,\r\n注文番号は10482です 🙏"),
            "next=" + quote("/account/?tab=security&ref=login", safe=""),
        )
    )
    assert query_to_pairs(body) == [
        ("csrfmiddlewaretoken", "AAECAwQF+/=="),
        ("username", "jane.doe+test@example.com"),
        ("message", "Hi,\r\n注文番号は10482です 🙏"),
        ("next", "/account/?tab=security&ref=login"),
    ]


@pytest.mark.parametrize(
    ("query_string", "max_fields"),
    [
        ("a=1", 1),
        ("a=1&b=2", 2),
        ("x&x&x", 3),
        ("x&x&", 3),
        ("&&", 3),
    ],
)
def test_max_fields_not_exceeded(query_string: str, max_fields: int) -> None:
    expected = parse_qsl(
        query_string, keep_blank_values=True, max_num_fields=max_fields
    )
    assert query_to_pairs(query_string, max_fields=max_fields) == expected


@pytest.mark.parametrize(
    ("query_string", "max_fields"),
    [
        ("a=1", 0),
        ("a=1&b=2", 1),
        ("x&x&x&x", 3),
        ("x&x&x&", 3),
        ("&&&", 3),
    ],
)
def test_max_fields_exceeded(query_string: str, max_fields: int) -> None:
    with pytest.raises(ValueError, match="Max number of fields exceeded"):
        parse_qsl(query_string, keep_blank_values=True, max_num_fields=max_fields)
    with pytest.raises(ValueError, match="Max number of fields exceeded"):
        query_to_pairs(query_string, max_fields=max_fields)


def test_max_fields_empty_query_string() -> None:
    # parse_qsl on Python 3.10 raises here, later versions have no fields to count
    assert query_to_pairs("", max_fields=0) == []


def test_max_fields_none_is_unlimited() -> None:
    assert len(query_to_pairs("x&" * 100_000)) == 100_000


def test_url_query_matches_parse_qsl() -> None:
    """Empty fields and invalid escapes are handled like parse_qsl."""
    query_string = "a=1&&b&c=%FF&d=%E2%82&"
    expected = parse_qsl(query_string, keep_blank_values=True)
    assert expected == [("a", "1"), ("b", ""), ("c", "\ufffd"), ("d", "\ufffd")]
    assert list(URL(f"http://example.com/?{query_string}").query.items()) == expected
    url = URL("http://example.com/").update_query(query_string)
    assert list(url.query.items()) == expected
