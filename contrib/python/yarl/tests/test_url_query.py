from typing import List, Tuple
from urllib.parse import parse_qs, urlencode

import pytest
from multidict import MultiDict, MultiDictProxy

from yarl import URL

# ========================================
# Basic chars in query values
# ========================================

URLS_WITH_BASIC_QUERY_VALUES: List[Tuple[URL, MultiDict]] = [
    # Empty strings, keys and values
    (
        URL("http://example.com"),
        MultiDict(),
    ),
    (
        URL("http://example.com?a="),
        MultiDict([("a", "")]),
    ),
    # ASCII chars
    (
        URL("http://example.com?a+b=c+d"),
        MultiDict({"a b": "c d"}),
    ),
    (
        URL("http://example.com?a=1&b=2"),
        MultiDict([("a", "1"), ("b", "2")]),
    ),
    (
        URL("http://example.com?a=1&b=2&a=3"),
        MultiDict([("a", "1"), ("b", "2"), ("a", "3")]),
    ),
    # Non-ASCI BMP chars
    (
        URL("http://example.com?ключ=знач"),
        MultiDict({"ключ": "знач"}),
    ),
    (
        URL("http://example.com?foo=ᴜɴɪᴄᴏᴅᴇ"),
        MultiDict({"foo": "ᴜɴɪᴄᴏᴅᴇ"}),
    ),
    # Non-BMP chars
    (
        URL("http://example.com?bar=𝕦𝕟𝕚𝕔𝕠𝕕𝕖"),
        MultiDict({"bar": "𝕦𝕟𝕚𝕔𝕠𝕕𝕖"}),
    ),
]


@pytest.mark.parametrize(
    "original_url, expected_query",
    URLS_WITH_BASIC_QUERY_VALUES,
)
def test_query_basic_parsing(original_url, expected_query):
    assert isinstance(original_url.query, MultiDictProxy)
    assert original_url.query == expected_query


@pytest.mark.parametrize(
    "original_url, expected_query",
    URLS_WITH_BASIC_QUERY_VALUES,
)
def test_query_basic_update_query(original_url, expected_query):
    new_url = original_url.update_query({})
    assert new_url == original_url


def test_query_dont_unqoute_twice():
    sample_url = "http://base.place?" + urlencode({"a": "/////"})
    query = urlencode({"url": sample_url})
    full_url = "http://test_url.aha?" + query

    url = URL(full_url)
    assert url.query["url"] == sample_url


# ========================================
# Reserved chars in query values
# ========================================

# See https://github.com/python/cpython#87133, which introduced a new
# `separator` keyword argument to `urllib.parse.parse_qs` (among others).
# If the name doesn't exist as a variable in the function bytecode, the
# test is expected to fail.
_SEMICOLON_XFAIL = pytest.mark.xfail(
    condition="separator" not in parse_qs.__code__.co_varnames,
    reason=(
        "Python versions < 3.7.10, < 3.8.8 and < 3.9.2 lack a fix for "
        'CVE-2021-23336 dropping ";" as a valid query parameter separator, '
        "making this test fail."
    ),
    strict=True,
)


URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES = [
    # Ampersand
    (URL("http://127.0.0.1/?a=10&b=20"), 2, "10"),
    (URL("http://127.0.0.1/?a=10%26b=20"), 1, "10&b=20"),
    (URL("http://127.0.0.1/?a=10%3Bb=20"), 1, "10;b=20"),
    # Semicolon, which is *not* a query parameter separator as of RFC3986
    (URL("http://127.0.0.1/?a=10;b=20"), 1, "10;b=20"),
    (URL("http://127.0.0.1/?a=10%26b=20"), 1, "10&b=20"),
    (URL("http://127.0.0.1/?a=10%3Bb=20"), 1, "10;b=20"),
]
URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES_W_XFAIL = [
    # Ampersand
    *URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES[:3],
    # Semicolon, which is *not* a query parameter separator as of RFC3986
    # Mark the first of these as expecting to fail on old Python patch releases.
    pytest.param(*URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES[3], marks=_SEMICOLON_XFAIL),
    *URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES[4:],
]


@pytest.mark.parametrize(
    "original_url, expected_query_len, expected_value_a",
    URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES_W_XFAIL,
)
def test_query_separators_from_parsing(
    original_url,
    expected_query_len,
    expected_value_a,
):
    assert len(original_url.query) == expected_query_len
    assert original_url.query["a"] == expected_value_a


@pytest.mark.parametrize(
    "original_url, expected_query_len, expected_value_a",
    URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES_W_XFAIL,
)
def test_query_separators_from_update_query(
    original_url,
    expected_query_len,
    expected_value_a,
):
    new_url = original_url.update_query({"c": expected_value_a})
    assert new_url.query["a"] == expected_value_a
    assert new_url.query["c"] == expected_value_a


@pytest.mark.parametrize(
    "original_url, expected_query_len, expected_value_a",
    URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES,
)
def test_query_separators_from_with_query(
    original_url,
    expected_query_len,
    expected_value_a,
):
    new_url = original_url.with_query({"c": expected_value_a})
    assert new_url.query["c"] == expected_value_a


@pytest.mark.parametrize(
    "original_url, expected_query_len, expected_value_a",
    URLS_WITH_RESERVED_CHARS_IN_QUERY_VALUES,
)
def test_query_from_empty_update_query(
    original_url,
    expected_query_len,
    expected_value_a,
):
    new_url = original_url.update_query({})

    assert new_url.query["a"] == original_url.query["a"]

    if "b" in original_url.query:
        assert new_url.query["b"] == original_url.query["b"]
