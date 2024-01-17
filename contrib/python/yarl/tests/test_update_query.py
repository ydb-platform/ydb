import enum

import pytest
from multidict import MultiDict

from yarl import URL

# with_query


def test_with_query():
    url = URL("http://example.com")
    assert str(url.with_query({"a": "1"})) == "http://example.com/?a=1"


def test_update_query():
    url = URL("http://example.com/")
    assert str(url.update_query({"a": "1"})) == "http://example.com/?a=1"
    assert str(URL("test").update_query(a=1)) == "test?a=1"

    url = URL("http://example.com/?foo=bar")
    expected_url = URL("http://example.com/?foo=bar&baz=foo")

    assert url.update_query({"baz": "foo"}) == expected_url
    assert url.update_query(baz="foo") == expected_url
    assert url.update_query("baz=foo") == expected_url


def test_update_query_with_args_and_kwargs():
    url = URL("http://example.com/")

    with pytest.raises(ValueError):
        url.update_query("a", foo="bar")


def test_update_query_with_multiple_args():
    url = URL("http://example.com/")

    with pytest.raises(ValueError):
        url.update_query("a", "b")


def test_update_query_with_none_arg():
    url = URL("http://example.com/?foo=bar&baz=foo")
    expected_url = URL("http://example.com/")
    assert url.update_query(None) == expected_url


def test_update_query_with_empty_dict():
    url = URL("http://example.com/?foo=bar&baz=foo")
    assert url.update_query({}) == url


def test_with_query_list_of_pairs():
    url = URL("http://example.com")
    assert str(url.with_query([("a", "1")])) == "http://example.com/?a=1"


def test_with_query_list_non_pairs():
    url = URL("http://example.com")
    with pytest.raises(ValueError):
        url.with_query(["a=1", "b=2", "c=3"])


def test_with_query_kwargs():
    url = URL("http://example.com")
    q = url.with_query(query="1", query2="1").query
    assert q == dict(query="1", query2="1")


def test_with_query_kwargs_and_args_are_mutually_exclusive():
    url = URL("http://example.com")
    with pytest.raises(ValueError):
        url.with_query({"a": "2", "b": "4"}, a="1")


def test_with_query_only_single_arg_is_supported():
    url = URL("http://example.com")
    u1 = url.with_query(b=3)
    u2 = URL("http://example.com/?b=3")
    assert u1 == u2
    with pytest.raises(ValueError):
        url.with_query("a=1", "a=b")


def test_with_query_empty_dict():
    url = URL("http://example.com/?a=b")
    new_url = url.with_query({})
    assert new_url.query_string == ""
    assert str(new_url) == "http://example.com/"


def test_with_query_empty_str():
    url = URL("http://example.com/?a=b")
    assert str(url.with_query("")) == "http://example.com/"


def test_with_query_empty_value():
    url = URL("http://example.com/")
    assert str(url.with_query({"a": ""})) == "http://example.com/?a="


def test_with_query_str():
    url = URL("http://example.com")
    assert str(url.with_query("a=1&b=2")) == "http://example.com/?a=1&b=2"


def test_with_query_str_non_ascii_and_spaces():
    url = URL("http://example.com")
    url2 = url.with_query("a=1 2&b=знач")
    assert url2.raw_query_string == "a=1+2&b=%D0%B7%D0%BD%D0%B0%D1%87"
    assert url2.query_string == "a=1 2&b=знач"


def test_with_query_int():
    url = URL("http://example.com")
    assert url.with_query({"a": 1}) == URL("http://example.com/?a=1")


def test_with_query_kwargs_int():
    url = URL("http://example.com")
    assert url.with_query(b=2) == URL("http://example.com/?b=2")


def test_with_query_list_int():
    url = URL("http://example.com")
    assert str(url.with_query([("a", 1)])) == "http://example.com/?a=1"


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param({"a": []}, "", id="empty list"),
        pytest.param({"a": ()}, "", id="empty tuple"),
        pytest.param({"a": [1]}, "/?a=1", id="single list"),
        pytest.param({"a": (1,)}, "/?a=1", id="single tuple"),
        pytest.param({"a": [1, 2]}, "/?a=1&a=2", id="list"),
        pytest.param({"a": (1, 2)}, "/?a=1&a=2", id="tuple"),
        pytest.param({"a[]": [1, 2]}, "/?a%5B%5D=1&a%5B%5D=2", id="key with braces"),
        pytest.param({"&": [1, 2]}, "/?%26=1&%26=2", id="quote key"),
        pytest.param({"a": ["1", 2]}, "/?a=1&a=2", id="mixed types"),
        pytest.param({"&": ["=", 2]}, "/?%26=%3D&%26=2", id="quote key and value"),
        pytest.param({"a": 1, "b": [2, 3]}, "/?a=1&b=2&b=3", id="single then list"),
        pytest.param({"a": [1, 2], "b": 3}, "/?a=1&a=2&b=3", id="list then single"),
        pytest.param({"a": ["1&a=2", 3]}, "/?a=1%26a%3D2&a=3", id="ampersand then int"),
        pytest.param({"a": [1, "2&a=3"]}, "/?a=1&a=2%26a%3D3", id="int then ampersand"),
    ],
)
def test_with_query_sequence(query, expected):
    url = URL("http://example.com")
    expected = "http://example.com{expected}".format_map(locals())
    assert str(url.with_query(query)) == expected


@pytest.mark.parametrize(
    "query",
    [
        pytest.param({"a": [[1]]}, id="nested"),
        pytest.param([("a", [1, 2])], id="tuple list"),
    ],
)
def test_with_query_sequence_invalid_use(query):
    url = URL("http://example.com")
    with pytest.raises(TypeError, match="Invalid variable type"):
        url.with_query(query)


class _CStr(str):
    pass


class _EmptyStrEr:
    def __str__(self):
        return ""  # pragma: no cover  # <-- this should never happen


class _CInt(int, _EmptyStrEr):
    pass


class _CFloat(float, _EmptyStrEr):
    pass


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("1", "1", id="str"),
        pytest.param(_CStr("1"), "1", id="custom str"),
        pytest.param(1, "1", id="int"),
        pytest.param(_CInt(1), "1", id="custom int"),
        pytest.param(1.1, "1.1", id="float"),
        pytest.param(_CFloat(1.1), "1.1", id="custom float"),
    ],
)
def test_with_query_valid_type(value, expected):
    url = URL("http://example.com")
    expected = "http://example.com/?a={expected}".format_map(locals())
    assert str(url.with_query({"a": value})) == expected


@pytest.mark.parametrize(
    ("value", "exc_type"),
    [
        pytest.param(True, TypeError, id="bool"),
        pytest.param(None, TypeError, id="none"),
        pytest.param(float("inf"), ValueError, id="non-finite float"),
        pytest.param(float("nan"), ValueError, id="NaN float"),
    ],
)
def test_with_query_invalid_type(value, exc_type):
    url = URL("http://example.com")
    with pytest.raises(exc_type):
        url.with_query({"a": value})


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("1", "1", id="str"),
        pytest.param(_CStr("1"), "1", id="custom str"),
        pytest.param(1, "1", id="int"),
        pytest.param(_CInt(1), "1", id="custom int"),
        pytest.param(1.1, "1.1", id="float"),
        pytest.param(_CFloat(1.1), "1.1", id="custom float"),
    ],
)
def test_with_query_list_valid_type(value, expected):
    url = URL("http://example.com")
    expected = "http://example.com/?a={expected}".format_map(locals())
    assert str(url.with_query([("a", value)])) == expected


@pytest.mark.parametrize(
    ("value"), [pytest.param(True, id="bool"), pytest.param(None, id="none")]
)
def test_with_query_list_invalid_type(value):
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        url.with_query([("a", value)])


def test_with_int_enum():
    class IntEnum(int, enum.Enum):
        A = 1

    url = URL("http://example.com/path")
    url2 = url.with_query(a=IntEnum.A)
    assert str(url2) == "http://example.com/path?a=1"


def test_with_float_enum():
    class FloatEnum(float, enum.Enum):
        A = 1.1

    url = URL("http://example.com/path")
    url2 = url.with_query(a=FloatEnum.A)
    assert str(url2) == "http://example.com/path?a=1.1"


def test_with_query_multidict():
    url = URL("http://example.com/path")
    q = MultiDict([("a", "b"), ("c", "d")])
    assert str(url.with_query(q)) == "http://example.com/path?a=b&c=d"


def test_with_multidict_with_spaces_and_non_ascii():
    url = URL("http://example.com")
    url2 = url.with_query({"a b": "ю б"})
    assert url2.raw_query_string == "a+b=%D1%8E+%D0%B1"


def test_with_query_multidict_with_unsafe():
    url = URL("http://example.com/path")
    url2 = url.with_query({"a+b": "?=+&;"})
    assert url2.raw_query_string == "a%2Bb=?%3D%2B%26%3B"
    assert url2.query_string == "a%2Bb=?%3D%2B%26%3B"
    assert url2.query == {"a+b": "?=+&;"}


def test_with_query_None():
    url = URL("http://example.com/path?a=b")
    assert url.with_query(None).query_string == ""


def test_with_query_bad_type():
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        url.with_query(123)


def test_with_query_bytes():
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        url.with_query(b"123")


def test_with_query_bytearray():
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        url.with_query(bytearray(b"123"))


def test_with_query_memoryview():
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        url.with_query(memoryview(b"123"))


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param([("key", "1;2;3")], "?key=1%3B2%3B3", id="tuple list semicolon"),
        pytest.param({"key": "1;2;3"}, "?key=1%3B2%3B3", id="mapping semicolon"),
        pytest.param([("key", "1&a=2")], "?key=1%26a%3D2", id="tuple list ampersand"),
        pytest.param({"key": "1&a=2"}, "?key=1%26a%3D2", id="mapping ampersand"),
        pytest.param([("&", "=")], "?%26=%3D", id="tuple list quote key"),
        pytest.param({"&": "="}, "?%26=%3D", id="mapping quote key"),
        pytest.param(
            [("a[]", "3")],
            "?a%5B%5D=3",
            id="quote one key braces",
        ),
        pytest.param(
            [("a[]", "3"), ("a[]", "4")],
            "?a%5B%5D=3&a%5B%5D=4",
            id="quote many key braces",
        ),
    ],
)
def test_with_query_params(query, expected):
    url = URL("http://example.com/get")
    url2 = url.with_query(query)
    assert str(url2) == ("http://example.com/get" + expected)


def test_with_query_only():
    url = URL()
    url2 = url.with_query(key="value")
    assert str(url2) == "?key=value"


def test_with_query_complex_url():
    target_url = "http://example.com/?game=bulls+%26+cows"
    url = URL("/redir").with_query({"t": target_url})
    assert url.query["t"] == target_url


def test_update_query_multiple_keys():
    url = URL("http://example.com/path?a=1&a=2")
    u2 = url.update_query([("a", "3"), ("a", "4")])

    assert str(u2) == "http://example.com/path?a=3&a=4"


# mod operator


def test_update_query_with_mod_operator():
    url = URL("http://example.com/")
    assert str(url % {"a": "1"}) == "http://example.com/?a=1"
    assert str(url % [("a", "1")]) == "http://example.com/?a=1"
    assert str(url % "a=1&b=2") == "http://example.com/?a=1&b=2"
    assert str(url % {"a": "1"} % {"b": "2"}) == "http://example.com/?a=1&b=2"
    assert str(url % {"a": "1"} % {"a": "3", "b": "2"}) == "http://example.com/?a=3&b=2"
    assert str(url / "foo" % {"a": "1"}) == "http://example.com/foo?a=1"
