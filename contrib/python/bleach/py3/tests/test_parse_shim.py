from dataclasses import dataclass
import inspect

import pytest

from bleach.parse_shim import urlparse


@dataclass
class ParseResult:
    scheme: str = ""
    netloc: str = ""
    path: str = ""
    params: str = ""
    query: str = ""
    fragment: str = ""


# Tests from
# https://github.com/web-platform-tests/wpt/blob/master/url/resources/urltestdata.json
# commit ee566de4c5c65d7e8af8b2500f9b85a646ffeaa5


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("", ParseResult()),
        ("http://example\t.\norg", ParseResult(scheme="http", netloc="example.org")),
        (
            "http://user:pass@foo:21/bar;par?b#c",
            ParseResult(
                scheme="http",
                netloc="user:pass@foo:21",
                path="/bar",
                params="par",
                query="b",
                fragment="c",
            ),
        ),
        ("https://test:@test", ParseResult(scheme="https", netloc="test:@test")),
        ("https://:@test", ParseResult(scheme="https", netloc=":@test")),
        (
            "non-special://test:@test/x",
            ParseResult(scheme="non-special", netloc="test:@test", path="/x"),
        ),
        (
            "non-special://:@test/x",
            ParseResult(scheme="non-special", netloc=":@test", path="/x"),
        ),
        ("http:foo.com", ParseResult(scheme="http", path="foo.com")),
        # NOTE(willkg): The wpt tests set the scheme to http becaue that's what
        # the base url is. Since our parser is not using a baseurl, it sets the
        # scheme to "". Further, our parser includes spaces at the beginning,
        # but I don't see that as being problematic.
        ("\t   :foo.com   \n", ParseResult(path="   :foo.com   ")),
        # NOTE(willkg): The wpt tests set the path to "/foo/foo.com" because
        # the base url is at "/foo"
        (" foo.com  ", ParseResult(path=" foo.com  ")),
        ("a:\t foo.com", ParseResult(scheme="a", path=" foo.com")),
        (
            "http://f:21/ b ? d # e ",
            ParseResult(
                scheme="http", netloc="f:21", path="/ b ", query=" d ", fragment=" e "
            ),
        ),
        (
            "lolscheme:x x#x x",
            ParseResult(scheme="lolscheme", path="x x", fragment="x x"),
        ),
        ("http://f:/c", ParseResult(scheme="http", netloc="f:", path="/c")),
        ("http://f:0/c", ParseResult(scheme="http", netloc="f:0", path="/c")),
        # NOTE(willkg): The wpt tests normalize the 0000000000000 to 0 so the
        # netloc should be "f:0".
        (
            "http://f:00000000000000/c",
            ParseResult(scheme="http", netloc="f:00000000000000", path="/c"),
        ),
        # NOTE(willkg): The wpt tests drop the 0000000000000000000 altogether
        # so the netloc should be "f".
        (
            "http://f:00000000000000000000080/c",
            ParseResult(scheme="http", netloc="f:00000000000000000000080", path="/c"),
        ),
        # This is an invalid ipv6 url
        ("http://2001::1]", ValueError),
        # NOTE(willkg): The wpt tests show this as a parse error, but our
        # parser "parses" it.
        ("http://f:b/c", ParseResult(scheme="http", netloc="f:b", path="/c")),
        # NOTE(willkg): The wpt tests show this as a parse error, but our
        # parser "parses" it.
        ("http://f: /c", ParseResult(scheme="http", netloc="f: ", path="/c")),
        # NOTE(willkg): The wpt tests show this as a parse error, but our
        # parser "parses" it.
        ("http://f:999999/c", ParseResult(scheme="http", netloc="f:999999", path="/c")),
    ],
)
def test_urlparse(uri, expected):
    if inspect.isclass(expected) and issubclass(expected, BaseException):
        with pytest.raises(expected):
            urlparse(uri)

    else:
        parsed = urlparse(uri)
        print(parsed)
        assert parsed.scheme == expected.scheme
        assert parsed.netloc == expected.netloc
        assert parsed.path == expected.path
        assert parsed.params == expected.params
        assert parsed.query == expected.query
        assert parsed.fragment == expected.fragment
