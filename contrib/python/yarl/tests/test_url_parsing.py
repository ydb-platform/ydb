import sys

import pytest

from yarl import URL


class TestScheme:
    def test_scheme_path(self):
        u = URL("scheme:path")
        assert u.scheme == "scheme"
        assert u.host is None
        assert u.path == "path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_scheme_path_other(self):
        u = URL("scheme:path:other")
        assert u.scheme == "scheme"
        assert u.host is None
        assert u.path == "path:other"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_complex_scheme(self):
        u = URL("allow+chars-33.:path")
        assert u.scheme == "allow+chars-33."
        assert u.host is None
        assert u.path == "path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_scheme_only(self):
        u = URL("simple:")
        assert u.scheme == "simple"
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == ""

    def test_no_scheme1(self):
        u = URL("google.com:80")
        # See: https://bugs.python.org/issue27657
        if (
            sys.version_info[:3] == (3, 7, 6)
            or sys.version_info[:3] == (3, 8, 1)
            or sys.version_info >= (3, 9, 0)
        ):
            assert u.scheme == "google.com"
            assert u.host is None
            assert u.path == "80"
        else:
            assert u.scheme == ""
            assert u.host is None
            assert u.path == "google.com:80"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_no_scheme2(self):
        u = URL("google.com:80/root")
        assert u.scheme == "google.com"
        assert u.host is None
        assert u.path == "80/root"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_not_a_scheme1(self):
        u = URL("not_cheme:path")
        assert u.scheme == ""
        assert u.host is None
        assert u.path == "not_cheme:path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_not_a_scheme2(self):
        u = URL("signals37:book")
        assert u.scheme == "signals37"
        assert u.host is None
        assert u.path == "book"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_scheme_rel_path1(self):
        u = URL(":relative-path")
        assert u.scheme == ""
        assert u.host is None
        assert u.path == ":relative-path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_scheme_rel_path2(self):
        u = URL(":relative/path")
        assert u.scheme == ""
        assert u.host is None
        assert u.path == ":relative/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_scheme_weird(self):
        u = URL("://and-this")
        assert u.scheme == ""
        assert u.host is None
        assert u.path == "://and-this"
        assert u.query_string == ""
        assert u.fragment == ""


class TestHost:
    def test_canonical(self):
        u = URL("scheme://host/path")
        assert u.scheme == "scheme"
        assert u.host == "host"
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_absolute_no_scheme(self):
        u = URL("//host/path")
        assert u.scheme == ""
        assert u.host == "host"
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_absolute_no_scheme_complex_host(self):
        u = URL("//host+path")
        assert u.scheme == ""
        assert u.host == "host+path"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_absolute_no_scheme_simple_host(self):
        u = URL("//host")
        assert u.scheme == ""
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_weird_host(self):
        u = URL("//this+is$also&host!")
        assert u.scheme == ""
        assert u.host == "this+is$also&host!"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_scheme_no_host(self):
        u = URL("scheme:/host/path")
        assert u.scheme == "scheme"
        assert u.host is None
        assert u.path == "/host/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_scheme_no_host2(self):
        u = URL("scheme:///host/path")
        assert u.scheme == "scheme"
        assert u.host is None
        assert u.path == "/host/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_no_scheme_no_host(self):
        u = URL("scheme//host/path")
        assert u.scheme == ""
        assert u.host is None
        assert u.path == "scheme//host/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_ipv4(self):
        u = URL("//127.0.0.1/")
        assert u.scheme == ""
        assert u.host == "127.0.0.1"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_ipv6(self):
        u = URL("//[::1]/")
        assert u.scheme == ""
        assert u.host == "::1"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_ipvfuture_address(self):
        u = URL("//[v1.-1]/")
        assert u.scheme == ""
        assert u.host == "v1.-1"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""


class TestPort:
    def test_canonical(self):
        u = URL("//host:80/path")
        assert u.scheme == ""
        assert u.host == "host"
        assert u.port == 80
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_no_path(self):
        u = URL("//host:80")
        assert u.scheme == ""
        assert u.host == "host"
        assert u.port == 80
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    @pytest.mark.xfail(
        # FIXME: remove "no cover" pragmas upon xfail marker deletion
        reason="https://github.com/aio-libs/yarl/issues/821",
        raises=ValueError,
    )
    def test_no_host(self):
        u = URL("//:80")
        assert u.scheme == ""  # pragma: no cover
        assert u.host == ""  # pragma: no cover
        assert u.port == 80  # pragma: no cover
        assert u.path == "/"  # pragma: no cover
        assert u.query_string == ""  # pragma: no cover
        assert u.fragment == ""  # pragma: no cover

    def test_double_port(self):
        with pytest.raises(ValueError):
            URL("//h:22:80/")

    def test_bad_port(self):
        with pytest.raises(ValueError):
            URL("//h:no/path")

    def test_another_bad_port(self):
        with pytest.raises(ValueError):
            URL("//h:22:no/path")

    def test_bad_port_again(self):
        with pytest.raises(ValueError):
            URL("//h:-80/path")


class TestUserInfo:
    def test_canonical(self):
        u = URL("sch://user@host/")
        assert u.scheme == "sch"
        assert u.user == "user"
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_user_pass(self):
        u = URL("//user:pass@host")
        assert u.scheme == ""
        assert u.user == "user"
        assert u.password == "pass"
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_complex_userinfo(self):
        u = URL("//user:pas:and:more@host")
        assert u.scheme == ""
        assert u.user == "user"
        assert u.password == "pas:and:more"
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_no_user(self):
        u = URL("//:pas:@host")
        assert u.scheme == ""
        assert u.user is None
        assert u.password == "pas:"
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_weird_user(self):
        u = URL("//!($&')*+,;=@host")
        assert u.scheme == ""
        assert u.user == "!($&')*+,;="
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_weird_user2(self):
        u = URL("//user@info@ya.ru")
        assert u.scheme == ""
        assert u.user == "user@info"
        assert u.password is None
        assert u.host == "ya.ru"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_weird_user3(self):
        u = URL("//%5Bsome%5D@host")
        assert u.scheme == ""
        assert u.user == "[some]"
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""


class TestQuery_String:
    def test_simple(self):
        u = URL("?query")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == "query"
        assert u.fragment == ""

    def test_scheme_query(self):
        u = URL("http:?query")
        assert u.scheme == "http"
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == "query"
        assert u.fragment == ""

    def test_abs_url_query(self):
        u = URL("//host?query")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == "query"
        assert u.fragment == ""

    def test_abs_url_path_query(self):
        u = URL("//host/path?query")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/path"
        assert u.query_string == "query"
        assert u.fragment == ""

    def test_double_question_mark(self):
        u = URL("//ho?st/path?query")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "ho"
        assert u.path == "/"
        assert u.query_string == "st/path?query"
        assert u.fragment == ""

    def test_complex_query(self):
        u = URL("?a://b:c@d.e/f?g#h")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == "a://b:c@d.e/f?g"
        assert u.fragment == "h"

    def test_query_in_fragment(self):
        u = URL("#?query")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == "?query"


class TestFragment:
    def test_simple(self):
        u = URL("#frag")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == "frag"

    def test_scheme_frag(self):
        u = URL("http:#frag")
        assert u.scheme == "http"
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == "frag"

    def test_host_frag(self):
        u = URL("//host#frag")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == "frag"

    def test_scheme_path_frag(self):
        u = URL("//host/path#frag")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == "frag"

    def test_scheme_query_frag(self):
        u = URL("//host?query#frag")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == "query"
        assert u.fragment == "frag"

    def test_host_frag_query(self):
        u = URL("//ho#st/path?query")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "ho"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == "st/path?query"

    def test_complex_frag(self):
        u = URL("#a://b:c@d.e/f?g#h")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == "a://b:c@d.e/f?g#h"


class TestStripEmptyParts:
    def test_all_empty(self):
        with pytest.raises(ValueError):
            URL("//@:?#")

    def test_path_only(self):
        u = URL("///path")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_user(self):
        u = URL("//@host")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_port(self):
        u = URL("//host:")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_port_and_path(self):
        u = URL("//host:/")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host == "host"
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_path_only(self):
        u = URL("/")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == "/"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_relative_path_only(self):
        u = URL("path")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == "path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_path(self):
        u = URL("/path")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_query_with_path(self):
        u = URL("/path?")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_query(self):
        u = URL("?")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_query_with_frag(self):
        u = URL("?#frag")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == "frag"

    def test_path_empty_frag(self):
        u = URL("/path#")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == "/path"
        assert u.query_string == ""
        assert u.fragment == ""

    def test_empty_path(self):
        u = URL("#")
        assert u.scheme == ""
        assert u.user is None
        assert u.password is None
        assert u.host is None
        assert u.path == ""
        assert u.query_string == ""
        assert u.fragment == ""
