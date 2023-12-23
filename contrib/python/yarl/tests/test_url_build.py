import pytest

from yarl import URL

# build classmethod


def test_build_without_arguments():
    u = URL.build()
    assert str(u) == ""


def test_build_simple():
    u = URL.build(scheme="http", host="127.0.0.1")
    assert str(u) == "http://127.0.0.1"


def test_build_with_scheme():
    u = URL.build(scheme="blob", path="path")
    assert str(u) == "blob:path"


def test_build_with_host():
    u = URL.build(host="127.0.0.1")
    assert str(u) == "//127.0.0.1"
    assert u == URL("//127.0.0.1")


def test_build_with_scheme_and_host():
    u = URL.build(scheme="http", host="127.0.0.1")
    assert str(u) == "http://127.0.0.1"
    assert u == URL("http://127.0.0.1")


@pytest.mark.parametrize(
    ("port", "exc", "match"),
    [
        pytest.param(
            8000,
            ValueError,
            r"""(?x)
            ^
            Can't\ build\ URL\ with\ "port"\ but\ without\ "host"\.
            $
            """,
            id="port-only",
        ),
        pytest.param(
            "", TypeError, r"^The port is required to be int\.$", id="port-str"
        ),
    ],
)
def test_build_with_port(port, exc, match):
    print(match)
    with pytest.raises(exc, match=match):
        URL.build(port=port)


def test_build_with_user():
    u = URL.build(scheme="http", host="127.0.0.1", user="foo")
    assert str(u) == "http://foo@127.0.0.1"


def test_build_with_user_password():
    u = URL.build(scheme="http", host="127.0.0.1", user="foo", password="bar")
    assert str(u) == "http://foo:bar@127.0.0.1"


def test_build_with_query_and_query_string():
    with pytest.raises(ValueError):
        URL.build(
            scheme="http",
            host="127.0.0.1",
            user="foo",
            password="bar",
            port=8000,
            path="/index.html",
            query=dict(arg="value1"),
            query_string="arg=value1",
            fragment="top",
        )


def test_build_with_all():
    u = URL.build(
        scheme="http",
        host="127.0.0.1",
        user="foo",
        password="bar",
        port=8000,
        path="/index.html",
        query_string="arg=value1",
        fragment="top",
    )
    assert str(u) == "http://foo:bar@127.0.0.1:8000/index.html?arg=value1#top"


def test_build_with_authority_and_host():
    with pytest.raises(ValueError):
        URL.build(authority="host.com", host="example.com")


def test_build_with_authority():
    url = URL.build(scheme="http", authority="степан:bar@host.com:8000", path="path")
    assert (
        str(url) == "http://%D1%81%D1%82%D0%B5%D0%BF%D0%B0%D0%BD:bar@host.com:8000/path"
    )


def test_build_with_authority_without_encoding():
    url = URL.build(
        scheme="http", authority="foo:bar@host.com:8000", path="path", encoded=True
    )
    assert str(url) == "http://foo:bar@host.com:8000/path"


def test_query_str():
    u = URL.build(scheme="http", host="127.0.0.1", path="/", query_string="arg=value1")
    assert str(u) == "http://127.0.0.1/?arg=value1"


def test_query_dict():
    u = URL.build(scheme="http", host="127.0.0.1", path="/", query=dict(arg="value1"))

    assert str(u) == "http://127.0.0.1/?arg=value1"


def test_build_path_quoting():
    u = URL.build(
        scheme="http",
        host="127.0.0.1",
        path="/фотографія.jpg",
        query=dict(arg="Привіт"),
    )

    assert u == URL("http://127.0.0.1/фотографія.jpg?arg=Привіт")
    assert str(u) == (
        "http://127.0.0.1/"
        "%D1%84%D0%BE%D1%82%D0%BE%D0%B3%D1%80%D0%B0%D1%84%D1%96%D1%8F.jpg?"
        "arg=%D0%9F%D1%80%D0%B8%D0%B2%D1%96%D1%82"
    )


def test_build_query_quoting():
    u = URL.build(
        scheme="http",
        host="127.0.0.1",
        path="/фотографія.jpg",
        query="arg=Привіт",
    )

    assert u == URL("http://127.0.0.1/фотографія.jpg?arg=Привіт")
    assert str(u) == (
        "http://127.0.0.1/"
        "%D1%84%D0%BE%D1%82%D0%BE%D0%B3%D1%80%D0%B0%D1%84%D1%96%D1%8F.jpg?"
        "arg=%D0%9F%D1%80%D0%B8%D0%B2%D1%96%D1%82"
    )


def test_build_query_only():
    u = URL.build(query={"key": "value"})

    assert str(u) == "?key=value"


def test_build_drop_dots():
    u = URL.build(scheme="http", host="example.com", path="/path/../to")
    assert str(u) == "http://example.com/to"


def test_build_encode():
    u = URL.build(
        scheme="http",
        host="оун-упа.укр",
        path="/шлях/криївка",
        query_string="ключ=знач",
        fragment="фраг",
    )
    expected = (
        "http://xn----8sb1bdhvc.xn--j1amh"
        "/%D1%88%D0%BB%D1%8F%D1%85/%D0%BA%D1%80%D0%B8%D1%97%D0%B2%D0%BA%D0%B0"
        "?%D0%BA%D0%BB%D1%8E%D1%87=%D0%B7%D0%BD%D0%B0%D1%87"
        "#%D1%84%D1%80%D0%B0%D0%B3"
    )
    assert str(u) == expected


def test_build_already_encoded():
    # resulting URL is invalid but not encoded
    u = URL.build(
        scheme="http",
        host="оун-упа.укр",
        path="/шлях/криївка",
        query_string="ключ=знач",
        fragment="фраг",
        encoded=True,
    )
    assert str(u) == "http://оун-упа.укр/шлях/криївка?ключ=знач#фраг"


def test_build_percent_encoded():
    u = URL.build(
        scheme="http",
        host="%2d.org",
        user="u%2d",
        password="p%2d",
        path="/%2d",
        query_string="k%2d=v%2d",
        fragment="f%2d",
    )
    assert str(u) == "http://u%252d:p%252d@%2d.org/%252d?k%252d=v%252d#f%252d"
    assert u.raw_host == "%2d.org"
    assert u.host == "%2d.org"
    assert u.raw_user == "u%252d"
    assert u.user == "u%2d"
    assert u.raw_password == "p%252d"
    assert u.password == "p%2d"
    assert u.raw_authority == "u%252d:p%252d@%2d.org"
    assert u.authority == "u%2d:p%2d@%2d.org:80"
    assert u.raw_path == "/%252d"
    assert u.path == "/%2d"
    assert u.query == {"k%2d": "v%2d"}
    assert u.raw_query_string == "k%252d=v%252d"
    assert u.query_string == "k%2d=v%2d"
    assert u.raw_fragment == "f%252d"
    assert u.fragment == "f%2d"


def test_build_with_authority_percent_encoded():
    u = URL.build(scheme="http", authority="u%2d:p%2d@%2d.org")
    assert str(u) == "http://u%252d:p%252d@%2d.org"
    assert u.raw_host == "%2d.org"
    assert u.host == "%2d.org"
    assert u.raw_user == "u%252d"
    assert u.user == "u%2d"
    assert u.raw_password == "p%252d"
    assert u.password == "p%2d"
    assert u.raw_authority == "u%252d:p%252d@%2d.org"
    assert u.authority == "u%2d:p%2d@%2d.org:80"


def test_build_with_authority_percent_encoded_already_encoded():
    u = URL.build(scheme="http", authority="u%2d:p%2d@%2d.org", encoded=True)
    assert str(u) == "http://u%2d:p%2d@%2d.org"
    assert u.raw_host == "%2d.org"
    assert u.host == "%2d.org"
    assert u.user == "u-"
    assert u.raw_user == "u%2d"
    assert u.password == "p-"
    assert u.raw_password == "p%2d"
    assert u.authority == "u-:p-@%2d.org:80"
    assert u.raw_authority == "u%2d:p%2d@%2d.org"


def test_build_with_authority_with_path_with_leading_slash():
    u = URL.build(scheme="http", host="example.com", path="/path_with_leading_slash")
    assert str(u) == "http://example.com/path_with_leading_slash"


def test_build_with_authority_with_empty_path():
    u = URL.build(scheme="http", host="example.com", path="")
    assert str(u) == "http://example.com"


def test_build_with_authority_with_path_without_leading_slash():
    with pytest.raises(ValueError):
        URL.build(scheme="http", host="example.com", path="path_without_leading_slash")


def test_build_with_none_host():
    with pytest.raises(TypeError, match="NoneType is illegal for.*host"):
        URL.build(scheme="http", host=None)


def test_build_with_none_path():
    with pytest.raises(TypeError):
        URL.build(scheme="http", host="example.com", path=None)


def test_build_with_none_query_string():
    with pytest.raises(TypeError):
        URL.build(scheme="http", host="example.com", query_string=None)


def test_build_with_none_fragment():
    with pytest.raises(TypeError):
        URL.build(scheme="http", host="example.com", fragment=None)
