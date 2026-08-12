import pytest

from yarl import URL

# build classmethod


def test_build_without_arguments() -> None:
    u = URL.build()
    assert str(u) == ""


def test_build_simple() -> None:
    u = URL.build(scheme="http", host="127.0.0.1")
    assert str(u) == "http://127.0.0.1"


def test_url_build_ipv6() -> None:
    u = URL.build(scheme="http", host="::1")
    assert str(u) == "http://[::1]"


def test_url_build_ipv6_brackets_encoded() -> None:
    u = URL.build(scheme="http", host="[::1]", encoded=True)
    assert str(u) == "http://[::1]"


def test_url_build_ipv6_brackets_not_encoded() -> None:
    u = URL.build(scheme="http", host="::1", encoded=False)
    assert str(u) == "http://[::1]"


def test_url_ipv4_in_ipv6() -> None:
    u = URL.build(scheme="http", host="2001:db8:122:344::192.0.2.33")
    assert str(u) == "http://[2001:db8:122:344::c000:221]"


@pytest.mark.parametrize(
    "zone",
    (
        "\r\nX-Injected: evil",
        "\x00evil",
    ),
    ids=("crlf-injection", "null-byte"),
)
def test_url_build_ipv6_zone_id_invalid_chars(zone: str) -> None:
    """Zone IDs with control characters must be rejected by validate_host."""
    with pytest.raises(
        ValueError, match="Invalid characters in zone identifier"
    ) as ctx:
        URL.build(scheme="http", host=f"::1%{zone}", path="/")
    error = str(ctx.value)
    assert zone not in error
    assert repr(zone) not in error


def test_url_build_ipv6_zone_id_empty() -> None:
    """A bare trailing '%' (empty zone) is rejected per RFC 9844 §6.3."""
    with pytest.raises(ValueError, match="Invalid characters in zone identifier"):
        URL.build(scheme="http", host="::1%", path="/")


@pytest.mark.parametrize(
    "zone",
    (
        "eth0",
        "1",
        "zone with spaces",
        "Ethernet (LAN)",
        "日本語",
    ),
    ids=("iface-name", "numeric", "spaces", "parens", "unicode"),
)
def test_url_build_ipv6_zone_id_valid(zone: str) -> None:
    """Zone IDs accept any non-CTL text per RFC 4007 §11.2."""
    u = URL.build(scheme="http", host=f"::1%{zone}", path="/")
    assert u.host == f"::1%{zone}"
    assert URL(str(u)).host == f"::1%{zone}"


def test_url_build_ipv6_zone_id_bare_percent_round_trip() -> None:
    """Programmatic hosts keep the bare ``%`` zone separator.

    ``_encode_host`` falls back to partitioning on ``%`` when no
    ``%25`` is present so that hosts constructed from RFC 4007 scoped
    literals keep working; this pins that fallback (#998).
    """
    u = URL.build(scheme="http", host="fe80::1%eth0")
    assert str(u) == "http://[fe80::1%eth0]"
    assert u.raw_host == "fe80::1%eth0"
    assert u.host == "fe80::1%eth0"
    assert URL(str(u)) == u


def test_url_build_ipv6_zone_id_numeric_scope_percent25() -> None:
    """A literal ``%25`` in a programmatic host is read as the separator.

    On Windows, zone identifiers are numeric, so the RFC 4007 literal
    ``fe80::1%25`` means scope 25 and ``fe80::1%254`` means scope 254.
    The current heuristic treats the ``%25`` byte sequence as the
    RFC 6874 encoded separator instead: the first raises for an empty
    zone and the second is parsed as zone ``4``. Documents current
    behavior (#998); not an endorsement of it.
    """
    with pytest.raises(ValueError, match="Invalid characters in zone identifier"):
        URL.build(scheme="http", host="fe80::1%25")
    u = URL.build(scheme="http", host="fe80::1%254")
    assert str(u) == "http://[fe80::1%254]"
    assert u.raw_host == "fe80::1%254"
    assert u.host == "fe80::1%4"


@pytest.mark.parametrize(
    "zone",
    (
        "e/h",
        "a?b",
        "a#c",
    ),
    ids=("slash", "question", "hash"),
)
def test_url_build_ipv6_zone_id_reserved_chars_break_round_trip(zone: str) -> None:
    """Reserved characters in a zone produce URLs yarl cannot re-parse.

    RFC 6874 §2 requires non-unreserved zone characters to be
    percent-encoded; the liberal RFC 4007 policy emits them raw, so
    the serialized URL fails yarl's own parser. Documents current
    behavior (#998).
    """
    u = URL.build(scheme="http", host=f"fe80::1%{zone}", path="/x")
    assert str(u) == f"http://[fe80::1%{zone}]/x"
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL(str(u))


def test_url_build_ipv6_zone_id_non_ascii_not_ascii_encodable() -> None:
    """Non-ASCII zone identifiers make the URL non-ASCII-serializable."""
    u = URL.build(scheme="http", host="fe80::1%日本語", path="/")
    with pytest.raises(UnicodeEncodeError):
        bytes(u)


def test_url_build_ipv6_zone_id_empty_authority_not_validated() -> None:
    """``authority=`` bypasses the empty-zone rejection of ``host=``.

    The authority path uses ``validate_host=False``; documents the
    asymmetry with ``test_url_build_ipv6_zone_id_empty`` (#998).
    """
    u = URL.build(scheme="http", authority="[fe80::1%25]")
    assert str(u) == "http://[fe80::1%25]"
    assert u.host == "fe80::1%"


def test_build_with_scheme() -> None:
    u = URL.build(scheme="blob", path="path")
    assert str(u) == "blob:path"


def test_build_with_host() -> None:
    u = URL.build(host="127.0.0.1")
    assert str(u) == "//127.0.0.1"
    assert u == URL("//127.0.0.1")


def test_build_with_scheme_and_host() -> None:
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
            "", TypeError, r"^The port is required to be int, got .*\.$", id="port-str"
        ),
    ],
)
def test_build_with_port(port: int, exc: type[Exception], match: str) -> None:
    with pytest.raises(exc, match=match):
        URL.build(port=port)


def test_build_with_user() -> None:
    u = URL.build(scheme="http", host="127.0.0.1", user="foo")
    assert str(u) == "http://foo@127.0.0.1"


def test_build_with_user_password() -> None:
    u = URL.build(scheme="http", host="127.0.0.1", user="foo", password="bar")
    assert str(u) == "http://foo:bar@127.0.0.1"


def test_build_with_query_and_query_string() -> None:
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


def test_build_with_all() -> None:
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


def test_build_with_authority_and_host() -> None:
    with pytest.raises(ValueError):
        URL.build(authority="host.com", host="example.com")


@pytest.mark.parametrize(
    ("host", "is_authority"),
    [
        ("user:pass@host.com", True),
        ("user@host.com", True),
        ("host:com", False),
        ("not_percent_encoded%Zf", False),
        ("still_not_percent_encoded%fZ", False),
        *(("other_gen_delim_" + c, False) for c in "/?#[]"),
    ],
)
def test_build_with_invalid_host(host: str, is_authority: bool) -> None:
    match = r"Host '[^']+' cannot contain '[^']+' \(at position \d+\)"
    if is_authority:
        match += ", if .* use 'authority' instead of 'host'"
    with pytest.raises(ValueError, match=f"{match}$"):
        URL.build(host=host)


@pytest.mark.parametrize(
    "host",
    [
        "127.0.0.1／allowed.example",
        "127.0.0.1？allowed.example",
        "127.0.0.1＃allowed.example",
    ],
    ids=["fullwidth-solidus", "fullwidth-question", "fullwidth-number-sign"],
)
def test_build_with_host_delimiter_from_normalization(host: str) -> None:
    # A non-ascii character that expands to a URL delimiter under IDNA/NFKC
    # normalization must be rejected, matching the parser's _check_netloc.
    match = r"cannot contain '[/?#]' after IDNA normalization to "
    with pytest.raises(ValueError, match=match):
        URL.build(host=host)


@pytest.mark.parametrize(
    "ignorable",
    ["\u00ad", "\u200b", "\u2060", "\ufeff"],
    ids=["soft-hyphen", "zwsp", "word-joiner", "bom"],
)
def test_build_with_host_default_ignorable(ignorable: str) -> None:
    # IDNA strips default-ignorable code points, so the builder must reject a
    # host containing one instead of collapsing it to a different host.
    with pytest.raises(ValueError, match="cannot contain"):
        URL.build(host=f"e{ignorable}vil.com")


def test_build_with_authority() -> None:
    url = URL.build(scheme="http", authority="степан:bar@host.com:8000", path="/path")
    assert (
        str(url) == "http://%D1%81%D1%82%D0%B5%D0%BF%D0%B0%D0%BD:bar@host.com:8000/path"
    )


def test_build_with_authority_no_leading_flash() -> None:
    msg = r"Path in a URL with authority should start with a slash \('/'\) if set"
    with pytest.raises(ValueError, match=msg):
        URL.build(scheme="http", authority="степан:bar@host.com:8000", path="path")


def test_build_with_authority_without_encoding() -> None:
    url = URL.build(
        scheme="http", authority="foo:bar@host.com:8000", path="path", encoded=True
    )
    assert str(url) == "http://foo:bar@host.com:8000/path"


def test_build_with_authority_empty_host_no_scheme() -> None:
    url = URL.build(authority="", path="path")
    assert str(url) == "path"


def test_build_with_authority_and_only_user() -> None:
    url = URL.build(scheme="https", authority="user:@foo.com", path="/path")
    assert str(url) == "https://user:@foo.com/path"


def test_build_with_authority_with_port() -> None:
    url = URL.build(scheme="https", authority="foo.com:8080", path="/path")
    assert str(url) == "https://foo.com:8080/path"


def test_build_with_authority_with_ipv6() -> None:
    url = URL.build(scheme="https", authority="[::1]", path="/path")
    assert str(url) == "https://[::1]/path"


def test_build_with_authority_with_ipv6_and_port() -> None:
    url = URL.build(scheme="https", authority="[::1]:81", path="/path")
    assert str(url) == "https://[::1]:81/path"


def test_query_str() -> None:
    u = URL.build(scheme="http", host="127.0.0.1", path="/", query_string="arg=value1")
    assert str(u) == "http://127.0.0.1/?arg=value1"


def test_query_dict() -> None:
    u = URL.build(scheme="http", host="127.0.0.1", path="/", query=dict(arg="value1"))

    assert str(u) == "http://127.0.0.1/?arg=value1"


def test_build_path_quoting() -> None:
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


def test_build_query_quoting() -> None:
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


def test_build_query_only() -> None:
    u = URL.build(query={"key": "value"})

    assert str(u) == "?key=value"


def test_build_drop_dots() -> None:
    u = URL.build(scheme="http", host="example.com", path="/path/../to")
    assert str(u) == "http://example.com/to"


def test_build_encode() -> None:
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


def test_build_already_encoded() -> None:
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


def test_build_already_encoded_username_password() -> None:
    u = URL.build(
        scheme="http",
        host="x.org",
        path="/x/y/z",
        query_string="x=z",
        fragment="any",
        user="u",
        password="p",
        encoded=True,
    )
    assert str(u) == "http://u:p@x.org/x/y/z?x=z#any"
    assert u.host_subcomponent == "x.org"


def test_build_already_encoded_empty_host() -> None:
    u = URL.build(
        host="",
        path="/x/y/z",
        query_string="x=z",
        fragment="any",
        encoded=True,
    )
    assert str(u) == "/x/y/z?x=z#any"
    assert u.host_subcomponent is None


def test_build_percent_encoded() -> None:
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


def test_build_with_authority_percent_encoded() -> None:
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


def test_build_with_authority_percent_encoded_already_encoded() -> None:
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


def test_build_with_authority_with_path_with_leading_slash() -> None:
    u = URL.build(scheme="http", host="example.com", path="/path_with_leading_slash")
    assert str(u) == "http://example.com/path_with_leading_slash"


def test_build_with_authority_with_empty_path() -> None:
    u = URL.build(scheme="http", host="example.com", path="")
    assert str(u) == "http://example.com"


def test_build_with_authority_with_path_without_leading_slash() -> None:
    with pytest.raises(ValueError):
        URL.build(scheme="http", host="example.com", path="path_without_leading_slash")


def test_build_with_none_host() -> None:
    with pytest.raises(TypeError, match="NoneType is illegal for.*host"):
        URL.build(scheme="http", host=None)  # type: ignore[arg-type]


def test_build_with_none_path() -> None:
    with pytest.raises(TypeError):
        URL.build(scheme="http", host="example.com", path=None)  # type: ignore[arg-type]


def test_build_with_none_query_string() -> None:
    with pytest.raises(TypeError):
        URL.build(scheme="http", host="example.com", query_string=None)  # type: ignore[arg-type]


def test_build_with_none_fragment() -> None:
    with pytest.raises(TypeError):
        URL.build(scheme="http", host="example.com", fragment=None)  # type: ignore[arg-type]


def test_build_uppercase_host() -> None:
    u = URL.build(
        host="UPPER.case",
        encoded=False,
    )
    assert u.host == "upper.case"
