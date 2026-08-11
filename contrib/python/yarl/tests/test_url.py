import platform
import sys
from enum import Enum
from urllib.parse import SplitResult, quote, unquote

import pytest

from yarl import URL
from yarl._url import _DEFAULT_IGNORABLE_RE, _idna_encode

_WHATWG_C0_CONTROL_OR_SPACE = (
    "\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10"
    "\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f "
)
_VERTICAL_COLON = "\ufe13"  # normalizes to ":"
_FULL_WITH_NUMBER_SIGN = "\uff03"  # normalizes to "#"
_ACCOUNT_OF = "\u2100"  # normalizes to "a/c"
_FULLWIDTH_PERCENT = "\uff05"  # normalizes to "%"
_SMALL_PERCENT = "\ufe6a"  # normalizes to "%"


def test_inheritance() -> None:
    with pytest.raises(TypeError) as ctx:

        class MyURL(URL):
            pass

    assert (
        "Inheriting a class "
        "<class '__tests__.test_url.test_inheritance.<locals>.MyURL'> "
        "from URL is forbidden" == str(ctx.value)
    )


def test_str_subclass() -> None:
    class S(str):
        pass

    assert str(URL(S("http://example.com"))) == "http://example.com"


def test_is() -> None:
    u1 = URL("http://example.com")
    u2 = URL(u1)
    assert u1 is u2


def test_bool() -> None:
    assert URL("http://example.com")
    assert not URL()
    assert not URL("")


def test_absolute_url_without_host() -> None:
    with pytest.raises(ValueError):
        URL("http://:8080/")


def test_url_is_not_str() -> None:
    url = URL("http://example.com")
    assert not isinstance(url, str)  # type: ignore[unreachable]


def test_str() -> None:
    url = URL("http://example.com:8888/path/to?a=1&b=2")
    assert str(url) == "http://example.com:8888/path/to?a=1&b=2"


def test_repr() -> None:
    url = URL("http://example.com")
    assert "URL('http://example.com')" == repr(url)


def test_origin() -> None:
    url = URL("http://user:password@example.com:8888/path/to?a=1&b=2")
    assert URL("http://example.com:8888") == url.origin()


def test_origin_is_equal_to_self() -> None:
    url = URL("http://example.com:8888")
    assert url.origin() == url


def test_origin_with_no_auth() -> None:
    url = URL("http://example.com:8888/path/to?a=1&b=2")
    assert URL("http://example.com:8888") == url.origin()


def test_origin_nonascii() -> None:
    url = URL("http://user:password@оун-упа.укр:8888/path/to?a=1&b=2")
    assert str(url.origin()) == "http://xn----8sb1bdhvc.xn--j1amh:8888"


def test_origin_ipv6() -> None:
    url = URL("http://user:password@[::1]:8888/path/to?a=1&b=2")
    assert str(url.origin()) == "http://[::1]:8888"


def test_origin_not_absolute_url() -> None:
    url = URL("/path/to?a=1&b=2")
    with pytest.raises(ValueError):
        url.origin()


def test_origin_no_scheme() -> None:
    url = URL("//user:password@example.com:8888/path/to?a=1&b=2")
    with pytest.raises(ValueError):
        url.origin()


def test_drop_dots() -> None:
    u = URL("http://example.com/path/../to")
    assert str(u) == "http://example.com/to"


def test_abs_cmp() -> None:
    assert URL("http://example.com:8888") == URL("http://example.com:8888")
    assert URL("http://example.com:8888/") == URL("http://example.com:8888/")
    assert URL("http://example.com:8888/") == URL("http://example.com:8888")
    assert URL("http://example.com:8888") == URL("http://example.com:8888/")


def test_abs_hash() -> None:
    url = URL("http://example.com:8888")
    url_trailing = URL("http://example.com:8888/")
    assert hash(url) == hash(url_trailing)


# properties


def test_scheme() -> None:
    url = URL("http://example.com")
    assert "http" == url.scheme


def test_raw_user() -> None:
    url = URL("http://user@example.com")
    assert "user" == url.raw_user
    assert url.raw_user == SplitResult(*url._val).username


def test_raw_user_non_ascii() -> None:
    url = URL("http://бажан@example.com")
    assert "%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD" == url.raw_user
    assert url.raw_user == SplitResult(*url._val).username


def test_no_user() -> None:
    url = URL("http://example.com")
    assert url.user is None


def test_user_non_ascii() -> None:
    url = URL("http://бажан@example.com")
    assert "бажан" == url.user


def test_raw_password() -> None:
    url = URL("http://user:password@example.com")
    assert "password" == url.raw_password
    assert url.raw_password == SplitResult(*url._val).password


def test_raw_password_non_ascii() -> None:
    url = URL("http://user:пароль@example.com")
    assert "%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C" == url.raw_password
    assert url.raw_password == SplitResult(*url._val).password


def test_password_non_ascii() -> None:
    url = URL("http://user:пароль@example.com")
    assert "пароль" == url.password


def test_password_without_user() -> None:
    url = URL("http://:password@example.com")
    assert url.user is None
    assert "password" == url.password


def test_empty_password_without_user() -> None:
    url = URL("http://:@example.com")
    assert url.user is None
    assert url.password == ""
    assert url.raw_password == ""
    assert url.raw_password == SplitResult(*url._val).password


def test_user_empty_password() -> None:
    url = URL("http://user:@example.com")
    assert "user" == url.user
    assert "" == url.password


def test_raw_host() -> None:
    url = URL("http://example.com")
    assert "example.com" == url.raw_host
    assert url.raw_host == SplitResult(*url._val).hostname


@pytest.mark.parametrize(
    ("host"),
    [
        ("example.com"),
        ("[::1]"),
        ("xn--gnter-4ya.com"),
    ],
)
def test_host_subcomponent(host: str) -> None:
    url = URL(f"http://{host}")
    assert url.host_subcomponent == host


@pytest.mark.parametrize(
    ("input", "result"),
    [
        ("/", None),
        ("http://example.com", "example.com"),
        ("http://[::1]", "[::1]"),
        ("http://xn--gnter-4ya.com", "xn--gnter-4ya.com"),
        ("http://example.com.", "example.com"),
        ("https://example.com.", "example.com"),
        ("http://example.com:80", "example.com"),
        ("http://example.com:8080", "example.com:8080"),
        ("http://[::1]:8080", "[::1]:8080"),
    ],
)
def test_host_port_subcomponent(input: str, result: str) -> None:
    url = URL(input)
    assert url.host_port_subcomponent == result


def test_host_subcomponent_return_idna_encoded_host() -> None:
    url = URL("http://оун-упа.укр")
    assert url.host_subcomponent == "xn----8sb1bdhvc.xn--j1amh"


def test_invalid_idna_hyphen_encoding() -> None:
    url = URL("http://x-----xn1agdj.tld")
    assert url.host == "x-----xn1agdj.tld"


def test_invalid_idna_a_label_encoding() -> None:
    url = URL("http://xn--d.tld")
    assert url.raw_host == "xn--d.tld"


def test_raw_host_non_ascii() -> None:
    url = URL("http://оун-упа.укр")
    assert "xn----8sb1bdhvc.xn--j1amh" == url.raw_host
    assert url.raw_host == SplitResult(*url._val).hostname


def test_host_non_ascii() -> None:
    url = URL("http://оун-упа.укр")
    assert "оун-упа.укр" == url.host


def test_localhost() -> None:
    url = URL("http://[::1]")
    assert "::1" == url.host


def test_host_with_underscore() -> None:
    url = URL("http://abc_def.com")
    assert "abc_def.com" == url.host


def test_raw_host_when_port_is_specified() -> None:
    url = URL("http://example.com:8888")
    assert "example.com" == url.raw_host
    assert url.raw_host == SplitResult(*url._val).hostname


def test_raw_host_from_str_with_ipv4() -> None:
    url = URL("http://127.0.0.1:80")
    assert url.raw_host == "127.0.0.1"
    assert url.raw_host == SplitResult(*url._val).hostname


def test_raw_host_from_str_with_ipv6() -> None:
    url = URL("http://[::1]:80")
    assert url.raw_host == "::1"
    assert url.raw_host == SplitResult(*url._val).hostname


def test_authority_full() -> None:
    url = URL("http://user:passwd@host.com:8080/path")
    assert url.raw_authority == "user:passwd@host.com:8080"
    assert url.authority == "user:passwd@host.com:8080"


def test_authority_short() -> None:
    url = URL("http://host.com/path")
    assert url.raw_authority == "host.com"


def test_authority_full_nonasci() -> None:
    url = URL("http://степан:пароль@слава.укр:8080/path")
    assert url.raw_authority == (
        "%D1%81%D1%82%D0%B5%D0%BF%D0%B0%D0%BD:"
        "%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C@"
        "xn--80aaf8a3a.xn--j1amh:8080"
    )
    assert url.authority == "степан:пароль@слава.укр:8080"


def test_authority_unknown_scheme() -> None:
    v = "scheme://user:password@example.com:43/path/to?a=1&b=2"
    url = URL(v)
    assert str(url) == v


def test_lowercase() -> None:
    url = URL("http://gitHUB.com")
    assert url.raw_host == "github.com"
    assert url.host == url.raw_host
    assert url.raw_host == SplitResult(*url._val).hostname


def test_lowercase_nonascii() -> None:
    url = URL("http://Слава.Укр")
    assert url.raw_host == "xn--80aaf8a3a.xn--j1amh"
    assert url.raw_host == SplitResult(*url._val).hostname
    assert url.host == "слава.укр"


def test_compressed_ipv6() -> None:
    url = URL("http://[1DEC:0:0:0::1]")
    assert url.raw_host == "1dec::1"
    assert url.host == url.raw_host
    assert url.raw_host == SplitResult(*url._val).hostname


def test_ipv6_missing_left_bracket() -> None:
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL("http://[1dec:0:0:0::1/")


def test_ipv6_missing_right_bracket() -> None:
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL("http://[1dec:0:0:0::1/")


@pytest.mark.parametrize(
    "url",
    (
        "http://[]/",
        "http://[1]/",
        "http://[127.0.0.1]/",
    ),
    ids=(
        "empty-IPv6-like-URL",
        "no-colons-in-IPv6",
        "IPv4-inside-brackets",
    ),
)
def test_ipv6_invalid_url(url: str) -> None:
    with pytest.raises(
        ValueError, match="The IPv6 content between brackets is not valid"
    ):
        URL(url)


def test_ipv6_brackets_in_reversed_order() -> None:
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL("http://]1dec:0:0:0::1[/")


@pytest.mark.parametrize(
    "url",
    (
        "http://127.0.0.1[aa::ff]",
        "http://127.0.0.1[aa::ff]/",
        "http://127.0.0.1[aa::ff]:8080/",
        "http://user@127.0.0.1[aa::ff]/",
        "http://example.com[::1]/",
    ),
    ids=(
        "ipv4-before-bracket",
        "ipv4-before-bracket-with-path",
        "ipv4-before-bracket-with-port",
        "userinfo-ipv4-before-bracket",
        "hostname-before-bracket",
    ),
)
def test_host_with_text_before_bracket_is_invalid(url: str) -> None:
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL(url)


@pytest.mark.parametrize(
    "url",
    (
        "http://[::1]allowed.example:1/",
        "http://[::1]evil.com/",
        "http://[::1]evil.com:8080/",
        "http://user@[::1]evil.com:1/",
        "http://[::1]evil/",
        "http://[::1].:80/",
    ),
    ids=(
        "suffix-with-port",
        "suffix-no-port",
        "suffix-with-explicit-port",
        "userinfo-suffix-with-port",
        "short-suffix-no-port",
        "dot-suffix-with-port",
    ),
)
def test_host_with_text_after_bracket_is_invalid(url: str) -> None:
    """Text after the closing bracket of an IP-literal is invalid.

    Per RFC 3986 §3.2.2, after the closing ']' of an IP-literal only
    ':' <port> or end-of-authority is valid. Previously yarl silently
    dropped the suffix (e.g. '[::1]allowed.example:1' -> '[::1]:1'),
    changing the effective host identity.
    """
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL(url)


def test_build_authority_with_text_after_bracket_is_invalid() -> None:
    """URL.build(authority=...) must also reject text after ']'."""
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL.build(scheme="http", authority="[::1]allowed.example:1", path="/")


def test_ipfuture_brackets_not_allowed() -> None:
    with pytest.raises(ValueError, match="IPvFuture address is invalid"):
        URL("http://[v10]/")


@pytest.mark.parametrize(
    "url",
    (
        "http://[:localhost[]].google:80",
        "http://[:localhost[]].google",
        "http://[:attacker.com[]]:80",
        "http://[:evil.com[]].bank.com:443",
        "http://[:127.0.0.1[]]:80",
        "http://[v1.:attacker[]].bank.com:80",
    ),
    ids=(
        "host-confusion-with-port",
        "host-confusion-without-port",
        "attacker-host-injection",
        "domain-allowlist-bypass",
        "private-ip-injection",
        "ipvfuture-bracket-abuse",
    ),
)
def test_malformed_bracketed_host_rejected(url: str) -> None:
    """Reject URLs with multiple brackets to prevent host confusion (SSRF)."""
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL(url)


def test_malformed_bracketed_host_in_authority() -> None:
    """Reject malformed brackets via URL.build(authority=...) path."""
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL.build(scheme="http", authority="[:localhost[]].google:80")


def test_userinfo_with_bracketed_host_is_valid() -> None:
    """Ensure the multi-bracket check still accepts userinfo + IP-literal host."""
    url = URL("http://user:pass@[::1]:8080/")
    assert url.user == "user"
    assert url.password == "pass"
    assert url.raw_host == "::1"
    assert url.port == 8080


def test_ipv4_zone() -> None:
    # I'm unsure if it is correct.
    url = URL("http://1.2.3.4%тест%42:123")
    assert url.raw_host == "1.2.3.4%тест%42"
    assert url.host == url.raw_host
    assert url.raw_host == SplitResult(*url._val).hostname


def test_ipv4_zone_percent25() -> None:
    """The ``%25`` decode in ``URL.host`` also covers IPv4 hosts.

    Zone identifiers on IPv4 addresses are outside any RFC; this pins
    the digit-branch behavior of the ``%25`` handling (#998).
    """
    url = URL("http://1.2.3.4%25eth0:123/")
    assert url.raw_host == "1.2.3.4%25eth0"
    assert url.host == "1.2.3.4%eth0"
    assert url.port == 123


def test_ipv6_zone_rfc6874() -> None:
    url = URL("http://[fe80::1%251]/")
    assert url.raw_host == "fe80::1%251"
    assert url.host == "fe80::1%1"
    assert url.host_subcomponent == "[fe80::1%251]"
    assert url.host_port_subcomponent == "[fe80::1%251]"
    assert url.authority == "fe80::1%1:80"
    assert url.human_repr() == "http://[fe80::1%1]/"
    assert str(url) == "http://[fe80::1%251]/"
    assert URL(str(url)) == url


def test_ipv6_zone_rfc6874_named_zone() -> None:
    url = URL("http://[fe80::1%25eth0]/")
    assert url.raw_host == "fe80::1%25eth0"
    assert url.host == "fe80::1%eth0"
    assert url.host_subcomponent == "[fe80::1%25eth0]"
    assert url.authority == "fe80::1%eth0:80"
    assert url.human_repr() == "http://[fe80::1%eth0]/"
    assert str(url) == "http://[fe80::1%25eth0]/"
    assert URL(str(url)) == url


def test_ipv6_zone_rfc6874_with_port() -> None:
    url = URL("http://[fe80::1%251]:8080/")
    assert url.raw_host == "fe80::1%251"
    assert url.host == "fe80::1%1"
    assert url.port == 8080
    assert url.host_port_subcomponent == "[fe80::1%251]:8080"
    assert url.authority == "fe80::1%1:8080"
    assert str(url) == "http://[fe80::1%251]:8080/"


def test_ipv6_zone_rfc6874_pct_encoded_inside_zone() -> None:
    # Multiple ``%25`` sequences: ``_encode_host`` partitions on the
    # first one (the RFC 6874 separator); ``.host`` then decodes every
    # ``%25`` in the result, including the one that originally encoded
    # a ``%`` inside the zone identifier.
    url = URL("http://[fe80::1%25foo%252fbar]/")
    assert url.raw_host == "fe80::1%25foo%252fbar"
    assert url.host == "fe80::1%foo%2fbar"
    assert url.host_subcomponent == "[fe80::1%25foo%252fbar]"
    assert str(url) == "http://[fe80::1%25foo%252fbar]/"
    assert URL(str(url)) == url


def test_ipv6_zone_bare_percent_parse() -> None:
    """A bare ``%`` zone separator is accepted and preserved verbatim.

    This is the pre-RFC 6874 de-facto form; RFC 6874 §3 suggests
    (non-normatively) accepting it, and curl/wget/urllib3 all do.
    yarl does not re-encode it to ``%25``.
    """
    url = URL("http://[fe80::1%eth0]:8080/")
    assert url.raw_host == "fe80::1%eth0"
    assert url.host == "fe80::1%eth0"
    assert url.host_subcomponent == "[fe80::1%eth0]"
    assert url.host_port_subcomponent == "[fe80::1%eth0]:8080"
    assert url.authority == "fe80::1%eth0:8080"
    assert url.human_repr() == "http://[fe80::1%eth0]:8080/"
    assert str(url) == "http://[fe80::1%eth0]:8080/"
    assert URL(str(url)) == url


def test_ipv6_zone_bare_percent_hex_ambiguous() -> None:
    """A bare ``%`` is accepted even when followed by two hex digits.

    RFC 6874 §3 (non-normative) suggests accepting a bare ``%`` only
    when it is not followed by two valid hexadecimal characters; like
    the rest of the ecosystem, yarl accepts it unconditionally, so
    ``%ee1`` is zone ``ee1`` rather than an error (#998).
    """
    url = URL("http://[fe80::a%ee1]/")
    assert url.raw_host == "fe80::a%ee1"
    assert url.host == "fe80::a%ee1"


def test_ipv6_zone_empty_zone_parse() -> None:
    """The string-parse path accepts empty zone identifiers.

    ``URL.build(host=...)`` rejects them (RFC 9844 §6.3), but parsing
    uses ``validate_host=False``; this documents the asymmetry (#998).
    """
    url = URL("http://[fe80::1%25]/")
    assert url.raw_host == "fe80::1%25"
    assert url.host == "fe80::1%"
    url = URL("http://[fe80::1%]/")
    assert url.raw_host == "fe80::1%"
    assert url.host == "fe80::1%"


def test_ipv6_zone_rfc6874_multiple_percent25() -> None:
    """Only the first ``%25`` is the separator; later ones are zone text."""
    url = URL("http://[fe80::1%25a%25b]/")
    assert url.raw_host == "fe80::1%25a%25b"
    assert url.host == "fe80::1%a%b"
    assert str(url) == "http://[fe80::1%25a%25b]/"
    assert URL(str(url)) == url


def test_ipv6_zone_rfc6874_encoded_url() -> None:
    """The ``.host`` zone decode applies to pre-encoded URLs as well."""
    url = URL("http://[fe80::1%25eth0]/", encoded=True)
    assert url.raw_host == "fe80::1%25eth0"
    assert url.host == "fe80::1%eth0"
    assert str(url) == "http://[fe80::1%25eth0]/"


def test_ipv6_zone_separator_spellings_not_equal() -> None:
    """The two zone separator spellings are distinct URLs.

    yarl performs no normalization between ``%25`` and bare ``%``, so
    URLs denoting the same scoped address compare unequal even though
    ``.host`` decodes both to the same value (#998).
    """
    encoded = URL("http://[fe80::1%25eth0]/")
    bare = URL("http://[fe80::1%eth0]/")
    assert encoded != bare
    assert encoded.host == bare.host


def test_ipv6_zone_rfc6874_global_address() -> None:
    """Zone identifiers are accepted on non-link-local addresses.

    RFC 6874 §4 restricts zone usage to link-local addresses
    (fe80::/10); yarl, like curl and urllib3, does not enforce
    this (#998).
    """
    url = URL("http://[2001:db8::1%25eth0]/")
    assert url.raw_host == "2001:db8::1%25eth0"
    assert url.host == "2001:db8::1%eth0"


def test_ipv6_zone_rfc6874_mutation_apis() -> None:
    """The encoded zone survives URL mutation APIs byte-for-byte."""
    url = URL("http://[fe80::1%25eth0]:8080/p?q=1")
    assert str(url.origin()) == "http://[fe80::1%25eth0]:8080"
    assert str(url.with_port(9000)) == "http://[fe80::1%25eth0]:9000/p?q=1"
    assert str(url.with_user("u")) == "http://u@[fe80::1%25eth0]:8080/p?q=1"
    assert str(url.join(URL("/other"))) == "http://[fe80::1%25eth0]:8080/other"


def test_port_for_explicit_port() -> None:
    url = URL("http://example.com:8888")
    assert 8888 == url.port
    assert url.explicit_port == SplitResult(*url._val).port


def test_port_for_implicit_port() -> None:
    url = URL("http://example.com")
    assert 80 == url.port
    assert url.explicit_port == SplitResult(*url._val).port


def test_port_for_relative_url() -> None:
    url = URL("/path/to")
    assert url.port is None
    assert url.explicit_port is None


def test_port_for_unknown_scheme() -> None:
    url = URL("unknown://example.com")
    assert url.port is None
    assert url.explicit_port is None


def test_explicit_zero_port() -> None:
    url = URL("http://example.com:0")
    assert url.explicit_port == 0
    assert url.port == 0


def test_explicit_port_for_explicit_port() -> None:
    url = URL("http://example.com:8888")
    assert 8888 == url.explicit_port
    assert url.explicit_port == SplitResult(*url._val).port


def test_explicit_port_for_implicit_port() -> None:
    url = URL("http://example.com")
    assert url.explicit_port is None
    assert url.explicit_port == SplitResult(*url._val).port


def test_explicit_port_for_relative_url() -> None:
    url = URL("/path/to")
    assert url.explicit_port is None
    assert url.explicit_port == SplitResult(*url._val).port


def test_explicit_port_for_unknown_scheme() -> None:
    url = URL("unknown://example.com")
    assert url.explicit_port is None
    assert url.explicit_port == SplitResult(*url._val).port


def test_raw_path_string_empty() -> None:
    url = URL("http://example.com")
    assert "/" == url.raw_path


def test_raw_path() -> None:
    url = URL("http://example.com/path/to")
    assert "/path/to" == url.raw_path


def test_raw_path_non_ascii() -> None:
    url = URL("http://example.com/шлях/сюди")
    assert "/%D1%88%D0%BB%D1%8F%D1%85/%D1%81%D1%8E%D0%B4%D0%B8" == url.raw_path


def test_path_non_ascii() -> None:
    url = URL("http://example.com/шлях/сюди")
    assert "/шлях/сюди" == url.path


def test_path_with_spaces() -> None:
    url = URL("http://example.com/a b/test")
    assert "/a b/test" == url.path

    url = URL("http://example.com/a b")
    assert "/a b" == url.path


def test_path_with_2F() -> None:
    """Path should decode %2F."""

    url = URL("http://example.com/foo/bar%2fbaz")
    assert url.path == "/foo/bar/baz"


def test_path_safe_with_2F() -> None:
    """Path safe should not decode %2F, otherwise it may look like a path separator."""

    url = URL("http://example.com/foo/bar%2fbaz")
    assert url.path_safe == "/foo/bar%2Fbaz"


def test_path_safe_with_25() -> None:
    """Path safe should not decode %25, otherwise it is prone to double unquoting."""

    url = URL("http://example.com/foo/bar%252Fbaz")
    assert url.path_safe == "/foo/bar%252Fbaz"
    unquoted = url.path_safe.replace("%2F", "/").replace("%25", "%")
    assert unquoted == "/foo/bar%2Fbaz"


def test_path_safe_with_no_netloc() -> None:
    """Path safe should not decode %2F, otherwise it may look like a path separator."""

    url = URL("/foo/bar%2fbaz")
    assert url.path_safe == "/foo/bar%2Fbaz"
    url = URL("")
    assert url.path_safe == ""
    url = URL("http://example.com")
    assert url.path_safe == "/"


@pytest.mark.parametrize(
    "original_path",
    [
        "m+@bar/baz",
        "m%2B@bar/baz",
        "m%252B@bar/baz",
        "m%2F@bar/baz",
    ],
)
def test_path_safe_only_round_trips(original_path: str) -> None:
    """Path safe can round trip with documented decode method."""
    encoded_once = quote(original_path, safe="")
    encoded_twice = quote(encoded_once, safe="")

    url = URL(f"http://example.com/{encoded_twice}")
    unquoted = url.path_safe.replace("%2F", "/").replace("%25", "%")
    assert unquoted == f"/{encoded_once}"
    assert unquote(unquoted) == f"/{original_path}"


def test_raw_path_for_empty_url() -> None:
    url = URL()
    assert "" == url.raw_path


def test_raw_path_for_colon_and_at() -> None:
    url = URL("http://example.com/path:abc@123")
    assert url.raw_path == "/path:abc@123"


def test_raw_query_string() -> None:
    url = URL("http://example.com?a=1&b=2")
    assert url.raw_query_string == "a=1&b=2"


def test_raw_query_string_non_ascii() -> None:
    url = URL("http://example.com?б=в&ю=к")
    assert url.raw_query_string == "%D0%B1=%D0%B2&%D1%8E=%D0%BA"


def test_query_string_non_ascii() -> None:
    url = URL("http://example.com?б=в&ю=к")
    assert url.query_string == "б=в&ю=к"


def test_path_qs() -> None:
    url = URL("http://example.com/")
    assert url.path_qs == "/"
    url = URL("http://example.com/?б=в&ю=к")
    assert url.path_qs == "/?б=в&ю=к"
    url = URL("http://example.com/path?б=в&ю=к")
    assert url.path_qs == "/path?б=в&ю=к"
    url = URL("/path?б=в&ю=к")
    assert url.path_qs == "/path?б=в&ю=к"
    url = URL("")
    assert url.path_qs == ""
    url = URL("http://example.com")
    assert url.path_qs == "/"


def test_raw_path_qs() -> None:
    url = URL("http://example.com/")
    assert url.raw_path_qs == "/"
    url = URL("http://example.com/?б=в&ю=к")
    assert url.raw_path_qs == "/?%D0%B1=%D0%B2&%D1%8E=%D0%BA"
    url = URL("http://example.com/path?б=в&ю=к")
    assert url.raw_path_qs == "/path?%D0%B1=%D0%B2&%D1%8E=%D0%BA"
    url = URL("http://example.com/шлях?a=1&b=2")
    assert url.raw_path_qs == "/%D1%88%D0%BB%D1%8F%D1%85?a=1&b=2"
    url = URL("/шлях?a=1&b=2")
    assert url.raw_path_qs == "/%D1%88%D0%BB%D1%8F%D1%85?a=1&b=2"
    url = URL("")
    assert url.raw_path_qs == ""
    url = URL("http://example.com")
    assert url.raw_path_qs == "/"


def test_query_string_spaces() -> None:
    url = URL("http://example.com?a+b=c+d&e=f+g")
    assert url.query_string == "a b=c d&e=f g"


# raw fragment


def test_raw_fragment_empty() -> None:
    url = URL("http://example.com")
    assert "" == url.raw_fragment


def test_raw_fragment() -> None:
    url = URL("http://example.com/path#anchor")
    assert "anchor" == url.raw_fragment


def test_raw_fragment_non_ascii() -> None:
    url = URL("http://example.com/path#якір")
    assert "%D1%8F%D0%BA%D1%96%D1%80" == url.raw_fragment


def test_raw_fragment_safe() -> None:
    url = URL("http://example.com/path#a?b/c:d@e")
    assert "a?b/c:d@e" == url.raw_fragment


def test_fragment_non_ascii() -> None:
    url = URL("http://example.com/path#якір")
    assert "якір" == url.fragment


def test_raw_parts_empty() -> None:
    url = URL("http://example.com")
    assert ("/",) == url.raw_parts


def test_raw_parts() -> None:
    url = URL("http://example.com/path/to")
    assert ("/", "path", "to") == url.raw_parts


def test_raw_parts_without_path() -> None:
    url = URL("http://example.com")
    assert ("/",) == url.raw_parts


def test_raw_path_parts_with_2F_in_path() -> None:
    url = URL("http://example.com/path%2Fto/three")
    assert ("/", "path%2Fto", "three") == url.raw_parts


def test_raw_path_parts_with_2f_in_path() -> None:
    url = URL("http://example.com/path%2fto/three")
    assert ("/", "path%2Fto", "three") == url.raw_parts


def test_raw_parts_for_relative_path() -> None:
    url = URL("path/to")
    assert ("path", "to") == url.raw_parts


def test_raw_parts_for_relative_path_starting_from_slash() -> None:
    url = URL("/path/to")
    assert ("/", "path", "to") == url.raw_parts


def test_raw_parts_for_relative_double_path() -> None:
    url = URL("path/to")
    assert ("path", "to") == url.raw_parts


def test_parts_for_empty_url() -> None:
    url = URL()
    assert ("",) == url.raw_parts


def test_raw_parts_non_ascii() -> None:
    url = URL("http://example.com/шлях/сюди")
    assert (
        "/",
        "%D1%88%D0%BB%D1%8F%D1%85",
        "%D1%81%D1%8E%D0%B4%D0%B8",
    ) == url.raw_parts


def test_parts_non_ascii() -> None:
    url = URL("http://example.com/шлях/сюди")
    assert ("/", "шлях", "сюди") == url.parts


def test_name_for_empty_url() -> None:
    url = URL()
    assert "" == url.raw_name


def test_raw_name() -> None:
    url = URL("http://example.com/path/to#frag")
    assert "to" == url.raw_name


def test_raw_name_root() -> None:
    url = URL("http://example.com/#frag")
    assert "" == url.raw_name


def test_raw_name_root2() -> None:
    url = URL("http://example.com")
    assert "" == url.raw_name


def test_raw_name_root3() -> None:
    url = URL("http://example.com/")
    assert "" == url.raw_name


def test_relative_raw_name() -> None:
    url = URL("path/to")
    assert "to" == url.raw_name


def test_relative_raw_name_starting_from_slash() -> None:
    url = URL("/path/to")
    assert "to" == url.raw_name


def test_relative_raw_name_slash() -> None:
    url = URL("/")
    assert "" == url.raw_name


def test_name_non_ascii() -> None:
    url = URL("http://example.com/шлях")
    assert url.name == "шлях"


def test_suffix_for_empty_url() -> None:
    url = URL()
    assert "" == url.raw_suffix


def test_raw_suffix() -> None:
    url = URL("http://example.com/path/to.txt#frag")
    assert ".txt" == url.raw_suffix


def test_raw_suffix_root() -> None:
    url = URL("http://example.com/#frag")
    assert "" == url.raw_suffix


def test_raw_suffix_root2() -> None:
    url = URL("http://example.com")
    assert "" == url.raw_suffix


def test_raw_suffix_root3() -> None:
    url = URL("http://example.com/")
    assert "" == url.raw_suffix


def test_relative_raw_suffix() -> None:
    url = URL("path/to")
    assert "" == url.raw_suffix


def test_relative_raw_suffix_starting_from_slash() -> None:
    url = URL("/path/to")
    assert "" == url.raw_suffix


def test_relative_raw_suffix_dot() -> None:
    url = URL(".")
    assert "" == url.raw_suffix


def test_suffix_non_ascii() -> None:
    url = URL("http://example.com/шлях.суфікс")
    assert url.suffix == ".суфікс"


def test_suffix_with_empty_name() -> None:
    url = URL("http://example.com/.hgrc")
    assert "" == url.raw_suffix


def test_suffix_multi_dot() -> None:
    url = URL("http://example.com/doc.tar.gz")
    assert ".gz" == url.raw_suffix


def test_suffix_with_dot_name() -> None:
    url = URL("http://example.com/doc.")
    assert "" == url.raw_suffix


def test_suffixes_for_empty_url() -> None:
    url = URL()
    assert () == url.raw_suffixes


def test_raw_suffixes() -> None:
    url = URL("http://example.com/path/to.txt#frag")
    assert (".txt",) == url.raw_suffixes


def test_raw_suffixes_root() -> None:
    url = URL("http://example.com/#frag")
    assert () == url.raw_suffixes


def test_raw_suffixes_root2() -> None:
    url = URL("http://example.com")
    assert () == url.raw_suffixes


def test_raw_suffixes_root3() -> None:
    url = URL("http://example.com/")
    assert () == url.raw_suffixes


def test_relative_raw_suffixes() -> None:
    url = URL("path/to")
    assert () == url.raw_suffixes


def test_relative_raw_suffixes_starting_from_slash() -> None:
    url = URL("/path/to")
    assert () == url.raw_suffixes


def test_relative_raw_suffixes_dot() -> None:
    url = URL(".")
    assert () == url.raw_suffixes


def test_suffixes_non_ascii() -> None:
    url = URL("http://example.com/шлях.суфікс")
    assert url.suffixes == (".суфікс",)


def test_suffixes_with_empty_name() -> None:
    url = URL("http://example.com/.hgrc")
    assert () == url.raw_suffixes


def test_suffixes_multi_dot() -> None:
    url = URL("http://example.com/doc.tar.gz")
    assert (".tar", ".gz") == url.raw_suffixes


def test_suffixes_with_dot_name() -> None:
    url = URL("http://example.com/doc.")
    assert () == url.raw_suffixes


def test_plus_in_path() -> None:
    url = URL("http://example.com/test/x+y%2Bz/:+%2B/")
    assert "/test/x+y+z/:++/" == url.path


def test_nonascii_in_qs() -> None:
    url = URL("http://example.com")
    url2 = url.with_query({"f\xf8\xf8": "f\xf8\xf8"})
    assert "http://example.com/?f%C3%B8%C3%B8=f%C3%B8%C3%B8" == str(url2)


def test_percent_encoded_in_qs() -> None:
    url = URL("http://example.com")
    url2 = url.with_query({"k%cf%80": "v%cf%80"})
    assert str(url2) == "http://example.com/?k%25cf%2580=v%25cf%2580"
    assert url2.raw_query_string == "k%25cf%2580=v%25cf%2580"
    assert url2.query_string == "k%cf%80=v%cf%80"
    assert url2.query == {"k%cf%80": "v%cf%80"}


# modifiers


def test_parent_raw_path() -> None:
    url = URL("http://example.com/path/to")
    assert url.parent.raw_path == "/path"


def test_parent_raw_parts() -> None:
    url = URL("http://example.com/path/to")
    assert url.parent.raw_parts == ("/", "path")


def test_double_parent_raw_path() -> None:
    url = URL("http://example.com/path/to")
    assert url.parent.parent.raw_path == "/"


def test_empty_parent_raw_path() -> None:
    url = URL("http://example.com/")
    assert url.parent.parent.raw_path == "/"


def test_empty_parent_raw_path2() -> None:
    url = URL("http://example.com")
    assert url.parent.parent.raw_path == "/"


def test_clear_fragment_on_getting_parent() -> None:
    url = URL("http://example.com/path/to#frag")
    assert URL("http://example.com/path") == url.parent


def test_clear_fragment_on_getting_parent_toplevel() -> None:
    url = URL("http://example.com/#frag")
    assert URL("http://example.com/") == url.parent


def test_clear_query_on_getting_parent() -> None:
    url = URL("http://example.com/path/to?a=b")
    assert URL("http://example.com/path") == url.parent


def test_clear_query_on_getting_parent_toplevel() -> None:
    url = URL("http://example.com/?a=b")
    assert URL("http://example.com/") == url.parent


# truediv


def test_div_root() -> None:
    url = URL("http://example.com") / "path" / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div_root_with_slash() -> None:
    url = URL("http://example.com/") / "path" / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div() -> None:
    url = URL("http://example.com/path") / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div_with_slash() -> None:
    url = URL("http://example.com/path/") / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div_path_starting_from_slash_is_forbidden() -> None:
    url = URL("http://example.com/path/")
    with pytest.raises(ValueError):
        url / "/to/others"


class StrEnum(str, Enum):
    spam = "ham"

    def __str__(self) -> str:
        return self.value


def test_div_path_srting_subclass() -> None:
    url = URL("http://example.com/path/") / StrEnum.spam
    assert str(url) == "http://example.com/path/ham"


def test_div_bad_type() -> None:
    url = URL("http://example.com/path/")
    with pytest.raises(TypeError):
        url / 3  # type: ignore[operator]


def test_div_cleanup_query_and_fragment() -> None:
    url = URL("http://example.com/path?a=1#frag")
    assert str(url / "to") == "http://example.com/path/to"


def test_div_for_empty_url() -> None:
    url = URL() / "a"
    assert url.raw_parts == ("a",)


def test_div_for_relative_url() -> None:
    url = URL("a") / "b"
    assert url.raw_parts == ("a", "b")


def test_div_for_relative_url_started_with_slash() -> None:
    url = URL("/a") / "b"
    assert url.raw_parts == ("/", "a", "b")


def test_div_non_ascii() -> None:
    url = URL("http://example.com/сюди")
    url2 = url / "туди"
    assert url2.path == "/сюди/туди"
    assert url2.raw_path == "/%D1%81%D1%8E%D0%B4%D0%B8/%D1%82%D1%83%D0%B4%D0%B8"
    assert url2.parts == ("/", "сюди", "туди")
    assert url2.raw_parts == (
        "/",
        "%D1%81%D1%8E%D0%B4%D0%B8",
        "%D1%82%D1%83%D0%B4%D0%B8",
    )


def test_div_percent_encoded() -> None:
    url = URL("http://example.com/path")
    url2 = url / "%cf%80"
    assert url2.path == "/path/%cf%80"
    assert url2.raw_path == "/path/%25cf%2580"
    assert url2.parts == ("/", "path", "%cf%80")
    assert url2.raw_parts == ("/", "path", "%25cf%2580")


def test_div_with_colon_and_at() -> None:
    url = URL("http://example.com/base") / "path:abc@123"
    assert url.raw_path == "/base/path:abc@123"


def test_div_with_dots() -> None:
    url = URL("http://example.com/base") / "../path/./to"
    assert url.raw_path == "/path/to"


# joinpath


@pytest.mark.parametrize(
    "base,to_join,expected",
    [
        pytest.param("", ("path", "to"), "http://example.com/path/to", id="root"),
        pytest.param(
            "/", ("path", "to"), "http://example.com/path/to", id="root-with-slash"
        ),
        pytest.param("/path", ("to",), "http://example.com/path/to", id="path"),
        pytest.param(
            "/path/", ("to",), "http://example.com/path/to", id="path-with-slash"
        ),
        pytest.param(
            "/path", ("",), "http://example.com/path/", id="path-add-trailing-slash"
        ),
        pytest.param(
            "/path?a=1#frag",
            ("to",),
            "http://example.com/path/to",
            id="cleanup-query-and-fragment",
        ),
        pytest.param("", ("path/",), "http://example.com/path/", id="trailing-slash"),
        pytest.param(
            "",
            (
                "path",
                "",
            ),
            "http://example.com/path/",
            id="trailing-slash-empty-string",
        ),
        pytest.param(
            "", ("path/", "to/"), "http://example.com/path/to/", id="duplicate-slash"
        ),
        pytest.param("", (), "http://example.com", id="empty-segments"),
        pytest.param(
            "/", ("path/",), "http://example.com/path/", id="base-slash-trailing-slash"
        ),
        pytest.param(
            "/",
            ("path/", "to/"),
            "http://example.com/path/to/",
            id="base-slash-duplicate-slash",
        ),
        pytest.param("/", (), "http://example.com", id="base-slash-empty-segments"),
    ],
)
def test_joinpath(base: str, to_join: tuple[str, ...], expected: str) -> None:
    url = URL(f"http://example.com{base}")
    assert str(url.joinpath(*to_join)) == expected


@pytest.mark.parametrize(
    "base,to_join,expected",
    [
        pytest.param("path", "a", "path/a", id="default_default"),
        pytest.param("path", "./a", "path/a", id="default_relative"),
        pytest.param("path/", "a", "path/a", id="empty-segment_default"),
        pytest.param("path/", "./a", "path/a", id="empty-segment_relative"),
        pytest.param("path", ".//a", "path//a", id="default_empty-segment"),
        pytest.param("path/", ".//a", "path//a", id="empty-segment_empty_segment"),
        pytest.param("path//", "a", "path//a", id="empty-segments_default"),
        pytest.param("path//", "./a", "path//a", id="empty-segments_relative"),
        pytest.param("path//", ".//a", "path///a", id="empty-segments_empty-segment"),
        pytest.param("path", "a/", "path/a/", id="default_trailing-empty-segment"),
        pytest.param("path", "a//", "path/a//", id="default_trailing-empty-segments"),
        pytest.param("path", "a//b", "path/a//b", id="default_embedded-empty-segment"),
        pytest.param(
            "path/a/b/c/d/e", "a/../../../../../../c", "path/c", id="long-backtrack"
        ),
        pytest.param(
            "path/a/b/c/d/e",
            "a/../../../././../../../c",
            "path/c",
            id="long-backtrack-with-dots",
        ),
        pytest.param("path/a/../../d/e", "a/../c", "d/e/c", id="backtrack-in-both"),
    ],
)
def test_joinpath_empty_segments(base: str, to_join: str, expected: str) -> None:
    url = URL(f"http://example.com/{base}")
    assert (
        f"http://example.com/{expected}" == str(url.joinpath(to_join))
        and str(url / to_join) == f"http://example.com/{expected}"
    )


def test_joinpath_backtrack_to_base() -> None:
    url = URL("http://example.com/../../c")
    new_url = url.joinpath("../../..")
    assert str(new_url) == "http://example.com"
    assert new_url.path == "/"
    assert new_url.raw_path == "/"


def test_joinpath_single_empty_segments() -> None:
    """joining standalone empty segments does not create empty segments"""
    a = URL("/1//2///3")
    assert a.parts == ("/", "1", "", "2", "", "", "3")
    b = URL("scheme://host").joinpath(*a.parts[1:])
    assert b.path == "/1/2/3"


@pytest.mark.parametrize(
    "url,to_join,expected",
    [
        pytest.param(URL(), ("a",), ("a",), id="empty-url"),
        pytest.param(URL("a"), ("b",), ("a", "b"), id="relative-path"),
        pytest.param(URL("a"), ("b", "", "c"), ("a", "b", "c"), id="empty-element"),
        pytest.param(URL("/a"), ("b"), ("/", "a", "b"), id="absolute-path"),
        pytest.param(URL(), ("a/",), ("a", ""), id="trailing-slash"),
        pytest.param(URL(), ("a/", "b/"), ("a", "b", ""), id="duplicate-slash"),
        pytest.param(URL(), (), ("",), id="empty-segments"),
    ],
)
def test_joinpath_relative(
    url: URL, to_join: tuple[str, ...], expected: tuple[str, ...]
) -> None:
    assert url.joinpath(*to_join).raw_parts == expected


@pytest.mark.parametrize(
    "url,to_join,encoded,e_path,e_raw_path,e_parts,e_raw_parts",
    [
        pytest.param(
            "http://example.com/сюди",
            ("туди",),
            False,
            "/сюди/туди",
            "/%D1%81%D1%8E%D0%B4%D0%B8/%D1%82%D1%83%D0%B4%D0%B8",
            ("/", "сюди", "туди"),
            ("/", "%D1%81%D1%8E%D0%B4%D0%B8", "%D1%82%D1%83%D0%B4%D0%B8"),
            id="non-ascii",
        ),
        pytest.param(
            "http://example.com/path",
            ("%cf%80",),
            False,
            "/path/%cf%80",
            "/path/%25cf%2580",
            ("/", "path", "%cf%80"),
            ("/", "path", "%25cf%2580"),
            id="percent-encoded",
        ),
        pytest.param(
            "http://example.com/path",
            ("%cf%80",),
            True,
            "/path/π",
            "/path/%cf%80",
            ("/", "path", "π"),
            ("/", "path", "%cf%80"),
            id="encoded-percent-encoded",
        ),
    ],
)
def test_joinpath_encoding(
    url: str,
    to_join: tuple[str, ...],
    encoded: bool,
    e_path: str,
    e_raw_path: str,
    e_parts: tuple[str, ...],
    e_raw_parts: tuple[str, ...],
) -> None:
    joined = URL(url).joinpath(*to_join, encoded=encoded)
    assert joined.path == e_path
    assert joined.raw_path == e_raw_path
    assert joined.parts == e_parts
    assert joined.raw_parts == e_raw_parts


@pytest.mark.parametrize(
    "to_join,expected",
    [
        pytest.param(("path:abc@123",), "/base/path:abc@123", id="with-colon-and-at"),
        pytest.param(("..", "path", ".", "to"), "/path/to", id="with-dots"),
    ],
)
def test_joinpath_edgecases(to_join: tuple[str, ...], expected: str) -> None:
    url = URL("http://example.com/base").joinpath(*to_join)
    assert url.raw_path == expected


def test_joinpath_path_starting_from_slash_is_forbidden() -> None:
    url = URL("http://example.com/path/")
    with pytest.raises(
        ValueError, match="Appending path .* starting from slash is forbidden"
    ):
        assert url.joinpath("/to/others")


PATHS = [
    # No dots
    ("", ""),
    ("path", "path"),
    # Single-dot
    ("path/to", "path/to"),
    ("././path/to", "path/to"),
    ("path/./to", "path/to"),
    ("path/././to", "path/to"),
    ("path/to/.", "path/to/"),
    ("path/to/./.", "path/to/"),
    # Double-dots
    ("../path/to", "path/to"),
    ("path/../to", "to"),
    ("path/../../to", "to"),
    # Non-ASCII characters
    ("μονοπάτι/../../να/ᴜɴɪ/ᴄᴏᴅᴇ", "να/ᴜɴɪ/ᴄᴏᴅᴇ"),
    ("μονοπάτι/../../να/𝕦𝕟𝕚/𝕔𝕠𝕕𝕖/.", "να/𝕦𝕟𝕚/𝕔𝕠𝕕𝕖/"),
]


@pytest.mark.parametrize("original,expected", PATHS)
def test_join_path_normalized(original: str, expected: str) -> None:
    """Test that joinpath normalizes paths."""
    base_url = URL("http://example.com")
    new_url = base_url.joinpath(original)
    assert new_url.path == f"/{expected}"


# with_path


def test_with_path() -> None:
    url = URL("http://example.com")
    url2 = url.with_path("/test")
    assert str(url2) == "http://example.com/test"
    assert url2.raw_path == "/test"
    assert url2.path == "/test"


def test_with_path_nonascii() -> None:
    url = URL("http://example.com")
    url2 = url.with_path("/π")
    assert str(url2) == "http://example.com/%CF%80"
    assert url2.raw_path == "/%CF%80"
    assert url2.path == "/π"


def test_with_path_percent_encoded() -> None:
    url = URL("http://example.com")
    url2 = url.with_path("/%cf%80")
    assert str(url2) == "http://example.com/%25cf%2580"
    assert url2.raw_path == "/%25cf%2580"
    assert url2.path == "/%cf%80"


def test_with_path_encoded() -> None:
    url = URL("http://example.com")
    url2 = url.with_path("/test", encoded=True)
    assert str(url2) == "http://example.com/test"
    assert url2.raw_path == "/test"
    assert url2.path == "/test"


def test_with_path_encoded_nonascii() -> None:
    url = URL("http://example.com")
    url2 = url.with_path("/π", encoded=True)
    assert str(url2) == "http://example.com/π"
    assert url2.raw_path == "/π"
    assert url2.path == "/π"


def test_with_path_encoded_percent_encoded() -> None:
    url = URL("http://example.com")
    url2 = url.with_path("/%cf%80", encoded=True)
    assert str(url2) == "http://example.com/%cf%80"
    assert url2.raw_path == "/%cf%80"
    assert url2.path == "/π"


def test_with_path_dots() -> None:
    url = URL("http://example.com")
    assert str(url.with_path("/test/.")) == "http://example.com/test/"


def test_with_path_relative() -> None:
    url = URL("/path")
    assert str(url.with_path("/new")) == "/new"


def test_with_path_query() -> None:
    url = URL("http://example.com?a=b")
    assert str(url.with_path("/test")) == "http://example.com/test"


def test_with_path_fragment() -> None:
    url = URL("http://example.com#frag")
    assert str(url.with_path("/test")) == "http://example.com/test"


@pytest.mark.parametrize(
    ("original_url", "keep_query", "keep_fragment", "expected_url"),
    [
        pytest.param(
            "http://example.com?a=b#frag",
            True,
            False,
            "http://example.com/test?a=b",
            id="query-only",
        ),
        pytest.param(
            "http://example.com?a=b#frag",
            False,
            True,
            "http://example.com/test#frag",
            id="fragment-only",
        ),
        pytest.param(
            "http://example.com?a=b#frag",
            True,
            True,
            "http://example.com/test?a=b#frag",
            id="all",
        ),
        pytest.param(
            "http://example.com?a=b#frag",
            False,
            False,
            "http://example.com/test",
            id="none",
        ),
    ],
)
def test_with_path_keep_query_keep_fragment_flags(
    original_url: str, keep_query: bool, keep_fragment: bool, expected_url: str
) -> None:
    url = URL(original_url)
    url2 = url.with_path("/test", keep_query=keep_query, keep_fragment=keep_fragment)
    assert str(url2) == expected_url


def test_with_path_empty() -> None:
    url = URL("http://example.com/test")
    assert str(url.with_path("")) == "http://example.com"


def test_with_path_leading_slash() -> None:
    url = URL("http://example.com")
    assert url.with_path("test").path == "/test"


# with_fragment


def test_with_fragment() -> None:
    url = URL("http://example.com")
    url2 = url.with_fragment("frag")
    assert str(url2) == "http://example.com/#frag"
    assert url2.raw_fragment == "frag"
    assert url2.fragment == "frag"


def test_with_fragment_safe() -> None:
    url = URL("http://example.com")
    u2 = url.with_fragment("a:b?c@d/e")
    assert str(u2) == "http://example.com/#a:b?c@d/e"


def test_with_fragment_non_ascii() -> None:
    url = URL("http://example.com")
    url2 = url.with_fragment("фрагм")
    assert url2.raw_fragment == "%D1%84%D1%80%D0%B0%D0%B3%D0%BC"
    assert url2.fragment == "фрагм"


def test_with_fragment_percent_encoded() -> None:
    url = URL("http://example.com")
    url2 = url.with_fragment("%cf%80")
    assert str(url2) == "http://example.com/#%25cf%2580"
    assert url2.raw_fragment == "%25cf%2580"
    assert url2.fragment == "%cf%80"


def test_with_fragment_None() -> None:
    url = URL("http://example.com/path#frag")
    url2 = url.with_fragment(None)
    assert str(url2) == "http://example.com/path"


def test_with_fragment_None_matching() -> None:
    url = URL("http://example.com/path")
    url2 = url.with_fragment(None)
    assert url is url2


def test_with_fragment_matching() -> None:
    url = URL("http://example.com/path#frag")
    url2 = url.with_fragment("frag")
    assert url is url2


def test_with_fragment_bad_type() -> None:
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        url.with_fragment(123)  # type: ignore[arg-type]


# with_name


def test_with_name() -> None:
    url = URL("http://example.com/a/b")
    assert url.raw_parts == ("/", "a", "b")
    url2 = url.with_name("c")
    assert url2.raw_parts == ("/", "a", "c")
    assert url2.parts == ("/", "a", "c")
    assert url2.raw_path == "/a/c"
    assert url2.path == "/a/c"


@pytest.mark.parametrize(
    ("original_url", "keep_query", "keep_fragment", "expected_url"),
    [
        pytest.param(
            "http://example.com/path/to?a=b#frag",
            True,
            False,
            "http://example.com/path/newname?a=b",
            id="query-only",
        ),
        pytest.param(
            "http://example.com/path/to?a=b#frag",
            False,
            True,
            "http://example.com/path/newname#frag",
            id="fragment-only",
        ),
        pytest.param(
            "http://example.com/path/to?a=b#frag",
            True,
            True,
            "http://example.com/path/newname?a=b#frag",
            id="all",
        ),
        pytest.param(
            "http://example.com/path/to?a=b#frag",
            False,
            False,
            "http://example.com/path/newname",
            id="none",
        ),
    ],
)
def test_with_name_keep_query_keep_fragment_flags(
    original_url: str, keep_query: bool, keep_fragment: bool, expected_url: str
) -> None:
    url = URL(original_url)
    url2 = url.with_name("newname", keep_query=keep_query, keep_fragment=keep_fragment)
    assert str(url2) == expected_url


def test_with_name_for_naked_path() -> None:
    url = URL("http://example.com")
    url2 = url.with_name("a")
    assert url2.raw_parts == ("/", "a")


def test_with_name_for_relative_path() -> None:
    url = URL("a")
    url2 = url.with_name("b")
    assert url2.raw_parts == ("b",)


def test_with_name_for_relative_path2() -> None:
    url = URL("a/b")
    url2 = url.with_name("c")
    assert url2.raw_parts == ("a", "c")


def test_with_name_for_relative_path_starting_from_slash() -> None:
    url = URL("/a")
    url2 = url.with_name("b")
    assert url2.raw_parts == ("/", "b")


def test_with_name_for_relative_path_starting_from_slash2() -> None:
    url = URL("/a/b")
    url2 = url.with_name("c")
    assert url2.raw_parts == ("/", "a", "c")


def test_with_name_empty() -> None:
    url = URL("http://example.com/path/to").with_name("")
    assert str(url) == "http://example.com/path/"


def test_with_name_non_ascii() -> None:
    url = URL("http://example.com/path").with_name("шлях")
    assert url.path == "/шлях"
    assert url.raw_path == "/%D1%88%D0%BB%D1%8F%D1%85"
    assert url.parts == ("/", "шлях")
    assert url.raw_parts == ("/", "%D1%88%D0%BB%D1%8F%D1%85")


def test_with_name_percent_encoded() -> None:
    url = URL("http://example.com/path")
    url2 = url.with_name("%cf%80")
    assert url2.raw_parts == ("/", "%25cf%2580")
    assert url2.parts == ("/", "%cf%80")
    assert url2.raw_path == "/%25cf%2580"
    assert url2.path == "/%cf%80"


def test_with_name_with_slash() -> None:
    with pytest.raises(ValueError):
        URL("http://example.com").with_name("a/b")


def test_with_name_non_str() -> None:
    with pytest.raises(TypeError):
        URL("http://example.com").with_name(123)  # type: ignore[arg-type]


def test_with_name_within_colon_and_at() -> None:
    url = URL("http://example.com/oldpath").with_name("path:abc@123")
    assert url.raw_path == "/path:abc@123"


def test_with_name_dot() -> None:
    with pytest.raises(ValueError):
        URL("http://example.com").with_name(".")


def test_with_name_double_dot() -> None:
    with pytest.raises(ValueError):
        URL("http://example.com").with_name("..")


# with_suffix


def test_with_suffix() -> None:
    url = URL("http://example.com/a/b")
    assert url.raw_parts == ("/", "a", "b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("/", "a", "b.c")
    assert url2.parts == ("/", "a", "b.c")
    assert url2.raw_path == "/a/b.c"
    assert url2.path == "/a/b.c"


def test_with_suffix_encoded_suffix() -> None:
    url = URL("http://example.com/a/b")
    url2 = url.with_suffix(". c")
    assert url2.raw_parts == ("/", "a", "b.%20c")
    assert url2.parts == ("/", "a", "b. c")
    assert url2.raw_path == "/a/b.%20c"
    assert url2.path == "/a/b. c"


def test_with_suffix_encoded_url() -> None:
    url = URL("http://example.com/a/b c")
    url2 = url.with_suffix(". d")
    url3 = url.with_suffix(".e")
    assert url2.raw_parts == ("/", "a", "b%20c.%20d")
    assert url2.parts == ("/", "a", "b c. d")
    assert url2.raw_path == "/a/b%20c.%20d"
    assert url2.path == "/a/b c. d"

    assert url3.raw_parts == ("/", "a", "b%20c.e")
    assert url3.parts == ("/", "a", "b c.e")
    assert url3.raw_path == "/a/b%20c.e"
    assert url3.path == "/a/b c.e"


@pytest.mark.parametrize(
    ("original_url", "keep_query", "keep_fragment", "expected_url"),
    [
        pytest.param(
            "http://example.com/path/to.txt?a=b#frag",
            True,
            False,
            "http://example.com/path/to.md?a=b",
            id="query-only",
        ),
        pytest.param(
            "http://example.com/path/to.txt?a=b#frag",
            False,
            True,
            "http://example.com/path/to.md#frag",
            id="fragment-only",
        ),
        pytest.param(
            "http://example.com/path/to.txt?a=b#frag",
            True,
            True,
            "http://example.com/path/to.md?a=b#frag",
            id="all",
        ),
        pytest.param(
            "http://example.com/path/to.txt?a=b#frag",
            False,
            False,
            "http://example.com/path/to.md",
            id="none",
        ),
    ],
)
def test_with_suffix_keep_query_keep_fragment_flags(
    original_url: str, keep_query: bool, keep_fragment: bool, expected_url: str
) -> None:
    url = URL(original_url)
    url2 = url.with_suffix(".md", keep_query=keep_query, keep_fragment=keep_fragment)
    assert str(url2) == expected_url


def test_with_suffix_for_naked_path() -> None:
    url = URL("http://example.com")
    with pytest.raises(ValueError) as excinfo:
        url.with_suffix(".a")
    (msg,) = excinfo.value.args
    assert msg == f"{url!r} has an empty name"


def test_with_suffix_for_relative_path() -> None:
    url = URL("a")
    url2 = url.with_suffix(".b")
    assert url2.raw_parts == ("a.b",)


def test_with_suffix_for_relative_path2() -> None:
    url = URL("a/b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("a", "b.c")


def test_with_suffix_for_relative_path_starting_from_slash() -> None:
    url = URL("/a")
    url2 = url.with_suffix(".b")
    assert url2.raw_parts == ("/", "a.b")


def test_with_suffix_for_relative_path_starting_from_slash2() -> None:
    url = URL("/a/b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("/", "a", "b.c")


def test_with_suffix_empty() -> None:
    url = URL("http://example.com/path/to").with_suffix("")
    assert str(url) == "http://example.com/path/to"


def test_with_suffix_non_ascii() -> None:
    url = URL("http://example.com/path").with_suffix(".шлях")
    assert url.path == "/path.шлях"
    assert url.raw_path == "/path.%D1%88%D0%BB%D1%8F%D1%85"
    assert url.parts == ("/", "path.шлях")
    assert url.raw_parts == ("/", "path.%D1%88%D0%BB%D1%8F%D1%85")


def test_with_suffix_percent_encoded() -> None:
    url = URL("http://example.com/path")
    url2 = url.with_suffix(".%cf%80")
    assert url2.raw_parts == ("/", "path.%25cf%2580")
    assert url2.parts == ("/", "path.%cf%80")
    assert url2.raw_path == "/path.%25cf%2580"
    assert url2.path == "/path.%cf%80"


def test_with_suffix_without_dot() -> None:
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com/a").with_suffix("b")
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix 'b'"


def test_with_suffix_non_str() -> None:
    with pytest.raises(TypeError) as excinfo:
        URL("http://example.com").with_suffix(123)  # type: ignore[arg-type]
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix type"


def test_with_suffix_dot() -> None:
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com").with_suffix(".")
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix '.'"


def test_with_suffix_with_slash() -> None:
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com/a").with_suffix("/.b")
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix '/.b'"


def test_with_suffix_with_slash2() -> None:
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com/a").with_suffix(".b/.d")
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix '.b/.d'"


def test_with_suffix_replace() -> None:
    url = URL("/a.b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("/", "a.c")


# is_absolute


def test_is_absolute_for_relative_url() -> None:
    url = URL("/path/to")
    assert not url.is_absolute()
    assert not url.absolute


def test_is_absolute_for_absolute_url() -> None:
    url = URL("http://example.com")
    assert url.is_absolute()
    assert url.absolute


def test_is_non_absolute_for_empty_url() -> None:
    url = URL()
    assert not url.is_absolute()
    assert not url.absolute


def test_is_non_absolute_for_empty_url2() -> None:
    url = URL("")
    assert not url.is_absolute()
    assert not url.absolute


def test_is_absolute_path_starting_from_double_slash() -> None:
    url = URL("//www.python.org")
    assert url.is_absolute()
    assert url.absolute


# is_default_port


def test_is_default_port_for_relative_url() -> None:
    url = URL("/path/to")
    assert not url.is_default_port()


def test_is_default_port_for_absolute_url_without_port() -> None:
    url = URL("http://example.com")
    assert url.is_default_port()


def test_is_default_port_for_absolute_url_with_default_port() -> None:
    url = URL("http://example.com:80")
    assert url.is_default_port()
    assert str(url) == "http://example.com"


def test_is_default_port_for_absolute_url_with_nondefault_port() -> None:
    url = URL("http://example.com:8080")
    assert not url.is_default_port()


def test_is_default_port_for_unknown_scheme() -> None:
    url = URL("unknown://example.com:8080")
    assert not url.is_default_port()


def test_handling_port_zero() -> None:
    url = URL("http://example.com:0")
    assert url.explicit_port == 0
    assert url.explicit_port == SplitResult(*url._val).port
    assert str(url) == "http://example.com:0"
    assert not url.is_default_port()


#


def test_no_scheme() -> None:
    url = URL("example.com")
    assert url.raw_host is None
    assert url.raw_path == "example.com"
    assert str(url) == "example.com"


def test_no_scheme2() -> None:
    url = URL("example.com/a/b")
    assert url.raw_host is None
    assert url.raw_path == "example.com/a/b"
    assert str(url) == "example.com/a/b"


def test_from_non_allowed() -> None:
    with pytest.raises(TypeError):
        URL(1234)  # type: ignore[arg-type]


def test_from_idna() -> None:
    url = URL("http://xn--jxagkqfkduily1i.eu")
    assert "http://xn--jxagkqfkduily1i.eu" == str(url)
    url = URL("http://xn--einla-pqa.de/")  # needs idna 2008
    assert "http://xn--einla-pqa.de/" == str(url)


def test_to_idna() -> None:
    url = URL("http://εμπορικόσήμα.eu")
    assert "http://xn--jxagkqfkduily1i.eu" == str(url)
    url = URL("http://einlaß.de/")
    assert "http://xn--einla-pqa.de/" == str(url)


def test_from_ascii_login() -> None:
    url = URL("http://%D0%B2%D0%B0%D1%81%D1%8F@host:1234/")
    assert ("http://%D0%B2%D0%B0%D1%81%D1%8F@host:1234/") == str(url)


def test_from_non_ascii_login() -> None:
    url = URL("http://бажан@host:1234/")
    assert ("http://%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD@host:1234/") == str(url)


def test_from_ascii_login_and_password() -> None:
    url = URL(
        "http://"
        "%D0%B2%D0%B0%D1%81%D1%8F"
        ":%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C"
        "@host:1234/"
    )
    assert (
        "http://"
        "%D0%B2%D0%B0%D1%81%D1%8F"
        ":%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C"
        "@host:1234/"
    ) == str(url)


def test_from_non_ascii_login_and_password() -> None:
    url = URL("http://бажан:пароль@host:1234/")
    assert (
        "http://"
        "%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD"
        ":%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C"
        "@host:1234/"
    ) == str(url)


def test_from_ascii_path() -> None:
    url = URL("http://example.com/%D0%BF%D1%83%D1%82%D1%8C/%D1%82%D1%83%D0%B4%D0%B0")
    assert (
        "http://example.com/%D0%BF%D1%83%D1%82%D1%8C/%D1%82%D1%83%D0%B4%D0%B0"
    ) == str(url)


def test_from_ascii_path_lower_case() -> None:
    url = URL("http://example.com/%d0%bf%d1%83%d1%82%d1%8c/%d1%82%d1%83%d0%b4%d0%b0")
    assert (
        "http://example.com/%D0%BF%D1%83%D1%82%D1%8C/%D1%82%D1%83%D0%B4%D0%B0"
    ) == str(url)


def test_from_non_ascii_path() -> None:
    url = URL("http://example.com/шлях/туди")
    assert (
        "http://example.com/%D1%88%D0%BB%D1%8F%D1%85/%D1%82%D1%83%D0%B4%D0%B8"
    ) == str(url)


def test_bytes() -> None:
    url = URL("http://example.com/шлях/туди")
    assert (
        b"http://example.com/%D1%88%D0%BB%D1%8F%D1%85/%D1%82%D1%83%D0%B4%D0%B8"
        == bytes(url)
    )


def test_from_ascii_query_parts() -> None:
    url = URL(
        "http://example.com/?%D0%BF%D0%B0%D1%80%D0%B0%D0%BC=%D0%B7%D0%BD%D0%B0%D1%87"
    )
    assert (
        "http://example.com/?%D0%BF%D0%B0%D1%80%D0%B0%D0%BC=%D0%B7%D0%BD%D0%B0%D1%87"
    ) == str(url)


def test_from_non_ascii_query_parts() -> None:
    url = URL("http://example.com/?парам=знач")
    assert (
        "http://example.com/?%D0%BF%D0%B0%D1%80%D0%B0%D0%BC=%D0%B7%D0%BD%D0%B0%D1%87"
    ) == str(url)


def test_from_non_ascii_query_parts2() -> None:
    url = URL("http://example.com/?п=з&ю=б")
    assert "http://example.com/?%D0%BF=%D0%B7&%D1%8E=%D0%B1" == str(url)


def test_from_ascii_fragment() -> None:
    url = URL("http://example.com/#%D1%84%D1%80%D0%B0%D0%B3%D0%BC%D0%B5%D0%BD%D1%82")
    assert (
        "http://example.com/#%D1%84%D1%80%D0%B0%D0%B3%D0%BC%D0%B5%D0%BD%D1%82"
    ) == str(url)


def test_from_bytes_with_non_ascii_fragment() -> None:
    url = URL("http://example.com/#фрагмент")
    assert (
        "http://example.com/#%D1%84%D1%80%D0%B0%D0%B3%D0%BC%D0%B5%D0%BD%D1%82"
    ) == str(url)


def test_to_str() -> None:
    url = URL("http://εμπορικόσήμα.eu/")
    assert "http://xn--jxagkqfkduily1i.eu/" == str(url)


def test_to_str_long() -> None:
    url = URL(
        "https://host-12345678901234567890123456789012345678901234567890-name:8888/"
    )
    expected = (
        "https://host-12345678901234567890123456789012345678901234567890-name:8888/"
    )
    assert expected == str(url)


def test_decoding_with_2F_in_path() -> None:
    url = URL("http://example.com/path%2Fto")
    assert "http://example.com/path%2Fto" == str(url)
    assert url == URL(str(url))


def test_decoding_with_26_and_3D_in_query() -> None:
    url = URL("http://example.com/?%26=%3D")
    assert "http://example.com/?%26=%3D" == str(url)
    assert url == URL(str(url))


def test_fragment_only_url() -> None:
    url = URL("#frag")
    assert str(url) == "#frag"


def test_url_from_url() -> None:
    url = URL("http://example.com")
    assert URL(url) == url
    assert URL(url).raw_parts == ("/",)


def test_lowercase_scheme() -> None:
    url = URL("HTTP://example.com")
    assert str(url) == "http://example.com"


def test_str_for_empty_url() -> None:
    url = URL()
    assert "" == str(url)


def test_parent_for_empty_url() -> None:
    url = URL()
    assert url == url.parent


def test_parent_for_relative_url_with_child() -> None:
    url = URL("path/to")
    assert url.parent == URL("path")
    assert SplitResult(*url.parent._val).path == "path"


def test_parent_for_relative_url() -> None:
    url = URL("path")
    assert url.parent == URL("")
    assert SplitResult(*url.parent._val).path == ""


def test_parent_for_no_netloc_url() -> None:
    url = URL("/path/to")
    assert url.parent == URL("/path")


def test_parent_for_top_level_no_netloc_url() -> None:
    url = URL("/")
    assert url.parent == URL("/")
    assert SplitResult(*url.parent._val).path == "/"


def test_parent_for_absolute_url() -> None:
    url = URL("http://go.to/path/to")
    assert url.parent == URL("http://go.to/path")


def test_parent_for_top_level_absolute_url() -> None:
    url = URL("http://go.to/")
    assert url.parent == URL("http://go.to/")
    assert SplitResult(*url.parent._val).path == "/"


def test_empty_value_for_query() -> None:
    url = URL("http://example.com/path").with_query({"a": ""})
    assert str(url) == "http://example.com/path?a="


def test_none_value_for_query() -> None:
    with pytest.raises(TypeError):
        URL("http://example.com/path").with_query({"a": None})  # type: ignore[dict-item]


def test_decode_pct_in_path() -> None:
    url = URL("http://www.python.org/%7Eguido")
    assert "http://www.python.org/~guido" == str(url)


def test_decode_pct_in_path_lower_case() -> None:
    url = URL("http://www.python.org/%7eguido")
    assert "http://www.python.org/~guido" == str(url)


# join


def test_join() -> None:
    base = URL("http://www.cwi.nl/%7Eguido/Python.html")
    url = URL("FAQ.html")
    url2 = base.join(url)
    assert str(url2) == "http://www.cwi.nl/~guido/FAQ.html"


def test_join_absolute() -> None:
    base = URL("http://www.cwi.nl/%7Eguido/Python.html")
    url = URL("//www.python.org/%7Eguido")
    url2 = base.join(url)
    assert str(url2) == "http://www.python.org/~guido"


def test_join_non_url() -> None:
    base = URL("http://example.com")
    with pytest.raises(TypeError):
        base.join("path/to")  # type: ignore[arg-type]


NORMAL = [
    ("g:h", "g:h"),
    ("g", "http://a/b/c/g"),
    ("./g", "http://a/b/c/g"),
    ("g/", "http://a/b/c/g/"),
    ("/g", "http://a/g"),
    ("//g", "http://g"),
    ("?y", "http://a/b/c/d;p?y"),
    ("g?y", "http://a/b/c/g?y"),
    ("#s", "http://a/b/c/d;p?q#s"),
    ("g#s", "http://a/b/c/g#s"),
    ("g?y#s", "http://a/b/c/g?y#s"),
    (";x", "http://a/b/c/;x"),
    ("g;x", "http://a/b/c/g;x"),
    ("g;x?y#s", "http://a/b/c/g;x?y#s"),
    ("", "http://a/b/c/d;p?q"),
    (".", "http://a/b/c/"),
    ("./", "http://a/b/c/"),
    ("..", "http://a/b/"),
    ("../", "http://a/b/"),
    ("../g", "http://a/b/g"),
    ("../..", "http://a/"),
    ("../../", "http://a/"),
    ("../../g", "http://a/g"),
]


@pytest.mark.parametrize("url,expected", NORMAL)
def test_join_from_rfc_3986_normal(url: str, expected: str) -> None:
    # test case from https://tools.ietf.org/html/rfc3986.html#section-5.4
    base = URL("http://a/b/c/d;p?q")
    url_obj = URL(url)
    expected_obj = URL(expected)
    assert base.join(url_obj) == expected_obj


ABNORMAL = [
    ("../../../g", "http://a/g"),
    ("../../../../g", "http://a/g"),
    ("/./g", "http://a/g"),
    ("/../g", "http://a/g"),
    ("g.", "http://a/b/c/g."),
    (".g", "http://a/b/c/.g"),
    ("g..", "http://a/b/c/g.."),
    ("..g", "http://a/b/c/..g"),
    ("./../g", "http://a/b/g"),
    ("./g/.", "http://a/b/c/g/"),
    ("g/./h", "http://a/b/c/g/h"),
    ("g/../h", "http://a/b/c/h"),
    ("g;x=1/./y", "http://a/b/c/g;x=1/y"),
    ("g;x=1/../y", "http://a/b/c/y"),
    ("g?y/./x", "http://a/b/c/g?y/./x"),
    ("g?y/../x", "http://a/b/c/g?y/../x"),
    ("g#s/./x", "http://a/b/c/g#s/./x"),
    ("g#s/../x", "http://a/b/c/g#s/../x"),
]


@pytest.mark.parametrize("url,expected", ABNORMAL)
def test_join_from_rfc_3986_abnormal(url: str, expected: str) -> None:
    # test case from https://tools.ietf.org/html/rfc3986.html#section-5.4.2
    base = URL("http://a/b/c/d;p?q")
    url_obj = URL(url)
    expected_obj = URL(expected)
    assert base.join(url_obj) == expected_obj


EMPTY_SEGMENTS = [
    (
        "https://web.archive.org/web/",
        "./https://github.com/aio-libs/yarl",
        "https://web.archive.org/web/https://github.com/aio-libs/yarl",
    ),
    (
        "https://web.archive.org/web/https://github.com/",
        "aio-libs/yarl",
        "https://web.archive.org/web/https://github.com/aio-libs/yarl",
    ),
]


@pytest.mark.parametrize("base,url,expected", EMPTY_SEGMENTS)
def test_join_empty_segments(base: str, url: str, expected: str) -> None:
    base_obj = URL(base)
    url_obj = URL(url)
    expected_obj = URL(expected)
    joined = base_obj.join(url_obj)
    assert joined == expected_obj


SIMPLE_BASE = "http://a/b/c/d"
URLLIB_URLJOIN = [
    ("", "http://a/b/c/g?y/./x", "http://a/b/c/g?y/./x"),
    ("", "http://a/./g", "http://a/./g"),
    ("svn://pathtorepo/dir1", "dir2", "svn://pathtorepo/dir2"),
    ("svn+ssh://pathtorepo/dir1", "dir2", "svn+ssh://pathtorepo/dir2"),
    ("ws://a/b", "g", "ws://a/g"),
    ("wss://a/b", "g", "wss://a/g"),
    # test for issue22118 duplicate slashes
    (SIMPLE_BASE + "/", "foo", SIMPLE_BASE + "/foo"),
    # Non-RFC-defined tests, covering variations of base and trailing
    # slashes
    ("http://a/b/c/d/e/", "../../f/g/", "http://a/b/c/f/g/"),
    ("http://a/b/c/d/e", "../../f/g/", "http://a/b/f/g/"),
    ("http://a/b/c/d/e/", "/../../f/g/", "http://a/f/g/"),
    ("http://a/b/c/d/e", "/../../f/g/", "http://a/f/g/"),
    ("http://a/b/c/d/e/", "../../f/g", "http://a/b/c/f/g"),
    ("http://a/b/", "../../f/g/", "http://a/f/g/"),
    ("a", "b", "b"),
    ("http:///", "..", "http:///"),
    ("a/", "b", "a/b"),
    ("a/b", "c", "a/c"),
    ("a/b/", "c", "a/b/c"),
    (
        "https://x.org/",
        "/?text=Hello+G%C3%BCnter",
        "https://x.org/?text=Hello+G%C3%BCnter",
    ),
    (
        "https://x.org/",
        "?text=Hello+G%C3%BCnter",
        "https://x.org/?text=Hello+G%C3%BCnter",
    ),
    ("http://example.com", "http://example.com", "http://example.com"),
    ("http://x.org", "https://x.org#fragment", "https://x.org#fragment"),
]


@pytest.mark.parametrize("base,url,expected", URLLIB_URLJOIN)
def test_join_cpython_urljoin(base: str, url: str, expected: str) -> None:
    # tests from cpython urljoin
    base_obj = URL(base)
    url_obj = URL(url)
    expected_obj = URL(expected)
    joined = base_obj.join(url_obj)
    assert joined == expected_obj


def test_join_preserves_leading_slash() -> None:
    """Test that join preserves leading slash in path."""
    base = URL.build(scheme="https", host="localhost", port=443)
    new = base.join(URL("") / "_msearch")
    assert str(new) == "https://localhost/_msearch"
    assert new.path == "/_msearch"


def test_empty_authority() -> None:
    assert URL("http:///").authority == ""


def test_split_result_non_decoded() -> None:
    with pytest.raises(ValueError):
        URL(SplitResult("http", "example.com", "path", "qs", "frag"))


def test_split_result_encoded() -> None:
    url = URL(SplitResult("http", "example.com", "path", "qs", "frag"), encoded=True)
    assert str(url) == "http://example.com/path?qs#frag"


def test_str_encoded() -> None:
    url = URL("http://example.com/path?qs#frag%2F%2D", encoded=True)
    assert str(url) == "http://example.com/path?qs#frag%2F%2D"


def test_subclassed_str_encoded() -> None:
    class S(str):
        """Subclass of str."""

    url = URL(S("http://example.com/path?qs#frag%2F%2D"), encoded=True)
    assert str(url) == "http://example.com/path?qs#frag%2F%2D"


def test_human_repr() -> None:
    url = URL("http://бажан:пароль@хост.домен:8080/шлях/сюди?арг=вал#фраг")
    s = url.human_repr()
    assert URL(s) == url
    assert s == "http://бажан:пароль@хост.домен:8080/шлях/сюди?арг=вал#фраг"


def test_human_repr_defaults() -> None:
    url = URL("шлях")
    s = url.human_repr()
    assert s == "шлях"


def test_human_repr_default_port() -> None:
    url = URL("http://бажан:пароль@хост.домен/шлях/сюди?арг=вал#фраг")
    s = url.human_repr()
    assert URL(s) == url
    assert s == "http://бажан:пароль@хост.домен/шлях/сюди?арг=вал#фраг"


def test_human_repr_ipv6() -> None:
    url = URL("http://[::1]:8080/path")
    s = url.human_repr()
    url2 = URL(s)
    assert url2 == url
    assert url2.host == "::1"
    assert s == "http://[::1]:8080/path"


def test_human_repr_delimiters() -> None:
    url = URL.build(
        scheme="http",
        user=" !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
        password=" !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
        host="хост.домен",
        port=8080,
        path="/ !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
        query={
            " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~": " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
        },
        fragment=" !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
    )
    s = url.human_repr()
    assert URL(s) == url
    assert (
        s == "http:// !\"%23$%25&'()*+,-.%2F%3A;<=>%3F%40%5B%5C%5D^_`{|}~"
        ": !\"%23$%25&'()*+,-.%2F%3A;<=>%3F%40%5B%5C%5D^_`{|}~"
        "@хост.домен:8080"
        "/ !\"%23$%25&'()*+,-./:;<=>%3F@[\\]^_`{|}~"
        "? !\"%23$%25%26'()*%2B,-./:%3B<%3D>?@[\\]^_`{|}~"
        "= !\"%23$%25%26'()*%2B,-./:%3B<%3D>?@[\\]^_`{|}~"
        "# !\"#$%25&'()*+,-./:;<=>?@[\\]^_`{|}~"
    )


def test_human_repr_non_printable() -> None:
    url = URL.build(
        scheme="http",
        user="бажан\n\xad\u200b",
        password="пароль\n\xad\u200b",
        host="хост.домен",
        port=8080,
        path="/шлях\n\xad\u200b",
        query={"арг\n\xad\u200b": "вал\n\xad\u200b"},
        fragment="фраг\n\xad\u200b",
    )
    s = url.human_repr()
    assert URL(s) == url
    assert (
        s == "http://бажан%0A%C2%AD%E2%80%8B:пароль%0A%C2%AD%E2%80%8B"
        "@хост.домен:8080"
        "/шлях%0A%C2%AD%E2%80%8B"
        "?арг%0A%C2%AD%E2%80%8B=вал%0A%C2%AD%E2%80%8B"
        "#фраг%0A%C2%AD%E2%80%8B"
    )


# relative


def test_relative() -> None:
    url = URL("http://user:pass@example.com:8080/path?a=b#frag")
    rel = url.relative()
    assert str(rel) == "/path?a=b#frag"


def test_relative_is_relative() -> None:
    url = URL("http://user:pass@example.com:8080/path?a=b#frag")
    rel = url.relative()
    assert not rel.is_absolute()
    assert not rel.absolute


def test_relative_abs_parts_are_removed() -> None:
    url = URL("http://user:pass@example.com:8080/path?a=b#frag")
    rel = url.relative()
    assert not rel.scheme
    assert not rel.user
    assert not rel.password
    assert not rel.host
    assert not rel.port


def test_relative_fails_on_rel_url() -> None:
    with pytest.raises(ValueError):
        URL("/path?a=b#frag").relative()


def test_slash_and_question_in_query() -> None:
    u = URL("http://example.com/path?http://example.com/p?a#b")
    assert u.query_string == "http://example.com/p?a"


def test_slash_and_question_in_fragment() -> None:
    u = URL("http://example.com/path#http://example.com/p?a")
    assert u.fragment == "http://example.com/p?a"


def test_requoting() -> None:
    u = URL("http://127.0.0.1/?next=http%3A//example.com/")
    assert u.raw_query_string == "next=http://example.com/"
    assert str(u) == "http://127.0.0.1/?next=http://example.com/"


def test_join_query_string() -> None:
    """Test that query strings are correctly joined."""
    original = URL("http://127.0.0.1:62869")
    path_url = URL(
        "/api?start=2022-03-27T14:05:00%2B03:00&end=2022-03-27T16:05:00%2B03:00"
    )
    assert path_url.query.get("start") == "2022-03-27T14:05:00+03:00"
    assert path_url.query.get("end") == "2022-03-27T16:05:00+03:00"
    new = original.join(path_url)
    assert new.query.get("start") == "2022-03-27T14:05:00+03:00"
    assert new.query.get("end") == "2022-03-27T16:05:00+03:00"


def test_join_query_string_with_special_chars() -> None:
    """Test url joining when the query string has non-ascii params."""
    original = URL("http://127.0.0.1")
    path_url = URL("/api?text=%D1%82%D0%B5%D0%BA%D1%81%D1%82")
    assert path_url.query.get("text") == "текст"
    new = original.join(path_url)
    assert new.query.get("text") == "текст"


def test_join_encoded_url() -> None:
    """Test that url encoded urls are correctly joined."""
    original = URL("http://127.0.0.1:62869")
    path_url = URL("/api/%34")
    assert original.path == "/"
    assert path_url.path == "/api/4"
    new = original.join(path_url)
    assert new.path == "/api/4"


# cache


def test_parsing_populates_cache() -> None:
    """Test that parsing a URL populates the cache."""
    url = URL("http://user:password@example.com:80/path?a=b#frag")
    assert url._cache["raw_user"] == "user"
    assert url._cache["raw_password"] == "password"
    assert url._cache["raw_host"] == "example.com"
    assert url._cache["explicit_port"] == 80
    assert url._cache["raw_query_string"] == "a=b"
    assert url._cache["raw_fragment"] == "frag"
    assert url._cache["scheme"] == "http"
    assert url._cache["raw_path"] == "/path"
    assert url.raw_user == "user"
    assert url.raw_password == "password"
    assert url.raw_host == "example.com"
    assert url.explicit_port == 80
    assert url.raw_query_string == "a=b"
    assert url.raw_fragment == "frag"
    assert url.scheme == "http"
    url._cache.clear()  # type: ignore[attr-defined]
    assert url.raw_user == "user"
    assert url.raw_password == "password"
    assert url.raw_host == "example.com"
    assert url.explicit_port == 80
    assert url.raw_query_string == "a=b"
    assert url.raw_fragment == "frag"
    assert url.scheme == "http"
    assert url.raw_path == "/path"
    assert url._cache["raw_user"] == "user"
    assert url._cache["raw_password"] == "password"
    assert url._cache["raw_host"] == "example.com"
    assert url._cache["explicit_port"] == 80
    assert url._cache["raw_query_string"] == "a=b"
    assert url._cache["raw_fragment"] == "frag"
    assert url._cache["scheme"] == "http"
    assert url._cache["raw_path"] == "/path"


def test_relative_url_populates_cache() -> None:
    """Test that parsing a relative URL populates the cache."""
    url = URL(".")
    assert url._cache["raw_query_string"] == ""
    assert url._cache["raw_fragment"] == ""
    assert url._cache["scheme"] == ""
    assert url._cache["raw_path"] == "."


def test_parsing_populates_cache_for_single_dot() -> None:
    """Test that parsing a URL populates the cache for a single dot path."""
    url = URL("http://example.com/.")
    # raw_path should be normalized to "/"
    assert url._cache["raw_path"] == "/"
    assert url._cache["raw_host"] == "example.com"
    assert url._cache["scheme"] == "http"
    assert url.raw_path == "/"


@pytest.mark.parametrize(
    ("host", "is_authority"),
    [
        *(("other_gen_delim_" + c, False) for c in "[]"),
    ],
)
def test_build_with_invalid_ipv6_host(host: str, is_authority: bool) -> None:
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        URL(f"http://{host}/")


@pytest.mark.parametrize(
    "url",
    [
        r"http://vulndetector.com\admin\api",
        r"http://example.com\path",
        r"http://example.com\../",
        r"http://\example.com",
        r"http://example.com\foo@evil.com/",
        r"http://user:pass@example.com\path",
        r"http://example.com:80\foo/",
        r"http://[::1]\path",
    ],
)
def test_url_with_backslash_in_netloc(url: str) -> None:
    with pytest.raises(
        ValueError, match=r"backslash \('\\'\) is not allowed in the authority"
    ):
        URL(url)


def test_url_with_backslash_in_path_after_ipv6_host() -> None:
    url = URL(r"http://[::1]/\path")

    assert str(url) == r"http://[::1]/%5Cpath"
    assert url.host == "::1"
    assert url.path == r"/\path"


@pytest.mark.parametrize("byte", ["\r", "\n", "\t"])
def test_unsafe_url_bytes_are_removed(byte: str) -> None:
    url = URL(f"http://example.com{byte}/")
    assert str(url) == "http://example.com/"


@pytest.mark.parametrize("byte", tuple(_WHATWG_C0_CONTROL_OR_SPACE))
def test_control_chars_are_removed(byte: str) -> None:
    url = URL(f"{byte}http://example.com/")
    assert str(url) == "http://example.com/"


@pytest.mark.parametrize(
    "disallowed_unicode", [_VERTICAL_COLON, _FULL_WITH_NUMBER_SIGN, _ACCOUNT_OF]
)
def test_url_with_invalid_unicode(disallowed_unicode: str) -> None:
    with pytest.raises(
        ValueError, match="contains invalid characters under NFKC normalization"
    ):
        URL(f"http://example.com{disallowed_unicode}80/")
    with pytest.raises(
        ValueError, match="contains invalid characters under NFKC normalization"
    ):
        URL(f"http://example.{disallowed_unicode}.com/frag")


@pytest.mark.parametrize(
    "percent_char",
    [_FULLWIDTH_PERCENT, _SMALL_PERCENT],
    ids=["fullwidth-percent-U+FF05", "small-percent-U+FE6A"],
)
def test_url_with_fullwidth_percent_rejected(percent_char: str) -> None:
    """NFKC normalization of fullwidth/small percent signs must be caught."""
    with pytest.raises(
        ValueError, match="contains invalid characters under NFKC normalization"
    ):
        URL(f"http://evil.com{percent_char}2e.internal/")


@pytest.mark.parametrize(
    "ignorable",
    [
        "\u00ad",  # SOFT HYPHEN
        "\u200b",  # ZERO WIDTH SPACE
        "\u2060",  # WORD JOINER
        "\u180e",  # MONGOLIAN VOWEL SEPARATOR
        "\u1806",  # MONGOLIAN TODO SOFT HYPHEN (nameprep fallback strips it)
        "\ufeff",  # ZERO WIDTH NO-BREAK SPACE
        "\U000e0001",  # LANGUAGE TAG
    ],
    ids=[
        "soft-hyphen",
        "zwsp",
        "word-joiner",
        "mvs",
        "todo-soft-hyphen",
        "bom",
        "language-tag",
    ],
)
def test_url_host_with_default_ignorable_rejected(ignorable: str) -> None:
    """Default-ignorable code points are stripped by IDNA, so reject them.

    Otherwise ``URL("http://e<ZWSP>vil.com").host`` would collapse to
    ``evil.com``, a different host than the string the caller validated.
    """
    with pytest.raises(ValueError, match="cannot contain"):
        URL(f"http://e{ignorable}vil.com/")


def _idna_collapses(code_point: int) -> bool:
    """True if IDNA encoding silently deletes ``code_point`` from a host."""
    try:
        return _idna_encode(f"ab{chr(code_point)}cd.com") == "abcd.com"
    except UnicodeError:
        return False


@pytest.mark.skipif(
    sys.platform != "linux" or platform.machine() != "x86_64",
    reason=(
        "Exhaustive IDNA sweep iterates ~140k code points; its result does not "
        "depend on the architecture, so it only runs on a native Linux x86_64 "
        "runner. It is too heavy for QEMU-emulated wheel-build arches, where it "
        "crashes the test workers."
    ),
)
def test_default_ignorable_covers_idna_stripped() -> None:
    """_DEFAULT_IGNORABLE_RE must match every code point IDNA silently deletes.

    This pins the hand-written character class to the installed ``idna`` and
    Unicode data: if a future version starts deleting a code point the regex
    does not cover, host confusion returns and this test fails loudly. The
    ranges cover every plane that holds such code points (the highest is
    ``U+E0FFF``); the empty planes in between hold none.
    """
    stripped = {
        cp
        for lo, hi in ((0x00A0, 0x20000), (0xE0000, 0xE1000))
        for cp in range(lo, hi)
        if not 0xD800 <= cp <= 0xDFFF and _idna_collapses(cp)
    }
    assert stripped, "expected IDNA to strip at least the known default-ignorables"
    uncovered = sorted(
        hex(cp) for cp in stripped if not _DEFAULT_IGNORABLE_RE.search(chr(cp))
    )
    assert not uncovered, f"IDNA strips these but the regex misses them: {uncovered}"
