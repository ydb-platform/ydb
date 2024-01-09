from enum import Enum
from urllib.parse import SplitResult

import pytest

from yarl import URL


def test_inheritance():
    with pytest.raises(TypeError) as ctx:

        class MyURL(URL):  # type: ignore[misc]
            pass

    assert (
        "Inheriting a class "
        "<class '__tests__.test_url.test_inheritance.<locals>.MyURL'> "
        "from URL is forbidden" == str(ctx.value)
    )


def test_str_subclass():
    class S(str):
        pass

    assert str(URL(S("http://example.com"))) == "http://example.com"


def test_is():
    u1 = URL("http://example.com")
    u2 = URL(u1)
    assert u1 is u2


def test_bool():
    assert URL("http://example.com")
    assert not URL()
    assert not URL("")


def test_absolute_url_without_host():
    with pytest.raises(ValueError):
        URL("http://:8080/")


def test_url_is_not_str():
    url = URL("http://example.com")
    assert not isinstance(url, str)


def test_str():
    url = URL("http://example.com:8888/path/to?a=1&b=2")
    assert str(url) == "http://example.com:8888/path/to?a=1&b=2"


def test_repr():
    url = URL("http://example.com")
    assert "URL('http://example.com')" == repr(url)


def test_origin():
    url = URL("http://user:password@example.com:8888/path/to?a=1&b=2")
    assert URL("http://example.com:8888") == url.origin()


def test_origin_nonascii():
    url = URL("http://user:password@оун-упа.укр:8888/path/to?a=1&b=2")
    assert str(url.origin()) == "http://xn----8sb1bdhvc.xn--j1amh:8888"


def test_origin_ipv6():
    url = URL("http://user:password@[::1]:8888/path/to?a=1&b=2")
    assert str(url.origin()) == "http://[::1]:8888"


def test_origin_not_absolute_url():
    url = URL("/path/to?a=1&b=2")
    with pytest.raises(ValueError):
        url.origin()


def test_origin_no_scheme():
    url = URL("//user:password@example.com:8888/path/to?a=1&b=2")
    with pytest.raises(ValueError):
        url.origin()


def test_drop_dots():
    u = URL("http://example.com/path/../to")
    assert str(u) == "http://example.com/to"


def test_abs_cmp():
    assert URL("http://example.com:8888") == URL("http://example.com:8888")
    assert URL("http://example.com:8888/") == URL("http://example.com:8888/")
    assert URL("http://example.com:8888/") == URL("http://example.com:8888")
    assert URL("http://example.com:8888") == URL("http://example.com:8888/")


def test_abs_hash():
    url = URL("http://example.com:8888")
    url_trailing = URL("http://example.com:8888/")
    assert hash(url) == hash(url_trailing)


# properties


def test_scheme():
    url = URL("http://example.com")
    assert "http" == url.scheme


def test_raw_user():
    url = URL("http://user@example.com")
    assert "user" == url.raw_user


def test_raw_user_non_ascii():
    url = URL("http://бажан@example.com")
    assert "%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD" == url.raw_user


def test_no_user():
    url = URL("http://example.com")
    assert url.user is None


def test_user_non_ascii():
    url = URL("http://бажан@example.com")
    assert "бажан" == url.user


def test_raw_password():
    url = URL("http://user:password@example.com")
    assert "password" == url.raw_password


def test_raw_password_non_ascii():
    url = URL("http://user:пароль@example.com")
    assert "%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C" == url.raw_password


def test_password_non_ascii():
    url = URL("http://user:пароль@example.com")
    assert "пароль" == url.password


def test_password_without_user():
    url = URL("http://:password@example.com")
    assert url.user is None
    assert "password" == url.password


def test_user_empty_password():
    url = URL("http://user:@example.com")
    assert "user" == url.user
    assert "" == url.password


def test_raw_host():
    url = URL("http://example.com")
    assert "example.com" == url.raw_host


def test_raw_host_non_ascii():
    url = URL("http://оун-упа.укр")
    assert "xn----8sb1bdhvc.xn--j1amh" == url.raw_host


def test_host_non_ascii():
    url = URL("http://оун-упа.укр")
    assert "оун-упа.укр" == url.host


def test_localhost():
    url = URL("http://[::1]")
    assert "::1" == url.host


def test_host_with_underscore():
    url = URL("http://abc_def.com")
    assert "abc_def.com" == url.host


def test_raw_host_when_port_is_specified():
    url = URL("http://example.com:8888")
    assert "example.com" == url.raw_host


def test_raw_host_from_str_with_ipv4():
    url = URL("http://127.0.0.1:80")
    assert url.raw_host == "127.0.0.1"


def test_raw_host_from_str_with_ipv6():
    url = URL("http://[::1]:80")
    assert url.raw_host == "::1"


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


def test_lowercase():
    url = URL("http://gitHUB.com")
    assert url.raw_host == "github.com"
    assert url.host == url.raw_host


def test_lowercase_nonascii():
    url = URL("http://Слава.Укр")
    assert url.raw_host == "xn--80aaf8a3a.xn--j1amh"
    assert url.host == "слава.укр"


def test_compressed_ipv6():
    url = URL("http://[1DEC:0:0:0::1]")
    assert url.raw_host == "1dec::1"
    assert url.host == url.raw_host


def test_ipv4_zone():
    # I'm unsure if it is correct.
    url = URL("http://1.2.3.4%тест%42:123")
    assert url.raw_host == "1.2.3.4%тест%42"
    assert url.host == url.raw_host


def test_port_for_explicit_port():
    url = URL("http://example.com:8888")
    assert 8888 == url.port


def test_port_for_implicit_port():
    url = URL("http://example.com")
    assert 80 == url.port


def test_port_for_relative_url():
    url = URL("/path/to")
    assert url.port is None


def test_port_for_unknown_scheme():
    url = URL("unknown://example.com")
    assert url.port is None


def test_explicit_port_for_explicit_port():
    url = URL("http://example.com:8888")
    assert 8888 == url.explicit_port


def test_explicit_port_for_implicit_port():
    url = URL("http://example.com")
    assert url.explicit_port is None


def test_explicit_port_for_relative_url():
    url = URL("/path/to")
    assert url.explicit_port is None


def test_explicit_port_for_unknown_scheme():
    url = URL("unknown://example.com")
    assert url.explicit_port is None


def test_raw_path_string_empty():
    url = URL("http://example.com")
    assert "/" == url.raw_path


def test_raw_path():
    url = URL("http://example.com/path/to")
    assert "/path/to" == url.raw_path


def test_raw_path_non_ascii():
    url = URL("http://example.com/шлях/сюди")
    assert "/%D1%88%D0%BB%D1%8F%D1%85/%D1%81%D1%8E%D0%B4%D0%B8" == url.raw_path


def test_path_non_ascii():
    url = URL("http://example.com/шлях/сюди")
    assert "/шлях/сюди" == url.path


def test_path_with_spaces():
    url = URL("http://example.com/a b/test")
    assert "/a b/test" == url.path

    url = URL("http://example.com/a b")
    assert "/a b" == url.path


def test_raw_path_for_empty_url():
    url = URL()
    assert "" == url.raw_path


def test_raw_path_for_colon_and_at():
    url = URL("http://example.com/path:abc@123")
    assert url.raw_path == "/path:abc@123"


def test_raw_query_string():
    url = URL("http://example.com?a=1&b=2")
    assert url.raw_query_string == "a=1&b=2"


def test_raw_query_string_non_ascii():
    url = URL("http://example.com?б=в&ю=к")
    assert url.raw_query_string == "%D0%B1=%D0%B2&%D1%8E=%D0%BA"


def test_query_string_non_ascii():
    url = URL("http://example.com?б=в&ю=к")
    assert url.query_string == "б=в&ю=к"


def test_path_qs():
    url = URL("http://example.com/")
    assert url.path_qs == "/"
    url = URL("http://example.com/?б=в&ю=к")
    assert url.path_qs == "/?б=в&ю=к"
    url = URL("http://example.com/path?б=в&ю=к")
    assert url.path_qs == "/path?б=в&ю=к"


def test_raw_path_qs():
    url = URL("http://example.com/")
    assert url.raw_path_qs == "/"
    url = URL("http://example.com/?б=в&ю=к")
    assert url.raw_path_qs == "/?%D0%B1=%D0%B2&%D1%8E=%D0%BA"
    url = URL("http://example.com/path?б=в&ю=к")
    assert url.raw_path_qs == "/path?%D0%B1=%D0%B2&%D1%8E=%D0%BA"
    url = URL("http://example.com/шлях?a=1&b=2")
    assert url.raw_path_qs == "/%D1%88%D0%BB%D1%8F%D1%85?a=1&b=2"


def test_query_string_spaces():
    url = URL("http://example.com?a+b=c+d&e=f+g")
    assert url.query_string == "a b=c d&e=f g"


# raw fragment


def test_raw_fragment_empty():
    url = URL("http://example.com")
    assert "" == url.raw_fragment


def test_raw_fragment():
    url = URL("http://example.com/path#anchor")
    assert "anchor" == url.raw_fragment


def test_raw_fragment_non_ascii():
    url = URL("http://example.com/path#якір")
    assert "%D1%8F%D0%BA%D1%96%D1%80" == url.raw_fragment


def test_raw_fragment_safe():
    url = URL("http://example.com/path#a?b/c:d@e")
    assert "a?b/c:d@e" == url.raw_fragment


def test_fragment_non_ascii():
    url = URL("http://example.com/path#якір")
    assert "якір" == url.fragment


def test_raw_parts_empty():
    url = URL("http://example.com")
    assert ("/",) == url.raw_parts


def test_raw_parts():
    url = URL("http://example.com/path/to")
    assert ("/", "path", "to") == url.raw_parts


def test_raw_parts_without_path():
    url = URL("http://example.com")
    assert ("/",) == url.raw_parts


def test_raw_path_parts_with_2F_in_path():
    url = URL("http://example.com/path%2Fto/three")
    assert ("/", "path%2Fto", "three") == url.raw_parts


def test_raw_path_parts_with_2f_in_path():
    url = URL("http://example.com/path%2fto/three")
    assert ("/", "path%2Fto", "three") == url.raw_parts


def test_raw_parts_for_relative_path():
    url = URL("path/to")
    assert ("path", "to") == url.raw_parts


def test_raw_parts_for_relative_path_starting_from_slash():
    url = URL("/path/to")
    assert ("/", "path", "to") == url.raw_parts


def test_raw_parts_for_relative_double_path():
    url = URL("path/to")
    assert ("path", "to") == url.raw_parts


def test_parts_for_empty_url():
    url = URL()
    assert ("",) == url.raw_parts


def test_raw_parts_non_ascii():
    url = URL("http://example.com/шлях/сюди")
    assert (
        "/",
        "%D1%88%D0%BB%D1%8F%D1%85",
        "%D1%81%D1%8E%D0%B4%D0%B8",
    ) == url.raw_parts


def test_parts_non_ascii():
    url = URL("http://example.com/шлях/сюди")
    assert ("/", "шлях", "сюди") == url.parts


def test_name_for_empty_url():
    url = URL()
    assert "" == url.raw_name


def test_raw_name():
    url = URL("http://example.com/path/to#frag")
    assert "to" == url.raw_name


def test_raw_name_root():
    url = URL("http://example.com/#frag")
    assert "" == url.raw_name


def test_raw_name_root2():
    url = URL("http://example.com")
    assert "" == url.raw_name


def test_raw_name_root3():
    url = URL("http://example.com/")
    assert "" == url.raw_name


def test_relative_raw_name():
    url = URL("path/to")
    assert "to" == url.raw_name


def test_relative_raw_name_starting_from_slash():
    url = URL("/path/to")
    assert "to" == url.raw_name


def test_relative_raw_name_slash():
    url = URL("/")
    assert "" == url.raw_name


def test_name_non_ascii():
    url = URL("http://example.com/шлях")
    assert url.name == "шлях"


def test_suffix_for_empty_url():
    url = URL()
    assert "" == url.raw_suffix


def test_raw_suffix():
    url = URL("http://example.com/path/to.txt#frag")
    assert ".txt" == url.raw_suffix


def test_raw_suffix_root():
    url = URL("http://example.com/#frag")
    assert "" == url.raw_suffix


def test_raw_suffix_root2():
    url = URL("http://example.com")
    assert "" == url.raw_suffix


def test_raw_suffix_root3():
    url = URL("http://example.com/")
    assert "" == url.raw_suffix


def test_relative_raw_suffix():
    url = URL("path/to")
    assert "" == url.raw_suffix


def test_relative_raw_suffix_starting_from_slash():
    url = URL("/path/to")
    assert "" == url.raw_suffix


def test_relative_raw_suffix_dot():
    url = URL(".")
    assert "" == url.raw_suffix


def test_suffix_non_ascii():
    url = URL("http://example.com/шлях.суфікс")
    assert url.suffix == ".суфікс"


def test_suffix_with_empty_name():
    url = URL("http://example.com/.hgrc")
    assert "" == url.raw_suffix


def test_suffix_multi_dot():
    url = URL("http://example.com/doc.tar.gz")
    assert ".gz" == url.raw_suffix


def test_suffix_with_dot_name():
    url = URL("http://example.com/doc.")
    assert "" == url.raw_suffix


def test_suffixes_for_empty_url():
    url = URL()
    assert () == url.raw_suffixes


def test_raw_suffixes():
    url = URL("http://example.com/path/to.txt#frag")
    assert (".txt",) == url.raw_suffixes


def test_raw_suffixes_root():
    url = URL("http://example.com/#frag")
    assert () == url.raw_suffixes


def test_raw_suffixes_root2():
    url = URL("http://example.com")
    assert () == url.raw_suffixes


def test_raw_suffixes_root3():
    url = URL("http://example.com/")
    assert () == url.raw_suffixes


def test_relative_raw_suffixes():
    url = URL("path/to")
    assert () == url.raw_suffixes


def test_relative_raw_suffixes_starting_from_slash():
    url = URL("/path/to")
    assert () == url.raw_suffixes


def test_relative_raw_suffixes_dot():
    url = URL(".")
    assert () == url.raw_suffixes


def test_suffixes_non_ascii():
    url = URL("http://example.com/шлях.суфікс")
    assert url.suffixes == (".суфікс",)


def test_suffixes_with_empty_name():
    url = URL("http://example.com/.hgrc")
    assert () == url.raw_suffixes


def test_suffixes_multi_dot():
    url = URL("http://example.com/doc.tar.gz")
    assert (".tar", ".gz") == url.raw_suffixes


def test_suffixes_with_dot_name():
    url = URL("http://example.com/doc.")
    assert () == url.raw_suffixes


def test_plus_in_path():
    url = URL("http://example.com/test/x+y%2Bz/:+%2B/")
    assert "/test/x+y+z/:++/" == url.path


def test_nonascii_in_qs():
    url = URL("http://example.com")
    url2 = url.with_query({"f\xf8\xf8": "f\xf8\xf8"})
    assert "http://example.com/?f%C3%B8%C3%B8=f%C3%B8%C3%B8" == str(url2)


def test_percent_encoded_in_qs():
    url = URL("http://example.com")
    url2 = url.with_query({"k%cf%80": "v%cf%80"})
    assert str(url2) == "http://example.com/?k%25cf%2580=v%25cf%2580"
    assert url2.raw_query_string == "k%25cf%2580=v%25cf%2580"
    assert url2.query_string == "k%cf%80=v%cf%80"
    assert url2.query == {"k%cf%80": "v%cf%80"}


# modifiers


def test_parent_raw_path():
    url = URL("http://example.com/path/to")
    assert url.parent.raw_path == "/path"


def test_parent_raw_parts():
    url = URL("http://example.com/path/to")
    assert url.parent.raw_parts == ("/", "path")


def test_double_parent_raw_path():
    url = URL("http://example.com/path/to")
    assert url.parent.parent.raw_path == "/"


def test_empty_parent_raw_path():
    url = URL("http://example.com/")
    assert url.parent.parent.raw_path == "/"


def test_empty_parent_raw_path2():
    url = URL("http://example.com")
    assert url.parent.parent.raw_path == "/"


def test_clear_fragment_on_getting_parent():
    url = URL("http://example.com/path/to#frag")
    assert URL("http://example.com/path") == url.parent


def test_clear_fragment_on_getting_parent_toplevel():
    url = URL("http://example.com/#frag")
    assert URL("http://example.com/") == url.parent


def test_clear_query_on_getting_parent():
    url = URL("http://example.com/path/to?a=b")
    assert URL("http://example.com/path") == url.parent


def test_clear_query_on_getting_parent_toplevel():
    url = URL("http://example.com/?a=b")
    assert URL("http://example.com/") == url.parent


# truediv


def test_div_root():
    url = URL("http://example.com") / "path" / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div_root_with_slash():
    url = URL("http://example.com/") / "path" / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div():
    url = URL("http://example.com/path") / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div_with_slash():
    url = URL("http://example.com/path/") / "to"
    assert str(url) == "http://example.com/path/to"
    assert url.raw_path == "/path/to"


def test_div_path_starting_from_slash_is_forbidden():
    url = URL("http://example.com/path/")
    with pytest.raises(ValueError):
        url / "/to/others"


class StrEnum(str, Enum):
    spam = "ham"

    def __str__(self):
        return self.value


def test_div_path_srting_subclass():
    url = URL("http://example.com/path/") / StrEnum.spam
    assert str(url) == "http://example.com/path/ham"


def test_div_bad_type():
    url = URL("http://example.com/path/")
    with pytest.raises(TypeError):
        url / 3


def test_div_cleanup_query_and_fragment():
    url = URL("http://example.com/path?a=1#frag")
    assert str(url / "to") == "http://example.com/path/to"


def test_div_for_empty_url():
    url = URL() / "a"
    assert url.raw_parts == ("a",)


def test_div_for_relative_url():
    url = URL("a") / "b"
    assert url.raw_parts == ("a", "b")


def test_div_for_relative_url_started_with_slash():
    url = URL("/a") / "b"
    assert url.raw_parts == ("/", "a", "b")


def test_div_non_ascii():
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


def test_div_percent_encoded():
    url = URL("http://example.com/path")
    url2 = url / "%cf%80"
    assert url2.path == "/path/%cf%80"
    assert url2.raw_path == "/path/%25cf%2580"
    assert url2.parts == ("/", "path", "%cf%80")
    assert url2.raw_parts == ("/", "path", "%25cf%2580")


def test_div_with_colon_and_at():
    url = URL("http://example.com/base") / "path:abc@123"
    assert url.raw_path == "/base/path:abc@123"


def test_div_with_dots():
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
            "/path?a=1#frag",
            ("to",),
            "http://example.com/path/to",
            id="cleanup-query-and-fragment",
        ),
        pytest.param("", ("path/",), "http://example.com/path/", id="trailing-slash"),
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
def test_joinpath(base, to_join, expected):
    url = URL(f"http://example.com{base}")
    assert str(url.joinpath(*to_join)) == expected


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
def test_joinpath_relative(url, to_join, expected):
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
    url, to_join, encoded, e_path, e_raw_path, e_parts, e_raw_parts
):
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
def test_joinpath_edgecases(to_join, expected):
    url = URL("http://example.com/base").joinpath(*to_join)
    assert url.raw_path == expected


def test_joinpath_path_starting_from_slash_is_forbidden():
    url = URL("http://example.com/path/")
    with pytest.raises(
        ValueError, match="Appending path .* starting from slash is forbidden"
    ):
        assert url.joinpath("/to/others")


# with_path


def test_with_path():
    url = URL("http://example.com")
    url2 = url.with_path("/test")
    assert str(url2) == "http://example.com/test"
    assert url2.raw_path == "/test"
    assert url2.path == "/test"


def test_with_path_nonascii():
    url = URL("http://example.com")
    url2 = url.with_path("/π")
    assert str(url2) == "http://example.com/%CF%80"
    assert url2.raw_path == "/%CF%80"
    assert url2.path == "/π"


def test_with_path_percent_encoded():
    url = URL("http://example.com")
    url2 = url.with_path("/%cf%80")
    assert str(url2) == "http://example.com/%25cf%2580"
    assert url2.raw_path == "/%25cf%2580"
    assert url2.path == "/%cf%80"


def test_with_path_encoded():
    url = URL("http://example.com")
    url2 = url.with_path("/test", encoded=True)
    assert str(url2) == "http://example.com/test"
    assert url2.raw_path == "/test"
    assert url2.path == "/test"


def test_with_path_encoded_nonascii():
    url = URL("http://example.com")
    url2 = url.with_path("/π", encoded=True)
    assert str(url2) == "http://example.com/π"
    assert url2.raw_path == "/π"
    assert url2.path == "/π"


def test_with_path_encoded_percent_encoded():
    url = URL("http://example.com")
    url2 = url.with_path("/%cf%80", encoded=True)
    assert str(url2) == "http://example.com/%cf%80"
    assert url2.raw_path == "/%cf%80"
    assert url2.path == "/π"


def test_with_path_dots():
    url = URL("http://example.com")
    assert str(url.with_path("/test/.")) == "http://example.com/test/"


def test_with_path_relative():
    url = URL("/path")
    assert str(url.with_path("/new")) == "/new"


def test_with_path_query():
    url = URL("http://example.com?a=b")
    assert str(url.with_path("/test")) == "http://example.com/test"


def test_with_path_fragment():
    url = URL("http://example.com#frag")
    assert str(url.with_path("/test")) == "http://example.com/test"


def test_with_path_empty():
    url = URL("http://example.com/test")
    assert str(url.with_path("")) == "http://example.com"


def test_with_path_leading_slash():
    url = URL("http://example.com")
    assert url.with_path("test").path == "/test"


# with_fragment


def test_with_fragment():
    url = URL("http://example.com")
    url2 = url.with_fragment("frag")
    assert str(url2) == "http://example.com/#frag"
    assert url2.raw_fragment == "frag"
    assert url2.fragment == "frag"


def test_with_fragment_safe():
    url = URL("http://example.com")
    u2 = url.with_fragment("a:b?c@d/e")
    assert str(u2) == "http://example.com/#a:b?c@d/e"


def test_with_fragment_non_ascii():
    url = URL("http://example.com")
    url2 = url.with_fragment("фрагм")
    assert url2.raw_fragment == "%D1%84%D1%80%D0%B0%D0%B3%D0%BC"
    assert url2.fragment == "фрагм"


def test_with_fragment_percent_encoded():
    url = URL("http://example.com")
    url2 = url.with_fragment("%cf%80")
    assert str(url2) == "http://example.com/#%25cf%2580"
    assert url2.raw_fragment == "%25cf%2580"
    assert url2.fragment == "%cf%80"


def test_with_fragment_None():
    url = URL("http://example.com/path#frag")
    url2 = url.with_fragment(None)
    assert str(url2) == "http://example.com/path"


def test_with_fragment_None_matching():
    url = URL("http://example.com/path")
    url2 = url.with_fragment(None)
    assert url is url2


def test_with_fragment_matching():
    url = URL("http://example.com/path#frag")
    url2 = url.with_fragment("frag")
    assert url is url2


def test_with_fragment_bad_type():
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        url.with_fragment(123)


# with_name


def test_with_name():
    url = URL("http://example.com/a/b")
    assert url.raw_parts == ("/", "a", "b")
    url2 = url.with_name("c")
    assert url2.raw_parts == ("/", "a", "c")
    assert url2.parts == ("/", "a", "c")
    assert url2.raw_path == "/a/c"
    assert url2.path == "/a/c"


def test_with_name_for_naked_path():
    url = URL("http://example.com")
    url2 = url.with_name("a")
    assert url2.raw_parts == ("/", "a")


def test_with_name_for_relative_path():
    url = URL("a")
    url2 = url.with_name("b")
    assert url2.raw_parts == ("b",)


def test_with_name_for_relative_path2():
    url = URL("a/b")
    url2 = url.with_name("c")
    assert url2.raw_parts == ("a", "c")


def test_with_name_for_relative_path_starting_from_slash():
    url = URL("/a")
    url2 = url.with_name("b")
    assert url2.raw_parts == ("/", "b")


def test_with_name_for_relative_path_starting_from_slash2():
    url = URL("/a/b")
    url2 = url.with_name("c")
    assert url2.raw_parts == ("/", "a", "c")


def test_with_name_empty():
    url = URL("http://example.com/path/to").with_name("")
    assert str(url) == "http://example.com/path/"


def test_with_name_non_ascii():
    url = URL("http://example.com/path").with_name("шлях")
    assert url.path == "/шлях"
    assert url.raw_path == "/%D1%88%D0%BB%D1%8F%D1%85"
    assert url.parts == ("/", "шлях")
    assert url.raw_parts == ("/", "%D1%88%D0%BB%D1%8F%D1%85")


def test_with_name_percent_encoded():
    url = URL("http://example.com/path")
    url2 = url.with_name("%cf%80")
    assert url2.raw_parts == ("/", "%25cf%2580")
    assert url2.parts == ("/", "%cf%80")
    assert url2.raw_path == "/%25cf%2580"
    assert url2.path == "/%cf%80"


def test_with_name_with_slash():
    with pytest.raises(ValueError):
        URL("http://example.com").with_name("a/b")


def test_with_name_non_str():
    with pytest.raises(TypeError):
        URL("http://example.com").with_name(123)


def test_with_name_within_colon_and_at():
    url = URL("http://example.com/oldpath").with_name("path:abc@123")
    assert url.raw_path == "/path:abc@123"


def test_with_name_dot():
    with pytest.raises(ValueError):
        URL("http://example.com").with_name(".")


def test_with_name_double_dot():
    with pytest.raises(ValueError):
        URL("http://example.com").with_name("..")


# with_suffix


def test_with_suffix():
    url = URL("http://example.com/a/b")
    assert url.raw_parts == ("/", "a", "b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("/", "a", "b.c")
    assert url2.parts == ("/", "a", "b.c")
    assert url2.raw_path == "/a/b.c"
    assert url2.path == "/a/b.c"


def test_with_suffix_for_naked_path():
    url = URL("http://example.com")
    with pytest.raises(ValueError) as excinfo:
        url.with_suffix(".a")
    (msg,) = excinfo.value.args
    assert msg == f"{url!r} has an empty name"


def test_with_suffix_for_relative_path():
    url = URL("a")
    url2 = url.with_suffix(".b")
    assert url2.raw_parts == ("a.b",)


def test_with_suffix_for_relative_path2():
    url = URL("a/b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("a", "b.c")


def test_with_suffix_for_relative_path_starting_from_slash():
    url = URL("/a")
    url2 = url.with_suffix(".b")
    assert url2.raw_parts == ("/", "a.b")


def test_with_suffix_for_relative_path_starting_from_slash2():
    url = URL("/a/b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("/", "a", "b.c")


def test_with_suffix_empty():
    url = URL("http://example.com/path/to").with_suffix("")
    assert str(url) == "http://example.com/path/to"


def test_with_suffix_non_ascii():
    url = URL("http://example.com/path").with_suffix(".шлях")
    assert url.path == "/path.шлях"
    assert url.raw_path == "/path.%D1%88%D0%BB%D1%8F%D1%85"
    assert url.parts == ("/", "path.шлях")
    assert url.raw_parts == ("/", "path.%D1%88%D0%BB%D1%8F%D1%85")


def test_with_suffix_percent_encoded():
    url = URL("http://example.com/path")
    url2 = url.with_suffix(".%cf%80")
    assert url2.raw_parts == ("/", "path.%25cf%2580")
    assert url2.parts == ("/", "path.%cf%80")
    assert url2.raw_path == "/path.%25cf%2580"
    assert url2.path == "/path.%cf%80"


def test_with_suffix_without_dot():
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com/a").with_suffix("b")
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix 'b'"


def test_with_suffix_non_str():
    with pytest.raises(TypeError) as excinfo:
        URL("http://example.com").with_suffix(123)
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix type"


def test_with_suffix_dot():
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com").with_suffix(".")
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix '.'"


def test_with_suffix_with_slash():
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com/a").with_suffix("/.b")
    (msg,) = excinfo.value.args
    assert msg == "Invalid suffix '/.b'"


def test_with_suffix_with_slash2():
    with pytest.raises(ValueError) as excinfo:
        URL("http://example.com/a").with_suffix(".b/.d")
    (msg,) = excinfo.value.args
    assert msg == "Slash in name is not allowed"


def test_with_suffix_replace():
    url = URL("/a.b")
    url2 = url.with_suffix(".c")
    assert url2.raw_parts == ("/", "a.c")


# is_absolute


def test_is_absolute_for_relative_url():
    url = URL("/path/to")
    assert not url.is_absolute()


def test_is_absolute_for_absolute_url():
    url = URL("http://example.com")
    assert url.is_absolute()


def test_is_non_absolute_for_empty_url():
    url = URL()
    assert not url.is_absolute()


def test_is_non_absolute_for_empty_url2():
    url = URL("")
    assert not url.is_absolute()


def test_is_absolute_path_starting_from_double_slash():
    url = URL("//www.python.org")
    assert url.is_absolute()


# is_default_port


def test_is_default_port_for_relative_url():
    url = URL("/path/to")
    assert not url.is_default_port()


def test_is_default_port_for_absolute_url_without_port():
    url = URL("http://example.com")
    assert url.is_default_port()


def test_is_default_port_for_absolute_url_with_default_port():
    url = URL("http://example.com:80")
    assert url.is_default_port()


def test_is_default_port_for_absolute_url_with_nondefault_port():
    url = URL("http://example.com:8080")
    assert not url.is_default_port()


def test_is_default_port_for_unknown_scheme():
    url = URL("unknown://example.com:8080")
    assert not url.is_default_port()


#


def test_no_scheme():
    url = URL("example.com")
    assert url.raw_host is None
    assert url.raw_path == "example.com"
    assert str(url) == "example.com"


def test_no_scheme2():
    url = URL("example.com/a/b")
    assert url.raw_host is None
    assert url.raw_path == "example.com/a/b"
    assert str(url) == "example.com/a/b"


def test_from_non_allowed():
    with pytest.raises(TypeError):
        URL(1234)


def test_from_idna():
    url = URL("http://xn--jxagkqfkduily1i.eu")
    assert "http://xn--jxagkqfkduily1i.eu" == str(url)
    url = URL("http://xn--einla-pqa.de/")  # needs idna 2008
    assert "http://xn--einla-pqa.de/" == str(url)


def test_to_idna():
    url = URL("http://εμπορικόσήμα.eu")
    assert "http://xn--jxagkqfkduily1i.eu" == str(url)
    url = URL("http://einlaß.de/")
    assert "http://xn--einla-pqa.de/" == str(url)


def test_from_ascii_login():
    url = URL("http://" "%D0%B2%D0%B0%D1%81%D1%8F" "@host:1234/")
    assert ("http://" "%D0%B2%D0%B0%D1%81%D1%8F" "@host:1234/") == str(url)


def test_from_non_ascii_login():
    url = URL("http://бажан@host:1234/")
    assert ("http://%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD@host:1234/") == str(url)


def test_from_ascii_login_and_password():
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


def test_from_non_ascii_login_and_password():
    url = URL("http://бажан:пароль@host:1234/")
    assert (
        "http://"
        "%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD"
        ":%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C"
        "@host:1234/"
    ) == str(url)


def test_from_ascii_path():
    url = URL("http://example.com/" "%D0%BF%D1%83%D1%82%D1%8C/%D1%82%D1%83%D0%B4%D0%B0")
    assert (
        "http://example.com/" "%D0%BF%D1%83%D1%82%D1%8C/%D1%82%D1%83%D0%B4%D0%B0"
    ) == str(url)


def test_from_ascii_path_lower_case():
    url = URL("http://example.com/" "%d0%bf%d1%83%d1%82%d1%8c/%d1%82%d1%83%d0%b4%d0%b0")
    assert (
        "http://example.com/" "%D0%BF%D1%83%D1%82%D1%8C/%D1%82%D1%83%D0%B4%D0%B0"
    ) == str(url)


def test_from_non_ascii_path():
    url = URL("http://example.com/шлях/туди")
    assert (
        "http://example.com/%D1%88%D0%BB%D1%8F%D1%85/%D1%82%D1%83%D0%B4%D0%B8"
    ) == str(url)


def test_bytes():
    url = URL("http://example.com/шлях/туди")
    assert (
        b"http://example.com/%D1%88%D0%BB%D1%8F%D1%85/%D1%82%D1%83%D0%B4%D0%B8"
        == bytes(url)
    )


def test_from_ascii_query_parts():
    url = URL(
        "http://example.com/"
        "?%D0%BF%D0%B0%D1%80%D0%B0%D0%BC"
        "=%D0%B7%D0%BD%D0%B0%D1%87"
    )
    assert (
        "http://example.com/"
        "?%D0%BF%D0%B0%D1%80%D0%B0%D0%BC"
        "=%D0%B7%D0%BD%D0%B0%D1%87"
    ) == str(url)


def test_from_non_ascii_query_parts():
    url = URL("http://example.com/?парам=знач")
    assert (
        "http://example.com/"
        "?%D0%BF%D0%B0%D1%80%D0%B0%D0%BC"
        "=%D0%B7%D0%BD%D0%B0%D1%87"
    ) == str(url)


def test_from_non_ascii_query_parts2():
    url = URL("http://example.com/?п=з&ю=б")
    assert "http://example.com/?%D0%BF=%D0%B7&%D1%8E=%D0%B1" == str(url)


def test_from_ascii_fragment():
    url = URL("http://example.com/" "#%D1%84%D1%80%D0%B0%D0%B3%D0%BC%D0%B5%D0%BD%D1%82")
    assert (
        "http://example.com/" "#%D1%84%D1%80%D0%B0%D0%B3%D0%BC%D0%B5%D0%BD%D1%82"
    ) == str(url)


def test_from_bytes_with_non_ascii_fragment():
    url = URL("http://example.com/#фрагмент")
    assert (
        "http://example.com/" "#%D1%84%D1%80%D0%B0%D0%B3%D0%BC%D0%B5%D0%BD%D1%82"
    ) == str(url)


def test_to_str():
    url = URL("http://εμπορικόσήμα.eu/")
    assert "http://xn--jxagkqfkduily1i.eu/" == str(url)


def test_to_str_long():
    url = URL(
        "https://host-12345678901234567890123456789012345678901234567890" "-name:8888/"
    )
    expected = (
        "https://host-"
        "12345678901234567890123456789012345678901234567890"
        "-name:8888/"
    )
    assert expected == str(url)


def test_decoding_with_2F_in_path():
    url = URL("http://example.com/path%2Fto")
    assert "http://example.com/path%2Fto" == str(url)
    assert url == URL(str(url))


def test_decoding_with_26_and_3D_in_query():
    url = URL("http://example.com/?%26=%3D")
    assert "http://example.com/?%26=%3D" == str(url)
    assert url == URL(str(url))


def test_fragment_only_url():
    url = URL("#frag")
    assert str(url) == "#frag"


def test_url_from_url():
    url = URL("http://example.com")
    assert URL(url) == url
    assert URL(url).raw_parts == ("/",)


def test_lowercase_scheme():
    url = URL("HTTP://example.com")
    assert str(url) == "http://example.com"


def test_str_for_empty_url():
    url = URL()
    assert "" == str(url)


def test_parent_for_empty_url():
    url = URL()
    assert url is url.parent


def test_empty_value_for_query():
    url = URL("http://example.com/path").with_query({"a": ""})
    assert str(url) == "http://example.com/path?a="


def test_none_value_for_query():
    with pytest.raises(TypeError):
        URL("http://example.com/path").with_query({"a": None})


def test_decode_pct_in_path():
    url = URL("http://www.python.org/%7Eguido")
    assert "http://www.python.org/~guido" == str(url)


def test_decode_pct_in_path_lower_case():
    url = URL("http://www.python.org/%7eguido")
    assert "http://www.python.org/~guido" == str(url)


# join


def test_join():
    base = URL("http://www.cwi.nl/%7Eguido/Python.html")
    url = URL("FAQ.html")
    url2 = base.join(url)
    assert str(url2) == "http://www.cwi.nl/~guido/FAQ.html"


def test_join_absolute():
    base = URL("http://www.cwi.nl/%7Eguido/Python.html")
    url = URL("//www.python.org/%7Eguido")
    url2 = base.join(url)
    assert str(url2) == "http://www.python.org/~guido"


def test_join_non_url():
    base = URL("http://example.com")
    with pytest.raises(TypeError):
        base.join("path/to")


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
def test_join_from_rfc_3986_normal(url, expected):
    # test case from https://tools.ietf.org/html/rfc3986.html#section-5.4
    base = URL("http://a/b/c/d;p?q")
    url = URL(url)
    expected = URL(expected)
    assert base.join(url) == expected


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
def test_join_from_rfc_3986_abnormal(url, expected):
    # test case from https://tools.ietf.org/html/rfc3986.html#section-5.4.2
    base = URL("http://a/b/c/d;p?q")
    url = URL(url)
    expected = URL(expected)
    assert base.join(url) == expected


def test_split_result_non_decoded():
    with pytest.raises(ValueError):
        URL(SplitResult("http", "example.com", "path", "qs", "frag"))


def test_human_repr():
    url = URL("http://бажан:пароль@хост.домен:8080/шлях/сюди?арг=вал#фраг")
    s = url.human_repr()
    assert URL(s) == url
    assert s == "http://бажан:пароль@хост.домен:8080/шлях/сюди?арг=вал#фраг"


def test_human_repr_defaults():
    url = URL("шлях")
    s = url.human_repr()
    assert s == "шлях"


def test_human_repr_default_port():
    url = URL("http://бажан:пароль@хост.домен/шлях/сюди?арг=вал#фраг")
    s = url.human_repr()
    assert URL(s) == url
    assert s == "http://бажан:пароль@хост.домен/шлях/сюди?арг=вал#фраг"


def test_human_repr_ipv6():
    url = URL("http://[::1]:8080/path")
    s = url.human_repr()
    url2 = URL(s)
    assert url2 == url
    assert url2.host == "::1"
    assert s == "http://[::1]:8080/path"


def test_human_repr_delimiters():
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
        s == "http:// !\"%23$%25&'()*+,-.%2F%3A;<=>%3F%40%5B\\%5D^_`{|}~"
        ": !\"%23$%25&'()*+,-.%2F%3A;<=>%3F%40%5B\\%5D^_`{|}~"
        "@хост.домен:8080"
        "/ !\"%23$%25&'()*+,-./:;<=>%3F@[\\]^_`{|}~"
        "? !\"%23$%25%26'()*%2B,-./:%3B<%3D>?@[\\]^_`{|}~"
        "= !\"%23$%25%26'()*%2B,-./:%3B<%3D>?@[\\]^_`{|}~"
        "# !\"#$%25&'()*+,-./:;<=>?@[\\]^_`{|}~"
    )


def test_human_repr_non_printable():
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


def test_relative():
    url = URL("http://user:pass@example.com:8080/path?a=b#frag")
    rel = url.relative()
    assert str(rel) == "/path?a=b#frag"


def test_relative_is_relative():
    url = URL("http://user:pass@example.com:8080/path?a=b#frag")
    rel = url.relative()
    assert not rel.is_absolute()


def test_relative_abs_parts_are_removed():
    url = URL("http://user:pass@example.com:8080/path?a=b#frag")
    rel = url.relative()
    assert not rel.scheme
    assert not rel.user
    assert not rel.password
    assert not rel.host
    assert not rel.port


def test_relative_fails_on_rel_url():
    with pytest.raises(ValueError):
        URL("/path?a=b#frag").relative()


def test_slash_and_question_in_query():
    u = URL("http://example.com/path?http://example.com/p?a#b")
    assert u.query_string == "http://example.com/p?a"


def test_slash_and_question_in_fragment():
    u = URL("http://example.com/path#http://example.com/p?a")
    assert u.fragment == "http://example.com/p?a"


def test_requoting():
    u = URL("http://127.0.0.1/?next=http%3A//example.com/")
    assert u.raw_query_string == "next=http://example.com/"
    assert str(u) == "http://127.0.0.1/?next=http://example.com/"
