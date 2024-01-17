import pytest

from yarl import URL

# with_*


def test_with_scheme():
    url = URL("http://example.com")
    assert str(url.with_scheme("https")) == "https://example.com"


def test_with_scheme_uppercased():
    url = URL("http://example.com")
    assert str(url.with_scheme("HTTPS")) == "https://example.com"


def test_with_scheme_for_relative_url():
    with pytest.raises(ValueError):
        URL("path/to").with_scheme("http")


def test_with_scheme_invalid_type():
    url = URL("http://example.com")
    with pytest.raises(TypeError):
        assert str(url.with_scheme(123))


def test_with_user():
    url = URL("http://example.com")
    assert str(url.with_user("john")) == "http://john@example.com"


def test_with_user_non_ascii():
    url = URL("http://example.com")
    url2 = url.with_user("бажан")
    assert url2.raw_user == "%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD"
    assert url2.user == "бажан"
    assert url2.raw_authority == "%D0%B1%D0%B0%D0%B6%D0%B0%D0%BD@example.com"
    assert url2.authority == "бажан@example.com:80"


def test_with_user_percent_encoded():
    url = URL("http://example.com")
    url2 = url.with_user("%cf%80")
    assert url2.raw_user == "%25cf%2580"
    assert url2.user == "%cf%80"
    assert url2.raw_authority == "%25cf%2580@example.com"
    assert url2.authority == "%cf%80@example.com:80"


def test_with_user_for_relative_url():
    with pytest.raises(ValueError):
        URL("path/to").with_user("user")


def test_with_user_invalid_type():
    url = URL("http://example.com:123")
    with pytest.raises(TypeError):
        url.with_user(123)


def test_with_user_None():
    url = URL("http://john@example.com")
    assert str(url.with_user(None)) == "http://example.com"


def test_with_user_ipv6():
    url = URL("http://john:pass@[::1]:8080/")
    assert str(url.with_user(None)) == "http://[::1]:8080/"


def test_with_user_None_when_password_present():
    url = URL("http://john:pass@example.com")
    assert str(url.with_user(None)) == "http://example.com"


def test_with_password():
    url = URL("http://john@example.com")
    assert str(url.with_password("pass")) == "http://john:pass@example.com"


def test_with_password_ipv6():
    url = URL("http://john:pass@[::1]:8080/")
    assert str(url.with_password(None)) == "http://john@[::1]:8080/"


def test_with_password_non_ascii():
    url = URL("http://john@example.com")
    url2 = url.with_password("пароль")
    assert url2.raw_password == "%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C"
    assert url2.password == "пароль"
    assert url2.raw_authority == "john:%D0%BF%D0%B0%D1%80%D0%BE%D0%BB%D1%8C@example.com"
    assert url2.authority == "john:пароль@example.com:80"


def test_with_password_percent_encoded():
    url = URL("http://john@example.com")
    url2 = url.with_password("%cf%80")
    assert url2.raw_password == "%25cf%2580"
    assert url2.password == "%cf%80"
    assert url2.raw_authority == "john:%25cf%2580@example.com"
    assert url2.authority == "john:%cf%80@example.com:80"


def test_with_password_non_ascii_with_colon():
    url = URL("http://john@example.com")
    url2 = url.with_password("п:а")
    assert url2.raw_password == "%D0%BF%3A%D0%B0"
    assert url2.password == "п:а"


def test_with_password_for_relative_url():
    with pytest.raises(ValueError):
        URL("path/to").with_password("pass")


def test_with_password_None():
    url = URL("http://john:pass@example.com")
    assert str(url.with_password(None)) == "http://john@example.com"


def test_with_password_invalid_type():
    url = URL("http://example.com:123")
    with pytest.raises(TypeError):
        url.with_password(123)


def test_with_password_and_empty_user():
    url = URL("http://example.com")
    url2 = url.with_password("pass")
    assert url2.password == "pass"
    assert url2.user is None
    assert str(url2) == "http://:pass@example.com"


def test_from_str_with_host_ipv4():
    url = URL("http://host:80")
    url = url.with_host("192.168.1.1")
    assert url.raw_host == "192.168.1.1"


def test_from_str_with_host_ipv6():
    url = URL("http://host:80")
    url = url.with_host("::1")
    assert url.raw_host == "::1"


def test_with_host():
    url = URL("http://example.com:123")
    assert str(url.with_host("example.org")) == "http://example.org:123"


def test_with_host_empty():
    url = URL("http://example.com:123")
    with pytest.raises(ValueError):
        url.with_host("")


def test_with_host_non_ascii():
    url = URL("http://example.com:123")
    url2 = url.with_host("оун-упа.укр")
    assert url2.raw_host == "xn----8sb1bdhvc.xn--j1amh"
    assert url2.host == "оун-упа.укр"
    assert url2.raw_authority == "xn----8sb1bdhvc.xn--j1amh:123"
    assert url2.authority == "оун-упа.укр:123"


def test_with_host_percent_encoded():
    url = URL("http://%25cf%2580%cf%80:%25cf%2580%cf%80@example.com:123")
    url2 = url.with_host("%cf%80.org")
    assert url2.raw_host == "%cf%80.org"
    assert url2.host == "%cf%80.org"
    assert url2.raw_authority == "%25cf%2580%CF%80:%25cf%2580%CF%80@%cf%80.org:123"
    assert url2.authority == "%cf%80π:%cf%80π@%cf%80.org:123"


def test_with_host_for_relative_url():
    with pytest.raises(ValueError):
        URL("path/to").with_host("example.com")


def test_with_host_invalid_type():
    url = URL("http://example.com:123")
    with pytest.raises(TypeError):
        url.with_host(None)


def test_with_port():
    url = URL("http://example.com")
    assert str(url.with_port(8888)) == "http://example.com:8888"


def test_with_port_with_no_port():
    url = URL("http://example.com")
    assert str(url.with_port(None)) == "http://example.com"


def test_with_port_ipv6():
    url = URL("http://[::1]:8080/")
    assert str(url.with_port(80)) == "http://[::1]:80/"


def test_with_port_keeps_query_and_fragment():
    url = URL("http://example.com/?a=1#frag")
    assert str(url.with_port(8888)) == "http://example.com:8888/?a=1#frag"


def test_with_port_percent_encoded():
    url = URL("http://user%name:pass%word@example.com/")
    assert str(url.with_port(808)) == "http://user%25name:pass%25word@example.com:808/"


def test_with_port_for_relative_url():
    with pytest.raises(ValueError):
        URL("path/to").with_port(1234)


def test_with_port_invalid_type():
    with pytest.raises(TypeError):
        URL("http://example.com").with_port("123")
    with pytest.raises(TypeError):
        URL("http://example.com").with_port(True)


def test_with_port_invalid_range():
    with pytest.raises(ValueError):
        URL("http://example.com").with_port(-1)
