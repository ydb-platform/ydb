"""Test URL."""

# standard
from typing import Optional

# external
import pytest

# local
from validators import ValidationError, url


@pytest.mark.parametrize(
    "value",
    [
        "http://foobar.dk",
        "http://foobar.museum/foobar",
        "http://fo.com",
        "http://FOO.com",
        "http://foo.com/blah_blah",
        "http://foo.com/blah_blah/",
        "http://foo.com/blah_blah_(wikipedia)",
        "http://foo.com/blah_blah_(wikipedia)_(again)",
        "http://www.example.com/wpstyle/?p=364",
        "https://www.example.com?bar=baz",
        "http://✪df.ws/123",
        "http://userid:password@example.com:8080",
        "http://userid:password@example.com:8080/",
        "http://userid@example.com",
        "http://userid@example.com/",
        "http://userid@example.com:8080",
        "http://userid@example.com:8080/",
        "http://userid:password@example.com",
        "http://userid:password@example.com/",
        "http://142.42.1.1/",
        "http://142.42.1.1:8080/",
        "http://➡.ws/䨹",
        "http://⌘.ws",
        "http://⌘.ws/",
        "http://foo.com/blah_(wikipedia)#cite-1",
        "http://foo.com/blah_(wikipedia)_blah#cite-1",
        "http://foo.com/unicode_(✪)_in_parens",
        "http://foo.com/(something)?after=parens",
        "http://☺.damowmow.com/",
        "http://code.google.com/events/#&product=browser",
        "http://j.mp",
        "ftp://foo.bar/baz",
        "http://foo.bar/?q=Test%20URL-encoded%20stuff",
        "http://مثال.إختبار",
        "http://例子.测试",
        "http://उदाहरण.परीक्षा",
        "http://www.😉.com",
        "http://😉.com/😁",
        "http://উদাহরণ.বাংলা",
        "http://xn--d5b6ci4b4b3a.xn--54b7fta0cc",
        "http://дом-м.рф/1/asdf",
        "http://xn----gtbybh.xn--p1ai/1/asdf",
        "http://1337.net",
        "http://a.b-c.de",
        "http://a.b--c.de/",
        "http://0.0.0.0",
        "http://224.1.1.1",
        "http://223.255.255.254",
        "http://10.1.1.0",
        "http://10.1.1.1",
        "http://10.1.1.254",
        "http://10.1.1.255",
        "http://127.0.0.1:8080",
        "http://127.0.10.150",
        "http://47.96.118.255:2333/",
        "http://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:80/index.html",
        "http://[1080:0:0:0:8:800:200C:417A]/index.html",
        "http://[3ffe:2a00:100:7031::1]",
        "http://[1080::8:800:200C:417A]/foo",
        "http://[::192.9.5.5]/ipng",
        "http://[::FFFF:129.144.52.38]:80/index.html",
        "http://[2010:836B:4179::836B:4179]",
        "http://foo.bar",
        "http://foo.bar/📍",
        "http://google.com:9/test",
        "http://5.196.190.0/",
        "http://username:password@example.com:4010/",
        "http://username:password@112.168.10.10:4010/",
        "http://base-test-site.local",
        "http://президент.рф/",
        "http://10.24.90.255:83/",
        "https://travel-usa.com/wisconsin/旅行/",
        "http://:::::::::::::@exmp.com",
        "http://-.~_!$&'()*+,;=:%40:80%2f::::::@example.com",
        "https://exchange.jetswap.finance/#/swap",
        "https://www.foo.com/bar#/baz/test",
        "https://matrix.to/#/!BSqRHgvCtIsGittkBG:talk.puri.sm/$1551464398"
        + "853539kMJNP:matrix.org?via=talk.puri.sm&via=matrix.org&via=disroot.org",
        "https://example.org/path#2022%201040%20(Cornelius%20Morgan%20G).pdf",
        # when simple_host=True
        # "http://localhost",
        # "http://localhost:8000",
        # "http://pc:8081/",
        # "http://3628126748",
        # "http://foobar",
        # when strict_query=False
        # "https://www.example.com/foo/?bar=baz&inga=42&quux",
        # "https://foo.bar.net/baz.php?-/inga/test-lenient-query/",
        # "https://foo.com/img/bar/baz.jpg?-62169987208",
        # "https://example.com/foo/?bar#!baz/inga/8SA-M3as7A8",
    ],
)
def test_returns_true_on_valid_url(value: str):
    """Test returns true on valid url."""
    assert url(value)


@pytest.mark.parametrize(
    "value, private",
    [
        ("http://username:password@10.0.10.1/", True),
        ("http://username:password@192.168.10.10:4010/", True),
        ("http://127.0.0.1", True),
    ],
)
def test_returns_true_on_valid_private_url(value: str, private: Optional[bool]):
    """Test returns true on valid private url."""
    assert url(value, private=private)


@pytest.mark.parametrize(
    "value",
    [
        "foobar.dk",
        "http://127.0.0/asdf",
        "http://foobar.d",
        "http://foobar.12",
        "htp://foobar.com",
        "http://foobar..com",
        "http://fo..com",
        "http://",
        "http://.",
        "http://..",
        "http://../",
        "http://?",
        "http://??",
        "http://??/",
        "http://#",
        "http://##",
        "http://##/",
        "http://foo.bar?q=Spaces should be encoded",
        "//",
        "//a",
        "///a",
        "///",
        "http:///a",
        "foo.com",
        "rdar://1234",
        "h://test",
        "http:// shouldfail.com",
        ":// should fail",
        "http://foo.bar/foo(bar)baz quux",
        "http://-error-.invalid/",
        "http://www.\ufffd.ch",
        "http://-a.b.co",
        "http://a.b-.co",
        "http://1.1.1.1.1",
        "http://123.123.123",
        "http://.www.foo.bar/",
        "http://www.foo.bar./",
        "http://.www.foo.bar./",
        "http://127.12.0.260",
        'http://example.com/">user@example.com',
        "http://[2010:836B:4179::836B:4179",
        "http://2010:836B:4179::836B:4179",
        "http://2010:836B:4179::836B:4179:80/index.html",
        "https://example.org?q=search');alert(document.domain);",
        "https://www.example.com/foo/?bar=baz&inga=42&quux",
        "https://foo.com/img/bar/baz.jpg?-62169987208",
        "https://foo.bar.net/baz.php?-/inga/test-lenient-query/",
        "https://example.com/foo/?bar#!baz/inga/8SA-M3as7A8",
        "http://0.00.00.00.00.00.00.00.00.00.00.00.00.00.00."
        + "00.00.00.00.00.00.00.00.00.00.00.00.00.00.00.00."
        + "00.00.00.00.00.00.00.00.00.00.00.00.00.00.00.00."
        + "00.00.00.00.00.00.00.00.00.00.00.00.00.",  # ReDoS
        "http://172.20.201.135-10.10.10.1656172.20.11.80-10."
        + "10.10.1746172.16.9.13-192.168.17.68610.10.10.226-192."
        + "168.17.64610.10.10.226-192.168.17.63610.10.10.226-192."
        + "168.17.62610.10.10.226-192.168.17.61610.10.10.226-192."
        + "168.17.60610.10.10.226-192.168.17.59610.10.10.226-192."
        + "168.17.58610.10.10.226-192.168.17.57610.10.10.226-192."
        + "168.17.56610.10.10.226-192.168.17.55610.10.10.226-192."
        + "168.17.54610.10.10.226-192.168.17.53610.10.10.226-192."
        + "168.17.52610.10.10.226-192.168.17.51610.10.10.195-10."
        + "10.10.2610.10.10.194-192.168.17.685172.20.11.52-10.10."
        + "10.195510.10.10.226-192.168.17.50510.10.10.186-172.20."
        + "11.1510.10.10.165-198.41.0.54192.168.84.1-192.168.17."
        + "684192.168.222.1-192.168.17.684172.20.11.52-10.10.10."
        + "174410.10.10.232-172.20.201.198410.10.10.228-172.20.201."
        + "1983192.168.17.135-10.10.10.1423192.168.17.135-10.10.10."
        + "122310.10.10.224-172.20.201.198310.10.10.195-172.20.11."
        + "1310.10.10.160-172.20.201.198310.10.10.142-192.168.17."
        + "1352192.168.22.207-10.10.10.2242192.168.17.66-10.10.10."
        + "1122192.168.17.135-10.10.10.1122192.168.17.129-10.10.10."
        + "1122172.20.201.198-10.10.10.2282172.20.201.198-10.10.10."
        + "2242172.20.201.1-10.10.10.1652172.20.11.2-10.10.10.1412172."
        + "16.8.229-12.162.170.196210.10.10.212-192.168.22.133",  # ReDoS
    ],
)
def test_returns_failed_validation_on_invalid_url(value: str):
    """Test returns failed validation on invalid url."""
    assert isinstance(url(value), ValidationError)


@pytest.mark.parametrize(
    "value, private",
    [
        ("http://username:password@192.168.10.10:4010", False),
        ("http://username:password@127.0.0.1:8080", False),
        ("http://10.0.10.1", False),
        ("http://255.255.255.255", False),
    ],
)
def test_returns_failed_validation_on_invalid_private_url(value: str, private: Optional[bool]):
    """Test returns failed validation on invalid private url."""
    assert isinstance(url(value, private=private), ValidationError)
