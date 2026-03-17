import pytest

from bleach import clean, linkify


def test_japanese_safe_simple():
    assert clean("ヘルプとチュートリアル") == "ヘルプとチュートリアル"
    assert linkify("ヘルプとチュートリアル") == "ヘルプとチュートリアル"


def test_japanese_strip():
    assert clean("<em>ヘルプとチュートリアル</em>") == "<em>ヘルプとチュートリアル</em>"
    assert (
        clean("<span>ヘルプとチュートリアル</span>")
        == "&lt;span&gt;ヘルプとチュートリアル&lt;/span&gt;"
    )


def test_russian_simple():
    assert clean("Домашняя") == "Домашняя"
    assert linkify("Домашняя") == "Домашняя"


def test_mixed():
    assert clean("Домашняяヘルプとチュートリアル") == "Домашняяヘルプとチュートリアル"


def test_mixed_linkify():
    assert (
        linkify("Домашняя http://example.com ヘルプとチュートリアル")
        == 'Домашняя <a href="http://example.com" rel="nofollow">http://example.com</a> ヘルプとチュートリアル'
    )


@pytest.mark.parametrize(
    "uri",
    [
        "http://éxámplé.com/",
        "http://éxámplé.com/íàñá/",
        "http://éxámplé.com/íàñá/?foo=bar",
        "http://éxámplé.com/íàñá/?fóo=bár",
    ],
)
def test_url_utf8(uri):
    """Allow UTF8 characters in URLs themselves."""
    assert linkify(uri) == f'<a href="{uri}" rel="nofollow">{uri}</a>'
