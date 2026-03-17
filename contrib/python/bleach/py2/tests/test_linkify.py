from __future__ import unicode_literals

import re

import pytest
from six.moves.urllib_parse import quote_plus

from bleach import linkify, DEFAULT_CALLBACKS as DC
from bleach.linkifier import Linker, LinkifyFilter
from bleach.sanitizer import Cleaner


def test_empty():
    assert linkify("") == ""


def test_simple_link():
    assert (
        linkify("a http://example.com link")
        == 'a <a href="http://example.com" rel="nofollow">http://example.com</a> link'
    )
    assert (
        linkify("a https://example.com link")
        == 'a <a href="https://example.com" rel="nofollow">https://example.com</a> link'
    )
    assert (
        linkify("a example.com link")
        == 'a <a href="http://example.com" rel="nofollow">example.com</a> link'
    )


def test_trailing_slash():
    assert (
        linkify("http://examp.com/")
        == '<a href="http://examp.com/" rel="nofollow">http://examp.com/</a>'
    )
    assert (
        linkify("http://example.com/foo/")
        == '<a href="http://example.com/foo/" rel="nofollow">http://example.com/foo/</a>'
    )
    assert (
        linkify("http://example.com/foo/bar/")
        == '<a href="http://example.com/foo/bar/" rel="nofollow">http://example.com/foo/bar/</a>'
    )


def test_mangle_link():
    """We can muck with the href attribute of the link."""

    def filter_url(attrs, new=False):
        if not attrs.get((None, "href"), "").startswith("http://bouncer"):
            quoted = quote_plus(attrs[(None, "href")])
            attrs[(None, "href")] = "http://bouncer/?u={0!s}".format(quoted)
        return attrs

    assert (
        linkify("http://example.com", callbacks=DC + [filter_url])
        == '<a href="http://bouncer/?u=http%3A%2F%2Fexample.com" rel="nofollow">http://example.com</a>'
    )


def test_mangle_text():
    """We can muck with the inner text of a link."""

    def ft(attrs, new=False):
        attrs["_text"] = "bar"
        return attrs

    assert (
        linkify('http://ex.mp <a href="http://ex.mp/foo">foo</a>', callbacks=[ft])
        == '<a href="http://ex.mp">bar</a> <a href="http://ex.mp/foo">bar</a>'
    )


@pytest.mark.parametrize(
    "data,parse_email,expected",
    [
        ("a james@example.com mailto", False, "a james@example.com mailto"),
        ("a james@example.com.au mailto", False, "a james@example.com.au mailto"),
        (
            "a james@example.com mailto",
            True,
            'a <a href="mailto:james@example.com">james@example.com</a> mailto',
        ),
        (
            "aussie james@example.com.au mailto",
            True,
            'aussie <a href="mailto:james@example.com.au">james@example.com.au</a> mailto',
        ),
        # This is kind of a pathological case. I guess we do our best here.
        (
            'email to <a href="james@example.com">james@example.com</a>',
            True,
            'email to <a href="james@example.com" rel="nofollow">james@example.com</a>',
        ),
        (
            "<br>jinkyun@example.com",
            True,
            '<br><a href="mailto:jinkyun@example.com">jinkyun@example.com</a>',
        ),
        # Mailto links at the end of a sentence.
        (
            "mailto james@example.com.au.",
            True,
            'mailto <a href="mailto:james@example.com.au">james@example.com.au</a>.',
        ),
        # Incorrect email
        ('"\\\n"@opa.ru', True, '"\\\n"@opa.ru'),
    ],
)
def test_email_link(data, parse_email, expected):
    assert linkify(data, parse_email=parse_email) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            '"james"@example.com',
            """<a href='mailto:"james"@example.com'>"james"@example.com</a>""",
        ),
        (
            '"j\'ames"@example.com',
            """<a href="mailto:&quot;j'ames&quot;@example.com">"j'ames"@example.com</a>""",
        ),
        (
            '"ja>mes"@example.com',
            """<a href='mailto:"ja>mes"@example.com'>"ja&gt;mes"@example.com</a>""",
        ),
    ],
)
def test_email_link_escaping(data, expected):
    assert linkify(data, parse_email=True) == expected


def no_new_links(attrs, new=False):
    if new:
        return None
    return attrs


def no_old_links(attrs, new=False):
    if not new:
        return None
    return attrs


def noop(attrs, new=False):
    return attrs


@pytest.mark.parametrize(
    "callback,expected",
    [
        (
            [noop],
            'a <a href="http://ex.mp">ex.mp</a> <a href="http://example.com">example</a>',
        ),
        ([no_new_links, noop], 'a ex.mp <a href="http://example.com">example</a>'),
        ([noop, no_new_links], 'a ex.mp <a href="http://example.com">example</a>'),
        ([no_old_links, noop], 'a <a href="http://ex.mp">ex.mp</a> example'),
        ([noop, no_old_links], 'a <a href="http://ex.mp">ex.mp</a> example'),
        ([no_old_links, no_new_links], "a ex.mp example"),
    ],
)
def test_prevent_links(callback, expected):
    """Returning None from any callback should remove links or prevent them
    from being created."""
    text = 'a ex.mp <a href="http://example.com">example</a>'
    assert linkify(text, callbacks=callback) == expected


def test_set_attrs():
    """We can set random attributes on links."""

    def set_attr(attrs, new=False):
        attrs[(None, "rev")] = "canonical"
        return attrs

    assert (
        linkify("ex.mp", callbacks=[set_attr])
        == '<a href="http://ex.mp" rev="canonical">ex.mp</a>'
    )


def test_only_proto_links():
    """Only create links if there's a protocol."""

    def only_proto(attrs, new=False):
        if new and not attrs["_text"].startswith(("http:", "https:")):
            return None
        return attrs

    in_text = 'a ex.mp http://ex.mp <a href="/foo">bar</a>'
    assert (
        linkify(in_text, callbacks=[only_proto])
        == 'a ex.mp <a href="http://ex.mp">http://ex.mp</a> <a href="/foo">bar</a>'
    )


def test_stop_email():
    """Returning None should prevent a link from being created."""

    def no_email(attrs, new=False):
        if attrs[(None, "href")].startswith("mailto:"):
            return None
        return attrs

    text = "do not link james@example.com"

    assert linkify(text, parse_email=True, callbacks=[no_email]) == text


@pytest.mark.parametrize(
    "data,expected",
    [
        # tlds
        ("example.com", '<a href="http://example.com" rel="nofollow">example.com</a>'),
        ("example.co", '<a href="http://example.co" rel="nofollow">example.co</a>'),
        (
            "example.co.uk",
            '<a href="http://example.co.uk" rel="nofollow">example.co.uk</a>',
        ),
        ("example.edu", '<a href="http://example.edu" rel="nofollow">example.edu</a>'),
        ("example.xxx", '<a href="http://example.xxx" rel="nofollow">example.xxx</a>'),
        ("bit.ly/fun", '<a href="http://bit.ly/fun" rel="nofollow">bit.ly/fun</a>'),
        # non-tlds
        ("example.yyy", "example.yyy"),
        ("brie", "brie"),
    ],
)
def test_tlds(data, expected):
    assert linkify(data) == expected


@pytest.mark.parametrize(
    "data,expected",
    [
        ("< unrelated", "&lt; unrelated"),
        ("<U \x7f=&#;>", '<u \x7f="&amp;#;"></u>'),
    ],
)
def test_escaping(data, expected):
    assert linkify(data) == expected


def test_nofollow_off():
    assert (
        linkify("example.com", callbacks=[])
        == '<a href="http://example.com">example.com</a>'
    )


def test_link_in_html():
    assert (
        linkify("<i>http://yy.com</i>")
        == '<i><a href="http://yy.com" rel="nofollow">http://yy.com</a></i>'
    )
    assert (
        linkify("<em><strong>http://xx.com</strong></em>")
        == '<em><strong><a href="http://xx.com" rel="nofollow">http://xx.com</a></strong></em>'
    )


def test_links_https():
    assert (
        linkify("https://yy.com")
        == '<a href="https://yy.com" rel="nofollow">https://yy.com</a>'
    )


def test_add_rel_nofollow():
    """Verify that rel="nofollow" is added to an existing link"""
    assert (
        linkify('<a href="http://yy.com">http://yy.com</a>')
        == '<a href="http://yy.com" rel="nofollow">http://yy.com</a>'
    )


def test_url_with_path():
    assert (
        linkify("http://example.com/path/to/file")
        == '<a href="http://example.com/path/to/file" rel="nofollow">'
        "http://example.com/path/to/file</a>"
    )


def test_link_ftp():
    assert (
        linkify("ftp://ftp.mozilla.org/some/file")
        == '<a href="ftp://ftp.mozilla.org/some/file" rel="nofollow">'
        "ftp://ftp.mozilla.org/some/file</a>"
    )


def test_link_query():
    assert (
        linkify("http://xx.com/?test=win")
        == '<a href="http://xx.com/?test=win" rel="nofollow">http://xx.com/?test=win</a>'
    )
    assert (
        linkify("xx.com/?test=win")
        == '<a href="http://xx.com/?test=win" rel="nofollow">xx.com/?test=win</a>'
    )
    assert (
        linkify("xx.com?test=win")
        == '<a href="http://xx.com?test=win" rel="nofollow">xx.com?test=win</a>'
    )


def test_link_fragment():
    assert (
        linkify("http://xx.com/path#frag")
        == '<a href="http://xx.com/path#frag" rel="nofollow">http://xx.com/path#frag</a>'
    )


def test_link_entities():
    assert (
        linkify("http://xx.com/?a=1&b=2")
        == '<a href="http://xx.com/?a=1&amp;b=2" rel="nofollow">http://xx.com/?a=1&amp;b=2</a>'
    )


def test_escaped_html():
    """If I pass in escaped HTML, it should probably come out escaped."""
    s = "&lt;em&gt;strong&lt;/em&gt;"
    assert linkify(s) == s


def test_link_http_complete():
    assert (
        linkify("https://user:pass@ftp.mozilla.org/x/y.exe?a=b&c=d&e#f")
        == '<a href="https://user:pass@ftp.mozilla.org/x/y.exe?a=b&amp;c=d&amp;e#f" rel="nofollow">'
        "https://user:pass@ftp.mozilla.org/x/y.exe?a=b&amp;c=d&amp;e#f</a>"
    )


def test_non_url():
    """document.vulnerable should absolutely not be linkified."""
    s = "document.vulnerable"
    assert linkify(s) == s


def test_javascript_url():
    """javascript: urls should never be linkified."""
    s = "javascript:document.vulnerable"
    assert linkify(s) == s


def test_unsafe_url():
    """Any unsafe char ({}[]<>, etc.) in the path should end URL scanning."""
    assert (
        linkify('All your{"xx.yy.com/grover.png"}base are')
        == 'All your{"<a href="http://xx.yy.com/grover.png" rel="nofollow">xx.yy.com/grover.png</a>"}'
        "base are"
    )


def test_skip_tags():
    """Skip linkification in skip tags"""
    simple = "http://xx.com <pre>http://xx.com</pre>"
    linked = (
        '<a href="http://xx.com" rel="nofollow">http://xx.com</a> '
        "<pre>http://xx.com</pre>"
    )
    all_linked = (
        '<a href="http://xx.com" rel="nofollow">http://xx.com</a> '
        '<pre><a href="http://xx.com" rel="nofollow">http://xx.com'
        "</a></pre>"
    )
    assert linkify(simple, skip_tags=["pre"]) == linked
    assert linkify(simple) == all_linked

    already_linked = '<pre><a href="http://xx.com">xx</a></pre>'
    nofollowed = '<pre><a href="http://xx.com" rel="nofollow">xx</a></pre>'
    assert linkify(already_linked) == nofollowed
    assert linkify(already_linked, skip_tags=["pre"]) == nofollowed

    assert linkify(
        "<pre><code>http://example.com</code></pre>http://example.com",
        skip_tags=["pre"],
    ) == (
        "<pre><code>http://example.com</code></pre>"
        '<a href="http://example.com" rel="nofollow">http://example.com</a>'
    )


def test_libgl():
    """libgl.so.1 should not be linkified."""
    s = "libgl.so.1"
    assert linkify(s) == s


@pytest.mark.parametrize(
    "url,periods",
    [
        ("example.com", "."),
        ("example.com", "..."),
        ("ex.com/foo", "."),
        ("ex.com/foo", "...."),
    ],
)
def test_end_of_sentence(url, periods):
    """example.com. should match."""
    out = '<a href="http://{0!s}" rel="nofollow">{0!s}</a>{1!s}'
    intxt = "{0!s}{1!s}"

    assert linkify(intxt.format(url, periods)) == out.format(url, periods)


def test_end_of_clause():
    """example.com/foo, shouldn't include the ,"""
    assert (
        linkify("ex.com/foo, bar")
        == '<a href="http://ex.com/foo" rel="nofollow">ex.com/foo</a>, bar'
    )


def test_sarcasm():
    """Jokes should crash.<sarcasm/>"""
    assert linkify("Yeah right <sarcasm/>") == "Yeah right &lt;sarcasm/&gt;"


@pytest.mark.parametrize(
    "data,expected_data",
    [
        ("(example.com)", ("(", "example.com", "example.com", ")")),
        ("(example.com/)", ("(", "example.com/", "example.com/", ")")),
        ("(example.com/foo)", ("(", "example.com/foo", "example.com/foo", ")")),
        ("(((example.com/))))", ("(((", "example.com/", "example.com/", "))))")),
        ("example.com/))", ("", "example.com/", "example.com/", "))")),
        (
            "(foo http://example.com/)",
            ("(foo ", "example.com/", "http://example.com/", ")"),
        ),
        (
            "(foo http://example.com)",
            ("(foo ", "example.com", "http://example.com", ")"),
        ),
        (
            "http://en.wikipedia.org/wiki/Test_(assessment)",
            (
                "",
                "en.wikipedia.org/wiki/Test_(assessment)",
                "http://en.wikipedia.org/wiki/Test_(assessment)",
                "",
            ),
        ),
        (
            "(http://en.wikipedia.org/wiki/Test_(assessment))",
            (
                "(",
                "en.wikipedia.org/wiki/Test_(assessment)",
                "http://en.wikipedia.org/wiki/Test_(assessment)",
                ")",
            ),
        ),
        (
            "((http://en.wikipedia.org/wiki/Test_(assessment))",
            (
                "((",
                "en.wikipedia.org/wiki/Test_(assessment",
                "http://en.wikipedia.org/wiki/Test_(assessment",
                "))",
            ),
        ),
        (
            "(http://en.wikipedia.org/wiki/Test_(assessment)))",
            (
                "(",
                "en.wikipedia.org/wiki/Test_(assessment))",
                "http://en.wikipedia.org/wiki/Test_(assessment))",
                ")",
            ),
        ),
        (
            "(http://en.wikipedia.org/wiki/)Test_(assessment",
            (
                "(",
                "en.wikipedia.org/wiki/)Test_(assessment",
                "http://en.wikipedia.org/wiki/)Test_(assessment",
                "",
            ),
        ),
        (
            "hello (http://www.mu.de/blah.html) world",
            ("hello (", "www.mu.de/blah.html", "http://www.mu.de/blah.html", ") world"),
        ),
        (
            "hello (http://www.mu.de/blah.html). world",
            (
                "hello (",
                "www.mu.de/blah.html",
                "http://www.mu.de/blah.html",
                "). world",
            ),
        ),
    ],
)
def test_wrapping_parentheses(data, expected_data):
    """URLs wrapped in parantheses should not include them."""
    out = '{0!s}<a href="http://{1!s}" rel="nofollow">{2!s}</a>{3!s}'

    assert linkify(data) == out.format(*expected_data)


def test_parentheses_with_removing():
    expected = "(test.py)"
    assert linkify(expected, callbacks=[lambda *a: None]) == expected


@pytest.mark.parametrize(
    "data,expected_data",
    [
        # Test valid ports
        ("http://foo.com:8000", ("http://foo.com:8000", "")),
        ("http://foo.com:8000/", ("http://foo.com:8000/", "")),
        # Test non ports
        ("http://bar.com:xkcd", ("http://bar.com", ":xkcd")),
        ("http://foo.com:81/bar", ("http://foo.com:81/bar", "")),
        ("http://foo.com:", ("http://foo.com", ":")),
        # Test non-ascii ports
        ("http://foo.com:\u0663\u0669/", ("http://foo.com", ":\u0663\u0669/")),
        (
            "http://foo.com:\U0001d7e0\U0001d7d8/",
            ("http://foo.com", ":\U0001d7e0\U0001d7d8/"),
        ),
    ],
)
def test_ports(data, expected_data):
    """URLs can contain port numbers."""
    out = '<a href="{0}" rel="nofollow">{0}</a>{1}'
    assert linkify(data) == out.format(*expected_data)


def test_ignore_bad_protocols():
    assert linkify("foohttp://bar") == "foohttp://bar"
    assert (
        linkify("fohttp://exampl.com")
        == 'fohttp://<a href="http://exampl.com" rel="nofollow">exampl.com</a>'
    )


def test_link_emails_and_urls():
    """parse_email=True shouldn't prevent URLs from getting linkified."""
    assert linkify("http://example.com person@example.com", parse_email=True) == (
        '<a href="http://example.com" rel="nofollow">'
        'http://example.com</a> <a href="mailto:person@example.com">'
        "person@example.com</a>"
    )


def test_links_case_insensitive():
    """Protocols and domain names are case insensitive."""
    expect = '<a href="HTTP://EXAMPLE.COM" rel="nofollow">HTTP://EXAMPLE.COM</a>'
    assert linkify("HTTP://EXAMPLE.COM") == expect


def test_elements_inside_links():
    assert (
        linkify('<a href="#">hello<br></a>')
        == '<a href="#" rel="nofollow">hello<br></a>'
    )

    assert (
        linkify('<a href="#"><strong>bold</strong> hello<br></a>')
        == '<a href="#" rel="nofollow"><strong>bold</strong> hello<br></a>'
    )


def test_drop_link_tags():
    """Verify that dropping link tags *just* drops the tag and not the content"""
    html = (
        'first <a href="http://example.com/1/">second</a> third <a href="http://example.com/2/">'
        "fourth</a> fifth"
    )
    assert (
        linkify(html, callbacks=[lambda attrs, new: None])
        == "first second third fourth fifth"
    )


@pytest.mark.parametrize(
    "text, expected",
    [
        ("&lt;br&gt;", "&lt;br&gt;"),
        (
            "&lt;br&gt; http://example.com",
            '&lt;br&gt; <a href="http://example.com" rel="nofollow">http://example.com</a>',
        ),
        (
            "&lt;br&gt; <br> http://example.com",
            '&lt;br&gt; <br> <a href="http://example.com" rel="nofollow">http://example.com</a>',
        ),
    ],
)
def test_naughty_unescaping(text, expected):
    """Verify that linkify is not unescaping things it shouldn't be"""
    assert linkify(text) == expected


def test_hang():
    """This string would hang linkify. Issue #200"""
    assert (
        linkify("an@email.com<mailto:an@email.com>", parse_email=True)
        == '<a href="mailto:an@email.com">an@email.com</a>&lt;mailto:<a href="mailto:an@email.com">an@email.com</a>&gt;'  # noqa
    )


def test_hyphen_in_mail():
    """Test hyphens `-` in mails. Issue #300."""
    assert (
        linkify("ex@am-ple.com", parse_email=True)
        == '<a href="mailto:ex@am-ple.com">ex@am-ple.com</a>'
    )


def test_url_re_arg():
    """Verifies that a specified url_re is used"""
    fred_re = re.compile(r"""(fred\.com)""")

    linker = Linker(url_re=fred_re)
    assert (
        linker.linkify("a b c fred.com d e f")
        == 'a b c <a href="http://fred.com" rel="nofollow">fred.com</a> d e f'
    )

    assert (
        linker.linkify("a b c http://example.com d e f")
        == "a b c http://example.com d e f"
    )


def test_email_re_arg():
    """Verifies that a specified email_re is used"""
    fred_re = re.compile(r"""(fred@example\.com)""")

    linker = Linker(parse_email=True, email_re=fred_re)
    assert (
        linker.linkify("a b c fred@example.com d e f")
        == 'a b c <a href="mailto:fred@example.com">fred@example.com</a> d e f'
    )

    assert (
        linker.linkify("a b c jim@example.com d e f") == "a b c jim@example.com d e f"
    )


def test_recognized_tags_arg():
    """Verifies that recognized_tags works"""
    # The html parser doesn't recognize "sarcasm" as a tag, so it escapes it
    linker = Linker(recognized_tags=["p"])
    assert (
        linker.linkify("<p>http://example.com/</p><sarcasm>")
        == '<p><a href="http://example.com/" rel="nofollow">http://example.com/</a></p>&lt;sarcasm&gt;'  # noqa
    )

    # The html parser recognizes "sarcasm" as a tag and fixes it
    linker = Linker(recognized_tags=["p", "sarcasm"])
    assert (
        linker.linkify("<p>http://example.com/</p><sarcasm>")
        == '<p><a href="http://example.com/" rel="nofollow">http://example.com/</a></p><sarcasm></sarcasm>'  # noqa
    )


def test_linkify_idempotent():
    dirty = "<span>invalid & </span> < extra http://link.com<em>"
    assert linkify(linkify(dirty)) == linkify(dirty)


class TestLinkify:
    def test_no_href_links(self):
        s = '<a name="anchor">x</a>'
        assert linkify(s) == s

    def test_rel_already_there(self):
        """Make sure rel attribute is updated not replaced"""
        linked = 'Click <a href="http://example.com" rel="tooltip">' "here</a>."

        link_good = (
            'Click <a href="http://example.com" rel="tooltip nofollow">here</a>.'
        )

        assert linkify(linked) == link_good
        assert linkify(link_good) == link_good

    def test_only_text_is_linkified(self):
        some_text = "text"
        some_type = int
        no_type = None

        assert linkify(some_text) == some_text

        with pytest.raises(TypeError):
            linkify(some_type)

        with pytest.raises(TypeError):
            linkify(no_type)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("abc", "abc"),
        ("example.com", '<a href="http://example.com" rel="nofollow">example.com</a>'),
        (
            "http://example.com?b=1&c=2",
            '<a href="http://example.com?b=1&amp;c=2" rel="nofollow">http://example.com?b=1&amp;c=2</a>',
        ),
        (
            "http://example.com?b=1&amp;c=2",
            '<a href="http://example.com?b=1&amp;c=2" rel="nofollow">http://example.com?b=1&amp;c=2</a>',
        ),
        (
            "link: https://example.com/watch#anchor",
            'link: <a href="https://example.com/watch#anchor" rel="nofollow">https://example.com/watch#anchor</a>',
        ),
    ],
)
def test_linkify_filter(text, expected):
    cleaner = Cleaner(filters=[LinkifyFilter])
    assert cleaner.clean(text) == expected
