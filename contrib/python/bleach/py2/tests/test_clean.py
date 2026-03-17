from __future__ import unicode_literals

import os

import pytest

from bleach import clean
from bleach.html5lib_shim import Filter
from bleach.sanitizer import Cleaner
from html5lib.constants import rcdataElements


def test_clean_idempotent():
    """Make sure that applying the filter twice doesn't change anything."""
    dirty = "<span>invalid & </span> < extra http://link.com<em>"
    assert clean(clean(dirty)) == clean(dirty)


def test_only_text_is_cleaned():
    some_text = "text"
    some_type = int
    no_type = None

    assert clean(some_text) == some_text

    with pytest.raises(TypeError) as e:
        clean(some_type)
    assert "argument cannot be of 'type' type" in str(e.value)

    with pytest.raises(TypeError) as e:
        clean(no_type)
    assert "NoneType" in str(e.value)


def test_empty():
    assert clean("") == ""


def test_content_has_no_html():
    assert clean("no html string") == "no html string"


@pytest.mark.parametrize(
    "data, expected",
    [
        ("an <strong>allowed</strong> tag", "an <strong>allowed</strong> tag"),
        ("another <em>good</em> tag", "another <em>good</em> tag"),
    ],
)
def test_content_has_allowed_html(data, expected):
    assert clean(data) == expected


def test_html_is_lowercased():
    assert (
        clean('<A HREF="http://example.com">foo</A>')
        == '<a href="http://example.com">foo</a>'
    )


def test_invalid_uri_does_not_raise_error():
    assert clean('<a href="http://example.com]">text</a>') == "<a>text</a>"


@pytest.mark.parametrize(
    "data, should_strip, expected",
    [
        # Regular comment
        ("<!-- this is a comment -->", True, ""),
        # Open comment with no close comment bit
        ("<!-- open comment", True, ""),
        ("<!--open comment", True, ""),
        ("<!-- open comment", False, "<!-- open comment-->"),
        ("<!--open comment", False, "<!--open comment-->"),
        # Comment with text to the right
        ("<!-- comment -->text", True, "text"),
        ("<!--comment-->text", True, "text"),
        ("<!-- comment -->text", False, "<!-- comment -->text"),
        ("<!--comment-->text", False, "<!--comment-->text"),
        # Comment with text to the left
        ("text<!-- comment -->", True, "text"),
        ("text<!--comment-->", True, "text"),
        ("text<!-- comment -->", False, "text<!-- comment -->"),
        ("text<!--comment-->", False, "text<!--comment-->"),
    ],
)
def test_comments(data, should_strip, expected):
    assert clean(data, strip_comments=should_strip) == expected


def test_invalid_char_in_tag():
    assert (
        clean('<script/xss src="http://xx.com/xss.js"></script>')
        == '&lt;script/xss src="http://xx.com/xss.js"&gt;&lt;/script&gt;'
    )
    assert (
        clean('<script/src="http://xx.com/xss.js"></script>')
        == '&lt;script/src="http://xx.com/xss.js"&gt;&lt;/script&gt;'
    )


def test_unclosed_tag():
    assert clean("a <em>fixed tag") == "a <em>fixed tag</em>"
    assert (
        clean("<script src=http://xx.com/xss.js<b>")
        == "&lt;script src=http://xx.com/xss.js&lt;b&gt;"
    )
    assert (
        clean('<script src="http://xx.com/xss.js"<b>')
        == '&lt;script src="http://xx.com/xss.js"&lt;b&gt;'
    )
    assert (
        clean('<script src="http://xx.com/xss.js" <b>')
        == '&lt;script src="http://xx.com/xss.js" &lt;b&gt;'
    )


def test_nested_script_tag():
    assert (
        clean("<<script>script>evil()<</script>/script>")
        == "&lt;&lt;script&gt;script&gt;evil()&lt;&lt;/script&gt;/script&gt;"
    )
    assert (
        clean("<<x>script>evil()<</x>/script>")
        == "&lt;&lt;x&gt;script&gt;evil()&lt;&lt;/x&gt;/script&gt;"
    )
    assert (
        clean("<script<script>>evil()</script</script>>")
        == "&lt;script&lt;script&gt;&gt;evil()&lt;/script&lt;/script&gt;&gt;"
    )


@pytest.mark.parametrize(
    "text, expected",
    [
        ("an & entity", "an &amp; entity"),
        ("an < entity", "an &lt; entity"),
        ("tag < <em>and</em> entity", "tag &lt; <em>and</em> entity"),
    ],
)
def test_bare_entities_get_escaped_correctly(text, expected):
    assert clean(text) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        # Test character entities
        ("&amp;", "&amp;"),
        ("&nbsp;", "&nbsp;"),
        ("&nbsp; test string &nbsp;", "&nbsp; test string &nbsp;"),
        ("&lt;em&gt;strong&lt;/em&gt;", "&lt;em&gt;strong&lt;/em&gt;"),
        # Test character entity at beginning of string
        ("&amp;is cool", "&amp;is cool"),
        # Test it at the end of the string
        ("cool &amp;", "cool &amp;"),
        # Test bare ampersands and entities at beginning
        ("&&amp; is cool", "&amp;&amp; is cool"),
        # Test entities and bare ampersand at end
        ("&amp; is cool &amp;&", "&amp; is cool &amp;&amp;"),
        # Test missing semi-colon means we don't treat it like an entity
        ("this &amp that", "this &amp;amp that"),
        # Test a thing that looks like a character entity, but isn't because it's
        # missing a ; (&current)
        (
            "http://example.com?active=true&current=true",
            "http://example.com?active=true&amp;current=true",
        ),
        # Test character entities in attribute values are left alone
        ('<a href="?art&amp;copy">foo</a>', '<a href="?art&amp;copy">foo</a>'),
        ('<a href="?this=&gt;that">foo</a>', '<a href="?this=&gt;that">foo</a>'),
        # Ambiguous ampersands get escaped in attributes
        (
            '<a href="http://example.com/&xx;">foo</a>',
            '<a href="http://example.com/&amp;xx;">foo</a>',
        ),
        (
            '<a href="http://example.com?active=true&current=true">foo</a>',
            '<a href="http://example.com?active=true&amp;current=true">foo</a>',
        ),
        # Ambiguous ampersands in text are not escaped
        ("&xx;", "&xx;"),
        # Test numeric entities
        ("&#39;", "&#39;"),
        ("&#34;", "&#34;"),
        ("&#123;", "&#123;"),
        ("&#x0007b;", "&#x0007b;"),
        ("&#x0007B;", "&#x0007B;"),
        # Test non-numeric entities
        ("&#", "&amp;#"),
        ("&#<", "&amp;#&lt;"),
        # html5lib tokenizer unescapes character entities, so these would become '
        # and " which makes it possible to break out of html attributes.
        #
        # Verify that clean() doesn't unescape entities.
        ("&#39;&#34;", "&#39;&#34;"),
    ],
)
def test_character_entities_handling(text, expected):
    assert clean(text) == expected


@pytest.mark.parametrize(
    "data, kwargs, expected",
    [
        # All tags are allowed, so it strips nothing
        (
            "a test <em>with</em> <b>html</b> tags",
            {},
            "a test <em>with</em> <b>html</b> tags",
        ),
        # img tag is disallowed, so it's stripped
        (
            'a test <em>with</em> <img src="http://example.com/"> <b>html</b> tags',
            {},
            "a test <em>with</em>  <b>html</b> tags",
        ),
        # a tag is disallowed, so it's stripped
        (
            '<p><a href="http://example.com/">link text</a></p>',
            {"tags": ["p"]},
            "<p>link text</p>",
        ),
        # Test nested disallowed tag
        (
            "<p><span>multiply <span>nested <span>text</span></span></span></p>",
            {"tags": ["p"]},
            "<p>multiply nested text</p>",
        ),
        # (#271)
        ("<ul><li><script></li></ul>", {"tags": ["ul", "li"]}, "<ul><li></li></ul>"),
        # Test disallowed tag that's deep in the tree
        (
            '<p><a href="http://example.com/"><img src="http://example.com/"></a></p>',
            {"tags": ["a", "p"]},
            '<p><a href="http://example.com/"></a></p>',
        ),
        # Test isindex -- the parser expands this to a prompt (#279)
        ("<isindex>", {}, ""),
        # Test non-tags that are well-formed HTML (#280)
        ("Yeah right <sarcasm/>", {}, "Yeah right "),
        ("<sarcasm>", {}, ""),
        ("</sarcasm>", {}, ""),
        # These are non-tags, but also "malformed" so they don't get treated like
        # tags and stripped
        ("</ sarcasm>", {}, "&lt;/ sarcasm&gt;"),
        ("</ sarcasm >", {}, "&lt;/ sarcasm &gt;"),
        ("Foo <bar@example.com>", {}, "Foo "),
        ("Favorite movie: <name of movie>", {}, "Favorite movie: "),
        ("</3", {}, "&lt;/3"),
    ],
)
def test_stripping_tags(data, kwargs, expected):
    assert clean(data, strip=True, **kwargs) == expected
    assert clean("  " + data + "  ", strip=True, **kwargs) == "  " + expected + "  "
    assert (
        clean("abc " + data + " def", strip=True, **kwargs)
        == "abc " + expected + " def"
    )


@pytest.mark.parametrize(
    "data, expected",
    [
        # Disallowed tag is escaped
        (
            "<img src=\"javascript:alert('XSS');\">",
            "&lt;img src=\"javascript:alert('XSS');\"&gt;",
        ),
        # Test with parens
        ("<script>safe()</script>", "&lt;script&gt;safe()&lt;/script&gt;"),
        # Test with braces
        ("<style>body{}</style>", "&lt;style&gt;body{}&lt;/style&gt;"),
        # Test nested disallow tags (#271)
        ("<ul><li><script></li></ul>", "<ul><li>&lt;script&gt;</li></ul>"),
        # Test isindex -- the parser expands this to a prompt (#279)
        ("<isindex>", "&lt;isindex&gt;"),
        # Test non-tags (#280)
        ("<sarcasm/>", "&lt;sarcasm/&gt;"),
        ("<sarcasm>", "&lt;sarcasm&gt;"),
        ("</sarcasm>", "&lt;/sarcasm&gt;"),
        ("</ sarcasm>", "&lt;/ sarcasm&gt;"),
        ("</ sarcasm >", "&lt;/ sarcasm &gt;"),
        ("</3", "&lt;/3"),
        ("<bar@example.com>", "&lt;bar@example.com&gt;"),
        ("Favorite movie: <name of movie>", "Favorite movie: &lt;name of movie&gt;"),
    ],
)
def test_escaping_tags(data, expected):
    assert clean(data, strip=False) == expected
    assert clean("  " + data + "  ", strip=False) == "  " + expected + "  "
    assert clean("abc " + data + " def", strip=False) == "abc " + expected + " def"


@pytest.mark.parametrize(
    "data, expected",
    [
        ("<scri<script>pt>alert(1)</scr</script>ipt>", "pt&gt;alert(1)ipt&gt;"),
        ("<scri<scri<script>pt>pt>alert(1)</script>", "pt&gt;pt&gt;alert(1)"),
    ],
)
def test_stripping_tags_is_safe(data, expected):
    """Test stripping tags shouldn't result in malicious content"""
    assert clean(data, strip=True) == expected


def test_allowed_styles():
    """Test allowed styles"""
    ATTRS = ["style"]
    STYLE = ["color"]

    assert clean('<b style="top:0"></b>', attributes=ATTRS) == '<b style=""></b>'

    text = '<b style="color: blue;"></b>'
    assert clean(text, attributes=ATTRS, styles=STYLE) == text

    text = '<b style="top: 0; color: blue;"></b>'
    assert clean(text, attributes=ATTRS, styles=STYLE) == '<b style="color: blue;"></b>'


def test_href_with_wrong_tag():
    assert clean('<em href="fail">no link</em>') == "<em>no link</em>"


def test_disallowed_attr():
    IMG = [
        "img",
    ]
    IMG_ATTR = ["src"]

    assert clean('<a onclick="evil" href="test">test</a>') == '<a href="test">test</a>'
    assert (
        clean('<img onclick="evil" src="test" />', tags=IMG, attributes=IMG_ATTR)
        == '<img src="test">'
    )
    assert (
        clean('<img href="invalid" src="test" />', tags=IMG, attributes=IMG_ATTR)
        == '<img src="test">'
    )


def test_unquoted_attr_values_are_quoted():
    assert (
        clean("<abbr title=mytitle>myabbr</abbr>")
        == '<abbr title="mytitle">myabbr</abbr>'
    )


def test_unquoted_event_handler_attr_value():
    assert (
        clean('<a href="http://xx.com" onclick=foo()>xx.com</a>')
        == '<a href="http://xx.com">xx.com</a>'
    )


def test_invalid_filter_attr():
    IMG = [
        "img",
    ]
    IMG_ATTR = {
        "img": lambda tag, name, val: name == "src" and val == "http://example.com/"
    }

    assert (
        clean(
            '<img onclick="evil" src="http://example.com/" />',
            tags=IMG,
            attributes=IMG_ATTR,
        )
        == '<img src="http://example.com/">'
    )
    assert (
        clean(
            '<img onclick="evil" src="http://badhost.com/" />',
            tags=IMG,
            attributes=IMG_ATTR,
        )
        == "<img>"
    )


def test_poster_attribute():
    """Poster attributes should not allow javascript."""
    tags = ["video"]
    attrs = {"video": ["poster"]}

    test = '<video poster="javascript:alert(1)"></video>'
    assert clean(test, tags=tags, attributes=attrs) == "<video></video>"

    ok = '<video poster="/foo.png"></video>'
    assert clean(ok, tags=tags, attributes=attrs) == ok


def test_attributes_callable():
    """Verify attributes can take a callable"""
    ATTRS = lambda tag, name, val: name == "title"
    TAGS = ["a"]

    text = '<a href="/foo" title="blah">example</a>'
    assert clean(text, tags=TAGS, attributes=ATTRS) == '<a title="blah">example</a>'


def test_attributes_wildcard():
    """Verify attributes[*] works"""
    ATTRS = {
        "*": ["id"],
        "img": ["src"],
    }
    TAGS = ["img", "em"]

    text = (
        'both <em id="foo" style="color: black">can</em> have <img id="bar" src="foo"/>'
    )
    assert (
        clean(text, tags=TAGS, attributes=ATTRS)
        == 'both <em id="foo">can</em> have <img id="bar" src="foo">'
    )


def test_attributes_wildcard_callable():
    """Verify attributes[*] callable works"""
    ATTRS = {"*": lambda tag, name, val: name == "title"}
    TAGS = ["a"]

    assert (
        clean('<a href="/foo" title="blah">example</a>', tags=TAGS, attributes=ATTRS)
        == '<a title="blah">example</a>'
    )


def test_attributes_tag_callable():
    """Verify attributes[tag] callable works"""

    def img_test(tag, name, val):
        return name == "src" and val.startswith("https")

    ATTRS = {
        "img": img_test,
    }
    TAGS = ["img"]

    text = 'foo <img src="http://example.com" alt="blah"> baz'
    assert clean(text, tags=TAGS, attributes=ATTRS) == "foo <img> baz"
    text = 'foo <img src="https://example.com" alt="blah"> baz'
    assert (
        clean(text, tags=TAGS, attributes=ATTRS)
        == 'foo <img src="https://example.com"> baz'
    )


def test_attributes_tag_list():
    """Verify attributes[tag] list works"""
    ATTRS = {"a": ["title"]}
    TAGS = ["a"]

    assert (
        clean('<a href="/foo" title="blah">example</a>', tags=TAGS, attributes=ATTRS)
        == '<a title="blah">example</a>'
    )


def test_attributes_list():
    """Verify attributes list works"""
    ATTRS = ["title"]
    TAGS = ["a"]

    text = '<a href="/foo" title="blah">example</a>'
    assert clean(text, tags=TAGS, attributes=ATTRS) == '<a title="blah">example</a>'


@pytest.mark.parametrize(
    "data, kwargs, expected",
    [
        # javascript: is not allowed by default
        ("<a href=\"javascript:alert('XSS')\">xss</a>", {}, "<a>xss</a>"),
        # File protocol is not allowed by default
        ('<a href="file:///tmp/foo">foo</a>', {}, "<a>foo</a>"),
        # Specified protocols are allowed
        (
            '<a href="myprotocol://more_text">allowed href</a>',
            {"protocols": ["myprotocol"]},
            '<a href="myprotocol://more_text">allowed href</a>',
        ),
        # Unspecified protocols are not allowed
        (
            '<a href="http://example.com">invalid href</a>',
            {"protocols": ["myprotocol"]},
            "<a>invalid href</a>",
        ),
        # Anchors are ok
        (
            '<a href="#example.com">foo</a>',
            {"protocols": []},
            '<a href="#example.com">foo</a>',
        ),
        # Allow implicit http if allowed
        (
            '<a href="example.com">valid</a>',
            {"protocols": ["http"]},
            '<a href="example.com">valid</a>',
        ),
        (
            '<a href="example.com:8000">valid</a>',
            {"protocols": ["http"]},
            '<a href="example.com:8000">valid</a>',
        ),
        (
            '<a href="localhost">valid</a>',
            {"protocols": ["http"]},
            '<a href="localhost">valid</a>',
        ),
        (
            '<a href="localhost:8000">valid</a>',
            {"protocols": ["http"]},
            '<a href="localhost:8000">valid</a>',
        ),
        (
            '<a href="192.168.100.100">valid</a>',
            {"protocols": ["http"]},
            '<a href="192.168.100.100">valid</a>',
        ),
        (
            '<a href="192.168.100.100:8000">valid</a>',
            {"protocols": ["http"]},
            '<a href="192.168.100.100:8000">valid</a>',
        ),
        # Disallow implicit http if disallowed
        ('<a href="example.com">foo</a>', {"protocols": []}, "<a>foo</a>"),
        ('<a href="example.com:8000">foo</a>', {"protocols": []}, "<a>foo</a>"),
        ('<a href="localhost">foo</a>', {"protocols": []}, "<a>foo</a>"),
        ('<a href="localhost:8000">foo</a>', {"protocols": []}, "<a>foo</a>"),
        ('<a href="192.168.100.100">foo</a>', {"protocols": []}, "<a>foo</a>"),
        ('<a href="192.168.100.100:8000">foo</a>', {"protocols": []}, "<a>foo</a>"),
        # Disallowed protocols with sneaky character entities
        ('<a href="javas&#x09;cript:alert(1)">alert</a>', {}, "<a>alert</a>"),
        ('<a href="&#14;javascript:alert(1)">alert</a>', {}, "<a>alert</a>"),
        # Checking the uri should change it at all
        (
            '<a href="http://example.com/?foo&nbsp;bar">foo</a>',
            {},
            '<a href="http://example.com/?foo&nbsp;bar">foo</a>',
        ),
    ],
)
def test_uri_value_allowed_protocols(data, kwargs, expected):
    assert clean(data, **kwargs) == expected


def test_svg_attr_val_allows_ref():
    """Unescape values in svg attrs that allow url references"""
    # Local IRI, so keep it
    TAGS = ["svg", "rect"]
    ATTRS = {
        "rect": ["fill"],
    }

    text = '<svg><rect fill="url(#foo)" /></svg>'
    assert (
        clean(text, tags=TAGS, attributes=ATTRS)
        == '<svg><rect fill="url(#foo)"></rect></svg>'
    )

    # Non-local IRI, so drop it
    TAGS = ["svg", "rect"]
    ATTRS = {
        "rect": ["fill"],
    }
    text = '<svg><rect fill="url(http://example.com#foo)" /></svg>'
    assert clean(text, tags=TAGS, attributes=ATTRS) == "<svg><rect></rect></svg>"


@pytest.mark.parametrize(
    "text, expected",
    [
        (
            '<svg><pattern id="patt1" href="#patt2"></pattern></svg>',
            '<svg><pattern href="#patt2" id="patt1"></pattern></svg>',
        ),
        (
            '<svg><pattern id="patt1" xlink:href="#patt2"></pattern></svg>',
            # NOTE(willkg): Bug in html5lib serializer drops the xlink part
            '<svg><pattern id="patt1" href="#patt2"></pattern></svg>',
        ),
    ],
)
def test_svg_allow_local_href(text, expected):
    """Keep local hrefs for svg elements"""
    TAGS = ["svg", "pattern"]
    ATTRS = {
        "pattern": ["id", "href"],
    }
    assert clean(text, tags=TAGS, attributes=ATTRS) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        (
            '<svg><pattern id="patt1" href="https://example.com/patt"></pattern></svg>',
            '<svg><pattern id="patt1"></pattern></svg>',
        ),
        (
            '<svg><pattern id="patt1" xlink:href="https://example.com/patt"></pattern></svg>',
            '<svg><pattern id="patt1"></pattern></svg>',
        ),
    ],
)
def test_svg_allow_local_href_nonlocal(text, expected):
    """Drop non-local hrefs for svg elements"""
    TAGS = ["svg", "pattern"]
    ATTRS = {
        "pattern": ["id", "href"],
    }
    assert clean(text, tags=TAGS, attributes=ATTRS) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        # Convert bell
        ("1\a23", "1?23"),
        # Convert backpsace
        ("1\b23", "1?23"),
        # Convert formfeed
        ("1\v23", "1?23"),
        # Convert vertical tab
        ("1\f23", "1?23"),
        # Convert a bunch of characters in a string
        ("import y\bose\bm\bi\bt\be\b", "import y?ose?m?i?t?e?"),
    ],
)
def test_invisible_characters(data, expected):
    assert clean(data) == expected


def test_nonexistent_namespace():
    # Issue #352 involved this string kicking up a KeyError since the "c"
    # namespace didn't exist. After the fixes for Bleach 3.0, this no longer
    # goes through the HTML parser as a tag, so it doesn't tickle the bad
    # namespace code.
    assert clean("<d {c}>") == "&lt;d {c}&gt;"


@pytest.mark.parametrize(
    "tag",
    [
        "area",
        "base",
        "br",
        "embed",
        "hr",
        "img",
        "input",
        pytest.param(
            "keygen",
            marks=pytest.mark.xfail(
                reason="https://github.com/mozilla/bleach/issues/488"
            ),
        ),
        "link",
        "meta",
        "param",
        "source",
        pytest.param(
            "menuitem",
            marks=pytest.mark.xfail(
                reason="https://github.com/mozilla/bleach/issues/488"
            ),
        ),
        "track",
        pytest.param(
            "wbr",
            marks=pytest.mark.xfail(
                reason="https://github.com/mozilla/bleach/issues/488"
            ),
        ),
    ],
)
def test_self_closing_tags_self_close(tag):
    assert clean("<%s>" % tag, tags=[tag]) == "<%s>" % tag


# tags that get content passed through (i.e. parsed with parseRCDataRawtext)
_raw_tags = [
    "title",
    "textarea",
    "script",
    "style",
    "noembed",
    "noframes",
    "iframe",
    "xmp",
]


@pytest.mark.parametrize(
    "raw_tag, data, expected",
    [
        (
            raw_tag,
            "<noscript><%s></noscript><img src=x onerror=alert(1) />" % raw_tag,
            "<noscript>&lt;%s&gt;</noscript>&lt;img src=x onerror=alert(1) /&gt;"
            % raw_tag,
        )
        for raw_tag in _raw_tags
    ],
)
def test_noscript_rawtag_(raw_tag, data, expected):
    # refs: bug 1615315 / GHSA-q65m-pv3f-wr5r
    assert clean(data, tags=["noscript", raw_tag]) == expected


@pytest.mark.parametrize(
    "namespace_tag, rc_data_element_tag, data, expected",
    [
        (
            namespace_tag,
            rc_data_element_tag,
            "<%s><%s><img src=x onerror=alert(1)>"
            % (namespace_tag, rc_data_element_tag),
            "<%s><%s>&lt;img src=x onerror=alert(1)&gt;</%s></%s>"
            % (namespace_tag, rc_data_element_tag, rc_data_element_tag, namespace_tag),
        )
        for namespace_tag in ["math", "svg"]
        # https://dev.w3.org/html5/html-author/#rcdata-elements
        # https://html.spec.whatwg.org/index.html#parsing-html-fragments
        # in html5lib: 'style', 'script', 'xmp', 'iframe', 'noembed', 'noframes', and 'noscript'
        for rc_data_element_tag in rcdataElements
    ],
)
def test_namespace_rc_data_element_strip_false(
    namespace_tag, rc_data_element_tag, data, expected
):
    # refs: bug 1621692 / GHSA-m6xf-fq7q-8743
    #
    # browsers will pull the img out of the namespace and rc data tag resulting in XSS
    assert (
        clean(data, tags=[namespace_tag, rc_data_element_tag], strip=False) == expected
    )


@pytest.mark.parametrize(
    "namespace_tag, end_tag, eject_tag, data, expected",
    [
        # eject with style
        (
            "math",
            "p",
            "style",
            "<math></p><style><!--</style><img src/onerror=alert(1)>",
            "<math><p></p><style><!--&lt;/style&gt;&lt;img src/onerror=alert(1)&gt;--></style></math>",
        ),
        (
            "math",
            "br",
            "style",
            "<math></br><style><!--</style><img src/onerror=alert(1)>",
            "<math><br><style><!--&lt;/style&gt;&lt;img src/onerror=alert(1)&gt;--></style></math>",
        ),
        (
            "svg",
            "p",
            "style",
            "<svg></p><style><!--</style><img src/onerror=alert(1)>",
            "<svg><p></p><style><!--&lt;/style&gt;&lt;img src/onerror=alert(1)&gt;--></style></svg>",
        ),
        (
            "svg",
            "br",
            "style",
            "<svg></br><style><!--</style><img src/onerror=alert(1)>",
            "<svg><br><style><!--&lt;/style&gt;&lt;img src/onerror=alert(1)&gt;--></style></svg>",
        ),
        # eject with title
        (
            "math",
            "p",
            "title",
            "<math></p><title><!--</title><img src/onerror=alert(1)>",
            "<math><p></p><title><!--&lt;/title&gt;&lt;img src/onerror=alert(1)&gt;--></title></math>",
        ),
        (
            "math",
            "br",
            "title",
            "<math></br><title><!--</title><img src/onerror=alert(1)>",
            "<math><br><title><!--&lt;/title&gt;&lt;img src/onerror=alert(1)&gt;--></title></math>",
        ),
        (
            "svg",
            "p",
            "title",
            "<svg></p><title><!--</title><img src/onerror=alert(1)>",
            "<svg><p></p><title><!--&lt;/title&gt;&lt;img src/onerror=alert(1)&gt;--></title></svg>",
        ),
        (
            "svg",
            "br",
            "title",
            "<svg></br><title><!--</title><img src/onerror=alert(1)>",
            "<svg><br><title><!--&lt;/title&gt;&lt;img src/onerror=alert(1)&gt;--></title></svg>",
        ),
        # eject with noscript
        (
            "math",
            "p",
            "noscript",
            "<math></p><noscript><!--</noscript><img src/onerror=alert(1)>",
            "<math><p></p><noscript><!--&lt;/noscript&gt;&lt;img src/onerror=alert(1)&gt;--></noscript></math>",
        ),
        (
            "math",
            "br",
            "noscript",
            "<math></br><noscript><!--</noscript><img src/onerror=alert(1)>",
            "<math><br><noscript><!--&lt;/noscript&gt;&lt;img src/onerror=alert(1)&gt;--></noscript></math>",
        ),
        (
            "svg",
            "p",
            "noscript",
            "<svg></p><noscript><!--</noscript><img src/onerror=alert(1)>",
            "<svg><p></p><noscript><!--&lt;/noscript&gt;&lt;img src/onerror=alert(1)&gt;--></noscript></svg>",
        ),
        (
            "svg",
            "br",
            "noscript",
            "<svg></br><noscript><!--</noscript><img src/onerror=alert(1)>",
            "<svg><br><noscript><!--&lt;/noscript&gt;&lt;img src/onerror=alert(1)&gt;--></noscript></svg>",
        ),
        # eject with script
        (
            "math",
            "p",
            "script",
            "<math></p><script><!--</script><img src/onerror=alert(1)>",
            "<math><p></p><script><!--&lt;/script&gt;&lt;img src/onerror=alert(1)&gt;--></script></math>",
        ),
        (
            "math",
            "br",
            "script",
            "<math></br><script><!--</script><img src/onerror=alert(1)>",
            "<math><br><script><!--&lt;/script&gt;&lt;img src/onerror=alert(1)&gt;--></script></math>",
        ),
        (
            "svg",
            "p",
            "script",
            "<svg></p><script><!--</script><img src/onerror=alert(1)>",
            "<svg><p></p><script><!--&lt;/script&gt;&lt;img src/onerror=alert(1)&gt;--></script></svg>",
        ),
        (
            "svg",
            "br",
            "script",
            "<svg></br><script><!--</script><img src/onerror=alert(1)>",
            "<svg><br><script><!--&lt;/script&gt;&lt;img src/onerror=alert(1)&gt;--></script></svg>",
        ),
        # eject with noembed
        (
            "math",
            "p",
            "noembed",
            "<math></p><noembed><!--</noembed><img src/onerror=alert(1)>",
            "<math><p></p><noembed><!--&lt;/noembed&gt;&lt;img src/onerror=alert(1)&gt;--></noembed></math>",
        ),
        (
            "math",
            "br",
            "noembed",
            "<math></br><noembed><!--</noembed><img src/onerror=alert(1)>",
            "<math><br><noembed><!--&lt;/noembed&gt;&lt;img src/onerror=alert(1)&gt;--></noembed></math>",
        ),
        (
            "svg",
            "p",
            "noembed",
            "<svg></p><noembed><!--</noembed><img src/onerror=alert(1)>",
            "<svg><p></p><noembed><!--&lt;/noembed&gt;&lt;img src/onerror=alert(1)&gt;--></noembed></svg>",
        ),
        (
            "svg",
            "br",
            "noembed",
            "<svg></br><noembed><!--</noembed><img src/onerror=alert(1)>",
            "<svg><br><noembed><!--&lt;/noembed&gt;&lt;img src/onerror=alert(1)&gt;--></noembed></svg>",
        ),
        # eject with textarea
        (
            "math",
            "p",
            "textarea",
            "<math></p><textarea><!--</textarea><img src/onerror=alert(1)>",
            "<math><p></p><textarea><!--&lt;/textarea&gt;&lt;img src/onerror=alert(1)&gt;--></textarea></math>",
        ),
        (
            "math",
            "br",
            "textarea",
            "<math></br><textarea><!--</textarea><img src/onerror=alert(1)>",
            "<math><br><textarea><!--&lt;/textarea&gt;&lt;img src/onerror=alert(1)&gt;--></textarea></math>",
        ),
        (
            "svg",
            "p",
            "textarea",
            "<svg></p><textarea><!--</textarea><img src/onerror=alert(1)>",
            "<svg><p></p><textarea><!--&lt;/textarea&gt;&lt;img src/onerror=alert(1)&gt;--></textarea></svg>",
        ),
        (
            "svg",
            "br",
            "textarea",
            "<svg></br><textarea><!--</textarea><img src/onerror=alert(1)>",
            "<svg><br><textarea><!--&lt;/textarea&gt;&lt;img src/onerror=alert(1)&gt;--></textarea></svg>",
        ),
        # eject with noframes
        (
            "math",
            "p",
            "noframes",
            "<math></p><noframes><!--</noframes><img src/onerror=alert(1)>",
            "<math><p></p><noframes><!--&lt;/noframes&gt;&lt;img src/onerror=alert(1)&gt;--></noframes></math>",
        ),
        (
            "math",
            "br",
            "noframes",
            "<math></br><noframes><!--</noframes><img src/onerror=alert(1)>",
            "<math><br><noframes><!--&lt;/noframes&gt;&lt;img src/onerror=alert(1)&gt;--></noframes></math>",
        ),
        (
            "svg",
            "p",
            "noframes",
            "<svg></p><noframes><!--</noframes><img src/onerror=alert(1)>",
            "<svg><p></p><noframes><!--&lt;/noframes&gt;&lt;img src/onerror=alert(1)&gt;--></noframes></svg>",
        ),
        (
            "svg",
            "br",
            "noframes",
            "<svg></br><noframes><!--</noframes><img src/onerror=alert(1)>",
            "<svg><br><noframes><!--&lt;/noframes&gt;&lt;img src/onerror=alert(1)&gt;--></noframes></svg>",
        ),
        # eject with iframe
        (
            "math",
            "p",
            "iframe",
            "<math></p><iframe><!--</iframe><img src/onerror=alert(1)>",
            "<math><p></p><iframe><!--&lt;/iframe&gt;&lt;img src/onerror=alert(1)&gt;--></iframe></math>",
        ),
        (
            "math",
            "br",
            "iframe",
            "<math></br><iframe><!--</iframe><img src/onerror=alert(1)>",
            "<math><br><iframe><!--&lt;/iframe&gt;&lt;img src/onerror=alert(1)&gt;--></iframe></math>",
        ),
        (
            "svg",
            "p",
            "iframe",
            "<svg></p><iframe><!--</iframe><img src/onerror=alert(1)>",
            "<svg><p></p><iframe><!--&lt;/iframe&gt;&lt;img src/onerror=alert(1)&gt;--></iframe></svg>",
        ),
        (
            "svg",
            "br",
            "iframe",
            "<svg></br><iframe><!--</iframe><img src/onerror=alert(1)>",
            "<svg><br><iframe><!--&lt;/iframe&gt;&lt;img src/onerror=alert(1)&gt;--></iframe></svg>",
        ),
        # eject with xmp
        (
            "math",
            "p",
            "xmp",
            "<math></p><xmp><!--</xmp><img src/onerror=alert(1)>",
            "<math><p></p><xmp><!--&lt;/xmp&gt;&lt;img src/onerror=alert(1)&gt;--></xmp></math>",
        ),
        (
            "math",
            "br",
            "xmp",
            "<math></br><xmp><!--</xmp><img src/onerror=alert(1)>",
            "<math><br><xmp><!--&lt;/xmp&gt;&lt;img src/onerror=alert(1)&gt;--></xmp></math>",
        ),
        (
            "svg",
            "p",
            "xmp",
            "<svg></p><xmp><!--</xmp><img src/onerror=alert(1)>",
            "<svg><p></p><xmp><!--&lt;/xmp&gt;&lt;img src/onerror=alert(1)&gt;--></xmp></svg>",
        ),
        (
            "svg",
            "br",
            "xmp",
            "<svg></br><xmp><!--</xmp><img src/onerror=alert(1)>",
            "<svg><br><xmp><!--&lt;/xmp&gt;&lt;img src/onerror=alert(1)&gt;--></xmp></svg>",
        ),
    ],
)
def test_html_comments_escaped(namespace_tag, end_tag, eject_tag, data, expected):
    # refs: bug 1689399 / GHSA-vv2x-vrpj-qqpq
    #
    # p and br can be just an end tag (e.g. </p> == <p></p>)
    #
    # In browsers:
    #
    # * img and other tags break out of the svg or math namespace (e.g. <svg><img></svg> == <svg><img></svg>)
    # * style does not (e.g. <svg><style></svg> == <svg><style></style></svg>)
    # * style and other tags without child elements does not (e.g. <svg><style></svg> == <svg><style></style></svg>)
    # * the breaking tag ejects trailing elements (e.g. <svg><img><style></style></svg> == <svg></svg><img><style></style>)
    #
    # the ejected elements can trigger XSS
    assert (
        clean(data, tags=[namespace_tag, end_tag, eject_tag], strip_comments=False)
        == expected
    )


def get_ids_and_tests():
    """Retrieves regression tests from data/ directory

    :returns: list of ``(id, filedata)`` tuples

    """
    import yatest.common
    datadir = yatest.common.test_source_path("data")
    tests = [
        os.path.join(datadir, fn) for fn in os.listdir(datadir) if fn.endswith(".test")
    ]
    # Sort numerically which makes it easier to iterate through them
    tests.sort(key=lambda x: int(os.path.basename(x).split(".", 1)[0]))

    testcases = []
    for fn in tests:
        with open(fn) as fp:
            data = fp.read()
        testcases.append((os.path.basename(fn), data))

    return testcases


_regression_ids_and_tests = get_ids_and_tests()
_regression_ids = [item[0] for item in _regression_ids_and_tests]
_regression_tests = [item[1] for item in _regression_ids_and_tests]


@pytest.mark.parametrize("test_case", _regression_tests, ids=_regression_ids)
def test_regressions(test_case):
    """Regression tests for clean so we can see if there are issues"""
    test_data, expected = test_case.split("\n--\n")

    # NOTE(willkg): This strips input and expected which makes it easier to
    # maintain the files. If there comes a time when the input needs whitespace
    # at the beginning or end, then we'll have to figure out something else.
    test_data = test_data.strip()
    expected = expected.strip()

    assert clean(test_data) == expected


class TestCleaner:
    def test_basics(self):
        TAGS = ["span", "br"]
        ATTRS = {"span": ["style"]}

        cleaner = Cleaner(tags=TAGS, attributes=ATTRS)

        assert (
            cleaner.clean('a <br/><span style="color:red">test</span>')
            == 'a <br><span style="">test</span>'
        )

    def test_filters(self):
        # Create a Filter that changes all the attr values to "moo"
        class MooFilter(Filter):
            def __iter__(self):
                for token in Filter.__iter__(self):
                    if token["type"] in ["StartTag", "EmptyTag"] and token["data"]:
                        for attr, value in token["data"].items():
                            token["data"][attr] = "moo"

                    yield token

        ATTRS = {"img": ["rel", "src"]}
        TAGS = ["img"]

        cleaner = Cleaner(tags=TAGS, attributes=ATTRS, filters=[MooFilter])

        dirty = 'this is cute! <img src="http://example.com/puppy.jpg" rel="nofollow">'
        assert cleaner.clean(dirty) == 'this is cute! <img rel="moo" src="moo">'
