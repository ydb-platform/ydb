import pytest

from bleach import html5lib_shim


@pytest.mark.parametrize(
    "data, expected",
    [
        # Strings without character entities pass through as is
        ("", ""),
        ("abc", "abc"),
        # Handles character entities--both named and numeric
        ("&nbsp;", "\xa0"),
        ("&#32;", " "),
        ("&#x20;", " "),
        # Handles ambiguous ampersand
        ("&xx;", "&xx;"),
        # Handles multiple entities in the same string
        ("this &amp; that &amp; that", "this & that & that"),
        # Handles empty decimal and hex encoded code points
        ("&#x;", "&#x;"),
        ("&#;", "&#;"),
        # Handles too high unicode points
        ("&#x110000;", "&#x110000;"),
        ("&#x110111;", "&#x110111;"),
        ("&#9277809;", "&#9277809;"),
        # Handles negative unicode points
        ("&#-1;", "&#-1;"),
        ("&#x-1;", "&#x-1;"),
    ],
)
def test_convert_entities(data, expected):
    assert html5lib_shim.convert_entities(data) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        ("", ""),
        ("text", "text"),
        # & in Characters is escaped
        ("&", "&amp;"),
        # FIXME(willkg): This happens because the BleachHTMLTokenizer is ignoring
        # character entities. What it should be doing is creating Entity tokens
        # for character entities.
        #
        # That was too hard at the time I was fixing it, so I fixed it in
        # BleachSanitizerFilter. When that gest fixed correctly in the tokenizer,
        # then this test cases will get fixed.
        ("a &amp; b", "a &amp;amp; b"),  # should be 'a &amp; b'
        # & in HTML attribute values are escaped
        (
            '<a href="http://example.com?key=value&key2=value">tag</a>',
            '<a href="http://example.com?key=value&amp;key2=value">tag</a>',
        ),
        # & marking character entities in HTML attribute values aren't escaped
        (
            '<a href="http://example.com?key=value&amp;key2=value">tag</a>',
            '<a href="http://example.com?key=value&amp;key2=value">tag</a>',
        ),
        # & marking ambiguous character entities in attribute values are escaped
        # (&curren; is a character entity)
        (
            '<a href="http://example.com?key=value&current=value">tag</a>',
            '<a href="http://example.com?key=value&amp;current=value">tag</a>',
        ),
    ],
)
def test_serializer(data, expected):
    # Build a parser, walker, and serializer just like we do in clean()
    parser = html5lib_shim.BleachHTMLParser(
        tags=None, strip=True, consume_entities=False, namespaceHTMLElements=False
    )
    walker = html5lib_shim.getTreeWalker("etree")
    serializer = html5lib_shim.BleachHTMLSerializer(
        quote_attr_values="always",
        omit_optional_tags=False,
        escape_lt_in_attrs=True,
        resolve_entities=False,
        sanitize=False,
        alphabetical_attributes=False,
    )

    # Parse, walk, and then serialize the output
    dom = parser.parseFragment(data)
    serialized = serializer.render(walker(dom))

    assert serialized == expected


@pytest.mark.parametrize(
    "parser_args, data, expected",
    [
        # Make sure InputStreamWithMemory has charEncoding and changeEncoding
        ({}, '<meta charset="utf-8">', '<meta charset="utf-8">'),
        # Handle consume entities False--all entities are passed along and then
        # escaped when serialized
        (
            {"consume_entities": False},
            "text &amp;&gt;&quot;",
            "text &amp;amp;&amp;gt;&amp;quot;",
        ),
        # Handle consume entities True--all entities are consumed and converted
        # to their character equivalents and then &, <, and > are escaped when
        # serialized
        ({"consume_entities": True}, "text &amp;&gt;&quot;", 'text &amp;&gt;"'),
        # Test that "invalid-character-in-attribute-name" errors in tokenizing
        # result in attributes with invalid names getting dropped
        ({}, '<a href="http://example.com"">', '<a href="http://example.com"></a>'),
        ({}, "<a href='http://example.com''>", '<a href="http://example.com"></a>'),
        # Test that "expected-closing-tag-but-got-char" works when tags is None
        (
            {},
            "</ chars",
            "<!-- chars-->",
        ),
    ],
)
def test_bleach_html_parser(parser_args, data, expected):
    args = {"tags": None, "strip": True, "consume_entities": True}
    args.update(parser_args)

    # Build a parser, walker, and serializer just like we do in clean()
    parser = html5lib_shim.BleachHTMLParser(**args)
    walker = html5lib_shim.getTreeWalker("etree")
    serializer = html5lib_shim.BleachHTMLSerializer(
        quote_attr_values="always",
        omit_optional_tags=False,
        escape_lt_in_attrs=True,
        resolve_entities=False,
        sanitize=False,
        alphabetical_attributes=False,
    )

    # Parse, walk, and then serialize the output
    dom = parser.parseFragment(data)
    serialized = serializer.render(walker(dom))

    assert serialized == expected
