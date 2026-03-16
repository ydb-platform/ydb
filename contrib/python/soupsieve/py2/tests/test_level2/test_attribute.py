"""Test attribute selector."""
from __future__ import unicode_literals
from .. import util
from soupsieve import SelectorSyntaxError


class TestAttribute(util.TestCase):
    """Test attribute selector."""

    MARKUP = """
    <div id="div">
    <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3">Direct child</span>
    <pre id="pre">
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    MARKUP_CONTAINS = """
    <div id="div">
    <p id="0" class="test1 test2 test3">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3">Direct child</span>
    <pre id="pre" class="test-a test-b">
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    # Browsers normally replace NULL with `\uFFFD`, but some of the parsers
    # we test just strip out NULL, so we will simulate and just insert `\uFFFD` directly
    # to ensure consistent behavior in our tests across parsers.
    MARKUP_NULL = """
    <div id="div">
    <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3">Direct child</span>
    <pre id="\ufffdpre">
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    def test_attribute(self):
        """Test attribute."""

        self.assert_selector(
            self.MARKUP,
            "[href]",
            ["2"],
            flags=util.HTML
        )

    def test_attribute_with_spaces(self):
        """Test attribute with spaces."""

        # With spaces
        self.assert_selector(
            self.MARKUP,
            "[   href   ]",
            ["2"],
            flags=util.HTML
        )

    def test_multi_attribute(self):
        """Test multiple attribute."""

        self.assert_selector(
            """
            <div id="div">
            <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
            <a id="2" href="http://google.com">Link</a>
            <span id="3">Direct child</span>
            <pre id="pre">
            <span id="4" class="test">Child 1</span>
            <span id="5" class="test" data-test="test">Child 2</span>
            <span id="6">Child 3</span>
            <span id="6">Child 3</span>
            </pre>
            </div>
            """,
            "span[id].test[data-test=test]",
            ["5"],
            flags=util.HTML
        )

    def test_attribute_equal_no_quotes(self):
        """Test attribute with value that equals specified value (with no quotes)."""

        # No quotes
        self.assert_selector(
            self.MARKUP,
            '[id=\\35]',
            ["5"],
            flags=util.HTML
        )

    def test_attribute_equal_with_quotes(self):
        """Test attribute with value that equals specified value (with quotes)."""

        # Single quoted
        self.assert_selector(
            self.MARKUP,
            "[id='5']",
            ["5"],
            flags=util.HTML
        )

    def test_attribute_equal_with_double_quotes(self):
        """Test attribute with value that equals specified value (with double quotes)."""

        # Double quoted
        self.assert_selector(
            self.MARKUP,
            '[id="5"]',
            ["5"],
            flags=util.HTML
        )

    def test_attribute_equal_quotes_and_spaces(self):
        """Test attribute with value that equals specified value (quotes and spaces)."""

        # With spaces
        self.assert_selector(
            self.MARKUP,
            '[  id  =  "5"  ]',
            ["5"],
            flags=util.HTML
        )

    def test_attribute_equal_case_insensitive_attribute(self):
        """Test attribute with value that equals specified value (case insensitive attribute)."""

        self.assert_selector(
            self.MARKUP,
            '[ID="5"]',
            ["5"],
            flags=util.HTML
        )

    def test_attribute_bad(self):
        """Test attribute with a bad attribute."""

        self.assert_selector(
            '<span bad="5"></span>',
            '[  id  =  "5"  ]',
            [],
            flags=util.HTML
        )

    def test_attribute_escaped_newline(self):
        """Test attribute with escaped new line in quoted string."""

        self.assert_selector(
            self.MARKUP,
            '[id="pr\\\ne"]',
            ["pre"],
            flags=util.HTML
        )

    def test_attribute_equal_literal_null(self):
        """Test attribute with value that equals specified value with a literal null character."""

        self.assert_selector(
            self.MARKUP_NULL,
            '[id="\x00pre"]',
            ["\ufffdpre"],
            flags=util.HTML
        )

    def test_attribute_equal_escaped_null(self):
        """Test attribute with value that equals specified value with an escaped null character."""

        self.assert_selector(
            self.MARKUP_NULL,
            r'[id="\0 pre"]',
            ["\ufffdpre"],
            flags=util.HTML
        )

    def test_invalid_tag(self):
        """
        Test invalid tag.

        Tag must come first.
        """

        self.assert_raises('[href]p', SelectorSyntaxError)

    def test_malformed(self):
        """Test malformed."""

        # Malformed attribute
        self.assert_raises('div[attr={}]', SelectorSyntaxError)

    def test_attribute_type_html(self):
        """Type is treated as case insensitive in HTML."""

        markup = """
        <html>
        <body>
        <div id="div">
        <p type="TEST" id="0">Some text <span id="1"> in a paragraph</span>.</p>
        <a type="test" id="2" href="http://google.com">Link</a>
        <span id="3">Direct child</span>
        <pre id="pre">
        <span id="4">Child 1</span>
        <span id="5">Child 2</span>
        <span id="6">Child 3</span>
        </pre>
        </div>
        </body>
        </html>
        """

        self.assert_selector(
            markup,
            '[type="test"]',
            ["0", '2'],
            flags=util.HTML
        )

    def test_attribute_type_xml(self):
        """Type is treated as case sensitive in XML."""

        markup = """
        <html>
        <body>
        <div id="div">
        <p type="TEST" id="0">Some text <span id="1"> in a paragraph</span>.</p>
        <a type="test" id="2" href="http://google.com">Link</a>
        <span id="3">Direct child</span>
        <pre id="pre">
        <span id="4">Child 1</span>
        <span id="5">Child 2</span>
        <span id="6">Child 3</span>
        </pre>
        </div>
        </body>
        </html>
        """

        self.assert_selector(
            markup,
            '[type="test"]',
            ['2'],
            flags=util.XML
        )

    def test_attribute_type_xhtml(self):
        """Type is treated as case insensitive in XHTML."""

        markup = """
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN"
            "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
        <html lang="en" xmlns="http://www.w3.org/1999/xhtml">
        <head>
        </head>
        <body>
        <div id="div">
        <p type="TEST" id="0">Some text <span id="1"> in a paragraph</span>.</p>
        <a type="test" id="2" href="http://google.com">Link</a>
        <span id="3">Direct child</span>
        <pre id="pre">
        <span id="4">Child 1</span>
        <span id="5">Child 2</span>
        <span id="6">Child 3</span>
        </pre>
        </div>
        </body>
        </html>
        """

        self.assert_selector(
            markup,
            '[type="test"]',
            ['2'],
            flags=util.XHTML
        )

    def test_attribute_start_dash(self):
        """Test attribute whose dash separated value starts with the specified value."""

        self.assert_selector(
            """
            <div id="div">
            <p id="0" lang="en-us">Some text <span id="1"> in a paragraph</span>.</p>
            <a id="2" href="http://google.com">Link</a>
            <span id="3">Direct child</span>
            <pre id="pre">
            <span id="4">Child 1</span>
            <span id="5">Child 2</span>
            <span id="6">Child 3</span>
            </pre>
            </div>
            """,
            "[lang|=en]",
            ["0"],
            flags=util.HTML
        )

    def test_attribute_contains_space_middle(self):
        """Test attribute whose space separated list contains the specified value in the middle of the list."""

        # Middle of list
        self.assert_selector(
            self.MARKUP_CONTAINS,
            "[class~=test2]",
            ["0"],
            flags=util.HTML
        )

    def test_attribute_contains_space_start(self):
        """Test attribute whose space separated list contains the specified value at the start of the list."""

        # Start of list
        self.assert_selector(
            self.MARKUP_CONTAINS,
            "[class~=test-a]",
            ["pre"],
            flags=util.HTML
        )

    def test_attribute_contains_space_end(self):
        """Test attribute whose space separated list contains the specified value at the end of the list."""

        # End of list
        self.assert_selector(
            self.MARKUP_CONTAINS,
            "[class~=test-b]",
            ["pre"],
            flags=util.HTML
        )

    def test_attribute_contains_cannot_have_spaces(self):
        """Test attribute `~=` will match nothing when spaces are included."""

        # Shouldn't match anything
        self.assert_selector(
            self.MARKUP_CONTAINS,
            '[class~="test1 test2"]',
            [],
            flags=util.HTML
        )

    def test_attribute_contains_cannot_have_empty(self):
        """Test attribute `~=` will match nothing when value is empty."""

        self.assert_selector(
            self.MARKUP_CONTAINS,
            '[class~=""]',
            [],
            flags=util.HTML
        )

    def test_attribute_contains_cannot_have_escaped_spaces(self):
        """Test attribute `~=` will match nothing when escaped spaces are included."""

        self.assert_selector(
            self.MARKUP_CONTAINS,
            '[class~="test1\\ test2"]',
            [],
            flags=util.HTML
        )
