"""Test attribute selectors."""
from __future__ import unicode_literals
from .. import util
from soupsieve import SelectorSyntaxError


class TestAttribute(util.TestCase):
    """Test attribute selectors."""

    MARKUP = """
    <div>
    <p type="TEST" id="0" class="somewordshere">Some text <span id="1"> in a paragraph</span>.</p>
    <a type="test" id="2" href="http://google.com">Link</a>
    <span id="3" class="herewords">Direct child</span>
    <pre id="pre" class="wordshere">
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    def test_attribute_forced_case_insensitive(self):
        """Test attribute value case insensitivity."""

        self.assert_selector(
            self.MARKUP,
            "[class*=WORDS]",
            [],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            "[class*=WORDS i]",
            ["0", "3", "pre"],
            flags=util.HTML
        )

    def test_attribute_forced_case_insensitive_xml(self):
        """Test that attribute value case insensitivity can be forced in XML."""

        self.assert_selector(
            self.MARKUP,
            '[type="test" i]',
            ['0', '2'],
            flags=util.XML
        )

    def test_attribute_forced_case_insensitive_xhtml(self):
        """Test that attribute value case insensitivity can be forced in XHTML."""

        self.assert_selector(
            self.wrap_xhtml(self.MARKUP),
            '[type="test" i]',
            ['0', '2'],
            flags=util.XML
        )

    def test_attribute_forced_case_needs_value(self):
        """Test attribute value case insensitivity requires a value."""

        self.assert_raises('[id i]', SelectorSyntaxError)

    def test_attribute_type_case_sensitive(self):
        """Type is treated as case insensitive in HTML, so test that we can force the opposite."""

        self.assert_selector(
            self.MARKUP,
            '[type="test" s]',
            ['2'],
            flags=util.HTML
        )
