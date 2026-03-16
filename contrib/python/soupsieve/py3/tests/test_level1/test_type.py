"""Test type selectors."""
from .. import util
from soupsieve import SelectorSyntaxError


class TestType(util.TestCase):
    """Test type selectors."""

    MARKUP = """
    <Tag id="1">
    <tag id="2"></tag>
    <TAG id="3"></TAG>
    </Tag>
    """

    def test_basic_type(self):
        """Test type."""

        self.assert_selector(
            """
            <div>
            <p>Some text <span id="1"> in a paragraph</span>.</p>
            <a id="2" href="http://google.com">Link</a>
            </div>
            """,
            "span",
            ["1"],
            flags=util.HTML
        )

    def test_type_html(self):
        """Test type for HTML."""

        self.assert_selector(
            self.MARKUP,
            "tag",
            ["1", "2", "3"],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            "Tag",
            ["1", "2", "3"],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            "TAG",
            ["1", "2", "3"],
            flags=util.HTML
        )

    def test_type_xhtml(self):
        """Test type for XHTML."""

        self.assert_selector(
            self.wrap_xhtml(self.MARKUP),
            "tag",
            ["2"],
            flags=util.XHTML
        )

        self.assert_selector(
            self.wrap_xhtml(self.MARKUP),
            "Tag",
            ["1"],
            flags=util.XHTML
        )

        self.assert_selector(
            self.wrap_xhtml(self.MARKUP),
            "TAG",
            ["3"],
            flags=util.XHTML
        )

    def test_type_xml(self):
        """Test type for XML."""

        self.assert_selector(
            self.MARKUP,
            "tag",
            ["2"],
            flags=util.XML
        )

        self.assert_selector(
            self.MARKUP,
            "Tag",
            ["1"],
            flags=util.XML
        )

        self.assert_selector(
            self.MARKUP,
            "TAG",
            ["3"],
            flags=util.XML
        )

    def test_invalid_syntax(self):
        """Test invalid syntax."""

        self.assert_raises('div?', SelectorSyntaxError)
        self.assert_raises('-', SelectorSyntaxError)
