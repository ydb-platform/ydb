"""Test any link selectors."""
from .. import util


class TestAnyLink(util.TestCase):
    """Test any link selectors."""

    MARKUP = """
    <div id="div">
    <p id="0">Some text <span id="1" class="foo:bar:foobar"> in a paragraph</span>.
    <a id="2" class="bar" href="http://google.com">Link</a>
    <a id="3">Placeholder text.</a>
    </p>
    </div>
    """

    def test_anylink(self):
        """Test any link (all links are unvisited)."""

        self.assert_selector(
            self.MARKUP,
            ":any-link",
            ["2"],
            flags=util.HTML
        )

    def test_anylink_xml(self):
        """Test that any link will not match in XML (all links are unvisited)."""

        self.assert_selector(
            self.MARKUP,
            "a:any-link",
            [],
            flags=util.XML
        )

    def test_anylink_xhtml(self):
        """Test that any link will not match in XHTML (all links are unvisited)."""

        self.assert_selector(
            self.wrap_xhtml(self.MARKUP),
            "a:any-link",
            ['2'],
            flags=util.XHTML
        )

    def test_not_anylink(self):
        """Test the inverse of any link (all links are unvisited)."""

        self.assert_selector(
            self.MARKUP,
            ":not(a:any-link)",
            ["div", "0", "1", "2", "3"],
            flags=util.XML
        )
