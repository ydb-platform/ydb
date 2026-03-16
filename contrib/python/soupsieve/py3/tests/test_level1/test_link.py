"""Test link selectors."""
from .. import util


class TestLink(util.TestCase):
    """Test link selectors."""

    MARKUP = """
    <div>
    <p>Some text <span id="1" class="foo:bar:foobar"> in a paragraph</span>.
    <a id="2" class="bar" href="http://google.com">Link</a>
    <a id="3">Placeholder text.</a>
    </p>
    </div>
    """

    def test_link(self):
        """Test link (all links are unvisited)."""

        self.assert_selector(
            self.MARKUP,
            ":link",
            ["2"],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            r":\liNk",
            ["2"],
            flags=util.HTML
        )

    def test_tag_and_link(self):
        """Test link and tag (all links are unvisited)."""

        self.assert_selector(
            self.MARKUP,
            "a:link",
            [],
            flags=util.XML
        )
