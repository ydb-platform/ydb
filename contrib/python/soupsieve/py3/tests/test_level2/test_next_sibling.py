"""Test next sibling combinators."""
from .. import util


class TestNextSibling(util.TestCase):
    """Test next sibling combinators."""

    MARKUP = """
    <div>
    <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3">Direct child</span>
    <pre>
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    def test_direct_sibling(self):
        """Test direct sibling."""

        # Spaces
        self.assert_selector(
            self.MARKUP,
            "span + span",
            ["5", "6"],
            flags=util.HTML
        )

    def test_direct_sibling_no_spaces(self):
        """Test direct sibling with no spaces."""

        # No spaces
        self.assert_selector(
            self.MARKUP,
            "span+span",
            ["5", "6"],
            flags=util.HTML
        )

    def test_complex_direct_siblings(self):
        """Test direct sibling with no spaces."""

        # Complex
        self.assert_selector(
            self.MARKUP,
            "span#\\34 + span#\\35",
            ["5"],
            flags=util.HTML
        )
