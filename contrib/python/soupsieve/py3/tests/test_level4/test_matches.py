"""Test matches selectors."""
from .. import util


class TestMatches(util.TestCase):
    """Test matches selectors."""

    MARKUP = """
    <div>
    <p>Some text <span id="1"> in a paragraph</span>.
    <a id="2" href="http://google.com">Link</a>
    </p>
    </div>
    """

    def test_matches(self):
        """Test multiple selectors with "matches"."""

        self.assert_selector(
            self.MARKUP,
            ":matches(span, a)",
            ["1", "2"],
            flags=util.HTML
        )

    def test_nested_matches(self):
        """Test multiple nested selectors with "matches"."""

        self.assert_selector(
            self.MARKUP,
            ":matches(span, a:matches(#\\32))",
            ["1", "2"],
            flags=util.HTML
        )
