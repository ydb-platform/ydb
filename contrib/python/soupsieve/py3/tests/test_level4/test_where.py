"""Test where selectors."""
from .. import util


class TestWhere(util.TestCase):
    """Test where selectors."""

    MARKUP = """
    <div>
    <p>Some text <span id="1"> in a paragraph</span>.
    <a id="2" href="http://google.com">Link</a>
    </p>
    </div>
    """

    def test_where(self):
        """Test multiple selectors with "where"."""

        self.assert_selector(
            self.MARKUP,
            ":where(span, a)",
            ["1", "2"],
            flags=util.HTML
        )

    def test_nested_where(self):
        """Test multiple nested selectors with "where"."""

        self.assert_selector(
            self.MARKUP,
            ":where(span, a:where(#\\32))",
            ["1", "2"],
            flags=util.HTML
        )
