"""Test selector lists."""
from .. import util
from soupsieve import SelectorSyntaxError


class TestSelectorLists(util.TestCase):
    """Test selector lists."""

    def test_multiple_tags(self):
        """Test multiple selectors."""

        self.assert_selector(
            """
            <div>
            <p>Some text <span id="1"> in a paragraph</span>.
            <a id="2" href="http://google.com">Link</a>
            </p>
            </div>
            """,
            "span, a",
            ["1", "2"],
            flags=util.HTML
        )

    def test_invalid_start_comma(self):
        """Test that selectors cannot start with a comma."""

        self.assert_raises(', p', SelectorSyntaxError)

    def test_invalid_end_comma(self):
        """Test that selectors cannot end with a comma."""

        self.assert_raises('p,', SelectorSyntaxError)

    def test_invalid_double_comma(self):
        """Test that selectors cannot have double combinators."""

        self.assert_raises('div,, a', SelectorSyntaxError)
