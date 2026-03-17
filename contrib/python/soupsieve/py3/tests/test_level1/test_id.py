"""Test ID selectors."""
from .. import util
from soupsieve import SelectorSyntaxError


class TestId(util.TestCase):
    """Test ID selectors."""

    MARKUP = """
    <div>
    <p>Some text <span id="1"> in a paragraph</span>.
    <a id="2" href="http://google.com">Link</a>
    </p>
    </div>
    """

    def test_id(self):
        """Test ID."""

        self.assert_selector(
            self.MARKUP,
            "#\\31",
            ["1"],
            flags=util.HTML
        )

    def test_tag_and_id(self):
        """Test tag and ID."""

        self.assert_selector(
            self.MARKUP,
            "a#\\32",
            ["2"],
            flags=util.HTML
        )

    def test_malformed_id(self):
        """Test malformed ID."""

        # Malformed id
        self.assert_raises('td#.some-class', SelectorSyntaxError)
