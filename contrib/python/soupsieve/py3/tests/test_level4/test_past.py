"""Test past selectors."""
from .. import util


class TestPast(util.TestCase):
    """Test past selectors."""

    MARKUP = """
    <body>
    <div id="div">
    <p id="0">Some text <span id="1" class="foo:bar:foobar"> in a paragraph</span>.
    <a id="2" class="bar" href="http://google.com">Link</a>
    <a id="3">Placeholder text.</a>
    </p>
    </div>
    </body>
    """

    def test_past(self):
        """Test past (should match nothing)."""

        self.assert_selector(
            self.MARKUP,
            "p:past",
            [],
            flags=util.HTML
        )

    def test_not_past(self):
        """Test not past."""

        self.assert_selector(
            self.MARKUP,
            "p:not(:past)",
            ["0"],
            flags=util.HTML
        )
