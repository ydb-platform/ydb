"""Test future selectors."""
from .. import util


class TestFuture(util.TestCase):
    """Test future selectors."""

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

    def test_future(self):
        """Test future (should match nothing)."""

        self.assert_selector(
            self.MARKUP,
            "p:future",
            [],
            flags=util.HTML
        )

    def test_not_future(self):
        """Test not future."""

        self.assert_selector(
            self.MARKUP,
            "p:not(:future)",
            ["0"],
            flags=util.HTML
        )
