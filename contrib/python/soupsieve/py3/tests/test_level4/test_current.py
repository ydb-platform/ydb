"""Test current selectors."""
from .. import util


class TestCurrent(util.TestCase):
    """Test current selectors."""

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

    def test_current(self):
        """Test current (should match nothing)."""

        self.assert_selector(
            self.MARKUP,
            "p:current",
            [],
            flags=util.HTML
        )

    def test_not_current(self):
        """Test not current."""

        self.assert_selector(
            self.MARKUP,
            "p:not(:current)",
            ["0"],
            flags=util.HTML
        )

    def test_current_func(self):
        """Test the functional form of current (should match nothing)."""

        self.assert_selector(
            self.MARKUP,
            ":current(p, div, a)",
            [],
            flags=util.HTML
        )

    def test_current_func_nested(self):
        """Test the nested functional form of current (should match nothing)."""

        self.assert_selector(
            self.MARKUP,
            ":current(p, :not(div), a)",
            [],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            "body :not(:current(p, div, a))",
            ["div", "0", "1", "2", "3"],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            "body :not(:current(p, :not(div), a))",
            ["div", "0", "1", "2", "3"],
            flags=util.HTML
        )
