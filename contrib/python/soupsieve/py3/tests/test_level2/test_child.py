"""Test child combinators."""
from .. import util
from soupsieve import SelectorSyntaxError


class TestChild(util.TestCase):
    """Test child combinators."""

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

    def test_direct_child(self):
        """Test direct child."""

        # Spaces
        self.assert_selector(
            self.MARKUP,
            "div > span",
            ["3"],
            flags=util.HTML
        )

    def test_direct_child_no_spaces(self):
        """Test direct child with no spaces."""

        # No spaces
        self.assert_selector(
            self.MARKUP,
            "div>span",
            ["3"],
            flags=util.HTML
        )

    def test_invalid_double_combinator(self):
        """Test that selectors cannot have double combinators."""

        self.assert_raises('div >> p', SelectorSyntaxError)
        self.assert_raises('>> div > p', SelectorSyntaxError)

    def test_invalid_trailing_combinator(self):
        """Test that selectors cannot have a trailing combinator."""

        self.assert_raises('div >', SelectorSyntaxError)
        self.assert_raises('div >, div', SelectorSyntaxError)

    def test_invalid_combinator(self):
        """Test that we do not allow selectors in selector lists to start with combinators."""

        self.assert_raises('> p', SelectorSyntaxError)
        self.assert_raises('div, > a', SelectorSyntaxError)
