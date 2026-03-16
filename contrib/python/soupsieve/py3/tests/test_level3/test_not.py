"""Test not selectors."""
from .. import util
from bs4 import BeautifulSoup
from soupsieve import SelectorSyntaxError


class TestNot(util.TestCase):
    """Test not selectors."""

    MARKUP = """
    <div>
    <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3">Direct child</span>
    <pre id="pre">
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    def test_not(self):
        """Test not."""

        self.assert_selector(
            self.MARKUP,
            'div :not([id="1"])',
            ["0", "2", "3", "4", "5", "6", "pre"],
            flags=util.HTML
        )

    def test_not_and_type(self):
        """Test not with a tag."""

        self.assert_selector(
            self.MARKUP,
            'span:not([id="1"])',
            ["3", "4", "5", "6"],
            flags=util.HTML
        )

    def test_not_case(self):
        """Test not token case insensitivity."""

        self.assert_selector(
            self.MARKUP,
            'div :NOT([id="1"])',
            ["0", "2", "3", "4", "5", "6", "pre"],
            flags=util.HTML
        )

    def test_none_inputs(self):
        """Test weird inputs."""

        soup = BeautifulSoup('<span foo="something">text</span>', 'html.parser')
        soup.span['foo'] = None
        self.assertEqual(len(soup.select('span:not([foo])')), 0)

    def test_invalid_pseudo_empty(self):
        """Test pseudo class group with empty set."""

        self.assert_raises(':not()', SelectorSyntaxError)

    def test_invalid_pseudo_trailing_comma(self):
        """Test pseudo class group with trailing comma."""

        self.assert_raises(':not(.class,)', SelectorSyntaxError)

    def test_invalid_pseudo_leading_comma(self):
        """Test pseudo class group with leading comma."""

        self.assert_raises(':not(,.class)', SelectorSyntaxError)

    def test_invalid_pseudo_multi_comma(self):
        """Test pseudo class group with multiple commas."""

        self.assert_raises(':not(.this,,.that)', SelectorSyntaxError)
