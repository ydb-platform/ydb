"""Test first of type selectors."""
from .. import util


class TestFirstOfType(util.TestCase):
    """Test first of type selectors."""

    def test_first_of_type_at_start(self):
        """Test first of type which is also the first sibling."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """

        self.assert_selector(
            markup,
            "p:first-of-type",
            ['0'],
            flags=util.HTML
        )

    def test_first_of_type_at_middle(self):
        """Test first of type that is not the first sibling."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """

        self.assert_selector(
            markup,
            "span:first-of-type",
            ['2'],
            flags=util.HTML
        )

    def test_any_first_of_type(self):
        """Test any first of type."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """
        self.assert_selector(
            markup,
            "body :first-of-type",
            ['0', '2'],
            flags=util.HTML
        )
