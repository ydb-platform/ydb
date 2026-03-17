"""Test `nth` of type selectors."""
from .. import util


class TestNthOfType(util.TestCase):
    """Test `nth` of type selectors."""

    def test_nth_of_type(self):
        """Test `nth` of type."""

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
            "p:nth-of-type(3)",
            ['7'],
            flags=util.HTML
        )

    def test_nth_of_type_complex(self):
        """Test `nth` of type complex."""

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
            "p:nth-of-type(2n + 1)",
            ['0', '7', '9'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "span:nth-of-type(2n + 1)",
            ['2', '4', '6'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "body :nth-of-type(2n + 1)",
            ['0', '2', '4', '6', '7', '9'],
            flags=util.HTML
        )
