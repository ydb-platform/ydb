"""Test `nth` last child selectors."""
from .. import util


class TestNthLastChild(util.TestCase):
    """Test `nth` last child selectors."""

    def test_nth_last_child(self):
        """Test `nth` last child."""

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
            "p:nth-last-child(2)",
            ['10'],
            flags=util.HTML
        )

    def test_nth_last_child_complex(self):
        """Test `nth` last child complex."""

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
            "p:nth-last-child(2n + 1)",
            ['1', '7', '9'],
            flags=util.HTML
        )
