"""Test `nth` last of type selectors."""
from .. import util


class TestNthLastOfType(util.TestCase):
    """Test `nth` last of type selectors."""

    def test_nth_last_of_type(self):
        """Test `nth` last of type."""

        markup = """
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
        """

        self.assert_selector(
            markup,
            "p:nth-last-of-type(3)",
            ['8'],
            flags=util.HTML
        )

    def test_nth_last_of_type_complex(self):
        """Test `nth` last of type complex."""

        markup = """
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
        """

        self.assert_selector(
            markup,
            "p:nth-last-of-type(2n + 1)",
            ['1', '8', '10'],
            flags=util.HTML
        )
