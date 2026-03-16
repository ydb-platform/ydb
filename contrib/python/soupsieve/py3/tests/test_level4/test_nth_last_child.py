"""Test `nth` last child selectors."""
from .. import util


class TestNthLastChild(util.TestCase):
    """Test `nth` last child selectors."""

    def test_nth_child_of_s_complex(self):
        """Test `nth` child with selector (complex)."""

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
            ":nth-last-child(2n + 1 of p[id], span[id])",
            ['1', '3', '5', '7', '9', '11'],
            flags=util.HTML
        )
