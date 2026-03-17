"""Test `nth` child selectors."""
from .. import util


class TestNthChild(util.TestCase):
    """Test `nth` child selectors."""

    MARKUP = """
    <p id="0"></p>
    <p id="1"></p>
    <span id="2" class="test"></span>
    <span id="3"></span>
    <span id="4" class="test"></span>
    <span id="5"></span>
    <span id="6" class="test"></span>
    <p id="7"></p>
    <p id="8" class="test"></p>
    <p id="9"></p>
    <p id="10" class="test"></p>
    <span id="11"></span>
    """

    def test_nth_child_of_s_simple(self):
        """Test `nth` child with selector (simple)."""

        self.assert_selector(
            self.MARKUP,
            ":nth-child(-n+3 of p)",
            ['0', '1', '7'],
            flags=util.HTML
        )

    def test_nth_child_of_s_complex(self):
        """Test `nth` child with selector (complex)."""

        self.assert_selector(
            self.MARKUP,
            ":nth-child(2n + 1 of :is(p, span).test)",
            ['2', '6', '10'],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            ":nth-child(2n + 1 OF :is(p, span).test)",
            ['2', '6', '10'],
            flags=util.HTML
        )
