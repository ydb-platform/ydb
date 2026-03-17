"""Test required selectors."""
from .. import util


class TestRequired(util.TestCase):
    """Test required selectors."""

    MARKUP = """
    <form>
    <input id="1" type="name" required>
    <input id="2" type="checkbox" required>
    <input id="3" type="email">
    <textarea id="4" name="name" cols="30" rows="10" required></textarea>
    <select id="5" name="nm" required>
        <!-- options -->
    </select>
    </form>
    """

    def test_required(self):
        """Test required."""

        self.assert_selector(
            self.MARKUP,
            ":required",
            ['1', '2', '4', '5'],
            flags=util.HTML
        )

    def test_specific_required(self):
        """Test specific required."""

        self.assert_selector(
            self.MARKUP,
            "input:required",
            ['1', '2'],
            flags=util.HTML
        )
