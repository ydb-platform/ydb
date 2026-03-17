"""Test focus visible selectors."""
from .. import util


class TestFocusVisible(util.TestCase):
    """Test focus visible selectors."""

    MARKUP = """
    <form id="form">
      <input type="text">
    </form>
    """

    def test_focus_visible(self):
        """Test focus visible."""

        self.assert_selector(
            self.MARKUP,
            "form:focus-visible",
            [],
            flags=util.HTML
        )

    def test_not_focus_visible(self):
        """Test inverse of focus visible."""

        self.assert_selector(
            self.MARKUP,
            "form:not(:focus-visible)",
            ["form"],
            flags=util.HTML
        )
