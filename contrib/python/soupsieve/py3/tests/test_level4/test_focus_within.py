"""Test focus within selectors."""
from .. import util


class TestFocusWithin(util.TestCase):
    """Test focus within selectors."""

    MARKUP = """
    <form id="form">
      <input type="text">
    </form>
    """

    def test_focus_within(self):
        """Test focus within."""

        self.assert_selector(
            self.MARKUP,
            "form:focus-within",
            [],
            flags=util.HTML
        )

    def test_not_focus_within(self):
        """Test inverse of focus within."""

        self.assert_selector(
            self.MARKUP,
            "form:not(:focus-within)",
            ["form"],
            flags=util.HTML
        )
