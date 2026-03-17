"""Test invalid selectors."""
from .. import util


class TestInvalid(util.TestCase):
    """Test invalid selectors."""

    def test_user_invalid(self):
        """Test user invalid (matches nothing)."""

        markup = """
        <form id="form">
          <input id="1" type="text">
        </form>
        """

        self.assert_selector(
            markup,
            "input:user-invalid",
            [],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "input:not(:user-invalid)",
            ["1"],
            flags=util.HTML
        )
