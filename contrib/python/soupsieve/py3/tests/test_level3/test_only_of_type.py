"""Test only of type selectors."""
from .. import util


class TestOnlyOfType(util.TestCase):
    """Test only of type selectors."""

    def test_only_of_type(self):
        """Test only of type."""

        markup = """
        <div id="0">
            <p id="1"></p>
        </div>
        <span id="2"></span>
        <span id="3"></span>
        <p id="4"></p>
        <span id="5"></span>
        <span id="6"></span>
        <div>
            <p id="7"></p>
            <p id="8"></p>
        </div>
        """

        self.assert_selector(
            markup,
            "p:only-of-type",
            ['1', '4'],
            flags=util.HTML
        )
