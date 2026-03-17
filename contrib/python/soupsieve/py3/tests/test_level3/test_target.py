"""Test target selectors."""
from .. import util


class TestTarget(util.TestCase):
    """Test target selectors."""

    MARKUP = """
    <div>
    <h2 id="head-1">Header 1</h1>
    <div><p>content</p></div>
    <h2 id="head-2">Header 2</h1>
    <div><p>content</p></div>
    </div>
    """

    def test_target(self):
        """Test target."""

        self.assert_selector(
            self.MARKUP,
            "#head-2:target",
            [],
            flags=util.HTML
        )

    def test_not_target(self):
        """Test not target."""

        self.assert_selector(
            self.MARKUP,
            "#head-2:not(:target)",
            ["head-2"],
            flags=util.HTML
        )
