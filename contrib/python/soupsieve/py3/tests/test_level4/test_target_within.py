"""Test target within selectors."""
from .. import util


class TestTargetWithin(util.TestCase):
    """Test target within selectors."""

    MARKUP = """
    <a href="#head-2">Jump</a>
    <article id="article">
    <h2 id="head-1">Header 1</h1>
    <div><p>content</p></div>
    <h2 id="head-2">Header 2</h1>
    <div><p>content</p></div>
    </article>
    """

    def test_target_within(self):
        """Test target within."""

        self.assert_selector(
            self.MARKUP,
            "article:target-within",
            [],
            flags=util.HTML
        )

    def test_not_target_within(self):
        """Test inverse of target within."""

        self.assert_selector(
            self.MARKUP,
            "article:not(:target-within)",
            ["article"],
            flags=util.HTML
        )
