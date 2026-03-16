"""Test local link selectors."""
from .. import util


class TestLocalLink(util.TestCase):
    """Test local link selectors."""

    MARKUP = """
    <a id="1" href="./somelink/index.html">Link</link>
    <a id="2" href="http://somelink.com/somelink/index.html">Another link</a>
    """

    def test_local_link(self):
        """Test local link (matches nothing)."""

        self.assert_selector(
            self.MARKUP,
            "a:local-link",
            [],
            flags=util.HTML
        )

    def test_not_local_link(self):
        """Test not local link."""

        self.assert_selector(
            self.MARKUP,
            "a:not(:local-link)",
            ["1", "2"],
            flags=util.HTML
        )
