"""Test host selectors."""
from .. import util


class TestHost(util.TestCase):
    """Test host selectors."""

    MARKUP = """<h1>header</h1><div><p>some text</p></div>"""

    def test_host(self):
        """Test host (not supported)."""

        self.assert_selector(
            self.MARKUP,
            ":host",
            [],
            flags=util.HTML
        )

    def test_host_func(self):
        """Test host function (not supported)."""

        self.assert_selector(
            self.MARKUP,
            ":host(h1)",
            [],
            flags=util.HTML
        )
