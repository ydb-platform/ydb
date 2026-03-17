"""Test host context selectors."""
from .. import util


class TestHostContext(util.TestCase):
    """Test host context selectors."""

    def test_host_context(self):
        """Test host context (not supported)."""

        markup = """<h1>header</h1><div><p>some text</p></div>"""

        self.assert_selector(
            markup,
            ":host-context(h1, h2)",
            [],
            flags=util.HTML
        )
