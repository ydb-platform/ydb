"""Test subsequent sibling combinator."""
from .. import util


class TestSubsequentSibling(util.TestCase):
    """Test subsequent sibling combinator."""

    def test_subsequent_sibling(self):
        """Test subsequent sibling."""

        self.assert_selector(
            """
            <div>
            <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
            <a id="2" href="http://google.com">Link</a>
            <span id="3">Direct child</span>
            <pre>
            <span id="4">Child 1</span>
            <span id="5">Child 2</span>
            <span id="6">Child 3</span>
            </pre>
            </div>
            """,
            "p ~ span",
            ["3"],
            flags=util.HTML
        )
