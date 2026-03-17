"""Test only child selectors."""
from .. import util


class TestOnlyChild(util.TestCase):
    """Test only child selectors."""

    def test_only_child(self):
        """Test only child."""

        self.assert_selector(
            """
            <div id="div">
            <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
            <a id="2" href="http://google.com">Link</a>
            <span id="3">Direct child</span>
            <pre id="pre">
            <span id="4">Child 1</span>
            <span id="5">Child 2</span>
            <span id="6">Child 3</span>
            </pre>
            </div>
            """,
            "span:only-child",
            ["1"],
            flags=util.HTML
        )
