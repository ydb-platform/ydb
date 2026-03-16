"""Test last child selectors."""
from .. import util


class TestLastChild(util.TestCase):
    """Test last child selectors."""

    MARKUP = """
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
    """

    def test_last_child(self):
        """Test last child."""

        self.assert_selector(
            self.MARKUP,
            "span:last-child",
            ["1", "6"],
            flags=util.HTML
        )

    def test_last_child_case(self):
        """Test last child token's case insensitivity."""

        self.assert_selector(
            self.MARKUP,
            "span:LAST-CHILD",
            ["1", "6"],
            flags=util.HTML
        )
