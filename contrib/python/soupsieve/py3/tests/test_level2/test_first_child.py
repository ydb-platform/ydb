"""Test first child selector."""
from .. import util


class TestFirstChild(util.TestCase):
    """Test first child selector."""

    def test_first_child(self):
        """Test first child."""

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
            "span:first-child",
            ["1", "4"],
            flags=util.HTML
        )
