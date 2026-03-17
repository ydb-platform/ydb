"""Test universal type selector."""
from .. import util


class TestUniversal(util.TestCase):
    """Test universal type selector."""

    def test_universal_type(self):
        """Test universal type."""

        self.assert_selector(
            """
            <body>
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
            </body>
            """,
            "body *",
            ["0", "1", "2", "3", "4", "5", "6", "div", "pre"],
            flags=util.HTML
        )
