"""Test not selectors."""
from .. import util


class TestNot(util.TestCase):
    """Test not selectors."""

    def test_multi_nested_not(self):
        """Test nested not and multiple selectors."""

        markup = """
        <div>
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

        self.assert_selector(
            markup,
            'div :not(p, :not([id=\\35]))',
            ['5'],
            flags=util.HTML
        )
