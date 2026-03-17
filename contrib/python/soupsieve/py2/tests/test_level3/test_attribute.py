"""Test attribute selectors."""
from __future__ import unicode_literals
from .. import util


class TestAttribute(util.TestCase):
    """Test attribute selectors."""

    MARKUP = """
    <div id="div">
    <p id="0" class="herearesomewords">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3">Direct child</span>
    <pre id="pre">
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    def test_attribute_begins(self):
        """Test attribute whose value begins with the specified value."""

        self.assert_selector(
            self.MARKUP,
            "[class^=here]",
            ["0"],
            flags=util.HTML
        )

    def test_attribute_end(self):
        """Test attribute whose value ends with the specified value."""

        self.assert_selector(
            self.MARKUP,
            "[class$=words]",
            ["0"],
            flags=util.HTML
        )

    def test_attribute_contains(self):
        """Test attribute whose value contains the specified value."""

        markup = """
        <div id="div">
        <p id="0" class="somewordshere">Some text <span id="1"> in a paragraph</span>.</p>
        <a id="2" href="http://google.com">Link</a>
        <span id="3" class="herewords">Direct child</span>
        <pre id="pre" class="wordshere">
        <span id="4">Child 1</span>
        <span id="5">Child 2</span>
        <span id="6">Child 3</span>
        </pre>
        </div>
        """

        self.assert_selector(
            markup,
            "[class*=words]",
            ["0", "3", "pre"],
            flags=util.HTML
        )
