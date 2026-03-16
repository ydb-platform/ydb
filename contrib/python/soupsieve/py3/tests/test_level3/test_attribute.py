"""Test attribute selectors."""
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

    def test_attribute_contains_with_newlines(self):
        """Test attribute `*=` will match with new lines."""

        self.assert_selector(
            "<p><span id='1' title='foo bar'>foo1</span><span id='2' title='foo\nbar'>foo1</span></p>",
            "span[title*='bar']",
            ["1", "2"],
            flags=util.HTML
        )

    def test_attribute_starts_with_newlines(self):
        """Test attribute `^=` will match with new lines."""

        self.assert_selector(
            "<p><span id='1' title='foo bar'>foo1</span><span id='2' title='foo\nbar'>foo1</span></p>",
            "span[title^='foo']",
            ["1", "2"],
            flags=util.HTML
        )

    def test_attribute_ends_with_newlines(self):
        """Test attribute `$=` will match with new lines."""

        self.assert_selector(
            "<p><span id='1' title='foo bar'>foo1</span><span id='2' title='foo\nbar'>foo1</span></p>",
            "span[title$='bar']",
            ["1", "2"],
            flags=util.HTML
        )

    def test_attribute_dash_list_with_newlines(self):
        """Test attribute `|=` will match with new lines."""

        self.assert_selector(
            "<p><span id='1' title='fo-o bar'>foo1</span><span id='2' title='fo-o\nbar'>foo1</span></p>",
            "span[title|='fo']",
            ["1", "2"],
            flags=util.HTML
        )

    def test_attribute_space_list_with_newlines(self):
        """Test attribute `~=` will match with new lines."""

        self.assert_selector(
            "<p><span id='1' title='foo bar baz'>foo1</span><span id='2' title='foo\nbar baz'>foo1</span></p>",
            "span[title~='baz']",
            ["1", "2"],
            flags=util.HTML
        )
