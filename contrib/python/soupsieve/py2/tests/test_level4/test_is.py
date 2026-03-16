"""Test is selectors."""
from __future__ import unicode_literals
from .. import util
from soupsieve import SelectorSyntaxError


class TestIs(util.TestCase):
    """Test is selectors."""

    MARKUP = """
    <div>
    <p>Some text <span id="1"> in a paragraph</span>.
    <a id="2" href="http://google.com">Link</a>
    </p>
    </div>
    """

    def test_is(self):
        """Test multiple selectors with "is"."""

        self.assert_selector(
            self.MARKUP,
            ":is(span, a)",
            ["1", "2"],
            flags=util.HTML
        )

    def test_nested_is(self):
        """Test multiple nested selectors."""

        self.assert_selector(
            self.MARKUP,
            ":is(span, a:is(#\\32))",
            ["1", "2"],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            ":is(span, a:is(#\\32))",
            ["1", "2"],
            flags=util.HTML
        )

    def test_is_with_other_pseudo(self):
        """Test `:is()` behavior when paired with `:not()`."""

        # Each pseudo class is evaluated separately
        # So this will not match
        self.assert_selector(
            self.MARKUP,
            ":is(span):not(span)",
            [],
            flags=util.HTML
        )

    def test_multiple_is(self):
        """Test `:is()` behavior when paired with `:not()`."""

        # Each pseudo class is evaluated separately
        # So this will not match
        self.assert_selector(
            self.MARKUP,
            ":is(span):is(div)",
            [],
            flags=util.HTML
        )

        # Each pseudo class is evaluated separately
        # So this will match
        self.assert_selector(
            self.MARKUP,
            ":is(a):is(#\\32)",
            ['2'],
            flags=util.HTML
        )

    def test_invalid_pseudo_class_start_combinator(self):
        """Test invalid start combinator in pseudo-classes other than `:has()`."""

        self.assert_raises(':is(> div)', SelectorSyntaxError)
        self.assert_raises(':is(div, > div)', SelectorSyntaxError)

    def test_invalid_pseudo_orphan_close(self):
        """Test invalid, orphaned pseudo close."""

        self.assert_raises('div)', SelectorSyntaxError)

    def test_invalid_pseudo_dangling_comma(self):
        """Test pseudo class group with trailing comma."""

        self.assert_raises(':is(div,)', SelectorSyntaxError)

    def test_invalid_pseudo_open(self):
        """Test invalid pseudo close."""

        self.assert_raises(':is(div', SelectorSyntaxError)
