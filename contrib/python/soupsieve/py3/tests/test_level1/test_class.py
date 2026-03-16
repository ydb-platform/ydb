"""Test class selectors."""
from .. import util
from soupsieve import SelectorSyntaxError


class TestClass(util.TestCase):
    """Test class selectors."""

    MARKUP = """
    <div>
    <p>Some text <span id="1" class="foo"> in a paragraph</span>.
    <a id="2" class="bar" href="http://google.com">Link</a>
    </p>
    </div>
    """

    # Browsers normally replace NULL with `\uFFFD`, but some of the parsers
    # we test just strip out NULL, so we will simulate and just insert `\uFFFD` directly
    # to ensure consistent behavior in our tests across parsers.
    MARKUP_NULL = """
    <div>
    <p>Some text <span id="1" class="foo\ufffd"> in a paragraph</span>.
    <a id="2" class="\ufffdbar" href="http://google.com">Link</a>
    </p>
    </div>
    """

    def test_class(self):
        """Test class."""

        self.assert_selector(
            self.MARKUP,
            ".foo",
            ["1"],
            flags=util.HTML
        )

    def test_type_and_class(self):
        """Test type and class."""

        self.assert_selector(
            self.MARKUP,
            "a.bar",
            ["2"],
            flags=util.HTML
        )

    def test_type_and_class_escaped_null(self):
        """Test type and class with an escaped null character."""

        self.assert_selector(
            self.MARKUP_NULL,
            r"a.\0 bar",
            ["2"],
            flags=util.HTML
        )

    def test_type_and_class_escaped_eof(self):
        """Test type and class with an escaped EOF."""

        self.assert_selector(
            self.MARKUP_NULL,
            "span.foo\\",
            ["1"],
            flags=util.HTML
        )

    def test_malformed_class(self):
        """Test malformed class."""

        # Malformed class
        self.assert_raises('td.+#some-id', SelectorSyntaxError)

    def test_class_xhtml(self):
        """Test tag and class with XHTML since internally classes are stored different for XML."""

        self.assert_selector(
            self.wrap_xhtml(self.MARKUP),
            ".foo",
            ["1"],
            flags=util.XHTML
        )

    def test_multiple_classes(self):
        """Test multiple classes."""

        markup = """
        <div>
        <p>Some text <span id="1" class="foo"> in a paragraph</span>.
        <a id="2" class="bar" href="http://google.com">Link</a>
        <a id="3" class="foo" href="http://google.com">Link</a>
        <a id="4" class="foo bar" href="http://google.com">Link</a>
        </p>
        </div>
        """

        self.assert_selector(
            markup,
            "a.foo.bar",
            ["4"],
            flags=util.HTML
        )

    def test_malformed_pseudo_class(self):
        """Test malformed class."""

        # Malformed pseudo-class
        self.assert_raises('td:#id', SelectorSyntaxError)
