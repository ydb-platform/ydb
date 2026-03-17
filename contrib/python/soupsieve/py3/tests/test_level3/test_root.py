"""Test root selectors."""
from .. import util
import soupsieve as sv
from bs4 import BeautifulSoup
import pytest

IFRAME_TEXT = BeautifulSoup('<iframe><div></div></iframe>', 'html.parser').iframe.text == '<div></div>'


class TestRoot(util.TestCase):
    """Test root selectors."""

    MARKUP = """
    <html id="root">
    <head>
    </head>
    <body>
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
    </body>
    </html>
    """

    MARKUP_IFRAME = """
    <html id="root">
    <head>
    </head>
    <body>
    <div id="div">
    </div>
    <iframe src="https://something.com">
    <html id="root2">
    <head>
    </head>
    <body>
    <div id="div2">
    </div>
    </body>
    </html>
    </iframe>
    <div id="other-div"></div>
    </body>
    </html>
    """

    def test_root(self):
        """Test root."""

        # Root in HTML is `<html>`
        self.assert_selector(
            self.MARKUP,
            ":root",
            ["root"],
            flags=util.HTML
        )

    def test_root_iframe(self):
        """Test root."""

        # Root in HTML is `<html>`
        self.assert_selector(
            self.MARKUP_IFRAME,
            ":root",
            ["root"] if IFRAME_TEXT else ["root", "root2"],
            flags=util.PYHTML
        )

    def test_root_complex(self):
        """Test root within a complex selector."""

        self.assert_selector(
            self.MARKUP,
            ":root > body > div",
            ["div"],
            flags=util.HTML
        )

    def test_no_iframe(self):
        """Test that we don't count `iframe` as root."""

        self.assert_selector(
            self.MARKUP_IFRAME,
            ":root div",
            ["div", "other-div"] if IFRAME_TEXT else ["div", "div2", "other-div"],
            flags=util.PYHTML
        )

        self.assert_selector(
            self.MARKUP_IFRAME,
            ":root > body > div",
            ["div", "other-div"] if IFRAME_TEXT else ["div", "div2", "other-div"],
            flags=util.PYHTML
        )

    @pytest.mark.skipif(IFRAME_TEXT, reason="Requires old Python HTML handling")
    def test_iframe(self):
        """
        Test that we only count `iframe` as root since the scoped element is the root.

        Not all the parsers treat `iframe` content the same. `html5lib` for instance
        will escape the content in the `iframe`, so we are just going to test the builtin
        Python parser.
        """

        soup = self.soup(self.MARKUP_IFRAME, 'html.parser')

        ids = [el['id'] for el in sv.select(':root div', soup.iframe.html)]
        self.assertEqual(sorted(ids), sorted(['div2']))

        ids = [el['id'] for el in sv.select(':root > body > div', soup.iframe.html)]
        self.assertEqual(sorted(ids), sorted(['div2']))
    def test_no_root_double_tag(self):
        """Test when there is no root due to double root tags."""

        markup = """
        <div id="1"></div>
        <div id="2"></div>
        """

        soup = self.soup(markup, 'html.parser')
        self.assertEqual(soup.select(':root'), [])

    def test_no_root_text(self):
        """Test when there is no root due to HTML text."""

        markup = """
        text
        <div id="1"></div>
        """

        soup = self.soup(markup, 'html.parser')
        self.assertEqual(soup.select(':root'), [])

    def test_no_root_cdata(self):
        """Test when there is no root due to CDATA and tag."""

        markup = """
        <![CDATA[test]]>
        <div id="1"></div>
        """

        soup = self.soup(markup, 'html.parser')
        self.assertEqual(soup.select(':root'), [])

    def test_root_whitespace(self):
        """Test when there is root and white space."""

        markup = """

        <div id="1"></div>
        """

        soup = self.soup(markup, 'html.parser')
        ids = [el['id'] for el in soup.select(':root')]
        self.assertEqual(sorted(ids), sorted(['1']))

    def test_root_preprocess(self):
        """Test when there is root and pre-processing statement."""

        markup = """
        <?php ?>
        <div id="1"></div>
        """

        soup = self.soup(markup, 'html.parser')
        ids = [el['id'] for el in soup.select(':root')]
        self.assertEqual(sorted(ids), sorted(['1']))

    def test_root_doctype(self):
        """Test when there is root and doc type."""

        markup = """
        <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
        "http://www.w3.org/TR/html4/strict.dtd">
        <div id="1"></div>
        """

        soup = self.soup(markup, 'html.parser')
        ids = [el['id'] for el in soup.select(':root')]
        self.assertEqual(sorted(ids), sorted(['1']))
