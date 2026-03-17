"""Test contains selectors."""
from .. import util
import warnings
import soupsieve as sv
from bs4 import BeautifulSoup

IFRAME_TEXT = BeautifulSoup('<iframe><div></div></iframe>', 'html.parser').iframe.text == '<div></div>'


class TestSoupContains(util.TestCase):
    """Test soup-contains selectors."""

    MARKUP = """
    <body>
    <div id="1">
    Testing
    <span id="2"> that </span>
    contains works.
    </div>
    </body>
    """

    def test_contains(self):
        """Test contains."""

        self.assert_selector(
            self.MARKUP,
            'body span:-soup-contains(that)',
            ['2'],
            flags=util.HTML
        )

    def test_contains_quoted_with_space(self):
        """Test contains quoted with spaces."""

        self.assert_selector(
            self.MARKUP,
            'body span:-soup-contains(" that ")',
            ['2'],
            flags=util.HTML
        )

    def test_contains_quoted_without_space(self):
        """Test contains quoted with spaces."""

        self.assert_selector(
            self.MARKUP,
            'body :-soup-contains( "Testing" )',
            ['1'],
            flags=util.HTML
        )

    def test_contains_quoted_with_escaped_newline(self):
        """Test contains quoted with escaped newline."""

        self.assert_selector(
            self.MARKUP,
            'body :-soup-contains("Test\\\ning")',
            ['1'],
            flags=util.HTML
        )

    def test_contains_quoted_with_escaped_newline_with_carriage_return(self):
        """Test contains quoted with escaped newline with carriage return."""

        self.assert_selector(
            self.MARKUP,
            'body :-soup-contains("Test\\\r\ning")',
            ['1'],
            flags=util.HTML
        )

    def test_contains_list(self):
        """Test contains list."""

        self.assert_selector(
            self.MARKUP,
            'body span:-soup-contains("does not exist", "that")',
            ['2'],
            flags=util.HTML
        )

    def test_contains_multiple(self):
        """Test contains multiple."""

        self.assert_selector(
            self.MARKUP,
            'body span:-soup-contains("th"):-soup-contains("at")',
            ['2'],
            flags=util.HTML
        )

    def test_contains_multiple_not_match(self):
        """Test contains multiple with "not" and with a match."""

        self.assert_selector(
            self.MARKUP,
            'body span:not(:-soup-contains("does not exist")):-soup-contains("that")',
            ['2'],
            flags=util.HTML
        )

    def test_contains_multiple_not_no_match(self):
        """Test contains multiple with "not" and no match."""

        self.assert_selector(
            self.MARKUP,
            'body span:not(:-soup-contains("that")):-soup-contains("that")',
            [],
            flags=util.HTML
        )

    def test_contains_with_descendants(self):
        """Test that contains returns descendants as well as the top level that contain."""

        self.assert_selector(
            self.MARKUP,
            'body :-soup-contains(" that ")',
            ['1', '2'],
            flags=util.HTML
        )

    def test_contains_bad(self):
        """Test contains when it finds no text."""

        self.assert_selector(
            self.MARKUP,
            'body :-soup-contains(bad)',
            [],
            flags=util.HTML
        )

    def test_contains_escapes(self):
        """Test contains with escape characters."""

        markup = """
        <body>
        <div id="1">Testing<span id="2">
        that</span>contains works.</div>
        </body>
        """

        self.assert_selector(
            markup,
            r'body span:-soup-contains("\0a that")',
            ['2'],
            flags=util.HTML
        )

    def test_contains_cdata_html5(self):
        """Test contains CDATA in HTML5."""

        markup = """
        <body><div id="1">Testing that <span id="2"><![CDATA[that]]></span>contains works.</div></body>
        """

        self.assert_selector(
            markup,
            'body *:-soup-contains("that")',
            ['1'],
            flags=util.HTML5
        )

    def test_contains_cdata_py_html(self):
        """Test contains CDATA in Python HTML parser."""

        markup = """
        <body><div id="1">Testing that <span id="2"><![CDATA[that]]></span>contains works.</div></body>
        """

        self.assert_selector(
            markup,
            'body *:-soup-contains("that")',
            ['1'],
            flags=util.PYHTML
        )

    @util.skip_no_lxml
    def test_contains_cdata_lxml_html(self):
        """Test contains CDATA in `lxml` HTML parser."""

        from lxml import etree
        libxml_ver = etree.LIBXML_VERSION

        markup = """
        <body><div id="1">Testing that <span id="2"><![CDATA[that]]></span>contains works.</div></body>
        """

        results = ['1', '2'] if libxml_ver >= (2, 9, 11) else ['1']
        self.assert_selector(
            markup,
            'body *:-soup-contains("that")',
            results,
            flags=util.LXML_HTML
        )

    def test_contains_cdata_xhtml(self):
        """Test contains CDATA in XHTML."""

        markup = """
        <div id="1">Testing that <span id="2"><![CDATA[that]]></span>contains works.</div>
        """

        self.assert_selector(
            self.wrap_xhtml(markup),
            'body *:-soup-contains("that")',
            ['1', '2'],
            flags=util.XHTML
        )

    def test_contains_cdata_xml(self):
        """Test contains CDATA in XML."""

        markup = """
        <div id="1">Testing that <span id="2"><![CDATA[that]]></span>contains works.</div>
        """

        self.assert_selector(
            markup,
            '*:-soup-contains("that")',
            ['1', '2'],
            flags=util.XML
        )

    def test_contains_iframe(self):
        """Test contains with `iframe`."""

        markup = """
        <div id="1">
        <p>Testing text</p>
        <iframe>
        <html><body>
        <span id="2">iframe</span>
        </body></html>
        </iframe>
        </div>
        """

        self.assert_selector(
            markup,
            'div:-soup-contains("iframe")',
            [],
            flags=util.PYHTML
        )

        self.assert_selector(
            markup,
            'div:-soup-contains("text")',
            ['1'],
            flags=util.PYHTML
        )

        self.assert_selector(
            markup,
            'span:-soup-contains("iframe")',
            [] if IFRAME_TEXT else ['2'],
            flags=util.PYHTML
        )

    def test_contains_iframe_xml(self):
        """Test contains with `iframe` which shouldn't matter in XML."""

        markup = """
        <div id="1">
        <p>Testing text</p>
        <iframe>
        <html><body>
        <span id="2">iframe</span>
        </body></html>
        </iframe>
        </div>
        """

        self.assert_selector(
            markup,
            'div:-soup-contains("iframe")',
            ['1'],
            flags=util.XML
        )

        self.assert_selector(
            markup,
            'div:-soup-contains("text")',
            ['1'],
            flags=util.XML
        )

        self.assert_selector(
            markup,
            'span:-soup-contains("iframe")',
            ['2'],
            flags=util.XML
        )

    def test_contains_warn(self):
        """Test old alias raises a warning."""

        sv.purge()

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            # Trigger a warning.
            self.assert_selector(
                self.MARKUP,
                'body span:contains(that)',
                ['2'],
                flags=util.HTML
            )
            found = False
            target = "The pseudo class ':contains' is deprecated, ':-soup-contains' should be used moving forward."
            for warn in w:
                if target in str(warn.message):
                    found = True
                    break
            # Verify some things
            self.assertTrue(found)
