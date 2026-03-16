"""Test utilities."""
import unittest
import bs4
import textwrap
import soupsieve as sv
import pytest

try:
    from bs4.builder import HTML5TreeBuilder  # noqa: F401
    HTML5LIB_PRESENT = True
except ImportError:
    HTML5LIB_PRESENT = False

try:
    from bs4.builder import LXMLTreeBuilderForXML, LXMLTreeBuilder  # noqa: F401
    LXML_PRESENT = True
except ImportError:
    LXML_PRESENT = False

HTML5 = 0x1
HTML = 0x2
XHTML = 0x4
XML = 0x8
PYHTML = 0x10
LXML_HTML = 0x20


def skip_no_lxml(func):
    """Decorator that skips lxml is not available."""

    def skip_if(self, *args, **kwargs):
        """Skip conditional wrapper."""

        if LXML_PRESENT:
            return func(self, *args, **kwargs)
        else:
            raise pytest.skip('lxml is not found')


class TestCase(unittest.TestCase):
    """Test case."""

    def wrap_xhtml(self, html):
        """Wrap HTML content with XHTML header and body."""

        return f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN"
            "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
        <html lang="en" xmlns="http://www.w3.org/1999/xhtml">
        <head>
        </head>
        <body>
        {html}
        </body>
        </html>
        """

    def setUp(self):
        """Setup."""

        sv.purge()

    def purge(self):
        """Purge cache."""

        sv.purge()

    def compile_pattern(self, selectors, namespaces=None, custom=None, flags=0):
        """Compile pattern."""

        print('PATTERN: ', selectors)
        flags |= sv.DEBUG
        return sv.compile(selectors, namespaces=namespaces, custom=custom, flags=flags)

    def soup(self, markup, parser):
        """Get soup."""

        print('\n====PARSER: ', parser)
        return bs4.BeautifulSoup(textwrap.dedent(markup.replace('\r\n', '\n')), parser)

    def get_parsers(self, flags):
        """Get parsers."""

        mode = flags & 0x3F
        if mode == HTML:
            parsers = ('html5lib', 'lxml', 'html.parser')
        elif mode == PYHTML:
            parsers = ('html.parser',)
        elif mode == LXML_HTML:
            parsers = ('lxml',)
        elif mode in (HTML5, 0):
            parsers = ('html5lib',)
        elif mode in (XHTML, XML):
            parsers = ('xml',)

        return parsers

    def assert_raises(self, pattern, exception, namespace=None, custom=None):
        """Assert raises."""

        print('----Running Assert Test----')
        with self.assertRaises(exception):
            self.compile_pattern(pattern, namespaces=namespace, custom=custom)

    def assert_selector(self, markup, selectors, expected_ids, namespaces=None, custom=None, flags=0):
        """Assert selector."""

        if namespaces is None:
            namespaces = {}
        parsers = self.get_parsers(flags)

        print('----Running Selector Test----')
        selector = self.compile_pattern(selectors, namespaces, custom)

        for parser in available_parsers(*parsers):
            soup = self.soup(markup, parser)
            # print(soup)

            ids = []
            for el in selector.select(soup):
                print('TAG: ', el.name)
                ids.append(el.attrs['id'])
            self.assertEqual(sorted(ids), sorted(expected_ids))


def available_parsers(*parsers):
    """
    Filter a list of parsers, down to the available ones.

    If there are none, report the test as skipped to pytest.
    """

    ran_test = False
    for parser in parsers:
        if (
            (parser in ('xml', 'lxml') and not LXML_PRESENT) or
            (parser == 'html5lib' and not HTML5LIB_PRESENT)
        ):
            print(f'SKIPPED {parser}, not installed')
        else:
            ran_test = True
            yield parser
    if not ran_test:
        raise pytest.skip('no available parsers')


def requires_lxml(test):
    """Decorator that marks a test as requiring LXML."""

    return pytest.mark.skipif(
        not LXML_PRESENT, reason='test requires lxml')(test)


def requires_html5lib(test):
    """Decorator that marks a test as requiring html5lib."""

    return pytest.mark.skipif(
        not HTML5LIB_PRESENT, reason='test requires html5lib')(test)
