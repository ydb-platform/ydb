"""Test defined selectors."""
from .. import util


class TestDefined(util.TestCase):
    """Test defined selectors."""

    def test_defined_html(self):
        """Test defined HTML."""

        markup = """
        <!DOCTYPE html>
        <html>
        <head>
        </head>
        <body>
        <div id="0"></div>
        <div-custom id="1"></div-custom>
        <prefix:div id="2"></prefix:div>
        <prefix:div-custom id="3"></prefix:div-custom>
        </body>
        </html>
        """

        self.assert_selector(
            markup,
            'body :defined',
            ['0', '2', '3'],
            flags=util.HTML
        )

    @util.skip_no_lxml
    def test_defined_xhtml(self):
        """Test defined XHTML."""

        markup = """
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN"
            "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
        <html lang="en" xmlns="http://www.w3.org/1999/xhtml">
        <head>
        </head>
        <body>
        <div id="0"></div>
        <div-custom id="1"></div-custom>
        <prefix:div id="2"></prefix:div>
        <!--
        lxml seems to strip away the prefix in versions less than 4.4.0.
        This was most likely because prefix with no namespace is not really valid.
        XML does allow colons in names, but encourages them to be used for namespaces.
        This is a quirk of LXML, but it appears to be fine in 4.4.0+.
        -->
        <prefix:div-custom id="3"></prefix:div-custom>
        </body>
        </html>
        """

        from lxml import etree

        self.assert_selector(
            markup,
            'body :defined',
            # We should get 3, but for LXML versions less than 4.4.0 we don't for reasons stated above.
            ['0', '2'] if etree.LXML_VERSION < (4, 4, 0, 0) else ['0', '1', '2'],
            flags=util.XHTML
        )

    def test_defined_xml(self):
        """Test defined HTML."""

        markup = """
        <?xml version="1.0" encoding="UTF-8"?>
        <html>
        <head>
        </head>
        <body>
        <div id="0"></div>
        <div-custom id="1"></div-custom>
        <prefix:div id="2"></prefix:div>
        <prefix:div-custom id="3"></prefix:div-custom>
        </body>
        </html>
        """

        # Defined is a browser thing.
        # XML doesn't care about defined and this will match nothing in XML.
        self.assert_selector(
            markup,
            'body :defined',
            [],
            flags=util.XML
        )
