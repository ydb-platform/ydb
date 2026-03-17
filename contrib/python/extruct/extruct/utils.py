# mypy: disallow_untyped_defs=False
import lxml.html

from extruct.xmldom import XmlDomHTMLParser


def parse_html(html, encoding):
    """Parse HTML using lxml.html.HTMLParser, return a tree"""
    parser = lxml.html.HTMLParser(encoding=encoding)
    return lxml.html.fromstring(html, parser=parser)


def parse_xmldom_html(html, encoding):
    """Parse HTML using XmlDomHTMLParser, return a tree"""
    parser = XmlDomHTMLParser(encoding=encoding)
    return lxml.html.fromstring(html, parser=parser)
