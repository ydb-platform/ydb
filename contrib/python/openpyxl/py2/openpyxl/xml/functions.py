from __future__ import absolute_import
# Copyright (c) 2010-2019 openpyxl

"""
XML compatability functions
"""

# Python stdlib imports
import re
from functools import partial
# compatibility

# package imports
from openpyxl import DEFUSEDXML, LXML

if LXML is True:
    from lxml.etree import (
    Element,
    ElementTree,
    SubElement,
    register_namespace,
    QName,
    xmlfile,
    XMLParser,
    )
    from lxml.etree import XMLSyntaxError

    if DEFUSEDXML is True:
        from defusedxml.common import DefusedXmlException
        from defusedxml.cElementTree import iterparse
        from defusedxml.lxml import fromstring as _fromstring, tostring

        def fromstring(*args, **kwargs):
            try:
                return _fromstring(*args, **kwargs)
            except XMLSyntaxError as e:
                raise DefusedXmlException(str(e))
    else:
        from lxml.etree import fromstring, tostring
        from xml.etree.cElementTree import iterparse
        # do not resolve entities
        safe_parser = XMLParser(resolve_entities=False)
        fromstring = partial(fromstring, parser=safe_parser)

else:
    try:
        from xml.etree.cElementTree import (
        ElementTree,
        Element,
        SubElement,
        QName,
        register_namespace
        )
        if DEFUSEDXML is True:
            from defusedxml.cElementTree import (
            fromstring,
            tostring,
            iterparse,
            )
        else:
            from xml.etree.cElementTree import (
            fromstring,
            tostring,
            iterparse
            )
    except ImportError:
        from xml.etree.ElementTree import (
        ElementTree,
        Element,
        SubElement,
        QName,
        register_namespace
        )
        if DEFUSEDXML is True:
            from defusedxml.ElementTree import (
            fromstring,
            tostring,
            iterparse,
            )
        else:
            from xml.etree.ElementTree import (
            fromstring,
            tostring,
            iterparse,
            )
    from et_xmlfile import xmlfile


from openpyxl.xml.constants import (
    CHART_NS,
    DRAWING_NS,
    SHEET_DRAWING_NS,
    CHART_DRAWING_NS,
    SHEET_MAIN_NS,
    REL_NS,
    VTYPES_NS,
    COREPROPS_NS,
    DCTERMS_NS,
    DCTERMS_PREFIX,
    XML_NS
)

register_namespace(DCTERMS_PREFIX, DCTERMS_NS)
register_namespace('dcmitype', 'http://purl.org/dc/dcmitype/')
register_namespace('cp', COREPROPS_NS)
register_namespace('c', CHART_NS)
register_namespace('a', DRAWING_NS)
register_namespace('s', SHEET_MAIN_NS)
register_namespace('r', REL_NS)
register_namespace('vt', VTYPES_NS)
register_namespace('xdr', SHEET_DRAWING_NS)
register_namespace('cdr', CHART_DRAWING_NS)
register_namespace('xml', XML_NS)


tostring = partial(tostring, encoding="utf-8")

NS_REGEX = re.compile("({(?P<namespace>.*)})?(?P<localname>.*)")

def localname(node):
    if callable(node.tag):
        return "comment"
    m = NS_REGEX.match(node.tag)
    return m.group('localname')


def whitespace(node):
    if node.text != node.text.strip():
        node.set("{%s}space" % XML_NS, "preserve")
