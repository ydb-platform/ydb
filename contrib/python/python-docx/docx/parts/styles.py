# encoding: utf-8

"""
Provides StylesPart and related objects
"""

from __future__ import (
    absolute_import, division, print_function, unicode_literals
)

import os

from ..opc.constants import CONTENT_TYPE as CT
from ..opc.packuri import PackURI
from ..opc.part import XmlPart
from ..oxml import parse_xml
from ..styles.styles import Styles
from ..utils import get_file


class StylesPart(XmlPart):
    """
    Proxy for the styles.xml part containing style definitions for a document
    or glossary.
    """
    @classmethod
    def default(cls, package):
        """
        Return a newly created styles part, containing a default set of
        elements.
        """
        partname = PackURI('/word/styles.xml')
        content_type = CT.WML_STYLES
        element = parse_xml(cls._default_styles_xml())
        return cls(partname, content_type, element, package)

    @property
    def styles(self):
        """
        The |_Styles| instance containing the styles (<w:style> element
        proxies) for this styles part.
        """
        return Styles(self.element)

    @classmethod
    def _default_styles_xml(cls):
        """
        Return a bytestream containing XML for a default styles part.
        """
        f = get_file(
            'templates',
            'default-styles.xml'
        )
        xml_bytes = f.read()
        return xml_bytes
