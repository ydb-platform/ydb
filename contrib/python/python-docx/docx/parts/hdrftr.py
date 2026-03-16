# encoding: utf-8

"""Header and footer part objects"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os

from docx.opc.constants import CONTENT_TYPE as CT
from docx.oxml import parse_xml
from docx.parts.story import BaseStoryPart
from docx.utils import get_file


class FooterPart(BaseStoryPart):
    """Definition of a section footer."""

    @classmethod
    def new(cls, package):
        """Return newly created footer part."""
        partname = package.next_partname("/word/footer%d.xml")
        content_type = CT.WML_FOOTER
        element = parse_xml(cls._default_footer_xml())
        return cls(partname, content_type, element, package)

    @classmethod
    def _default_footer_xml(cls):
        """Return bytes containing XML for a default footer part."""
        f = get_file(
            'templates',
            'default-footer.xml'
        )
        xml_bytes = f.read()
        return xml_bytes


class HeaderPart(BaseStoryPart):
    """Definition of a section header."""

    @classmethod
    def new(cls, package):
        """Return newly created header part."""
        partname = package.next_partname("/word/header%d.xml")
        content_type = CT.WML_HEADER
        element = parse_xml(cls._default_header_xml())
        return cls(partname, content_type, element, package)

    @classmethod
    def _default_header_xml(cls):
        """Return bytes containing XML for a default header part."""
        f = get_file(
            'templates', 'default-header.xml'
        )
        xml_bytes = f.read()
        return xml_bytes
