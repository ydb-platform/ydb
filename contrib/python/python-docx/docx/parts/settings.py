# encoding: utf-8

"""
|SettingsPart| and closely related objects
"""

from __future__ import (
    absolute_import, division, print_function, unicode_literals
)

import os

from ..opc.constants import CONTENT_TYPE as CT
from ..opc.packuri import PackURI
from ..opc.part import XmlPart
from ..oxml import parse_xml
from ..settings import Settings
from ..utils import get_file


class SettingsPart(XmlPart):
    """
    Document-level settings part of a WordprocessingML (WML) package.
    """
    @classmethod
    def default(cls, package):
        """
        Return a newly created settings part, containing a default
        `w:settings` element tree.
        """
        partname = PackURI('/word/settings.xml')
        content_type = CT.WML_SETTINGS
        element = parse_xml(cls._default_settings_xml())
        return cls(partname, content_type, element, package)

    @property
    def settings(self):
        """
        A |Settings| proxy object for the `w:settings` element in this part,
        containing the document-level settings for this document.
        """
        return Settings(self.element)

    @classmethod
    def _default_settings_xml(cls):
        """
        Return a bytestream containing XML for a default settings part.
        """
        f = get_file(
            'templates',
            'default-settings.xml'
        )
        xml_bytes = f.read()
        return xml_bytes
