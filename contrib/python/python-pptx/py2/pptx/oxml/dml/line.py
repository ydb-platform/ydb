# encoding: utf-8

"""lxml custom element classes for DrawingML line-related XML elements."""

from __future__ import absolute_import, division, print_function, unicode_literals

from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.oxml.xmlchemy import BaseOxmlElement, OptionalAttribute


class CT_PresetLineDashProperties(BaseOxmlElement):
    """`a:prstDash` custom element class"""

    val = OptionalAttribute("val", MSO_LINE_DASH_STYLE)
