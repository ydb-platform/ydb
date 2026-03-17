# encoding: utf-8

"""
lxml custom element classes for shape-related XML elements.
"""

from __future__ import absolute_import

from .. import parse_xml
from ..ns import nsdecls
from .shared import BaseShapeElement
from ..simpletypes import ST_DrawingElementId, XsdUnsignedInt
from ..xmlchemy import BaseOxmlElement, OneAndOnlyOne, RequiredAttribute, ZeroOrOne


class CT_Connection(BaseShapeElement):
    """
    A `a:stCxn` or `a:endCxn` element specifying a connection between
    an end-point of a connector and a shape connection point.
    """

    id = RequiredAttribute("id", ST_DrawingElementId)
    idx = RequiredAttribute("idx", XsdUnsignedInt)


class CT_Connector(BaseShapeElement):
    """
    A line/connector shape ``<p:cxnSp>`` element
    """

    _tag_seq = ("p:nvCxnSpPr", "p:spPr", "p:style", "p:extLst")
    nvCxnSpPr = OneAndOnlyOne("p:nvCxnSpPr")
    spPr = OneAndOnlyOne("p:spPr")
    del _tag_seq

    @classmethod
    def new_cxnSp(cls, id_, name, prst, x, y, cx, cy, flipH, flipV):
        """
        Return a new ``<p:cxnSp>`` element tree configured as a base
        connector.
        """
        tmpl = cls._cxnSp_tmpl()
        flip = (' flipH="1"' if flipH else "") + (' flipV="1"' if flipV else "")
        xml = tmpl.format(
            **{
                "nsdecls": nsdecls("a", "p"),
                "id": id_,
                "name": name,
                "x": x,
                "y": y,
                "cx": cx,
                "cy": cy,
                "prst": prst,
                "flip": flip,
            }
        )
        return parse_xml(xml)

    @staticmethod
    def _cxnSp_tmpl():
        return (
            "<p:cxnSp {nsdecls}>\n"
            "  <p:nvCxnSpPr>\n"
            '    <p:cNvPr id="{id}" name="{name}"/>\n'
            "    <p:cNvCxnSpPr/>\n"
            "    <p:nvPr/>\n"
            "  </p:nvCxnSpPr>\n"
            "  <p:spPr>\n"
            "    <a:xfrm{flip}>\n"
            '      <a:off x="{x}" y="{y}"/>\n'
            '      <a:ext cx="{cx}" cy="{cy}"/>\n'
            "    </a:xfrm>\n"
            '    <a:prstGeom prst="{prst}">\n'
            "      <a:avLst/>\n"
            "    </a:prstGeom>\n"
            "  </p:spPr>\n"
            "  <p:style>\n"
            '    <a:lnRef idx="2">\n'
            '      <a:schemeClr val="accent1"/>\n'
            "    </a:lnRef>\n"
            '    <a:fillRef idx="0">\n'
            '      <a:schemeClr val="accent1"/>\n'
            "    </a:fillRef>\n"
            '    <a:effectRef idx="1">\n'
            '      <a:schemeClr val="accent1"/>\n'
            "    </a:effectRef>\n"
            '    <a:fontRef idx="minor">\n'
            '      <a:schemeClr val="tx1"/>\n'
            "    </a:fontRef>\n"
            "  </p:style>\n"
            "</p:cxnSp>"
        )


class CT_ConnectorNonVisual(BaseOxmlElement):
    """
    ``<p:nvCxnSpPr>`` element, container for the non-visual properties of
    a connector, such as name, id, etc.
    """

    cNvPr = OneAndOnlyOne("p:cNvPr")
    cNvCxnSpPr = OneAndOnlyOne("p:cNvCxnSpPr")
    nvPr = OneAndOnlyOne("p:nvPr")


class CT_NonVisualConnectorProperties(BaseOxmlElement):
    """
    `p:cNvCxnSpPr` element, container for the non-visual properties specific
    to a connector shape, such as connections and connector locking.
    """

    _tag_seq = ("a:cxnSpLocks", "a:stCxn", "a:endCxn", "a:extLst")
    stCxn = ZeroOrOne("a:stCxn", successors=_tag_seq[2:])
    endCxn = ZeroOrOne("a:endCxn", successors=_tag_seq[3:])
    del _tag_seq
