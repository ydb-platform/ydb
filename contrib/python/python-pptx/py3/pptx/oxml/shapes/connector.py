"""lxml custom element classes for XML elements related to the Connector shape."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from pptx.oxml import parse_xml
from pptx.oxml.ns import nsdecls
from pptx.oxml.shapes.shared import BaseShapeElement
from pptx.oxml.simpletypes import ST_DrawingElementId, XsdUnsignedInt
from pptx.oxml.xmlchemy import BaseOxmlElement, OneAndOnlyOne, RequiredAttribute, ZeroOrOne

if TYPE_CHECKING:
    from pptx.oxml.shapes.shared import CT_ShapeProperties


class CT_Connection(BaseShapeElement):
    """A `a:stCxn` or `a:endCxn` element.

    Specifies a connection between an end-point of a connector and a shape connection point.
    """

    id = RequiredAttribute("id", ST_DrawingElementId)
    idx = RequiredAttribute("idx", XsdUnsignedInt)


class CT_Connector(BaseShapeElement):
    """A line/connector shape `p:cxnSp` element"""

    _tag_seq = ("p:nvCxnSpPr", "p:spPr", "p:style", "p:extLst")
    nvCxnSpPr = OneAndOnlyOne("p:nvCxnSpPr")
    spPr: CT_ShapeProperties = OneAndOnlyOne("p:spPr")  # pyright: ignore[reportAssignmentType]
    del _tag_seq

    @classmethod
    def new_cxnSp(
        cls,
        id_: int,
        name: str,
        prst: str,
        x: int,
        y: int,
        cx: int,
        cy: int,
        flipH: bool,
        flipV: bool,
    ) -> CT_Connector:
        """Return a new `p:cxnSp` element tree configured as a base connector."""
        flip = (' flipH="1"' if flipH else "") + (' flipV="1"' if flipV else "")
        return cast(
            CT_Connector,
            parse_xml(
                f"<p:cxnSp {nsdecls('a', 'p')}>\n"
                f"  <p:nvCxnSpPr>\n"
                f'    <p:cNvPr id="{id_}" name="{name}"/>\n'
                f"    <p:cNvCxnSpPr/>\n"
                f"    <p:nvPr/>\n"
                f"  </p:nvCxnSpPr>\n"
                f"  <p:spPr>\n"
                f"    <a:xfrm{flip}>\n"
                f'      <a:off x="{x}" y="{y}"/>\n'
                f'      <a:ext cx="{cx}" cy="{cy}"/>\n'
                f"    </a:xfrm>\n"
                f'    <a:prstGeom prst="{prst}">\n'
                f"      <a:avLst/>\n"
                f"    </a:prstGeom>\n"
                f"  </p:spPr>\n"
                f"  <p:style>\n"
                f'    <a:lnRef idx="2">\n'
                f'      <a:schemeClr val="accent1"/>\n'
                f"    </a:lnRef>\n"
                f'    <a:fillRef idx="0">\n'
                f'      <a:schemeClr val="accent1"/>\n'
                f"    </a:fillRef>\n"
                f'    <a:effectRef idx="1">\n'
                f'      <a:schemeClr val="accent1"/>\n'
                f"    </a:effectRef>\n"
                f'    <a:fontRef idx="minor">\n'
                f'      <a:schemeClr val="tx1"/>\n'
                f"    </a:fontRef>\n"
                f"  </p:style>\n"
                f"</p:cxnSp>"
            ),
        )


class CT_ConnectorNonVisual(BaseOxmlElement):
    """
    `p:nvCxnSpPr` element, container for the non-visual properties of
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
