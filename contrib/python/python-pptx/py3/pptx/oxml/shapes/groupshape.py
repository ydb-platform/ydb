"""lxml custom element classes for shape-tree-related XML elements."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterator

from pptx.enum.shapes import MSO_CONNECTOR_TYPE
from pptx.oxml import parse_xml
from pptx.oxml.ns import nsdecls, qn
from pptx.oxml.shapes.autoshape import CT_Shape
from pptx.oxml.shapes.connector import CT_Connector
from pptx.oxml.shapes.graphfrm import CT_GraphicalObjectFrame
from pptx.oxml.shapes.picture import CT_Picture
from pptx.oxml.shapes.shared import BaseShapeElement
from pptx.oxml.xmlchemy import BaseOxmlElement, OneAndOnlyOne, ZeroOrOne
from pptx.util import Emu

if TYPE_CHECKING:
    from pptx.enum.shapes import PP_PLACEHOLDER
    from pptx.oxml.shapes import ShapeElement
    from pptx.oxml.shapes.shared import CT_Transform2D


class CT_GroupShape(BaseShapeElement):
    """Used for shape tree (`p:spTree`) as well as the group shape (`p:grpSp`) elements."""

    nvGrpSpPr: CT_GroupShapeNonVisual = OneAndOnlyOne(  # pyright: ignore[reportAssignmentType]
        "p:nvGrpSpPr"
    )
    grpSpPr: CT_GroupShapeProperties = OneAndOnlyOne(  # pyright: ignore[reportAssignmentType]
        "p:grpSpPr"
    )

    _shape_tags = (
        qn("p:sp"),
        qn("p:grpSp"),
        qn("p:graphicFrame"),
        qn("p:cxnSp"),
        qn("p:pic"),
        qn("p:contentPart"),
    )

    def add_autoshape(
        self, id_: int, name: str, prst: str, x: int, y: int, cx: int, cy: int
    ) -> CT_Shape:
        """Return new `p:sp` appended to the group/shapetree with specified attributes."""
        sp = CT_Shape.new_autoshape_sp(id_, name, prst, x, y, cx, cy)
        self.insert_element_before(sp, "p:extLst")
        return sp

    def add_cxnSp(
        self,
        id_: int,
        name: str,
        type_member: MSO_CONNECTOR_TYPE,
        x: int,
        y: int,
        cx: int,
        cy: int,
        flipH: bool,
        flipV: bool,
    ) -> CT_Connector:
        """Return new `p:cxnSp` appended to the group/shapetree with the specified attribues."""
        prst = MSO_CONNECTOR_TYPE.to_xml(type_member)
        cxnSp = CT_Connector.new_cxnSp(id_, name, prst, x, y, cx, cy, flipH, flipV)
        self.insert_element_before(cxnSp, "p:extLst")
        return cxnSp

    def add_freeform_sp(self, x: int, y: int, cx: int, cy: int) -> CT_Shape:
        """Append a new freeform `p:sp` with specified position and size."""
        shape_id = self._next_shape_id
        name = "Freeform %d" % (shape_id - 1,)
        sp = CT_Shape.new_freeform_sp(shape_id, name, x, y, cx, cy)
        self.insert_element_before(sp, "p:extLst")
        return sp

    def add_grpSp(self) -> CT_GroupShape:
        """Return `p:grpSp` element newly appended to this shape tree.

        The element contains no sub-shapes, is positioned at (0, 0), and has
        width and height of zero.
        """
        shape_id = self._next_shape_id
        name = "Group %d" % (shape_id - 1,)
        grpSp = CT_GroupShape.new_grpSp(shape_id, name)
        self.insert_element_before(grpSp, "p:extLst")
        return grpSp

    def add_pic(
        self, id_: int, name: str, desc: str, rId: str, x: int, y: int, cx: int, cy: int
    ) -> CT_Picture:
        """Append a `p:pic` shape to the group/shapetree having properties as specified in call."""
        pic = CT_Picture.new_pic(id_, name, desc, rId, x, y, cx, cy)
        self.insert_element_before(pic, "p:extLst")
        return pic

    def add_placeholder(
        self, id_: int, name: str, ph_type: PP_PLACEHOLDER, orient: str, sz: str, idx: int
    ) -> CT_Shape:
        """Append a newly-created placeholder `p:sp` shape having the specified properties."""
        sp = CT_Shape.new_placeholder_sp(id_, name, ph_type, orient, sz, idx)
        self.insert_element_before(sp, "p:extLst")
        return sp

    def add_table(
        self, id_: int, name: str, rows: int, cols: int, x: int, y: int, cx: int, cy: int
    ) -> CT_GraphicalObjectFrame:
        """Append a `p:graphicFrame` shape containing a table as specified in call."""
        graphicFrame = CT_GraphicalObjectFrame.new_table_graphicFrame(
            id_, name, rows, cols, x, y, cx, cy
        )
        self.insert_element_before(graphicFrame, "p:extLst")
        return graphicFrame

    def add_textbox(self, id_: int, name: str, x: int, y: int, cx: int, cy: int) -> CT_Shape:
        """Append a newly-created textbox `p:sp` shape having the specified position and size."""
        sp = CT_Shape.new_textbox_sp(id_, name, x, y, cx, cy)
        self.insert_element_before(sp, "p:extLst")
        return sp

    @property
    def chExt(self):
        """Descendent `p:grpSpPr/a:xfrm/a:chExt` element."""
        return self.grpSpPr.get_or_add_xfrm().get_or_add_chExt()

    @property
    def chOff(self):
        """Descendent `p:grpSpPr/a:xfrm/a:chOff` element."""
        return self.grpSpPr.get_or_add_xfrm().get_or_add_chOff()

    def get_or_add_xfrm(self) -> CT_Transform2D:
        """Return the `a:xfrm` grandchild element, newly-added if not present."""
        return self.grpSpPr.get_or_add_xfrm()

    def iter_ph_elms(self):
        """Generate each placeholder shape child element in document order."""
        for e in self.iter_shape_elms():
            if e.has_ph_elm:
                yield e

    def iter_shape_elms(self) -> Iterator[ShapeElement]:
        """Generate each child of this `p:spTree` element that corresponds to a shape.

        Items appear in XML document order.
        """
        for elm in self.iterchildren():
            if elm.tag in self._shape_tags:
                yield elm

    @property
    def max_shape_id(self) -> int:
        """Maximum int value assigned as @id in this slide.

        This is generally a shape-id, but ids can be assigned to other
        objects so we just check all @id values anywhere in the document
        (XML id-values have document scope).

        In practice, its minimum value is 1 because the spTree element itself
        is always assigned id="1".
        """
        id_str_lst = self.xpath("//@id")
        used_ids = [int(id_str) for id_str in id_str_lst if id_str.isdigit()]
        return max(used_ids) if used_ids else 0

    @classmethod
    def new_grpSp(cls, id_: int, name: str) -> CT_GroupShape:
        """Return new "loose" `p:grpSp` element having `id_` and `name`."""
        xml = (
            "<p:grpSp %s>\n"
            "  <p:nvGrpSpPr>\n"
            '    <p:cNvPr id="%%d" name="%%s"/>\n'
            "    <p:cNvGrpSpPr/>\n"
            "    <p:nvPr/>\n"
            "  </p:nvGrpSpPr>\n"
            "  <p:grpSpPr>\n"
            "    <a:xfrm>\n"
            '      <a:off x="0" y="0"/>\n'
            '      <a:ext cx="0" cy="0"/>\n'
            '      <a:chOff x="0" y="0"/>\n'
            '      <a:chExt cx="0" cy="0"/>\n'
            "    </a:xfrm>\n"
            "  </p:grpSpPr>\n"
            "</p:grpSp>" % nsdecls("a", "p", "r")
        ) % (id_, name)
        grpSp = parse_xml(xml)
        return grpSp

    def recalculate_extents(self) -> None:
        """Adjust x, y, cx, and cy to incorporate all contained shapes.

        This would typically be called when a contained shape is added,
        removed, or its position or size updated.

        This method is recursive "upwards" since a change in a group shape
        can change the position and size of its containing group.
        """
        if not self.tag == qn("p:grpSp"):
            return

        x, y, cx, cy = self._child_extents

        self.chOff.x = self.x = x
        self.chOff.y = self.y = y
        self.chExt.cx = self.cx = cx
        self.chExt.cy = self.cy = cy
        self.getparent().recalculate_extents()

    @property
    def xfrm(self) -> CT_Transform2D | None:
        """The `a:xfrm` grandchild element or |None| if not found."""
        return self.grpSpPr.xfrm

    @property
    def _child_extents(self) -> tuple[int, int, int, int]:
        """(x, y, cx, cy) tuple representing net position and size.

        The values are formed as a composite of the contained child shapes.
        """
        child_shape_elms = list(self.iter_shape_elms())

        if not child_shape_elms:
            return Emu(0), Emu(0), Emu(0), Emu(0)

        min_x = min([xSp.x for xSp in child_shape_elms])
        min_y = min([xSp.y for xSp in child_shape_elms])
        max_x = max([(xSp.x + xSp.cx) for xSp in child_shape_elms])
        max_y = max([(xSp.y + xSp.cy) for xSp in child_shape_elms])

        x = min_x
        y = min_y
        cx = max_x - min_x
        cy = max_y - min_y

        return x, y, cx, cy

    @property
    def _next_shape_id(self) -> int:
        """Return unique shape id suitable for use with a new shape element.

        The returned id is the next available positive integer drawing object
        id in shape tree, starting from 1 and making use of any gaps in
        numbering. In practice, the minimum id is 2 because the spTree
        element itself is always assigned id="1".
        """
        id_str_lst = self.xpath("//@id")
        used_ids = [int(id_str) for id_str in id_str_lst if id_str.isdigit()]
        for n in range(1, len(used_ids) + 2):
            if n not in used_ids:
                return n


class CT_GroupShapeNonVisual(BaseShapeElement):
    """`p:nvGrpSpPr` element."""

    cNvPr = OneAndOnlyOne("p:cNvPr")


class CT_GroupShapeProperties(BaseOxmlElement):
    """p:grpSpPr element"""

    get_or_add_xfrm: Callable[[], CT_Transform2D]

    _tag_seq = (
        "a:xfrm",
        "a:noFill",
        "a:solidFill",
        "a:gradFill",
        "a:blipFill",
        "a:pattFill",
        "a:grpFill",
        "a:effectLst",
        "a:effectDag",
        "a:scene3d",
        "a:extLst",
    )
    xfrm: CT_Transform2D | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:xfrm", successors=_tag_seq[1:]
    )
    effectLst = ZeroOrOne("a:effectLst", successors=_tag_seq[8:])
    del _tag_seq
