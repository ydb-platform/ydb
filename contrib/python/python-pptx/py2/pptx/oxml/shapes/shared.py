# encoding: utf-8

"""Common shape-related oxml objects."""

from __future__ import absolute_import, division, print_function, unicode_literals

from pptx.dml.fill import CT_GradientFillProperties
from pptx.enum.shapes import PP_PLACEHOLDER
from pptx.oxml.ns import qn
from pptx.oxml.simpletypes import (
    ST_Angle,
    ST_Coordinate,
    ST_Direction,
    ST_DrawingElementId,
    ST_LineWidth,
    ST_PlaceholderSize,
    ST_PositiveCoordinate,
    XsdBoolean,
    XsdString,
    XsdUnsignedInt,
)
from pptx.oxml.xmlchemy import (
    BaseOxmlElement,
    Choice,
    OptionalAttribute,
    OxmlElement,
    RequiredAttribute,
    ZeroOrOne,
    ZeroOrOneChoice,
)
from pptx.util import Emu


class BaseShapeElement(BaseOxmlElement):
    """
    Provides common behavior for shape element classes like CT_Shape,
    CT_Picture, etc.
    """

    @property
    def cx(self):
        return self._get_xfrm_attr("cx")

    @cx.setter
    def cx(self, value):
        self._set_xfrm_attr("cx", value)

    @property
    def cy(self):
        return self._get_xfrm_attr("cy")

    @cy.setter
    def cy(self, value):
        self._set_xfrm_attr("cy", value)

    @property
    def flipH(self):
        return bool(self._get_xfrm_attr("flipH"))

    @flipH.setter
    def flipH(self, value):
        self._set_xfrm_attr("flipH", value)

    @property
    def flipV(self):
        return bool(self._get_xfrm_attr("flipV"))

    @flipV.setter
    def flipV(self, value):
        self._set_xfrm_attr("flipV", value)

    def get_or_add_xfrm(self):
        """
        Return the ``<a:xfrm>`` grandchild element, newly-added if not
        present. This version works for ``<p:sp>``, ``<p:cxnSp>``, and
        ``<p:pic>`` elements, others will need to override.
        """
        return self.spPr.get_or_add_xfrm()

    @property
    def has_ph_elm(self):
        """
        True if this shape element has a ``<p:ph>`` descendant, indicating it
        is a placeholder shape. False otherwise.
        """
        return self.ph is not None

    @property
    def ph(self):
        """
        The ``<p:ph>`` descendant element if there is one, None otherwise.
        """
        ph_elms = self.xpath("./*[1]/p:nvPr/p:ph")
        if len(ph_elms) == 0:
            return None
        return ph_elms[0]

    @property
    def ph_idx(self):
        """
        Integer value of placeholder idx attribute. Raises |ValueError| if
        shape is not a placeholder.
        """
        ph = self.ph
        if ph is None:
            raise ValueError("not a placeholder shape")
        return ph.idx

    @property
    def ph_orient(self):
        """
        Placeholder orientation, e.g. 'vert'. Raises |ValueError| if shape is
        not a placeholder.
        """
        ph = self.ph
        if ph is None:
            raise ValueError("not a placeholder shape")
        return ph.orient

    @property
    def ph_sz(self):
        """
        Placeholder size, e.g. ST_PlaceholderSize.HALF, None if shape has no
        ``<p:ph>`` descendant.
        """
        ph = self.ph
        if ph is None:
            raise ValueError("not a placeholder shape")
        return ph.sz

    @property
    def ph_type(self):
        """
        Placeholder type, e.g. ST_PlaceholderType.TITLE ('title'), none if
        shape has no ``<p:ph>`` descendant.
        """
        ph = self.ph
        if ph is None:
            raise ValueError("not a placeholder shape")
        return ph.type

    @property
    def rot(self):
        """
        Float representing degrees this shape is rotated clockwise.
        """
        xfrm = self.xfrm
        if xfrm is None:
            return 0.0
        return xfrm.rot

    @rot.setter
    def rot(self, value):
        self.get_or_add_xfrm().rot = value

    @property
    def shape_id(self):
        """
        Integer id of this shape
        """
        return self._nvXxPr.cNvPr.id

    @property
    def shape_name(self):
        """
        Name of this shape
        """
        return self._nvXxPr.cNvPr.name

    @property
    def txBody(self):
        """
        Child ``<p:txBody>`` element, None if not present
        """
        return self.find(qn("p:txBody"))

    @property
    def x(self):
        return self._get_xfrm_attr("x")

    @x.setter
    def x(self, value):
        self._set_xfrm_attr("x", value)

    @property
    def xfrm(self):
        """
        The ``<a:xfrm>`` grandchild element or |None| if not found. This
        version works for ``<p:sp>``, ``<p:cxnSp>``, and ``<p:pic>``
        elements, others will need to override.
        """
        return self.spPr.xfrm

    @property
    def y(self):
        return self._get_xfrm_attr("y")

    @y.setter
    def y(self, value):
        self._set_xfrm_attr("y", value)

    @property
    def _nvXxPr(self):
        """
        Required non-visual shape properties element for this shape. Actual
        name depends on the shape type, e.g. ``<p:nvPicPr>`` for picture
        shape.
        """
        return self.xpath("./*[1]")[0]

    def _get_xfrm_attr(self, name):
        xfrm = self.xfrm
        if xfrm is None:
            return None
        return getattr(xfrm, name)

    def _set_xfrm_attr(self, name, value):
        xfrm = self.get_or_add_xfrm()
        setattr(xfrm, name, value)


class CT_ApplicationNonVisualDrawingProps(BaseOxmlElement):
    """
    ``<p:nvPr>`` element
    """

    ph = ZeroOrOne(
        "p:ph",
        successors=(
            "a:audioCd",
            "a:wavAudioFile",
            "a:audioFile",
            "a:videoFile",
            "a:quickTimeFile",
            "p:custDataLst",
            "p:extLst",
        ),
    )


class CT_LineProperties(BaseOxmlElement):
    """Custom element class for <a:ln> element"""

    _tag_seq = (
        "a:noFill",
        "a:solidFill",
        "a:gradFill",
        "a:pattFill",
        "a:prstDash",
        "a:custDash",
        "a:round",
        "a:bevel",
        "a:miter",
        "a:headEnd",
        "a:tailEnd",
        "a:extLst",
    )
    eg_lineFillProperties = ZeroOrOneChoice(
        (
            Choice("a:noFill"),
            Choice("a:solidFill"),
            Choice("a:gradFill"),
            Choice("a:pattFill"),
        ),
        successors=_tag_seq[4:],
    )
    prstDash = ZeroOrOne("a:prstDash", successors=_tag_seq[5:])
    custDash = ZeroOrOne("a:custDash", successors=_tag_seq[6:])
    del _tag_seq
    w = OptionalAttribute("w", ST_LineWidth, default=Emu(0))

    @property
    def eg_fillProperties(self):
        """
        Required to fulfill the interface used by dml.fill.
        """
        return self.eg_lineFillProperties

    @property
    def prstDash_val(self):
        """Return value of `val` attribute of `a:prstDash` child.

        Return |None| if not present.
        """
        prstDash = self.prstDash
        if prstDash is None:
            return None
        return prstDash.val

    @prstDash_val.setter
    def prstDash_val(self, val):
        self._remove_custDash()
        prstDash = self.get_or_add_prstDash()
        prstDash.val = val


class CT_NonVisualDrawingProps(BaseOxmlElement):
    """
    ``<p:cNvPr>`` custom element class.
    """

    _tag_seq = ("a:hlinkClick", "a:hlinkHover", "a:extLst")
    hlinkClick = ZeroOrOne("a:hlinkClick", successors=_tag_seq[1:])
    hlinkHover = ZeroOrOne("a:hlinkHover", successors=_tag_seq[2:])
    id = RequiredAttribute("id", ST_DrawingElementId)
    name = RequiredAttribute("name", XsdString)
    del _tag_seq


class CT_Placeholder(BaseOxmlElement):
    """
    ``<p:ph>`` custom element class.
    """

    type = OptionalAttribute("type", PP_PLACEHOLDER, default=PP_PLACEHOLDER.OBJECT)
    orient = OptionalAttribute("orient", ST_Direction, default=ST_Direction.HORZ)
    sz = OptionalAttribute("sz", ST_PlaceholderSize, default=ST_PlaceholderSize.FULL)
    idx = OptionalAttribute("idx", XsdUnsignedInt, default=0)


class CT_Point2D(BaseOxmlElement):
    """
    Custom element class for <a:off> element.
    """

    x = RequiredAttribute("x", ST_Coordinate)
    y = RequiredAttribute("y", ST_Coordinate)


class CT_PositiveSize2D(BaseOxmlElement):
    """
    Custom element class for <a:ext> element.
    """

    cx = RequiredAttribute("cx", ST_PositiveCoordinate)
    cy = RequiredAttribute("cy", ST_PositiveCoordinate)


class CT_ShapeProperties(BaseOxmlElement):
    """Custom element class for `p:spPr` element.

    Shared by `p:sp`, `p:cxnSp`,  and `p:pic` elements as well as a few more
    obscure ones.
    """

    _tag_seq = (
        "a:xfrm",
        "a:custGeom",
        "a:prstGeom",
        "a:noFill",
        "a:solidFill",
        "a:gradFill",
        "a:blipFill",
        "a:pattFill",
        "a:grpFill",
        "a:ln",
        "a:effectLst",
        "a:effectDag",
        "a:scene3d",
        "a:sp3d",
        "a:extLst",
    )
    xfrm = ZeroOrOne("a:xfrm", successors=_tag_seq[1:])
    custGeom = ZeroOrOne("a:custGeom", successors=_tag_seq[2:])
    prstGeom = ZeroOrOne("a:prstGeom", successors=_tag_seq[3:])
    eg_fillProperties = ZeroOrOneChoice(
        (
            Choice("a:noFill"),
            Choice("a:solidFill"),
            Choice("a:gradFill"),
            Choice("a:blipFill"),
            Choice("a:pattFill"),
            Choice("a:grpFill"),
        ),
        successors=_tag_seq[9:],
    )
    ln = ZeroOrOne("a:ln", successors=_tag_seq[10:])
    effectLst = ZeroOrOne("a:effectLst", successors=_tag_seq[11:])
    del _tag_seq

    @property
    def cx(self):
        """
        Shape width as an instance of Emu, or None if not present.
        """
        cx_str_lst = self.xpath("./a:xfrm/a:ext/@cx")
        if not cx_str_lst:
            return None
        return Emu(cx_str_lst[0])

    @property
    def cy(self):
        """
        Shape height as an instance of Emu, or None if not present.
        """
        cy_str_lst = self.xpath("./a:xfrm/a:ext/@cy")
        if not cy_str_lst:
            return None
        return Emu(cy_str_lst[0])

    @property
    def x(self):
        """
        The offset of the left edge of the shape from the left edge of the
        slide, as an instance of Emu. Corresponds to the value of the
        `./xfrm/off/@x` attribute. None if not present.
        """
        x_str_lst = self.xpath("./a:xfrm/a:off/@x")
        if not x_str_lst:
            return None
        return Emu(x_str_lst[0])

    @property
    def y(self):
        """
        The offset of the top of the shape from the top of the slide, as an
        instance of Emu. None if not present.
        """
        y_str_lst = self.xpath("./a:xfrm/a:off/@y")
        if not y_str_lst:
            return None
        return Emu(y_str_lst[0])

    def _new_gradFill(self):
        return CT_GradientFillProperties.new_gradFill()


class CT_Transform2D(BaseOxmlElement):
    """`a:xfrm` custom element class.

    NOTE: this is a composite including CT_GroupTransform2D, which appears
    with the `a:xfrm` tag in a group shape (including a slide `p:spTree`).
    """

    _tag_seq = ("a:off", "a:ext", "a:chOff", "a:chExt")
    off = ZeroOrOne("a:off", successors=_tag_seq[1:])
    ext = ZeroOrOne("a:ext", successors=_tag_seq[2:])
    chOff = ZeroOrOne("a:chOff", successors=_tag_seq[3:])
    chExt = ZeroOrOne("a:chExt", successors=_tag_seq[4:])
    del _tag_seq
    rot = OptionalAttribute("rot", ST_Angle, default=0.0)
    flipH = OptionalAttribute("flipH", XsdBoolean, default=False)
    flipV = OptionalAttribute("flipV", XsdBoolean, default=False)

    @property
    def x(self):
        off = self.off
        if off is None:
            return None
        return off.x

    @x.setter
    def x(self, value):
        off = self.get_or_add_off()
        off.x = value

    @property
    def y(self):
        off = self.off
        if off is None:
            return None
        return off.y

    @y.setter
    def y(self, value):
        off = self.get_or_add_off()
        off.y = value

    @property
    def cx(self):
        ext = self.ext
        if ext is None:
            return None
        return ext.cx

    @cx.setter
    def cx(self, value):
        ext = self.get_or_add_ext()
        ext.cx = value

    @property
    def cy(self):
        ext = self.ext
        if ext is None:
            return None
        return ext.cy

    @cy.setter
    def cy(self, value):
        ext = self.get_or_add_ext()
        ext.cy = value

    def _new_ext(self):
        ext = OxmlElement("a:ext")
        ext.cx = 0
        ext.cy = 0
        return ext

    def _new_off(self):
        off = OxmlElement("a:off")
        off.x = 0
        off.y = 0
        return off
