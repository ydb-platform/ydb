# encoding: utf-8

"""
Series-related oxml objects.
"""

from __future__ import absolute_import, print_function, unicode_literals

from .datalabel import CT_DLbls
from ..simpletypes import XsdUnsignedInt
from ..xmlchemy import (
    BaseOxmlElement,
    OneAndOnlyOne,
    OxmlElement,
    RequiredAttribute,
    ZeroOrMore,
    ZeroOrOne,
)


class CT_AxDataSource(BaseOxmlElement):
    """
    ``<c:cat>`` custom element class used in category charts to specify
    category labels and hierarchy.
    """

    multiLvlStrRef = ZeroOrOne("c:multiLvlStrRef", successors=())

    @property
    def lvls(self):
        """
        Return a list containing the `c:lvl` descendent elements in document
        order. These will only be present when the required single child
        is a `c:multiLvlStrRef` element. Returns an empty list when no
        `c:lvl` descendent elements are present.
        """
        return self.xpath(".//c:lvl")


class CT_DPt(BaseOxmlElement):
    """
    ``<c:dPt>`` custom element class, containing visual properties for a data
    point.
    """

    _tag_seq = (
        "c:idx",
        "c:invertIfNegative",
        "c:marker",
        "c:bubble3D",
        "c:explosion",
        "c:spPr",
        "c:pictureOptions",
        "c:extLst",
    )
    idx = OneAndOnlyOne("c:idx")
    marker = ZeroOrOne("c:marker", successors=_tag_seq[3:])
    spPr = ZeroOrOne("c:spPr", successors=_tag_seq[6:])
    del _tag_seq

    @classmethod
    def new_dPt(cls):
        """
        Return a newly created "loose" `c:dPt` element containing its default
        subtree.
        """
        dPt = OxmlElement("c:dPt")
        dPt.append(OxmlElement("c:idx"))
        return dPt


class CT_Lvl(BaseOxmlElement):
    """
    ``<c:lvl>`` custom element class used in multi-level categories to
    specify a level of hierarchy.
    """

    pt = ZeroOrMore("c:pt", successors=())


class CT_NumDataSource(BaseOxmlElement):
    """
    ``<c:yVal>`` custom element class used in XY and bubble charts, and
    perhaps others.
    """

    numRef = OneAndOnlyOne("c:numRef")

    @property
    def ptCount_val(self):
        """
        Return the value of `./c:numRef/c:numCache/c:ptCount/@val`,
        specifying how many `c:pt` elements are in this numeric data cache.
        Returns 0 if no `c:ptCount` element is present, as this is the least
        disruptive way to degrade when no cached point data is available.
        This situation is not expected, but is valid according to the schema.
        """
        results = self.xpath(".//c:ptCount/@val")
        return int(results[0]) if results else 0

    def pt_v(self, idx):
        """
        Return the Y value for data point *idx* in this cache, or None if no
        value is present for that data point.
        """
        results = self.xpath(".//c:pt[@idx=%d]" % idx)
        return results[0].value if results else None


class CT_SeriesComposite(BaseOxmlElement):
    """
    ``<c:ser>`` custom element class. Note there are several different series
    element types in the schema, such as ``CT_LineSer`` and ``CT_BarSer``,
    but they all share the same tag name. This class acts as a composite and
    depends on the caller not to do anything invalid for a series belonging
    to a particular plot type.
    """

    _tag_seq = (
        "c:idx",
        "c:order",
        "c:tx",
        "c:spPr",
        "c:invertIfNegative",
        "c:pictureOptions",
        "c:marker",
        "c:explosion",
        "c:dPt",
        "c:dLbls",
        "c:trendline",
        "c:errBars",
        "c:cat",
        "c:val",
        "c:xVal",
        "c:yVal",
        "c:shape",
        "c:smooth",
        "c:bubbleSize",
        "c:bubble3D",
        "c:extLst",
    )
    idx = OneAndOnlyOne("c:idx")
    order = OneAndOnlyOne("c:order")
    tx = ZeroOrOne("c:tx", successors=_tag_seq[3:])
    spPr = ZeroOrOne("c:spPr", successors=_tag_seq[4:])
    invertIfNegative = ZeroOrOne("c:invertIfNegative", successors=_tag_seq[5:])
    marker = ZeroOrOne("c:marker", successors=_tag_seq[7:])
    dPt = ZeroOrMore("c:dPt", successors=_tag_seq[9:])
    dLbls = ZeroOrOne("c:dLbls", successors=_tag_seq[10:])
    cat = ZeroOrOne("c:cat", successors=_tag_seq[13:])
    val = ZeroOrOne("c:val", successors=_tag_seq[14:])
    xVal = ZeroOrOne("c:xVal", successors=_tag_seq[15:])
    yVal = ZeroOrOne("c:yVal", successors=_tag_seq[16:])
    smooth = ZeroOrOne("c:smooth", successors=_tag_seq[18:])
    bubbleSize = ZeroOrOne("c:bubbleSize", successors=_tag_seq[19:])
    del _tag_seq

    @property
    def bubbleSize_ptCount_val(self):
        """
        Return the number of bubble size values as reflected in the `val`
        attribute of `./c:bubbleSize//c:ptCount`, or 0 if not present.
        """
        vals = self.xpath("./c:bubbleSize//c:ptCount/@val")
        if not vals:
            return 0
        return int(vals[0])

    @property
    def cat_ptCount_val(self):
        """
        Return the number of categories as reflected in the `val` attribute
        of `./c:cat//c:ptCount`, or 0 if not present.
        """
        vals = self.xpath("./c:cat//c:ptCount/@val")
        if not vals:
            return 0
        return int(vals[0])

    def get_dLbl(self, idx):
        """
        Return the `c:dLbl` element representing the label for the data point
        at offset *idx* in this series, or |None| if not present.
        """
        dLbls = self.dLbls
        if dLbls is None:
            return None
        return dLbls.get_dLbl_for_point(idx)

    def get_or_add_dLbl(self, idx):
        """
        Return the `c:dLbl` element representing the label of the point at
        offset *idx* in this series, newly created if not yet present.
        """
        dLbls = self.get_or_add_dLbls()
        return dLbls.get_or_add_dLbl_for_point(idx)

    def get_or_add_dPt_for_point(self, idx):
        """
        Return the `c:dPt` child representing the visual properties of the
        data point at index *idx*.
        """
        matches = self.xpath('c:dPt[c:idx[@val="%d"]]' % idx)
        if matches:
            return matches[0]
        dPt = self._add_dPt()
        dPt.idx.val = idx
        return dPt

    @property
    def xVal_ptCount_val(self):
        """
        Return the number of X values as reflected in the `val` attribute of
        `./c:xVal//c:ptCount`, or 0 if not present.
        """
        vals = self.xpath("./c:xVal//c:ptCount/@val")
        if not vals:
            return 0
        return int(vals[0])

    @property
    def yVal_ptCount_val(self):
        """
        Return the number of Y values as reflected in the `val` attribute of
        `./c:yVal//c:ptCount`, or 0 if not present.
        """
        vals = self.xpath("./c:yVal//c:ptCount/@val")
        if not vals:
            return 0
        return int(vals[0])

    def _new_dLbls(self):
        """Override metaclass method that creates `c:dLbls` element."""
        return CT_DLbls.new_dLbls()

    def _new_dPt(self):
        """
        Overrides the metaclass generated method to get `c:dPt` with minimal
        subtree.
        """
        return CT_DPt.new_dPt()


class CT_StrVal_NumVal_Composite(BaseOxmlElement):
    """
    ``<c:pt>`` element, can be either CT_StrVal or CT_NumVal complex type.
    Using this class for both, differentiating as needed.
    """

    v = OneAndOnlyOne("c:v")
    idx = RequiredAttribute("idx", XsdUnsignedInt)

    @property
    def value(self):
        """
        The float value of the text in the required ``<c:v>`` child.
        """
        return float(self.v.text)
