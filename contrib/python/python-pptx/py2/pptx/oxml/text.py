# encoding: utf-8

"""Custom element classes for text-related XML elements"""

from __future__ import absolute_import, division, print_function, unicode_literals

import re

from pptx.compat import to_unicode
from pptx.enum.lang import MSO_LANGUAGE_ID
from pptx.enum.text import (
    MSO_AUTO_SIZE,
    MSO_TEXT_UNDERLINE_TYPE,
    MSO_VERTICAL_ANCHOR,
    PP_PARAGRAPH_ALIGNMENT,
)
from pptx.exc import InvalidXmlError
from pptx.oxml import parse_xml
from pptx.oxml.dml.fill import CT_GradientFillProperties
from pptx.oxml.ns import nsdecls
from pptx.oxml.simpletypes import (
    ST_Coordinate32,
    ST_TextFontScalePercentOrPercentString,
    ST_TextFontSize,
    ST_TextIndentLevelType,
    ST_TextSpacingPercentOrPercentString,
    ST_TextSpacingPoint,
    ST_TextTypeface,
    ST_TextWrappingType,
    XsdBoolean,
)
from pptx.oxml.xmlchemy import (
    BaseOxmlElement,
    Choice,
    OneAndOnlyOne,
    OneOrMore,
    OptionalAttribute,
    RequiredAttribute,
    ZeroOrMore,
    ZeroOrOne,
    ZeroOrOneChoice,
)
from pptx.util import Emu, Length


class CT_RegularTextRun(BaseOxmlElement):
    """`a:r` custom element class"""

    rPr = ZeroOrOne("a:rPr", successors=("a:t",))
    t = OneAndOnlyOne("a:t")

    @property
    def text(self):
        """(unicode) str containing text of (required) `a:t` child"""
        text = self.t.text
        # t.text is None when t element is empty, e.g. '<a:t/>'
        return to_unicode(text) if text is not None else ""

    @text.setter
    def text(self, str):
        """*str* is unicode value to replace run text."""
        self.t.text = self._escape_ctrl_chars(str)

    @staticmethod
    def _escape_ctrl_chars(s):
        """Return str after replacing each control character with a plain-text escape.

        For example, a BEL character (x07) would appear as "_x0007_". Horizontal-tab
        (x09) and line-feed (x0A) are not escaped. All other characters in the range
        x00-x1F are escaped.
        """
        return re.sub(
            r"([\x00-\x08\x0B-\x1F])", lambda match: "_x%04X_" % ord(match.group(1)), s
        )


class CT_TextBody(BaseOxmlElement):
    """`p:txBody` custom element class.

    Also used for `c:txPr` in charts and perhaps other elements.
    """

    bodyPr = OneAndOnlyOne("a:bodyPr")
    p = OneOrMore("a:p")

    def clear_content(self):
        """Remove all `a:p` children, but leave any others.

        cf. lxml `_Element.clear()` method which removes all children.
        """
        for p in self.p_lst:
            self.remove(p)

    @property
    def defRPr(self):
        """
        ``<a:defRPr>`` element of required first ``p`` child, added with its
        ancestors if not present. Used when element is a ``<c:txPr>`` in
        a chart and the ``p`` element is used only to specify formatting, not
        content.
        """
        p = self.p_lst[0]
        pPr = p.get_or_add_pPr()
        defRPr = pPr.get_or_add_defRPr()
        return defRPr

    @property
    def is_empty(self):
        """True if only a single empty `a:p` element is present."""
        ps = self.p_lst
        if len(ps) > 1:
            return False

        if not ps:
            raise InvalidXmlError("p:txBody must have at least one a:p")

        if ps[0].text != "":
            return False
        return True

    @classmethod
    def new(cls):
        """
        Return a new ``<p:txBody>`` element tree
        """
        xml = cls._txBody_tmpl()
        txBody = parse_xml(xml)
        return txBody

    @classmethod
    def new_a_txBody(cls):
        """
        Return a new ``<a:txBody>`` element tree, suitable for use in a table
        cell and possibly other situations.
        """
        xml = cls._a_txBody_tmpl()
        txBody = parse_xml(xml)
        return txBody

    @classmethod
    def new_p_txBody(cls):
        """
        Return a new ``<p:txBody>`` element tree, suitable for use in an
        ``<p:sp>`` element.
        """
        xml = cls._p_txBody_tmpl()
        return parse_xml(xml)

    @classmethod
    def new_txPr(cls):
        """
        Return a ``<c:txPr>`` element tree suitable for use in a chart object
        like data labels or tick labels.
        """
        xml = (
            "<c:txPr %s>\n"
            "  <a:bodyPr/>\n"
            "  <a:lstStyle/>\n"
            "  <a:p>\n"
            "    <a:pPr>\n"
            "      <a:defRPr/>\n"
            "    </a:pPr>\n"
            "  </a:p>\n"
            "</c:txPr>\n"
        ) % nsdecls("c", "a")
        txPr = parse_xml(xml)
        return txPr

    def unclear_content(self):
        """Ensure p:txBody has at least one a:p child.

        Intuitively, reverse a ".clear_content()" operation to minimum
        conformance with spec (single empty paragraph).
        """
        if len(self.p_lst) > 0:
            return
        self.add_p()

    @classmethod
    def _a_txBody_tmpl(cls):
        return (
            "<a:txBody %s>\n"
            "  <a:bodyPr/>\n"
            "  <a:p/>\n"
            "</a:txBody>\n" % (nsdecls("a"))
        )

    @classmethod
    def _p_txBody_tmpl(cls):
        return (
            "<p:txBody %s>\n"
            "  <a:bodyPr/>\n"
            "  <a:p/>\n"
            "</p:txBody>\n" % (nsdecls("p", "a"))
        )

    @classmethod
    def _txBody_tmpl(cls):
        return (
            "<p:txBody %s>\n"
            "  <a:bodyPr/>\n"
            "  <a:lstStyle/>\n"
            "  <a:p/>\n"
            "</p:txBody>\n" % (nsdecls("a", "p"))
        )


class CT_TextBodyProperties(BaseOxmlElement):
    """
    <a:bodyPr> custom element class
    """

    eg_textAutoFit = ZeroOrOneChoice(
        (Choice("a:noAutofit"), Choice("a:normAutofit"), Choice("a:spAutoFit")),
        successors=("a:scene3d", "a:sp3d", "a:flatTx", "a:extLst"),
    )
    lIns = OptionalAttribute("lIns", ST_Coordinate32, default=Emu(91440))
    tIns = OptionalAttribute("tIns", ST_Coordinate32, default=Emu(45720))
    rIns = OptionalAttribute("rIns", ST_Coordinate32, default=Emu(91440))
    bIns = OptionalAttribute("bIns", ST_Coordinate32, default=Emu(45720))
    anchor = OptionalAttribute("anchor", MSO_VERTICAL_ANCHOR)
    wrap = OptionalAttribute("wrap", ST_TextWrappingType)

    @property
    def autofit(self):
        """
        The autofit setting for the text frame, a member of the
        ``MSO_AUTO_SIZE`` enumeration.
        """
        if self.noAutofit is not None:
            return MSO_AUTO_SIZE.NONE
        if self.normAutofit is not None:
            return MSO_AUTO_SIZE.TEXT_TO_FIT_SHAPE
        if self.spAutoFit is not None:
            return MSO_AUTO_SIZE.SHAPE_TO_FIT_TEXT
        return None

    @autofit.setter
    def autofit(self, value):
        if value is not None and value not in MSO_AUTO_SIZE._valid_settings:
            raise ValueError(
                "only None or a member of the MSO_AUTO_SIZE enumeration can "
                "be assigned to CT_TextBodyProperties.autofit, got %s" % value
            )
        self._remove_eg_textAutoFit()
        if value == MSO_AUTO_SIZE.NONE:
            self._add_noAutofit()
        elif value == MSO_AUTO_SIZE.TEXT_TO_FIT_SHAPE:
            self._add_normAutofit()
        elif value == MSO_AUTO_SIZE.SHAPE_TO_FIT_TEXT:
            self._add_spAutoFit()


class CT_TextCharacterProperties(BaseOxmlElement):
    """`a:rPr, a:defRPr, and `a:endParaRPr` custom element class.

    'rPr' is short for 'run properties', and it corresponds to the |Font|
    proxy class.
    """

    eg_fillProperties = ZeroOrOneChoice(
        (
            Choice("a:noFill"),
            Choice("a:solidFill"),
            Choice("a:gradFill"),
            Choice("a:blipFill"),
            Choice("a:pattFill"),
            Choice("a:grpFill"),
        ),
        successors=(
            "a:effectLst",
            "a:effectDag",
            "a:highlight",
            "a:uLnTx",
            "a:uLn",
            "a:uFillTx",
            "a:uFill",
            "a:latin",
            "a:ea",
            "a:cs",
            "a:sym",
            "a:hlinkClick",
            "a:hlinkMouseOver",
            "a:rtl",
            "a:extLst",
        ),
    )
    latin = ZeroOrOne(
        "a:latin",
        successors=(
            "a:ea",
            "a:cs",
            "a:sym",
            "a:hlinkClick",
            "a:hlinkMouseOver",
            "a:rtl",
            "a:extLst",
        ),
    )
    hlinkClick = ZeroOrOne(
        "a:hlinkClick", successors=("a:hlinkMouseOver", "a:rtl", "a:extLst")
    )

    lang = OptionalAttribute("lang", MSO_LANGUAGE_ID)
    sz = OptionalAttribute("sz", ST_TextFontSize)
    b = OptionalAttribute("b", XsdBoolean)
    i = OptionalAttribute("i", XsdBoolean)
    u = OptionalAttribute("u", MSO_TEXT_UNDERLINE_TYPE)

    def _new_gradFill(self):
        return CT_GradientFillProperties.new_gradFill()

    def add_hlinkClick(self, rId):
        """
        Add an <a:hlinkClick> child element with r:id attribute set to *rId*.
        """
        hlinkClick = self.get_or_add_hlinkClick()
        hlinkClick.rId = rId
        return hlinkClick


class CT_TextField(BaseOxmlElement):
    """
    <a:fld> field element, for either a slide number or date field
    """

    rPr = ZeroOrOne("a:rPr", successors=("a:pPr", "a:t"))
    t = ZeroOrOne("a:t", successors=())

    @property
    def text(self):
        """
        The text of the ``<a:t>`` child element.
        """
        t = self.t
        if t is None:
            return ""
        text = t.text
        return to_unicode(text) if text is not None else ""


class CT_TextFont(BaseOxmlElement):
    """
    Custom element class for <a:latin>, <a:ea>, <a:cs>, and <a:sym> child
    elements of CT_TextCharacterProperties, e.g. <a:rPr>.
    """

    typeface = RequiredAttribute("typeface", ST_TextTypeface)


class CT_TextLineBreak(BaseOxmlElement):
    """`a:br` line break element"""

    rPr = ZeroOrOne("a:rPr", successors=())

    @property
    def text(self):
        """Unconditionally a single vertical-tab character.

        A line break element can contain no text other than the implicit line feed it
        represents.
        """
        return "\v"


class CT_TextNormalAutofit(BaseOxmlElement):
    """
    <a:normAutofit> element specifying fit text to shape font reduction, etc.
    """

    fontScale = OptionalAttribute(
        "fontScale", ST_TextFontScalePercentOrPercentString, default=100.0
    )


class CT_TextParagraph(BaseOxmlElement):
    """`a:p` custom element class"""

    pPr = ZeroOrOne("a:pPr", successors=("a:r", "a:br", "a:fld", "a:endParaRPr"))
    r = ZeroOrMore("a:r", successors=("a:endParaRPr",))
    br = ZeroOrMore("a:br", successors=("a:endParaRPr",))
    endParaRPr = ZeroOrOne("a:endParaRPr", successors=())

    def add_br(self):
        """
        Return a newly appended <a:br> element.
        """
        return self._add_br()

    def add_r(self, text=None):
        """
        Return a newly appended <a:r> element.
        """
        r = self._add_r()
        if text:
            r.text = text
        return r

    def append_text(self, text):
        """Append `a:r` and `a:br` elements to *p* based on *text*.

        Any `\n` or `\v` (vertical-tab) characters in *text* delimit `a:r` (run)
        elements and themselves are translated to `a:br` (line-break) elements. The
        vertical-tab character appears in clipboard text from PowerPoint at "soft"
        line-breaks (new-line, but not new paragraph).
        """
        for idx, r_str in enumerate(re.split("\n|\v", text)):
            # ---breaks are only added *between* items, not at start---
            if idx > 0:
                self.add_br()
            # ---runs that would be empty are not added---
            if r_str:
                self.add_r(r_str)

    @property
    def content_children(self):
        """Sequence containing text-container child elements of this `a:p` element.

        These include `a:r`, `a:br`, and `a:fld`.
        """
        text_types = {CT_RegularTextRun, CT_TextLineBreak, CT_TextField}
        return tuple(elm for elm in self if type(elm) in text_types)

    @property
    def text(self):
        """str text contained in this paragraph."""
        # ---note this shadows the lxml _Element.text---
        return "".join([child.text for child in self.content_children])

    def _new_r(self):
        r_xml = "<a:r %s><a:t/></a:r>" % nsdecls("a")
        return parse_xml(r_xml)


class CT_TextParagraphProperties(BaseOxmlElement):
    """
    <a:pPr> custom element class
    """

    _tag_seq = (
        "a:lnSpc",
        "a:spcBef",
        "a:spcAft",
        "a:buClrTx",
        "a:buClr",
        "a:buSzTx",
        "a:buSzPct",
        "a:buSzPts",
        "a:buFontTx",
        "a:buFont",
        "a:buNone",
        "a:buAutoNum",
        "a:buChar",
        "a:buBlip",
        "a:tabLst",
        "a:defRPr",
        "a:extLst",
    )
    lnSpc = ZeroOrOne("a:lnSpc", successors=_tag_seq[1:])
    spcBef = ZeroOrOne("a:spcBef", successors=_tag_seq[2:])
    spcAft = ZeroOrOne("a:spcAft", successors=_tag_seq[3:])
    defRPr = ZeroOrOne("a:defRPr", successors=_tag_seq[16:])
    lvl = OptionalAttribute("lvl", ST_TextIndentLevelType, default=0)
    algn = OptionalAttribute("algn", PP_PARAGRAPH_ALIGNMENT)
    del _tag_seq

    @property
    def line_spacing(self):
        """
        The spacing between baselines of successive lines in this paragraph.
        A float value indicates a number of lines. A |Length| value indicates
        a fixed spacing. Value is contained in `./a:lnSpc/a:spcPts/@val` or
        `./a:lnSpc/a:spcPct/@val`. Value is |None| if no element is present.
        """
        lnSpc = self.lnSpc
        if lnSpc is None:
            return None
        if lnSpc.spcPts is not None:
            return lnSpc.spcPts.val
        return lnSpc.spcPct.val

    @line_spacing.setter
    def line_spacing(self, value):
        self._remove_lnSpc()
        if value is None:
            return
        if isinstance(value, Length):
            self._add_lnSpc().set_spcPts(value)
        else:
            self._add_lnSpc().set_spcPct(value)

    @property
    def space_after(self):
        """
        The EMU equivalent of the centipoints value in
        `./a:spcAft/a:spcPts/@val`.
        """
        spcAft = self.spcAft
        if spcAft is None:
            return None
        spcPts = spcAft.spcPts
        if spcPts is None:
            return None
        return spcPts.val

    @space_after.setter
    def space_after(self, value):
        self._remove_spcAft()
        if value is not None:
            self._add_spcAft().set_spcPts(value)

    @property
    def space_before(self):
        """
        The EMU equivalent of the centipoints value in
        `./a:spcBef/a:spcPts/@val`.
        """
        spcBef = self.spcBef
        if spcBef is None:
            return None
        spcPts = spcBef.spcPts
        if spcPts is None:
            return None
        return spcPts.val

    @space_before.setter
    def space_before(self, value):
        self._remove_spcBef()
        if value is not None:
            self._add_spcBef().set_spcPts(value)


class CT_TextSpacing(BaseOxmlElement):
    """
    Used for <a:lnSpc>, <a:spcBef>, and <a:spcAft> elements.
    """

    # this should actually be a OneAndOnlyOneChoice, but that's not
    # implemented yet.
    spcPct = ZeroOrOne("a:spcPct")
    spcPts = ZeroOrOne("a:spcPts")

    def set_spcPct(self, value):
        """
        Set spacing to *value* lines, e.g. 1.75 lines. A ./a:spcPts child is
        removed if present.
        """
        self._remove_spcPts()
        spcPct = self.get_or_add_spcPct()
        spcPct.val = value

    def set_spcPts(self, value):
        """
        Set spacing to *value* points. A ./a:spcPct child is removed if
        present.
        """
        self._remove_spcPct()
        spcPts = self.get_or_add_spcPts()
        spcPts.val = value


class CT_TextSpacingPercent(BaseOxmlElement):
    """
    <a:spcPct> element, specifying spacing in thousandths of a percent in its
    `val` attribute.
    """

    val = RequiredAttribute("val", ST_TextSpacingPercentOrPercentString)


class CT_TextSpacingPoint(BaseOxmlElement):
    """
    <a:spcPts> element, specifying spacing in centipoints in its `val`
    attribute.
    """

    val = RequiredAttribute("val", ST_TextSpacingPoint)
