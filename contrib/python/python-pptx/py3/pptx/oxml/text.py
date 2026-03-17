"""Custom element classes for text-related XML elements"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Callable, cast

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

if TYPE_CHECKING:
    from pptx.oxml.action import CT_Hyperlink


class CT_RegularTextRun(BaseOxmlElement):
    """`a:r` custom element class"""

    get_or_add_rPr: Callable[[], CT_TextCharacterProperties]

    rPr: CT_TextCharacterProperties | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:rPr", successors=("a:t",)
    )
    t: BaseOxmlElement = OneAndOnlyOne("a:t")  # pyright: ignore[reportAssignmentType]

    @property
    def text(self) -> str:
        """All text of (required) `a:t` child."""
        text = self.t.text
        # -- t.text is None when t element is empty, e.g. '<a:t/>' --
        return text or ""

    @text.setter
    def text(self, value: str):  # pyright: ignore[reportIncompatibleMethodOverride]
        self.t.text = self._escape_ctrl_chars(value)

    @staticmethod
    def _escape_ctrl_chars(s: str) -> str:
        """Return str after replacing each control character with a plain-text escape.

        For example, a BEL character (x07) would appear as "_x0007_". Horizontal-tab
        (x09) and line-feed (x0A) are not escaped. All other characters in the range
        x00-x1F are escaped.
        """
        return re.sub(r"([\x00-\x08\x0B-\x1F])", lambda match: "_x%04X_" % ord(match.group(1)), s)


class CT_TextBody(BaseOxmlElement):
    """`p:txBody` custom element class.

    Also used for `c:txPr` in charts and perhaps other elements.
    """

    add_p: Callable[[], CT_TextParagraph]
    p_lst: list[CT_TextParagraph]

    bodyPr: CT_TextBodyProperties = OneAndOnlyOne(  # pyright: ignore[reportAssignmentType]
        "a:bodyPr"
    )
    p: CT_TextParagraph = OneOrMore("a:p")  # pyright: ignore[reportAssignmentType]

    def clear_content(self):
        """Remove all `a:p` children, but leave any others.

        cf. lxml `_Element.clear()` method which removes all children.
        """
        for p in self.p_lst:
            self.remove(p)

    @property
    def defRPr(self) -> CT_TextCharacterProperties:
        """`a:defRPr` element of required first `p` child, added with its ancestors if not present.

        Used when element is a ``c:txPr`` in a chart and the `p` element is used only to specify
        formatting, not content.
        """
        p = self.p_lst[0]
        pPr = p.get_or_add_pPr()
        defRPr = pPr.get_or_add_defRPr()
        return defRPr

    @property
    def is_empty(self) -> bool:
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
        """Return a new `p:txBody` element tree."""
        xml = cls._txBody_tmpl()
        txBody = parse_xml(xml)
        return txBody

    @classmethod
    def new_a_txBody(cls) -> CT_TextBody:
        """Return a new `a:txBody` element tree.

        Suitable for use in a table cell and possibly other situations.
        """
        xml = cls._a_txBody_tmpl()
        txBody = cast(CT_TextBody, parse_xml(xml))
        return txBody

    @classmethod
    def new_p_txBody(cls):
        """Return a new `p:txBody` element tree, suitable for use in an `p:sp` element."""
        xml = cls._p_txBody_tmpl()
        return parse_xml(xml)

    @classmethod
    def new_txPr(cls):
        """Return a `c:txPr` element tree.

        Suitable for use in a chart object like data labels or tick labels.
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

        Intuitively, reverse a ".clear_content()" operation to minimum conformance with spec
        (single empty paragraph).
        """
        if len(self.p_lst) > 0:
            return
        self.add_p()

    @classmethod
    def _a_txBody_tmpl(cls):
        return "<a:txBody %s>\n" "  <a:bodyPr/>\n" "  <a:p/>\n" "</a:txBody>\n" % (nsdecls("a"))

    @classmethod
    def _p_txBody_tmpl(cls):
        return (
            "<p:txBody %s>\n" "  <a:bodyPr/>\n" "  <a:p/>\n" "</p:txBody>\n" % (nsdecls("p", "a"))
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
    """`a:bodyPr` custom element class."""

    _add_noAutofit: Callable[[], BaseOxmlElement]
    _add_normAutofit: Callable[[], CT_TextNormalAutofit]
    _add_spAutoFit: Callable[[], BaseOxmlElement]
    _remove_eg_textAutoFit: Callable[[], None]

    noAutofit: BaseOxmlElement | None
    normAutofit: CT_TextNormalAutofit | None
    spAutoFit: BaseOxmlElement | None

    eg_textAutoFit = ZeroOrOneChoice(
        (Choice("a:noAutofit"), Choice("a:normAutofit"), Choice("a:spAutoFit")),
        successors=("a:scene3d", "a:sp3d", "a:flatTx", "a:extLst"),
    )
    lIns: Length = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "lIns", ST_Coordinate32, default=Emu(91440)
    )
    tIns: Length = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "tIns", ST_Coordinate32, default=Emu(45720)
    )
    rIns: Length = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "rIns", ST_Coordinate32, default=Emu(91440)
    )
    bIns: Length = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "bIns", ST_Coordinate32, default=Emu(45720)
    )
    anchor: MSO_VERTICAL_ANCHOR | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "anchor", MSO_VERTICAL_ANCHOR
    )
    wrap: str | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "wrap", ST_TextWrappingType
    )

    @property
    def autofit(self):
        """The autofit setting for the text frame, a member of the `MSO_AUTO_SIZE` enumeration."""
        if self.noAutofit is not None:
            return MSO_AUTO_SIZE.NONE
        if self.normAutofit is not None:
            return MSO_AUTO_SIZE.TEXT_TO_FIT_SHAPE
        if self.spAutoFit is not None:
            return MSO_AUTO_SIZE.SHAPE_TO_FIT_TEXT
        return None

    @autofit.setter
    def autofit(self, value: MSO_AUTO_SIZE | None):
        if value is not None and value not in MSO_AUTO_SIZE:
            raise ValueError(
                f"only None or a member of the MSO_AUTO_SIZE enumeration can be assigned to"
                f" CT_TextBodyProperties.autofit, got {value}"
            )
        self._remove_eg_textAutoFit()
        if value == MSO_AUTO_SIZE.NONE:
            self._add_noAutofit()
        elif value == MSO_AUTO_SIZE.TEXT_TO_FIT_SHAPE:
            self._add_normAutofit()
        elif value == MSO_AUTO_SIZE.SHAPE_TO_FIT_TEXT:
            self._add_spAutoFit()


class CT_TextCharacterProperties(BaseOxmlElement):
    """Custom element class for `a:rPr`, `a:defRPr`, and `a:endParaRPr`.

    'rPr' is short for 'run properties', and it corresponds to the |Font| proxy class.
    """

    get_or_add_hlinkClick: Callable[[], CT_Hyperlink]
    get_or_add_latin: Callable[[], CT_TextFont]
    _remove_latin: Callable[[], None]
    _remove_hlinkClick: Callable[[], None]

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
    latin: CT_TextFont | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
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
    hlinkClick: CT_Hyperlink | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:hlinkClick", successors=("a:hlinkMouseOver", "a:rtl", "a:extLst")
    )

    lang: MSO_LANGUAGE_ID | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "lang", MSO_LANGUAGE_ID
    )
    sz: int | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "sz", ST_TextFontSize
    )
    b: bool | None = OptionalAttribute("b", XsdBoolean)  # pyright: ignore[reportAssignmentType]
    i: bool | None = OptionalAttribute("i", XsdBoolean)  # pyright: ignore[reportAssignmentType]
    u: MSO_TEXT_UNDERLINE_TYPE | None = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "u", MSO_TEXT_UNDERLINE_TYPE
    )

    def _new_gradFill(self):
        return CT_GradientFillProperties.new_gradFill()

    def add_hlinkClick(self, rId: str) -> CT_Hyperlink:
        """Add an `a:hlinkClick` child element with r:id attribute set to `rId`."""
        hlinkClick = self.get_or_add_hlinkClick()
        hlinkClick.rId = rId
        return hlinkClick


class CT_TextField(BaseOxmlElement):
    """`a:fld` field element, for either a slide number or date field."""

    get_or_add_rPr: Callable[[], CT_TextCharacterProperties]

    rPr: CT_TextCharacterProperties | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:rPr", successors=("a:pPr", "a:t")
    )
    t: BaseOxmlElement | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:t", successors=()
    )

    @property
    def text(self) -> str:  # pyright: ignore[reportIncompatibleMethodOverride]
        """The text of the `a:t` child element."""
        t = self.t
        if t is None:
            return ""
        return t.text or ""


class CT_TextFont(BaseOxmlElement):
    """Custom element class for `a:latin`, `a:ea`, `a:cs`, and `a:sym`.

    These occur as child elements of CT_TextCharacterProperties, e.g. `a:rPr`.
    """

    typeface: str = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "typeface", ST_TextTypeface
    )


class CT_TextLineBreak(BaseOxmlElement):
    """`a:br` line break element"""

    get_or_add_rPr: Callable[[], CT_TextCharacterProperties]

    rPr = ZeroOrOne("a:rPr", successors=())

    @property
    def text(self):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Unconditionally a single vertical-tab character.

        A line break element can contain no text other than the implicit line feed it
        represents.
        """
        return "\v"


class CT_TextNormalAutofit(BaseOxmlElement):
    """`a:normAutofit` element specifying fit text to shape font reduction, etc."""

    fontScale = OptionalAttribute(
        "fontScale", ST_TextFontScalePercentOrPercentString, default=100.0
    )


class CT_TextParagraph(BaseOxmlElement):
    """`a:p` custom element class"""

    get_or_add_endParaRPr: Callable[[], CT_TextCharacterProperties]
    get_or_add_pPr: Callable[[], CT_TextParagraphProperties]
    r_lst: list[CT_RegularTextRun]
    _add_br: Callable[[], CT_TextLineBreak]
    _add_r: Callable[[], CT_RegularTextRun]

    pPr: CT_TextParagraphProperties | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:pPr", successors=("a:r", "a:br", "a:fld", "a:endParaRPr")
    )
    r = ZeroOrMore("a:r", successors=("a:endParaRPr",))
    br = ZeroOrMore("a:br", successors=("a:endParaRPr",))
    endParaRPr: CT_TextCharacterProperties | None = ZeroOrOne(
        "a:endParaRPr", successors=()
    )  # pyright: ignore[reportAssignmentType]

    def add_br(self) -> CT_TextLineBreak:
        """Return a newly appended `a:br` element."""
        return self._add_br()

    def add_r(self, text: str | None = None) -> CT_RegularTextRun:
        """Return a newly appended `a:r` element."""
        r = self._add_r()
        if text:
            r.text = text
        return r

    def append_text(self, text: str):
        """Append `a:r` and `a:br` elements to `p` based on `text`.

        Any `\n` or `\v` (vertical-tab) characters in `text` delimit `a:r` (run) elements and
        themselves are translated to `a:br` (line-break) elements. The vertical-tab character
        appears in clipboard text from PowerPoint at "soft" line-breaks (new-line, but not new
        paragraph).
        """
        for idx, r_str in enumerate(re.split("\n|\v", text)):
            # ---breaks are only added _between_ items, not at start---
            if idx > 0:
                self.add_br()
            # ---runs that would be empty are not added---
            if r_str:
                self.add_r(r_str)

    @property
    def content_children(self) -> tuple[CT_RegularTextRun | CT_TextLineBreak | CT_TextField, ...]:
        """Sequence containing text-container child elements of this `a:p` element.

        These include `a:r`, `a:br`, and `a:fld`.
        """
        return tuple(
            e for e in self if isinstance(e, (CT_RegularTextRun, CT_TextLineBreak, CT_TextField))
        )

    @property
    def text(self) -> str:  # pyright: ignore[reportIncompatibleMethodOverride]
        """str text contained in this paragraph."""
        # ---note this shadows the lxml _Element.text---
        return "".join([child.text for child in self.content_children])

    def _new_r(self):
        r_xml = "<a:r %s><a:t/></a:r>" % nsdecls("a")
        return parse_xml(r_xml)


class CT_TextParagraphProperties(BaseOxmlElement):
    """`a:pPr` custom element class."""

    get_or_add_defRPr: Callable[[], CT_TextCharacterProperties]
    _add_lnSpc: Callable[[], CT_TextSpacing]
    _add_spcAft: Callable[[], CT_TextSpacing]
    _add_spcBef: Callable[[], CT_TextSpacing]
    _remove_lnSpc: Callable[[], None]
    _remove_spcAft: Callable[[], None]
    _remove_spcBef: Callable[[], None]

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
    lnSpc: CT_TextSpacing | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:lnSpc", successors=_tag_seq[1:]
    )
    spcBef: CT_TextSpacing | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:spcBef", successors=_tag_seq[2:]
    )
    spcAft: CT_TextSpacing | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:spcAft", successors=_tag_seq[3:]
    )
    defRPr: CT_TextCharacterProperties | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:defRPr", successors=_tag_seq[16:]
    )
    lvl: int = OptionalAttribute(  # pyright: ignore[reportAssignmentType]
        "lvl", ST_TextIndentLevelType, default=0
    )
    algn: PP_PARAGRAPH_ALIGNMENT | None = OptionalAttribute(
        "algn", PP_PARAGRAPH_ALIGNMENT
    )  # pyright: ignore[reportAssignmentType]
    del _tag_seq

    @property
    def line_spacing(self) -> float | Length | None:
        """The spacing between baselines of successive lines in this paragraph.

        A float value indicates a number of lines. A |Length| value indicates a fixed spacing.
        Value is contained in `./a:lnSpc/a:spcPts/@val` or `./a:lnSpc/a:spcPct/@val`. Value is
        |None| if no element is present.
        """
        lnSpc = self.lnSpc
        if lnSpc is None:
            return None
        if lnSpc.spcPts is not None:
            return lnSpc.spcPts.val
        return cast(CT_TextSpacingPercent, lnSpc.spcPct).val

    @line_spacing.setter
    def line_spacing(self, value: float | Length | None):
        self._remove_lnSpc()
        if value is None:
            return
        if isinstance(value, Length):
            self._add_lnSpc().set_spcPts(value)
        else:
            self._add_lnSpc().set_spcPct(value)

    @property
    def space_after(self) -> Length | None:
        """The EMU equivalent of the centipoints value in `./a:spcAft/a:spcPts/@val`."""
        spcAft = self.spcAft
        if spcAft is None:
            return None
        spcPts = spcAft.spcPts
        if spcPts is None:
            return None
        return spcPts.val

    @space_after.setter
    def space_after(self, value: Length | None):
        self._remove_spcAft()
        if value is not None:
            self._add_spcAft().set_spcPts(value)

    @property
    def space_before(self):
        """The EMU equivalent of the centipoints value in `./a:spcBef/a:spcPts/@val`."""
        spcBef = self.spcBef
        if spcBef is None:
            return None
        spcPts = spcBef.spcPts
        if spcPts is None:
            return None
        return spcPts.val

    @space_before.setter
    def space_before(self, value: Length | None):
        self._remove_spcBef()
        if value is not None:
            self._add_spcBef().set_spcPts(value)


class CT_TextSpacing(BaseOxmlElement):
    """Used for `a:lnSpc`, `a:spcBef`, and `a:spcAft` elements."""

    get_or_add_spcPct: Callable[[], CT_TextSpacingPercent]
    get_or_add_spcPts: Callable[[], CT_TextSpacingPoint]
    _remove_spcPct: Callable[[], None]
    _remove_spcPts: Callable[[], None]

    # this should actually be a OneAndOnlyOneChoice, but that's not
    # implemented yet.
    spcPct: CT_TextSpacingPercent | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:spcPct"
    )
    spcPts: CT_TextSpacingPoint | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "a:spcPts"
    )

    def set_spcPct(self, value: float):
        """Set spacing to `value` lines, e.g. 1.75 lines.

        A ./a:spcPts child is removed if present.
        """
        self._remove_spcPts()
        spcPct = self.get_or_add_spcPct()
        spcPct.val = value

    def set_spcPts(self, value: Length):
        """Set spacing to `value` points. A ./a:spcPct child is removed if present."""
        self._remove_spcPct()
        spcPts = self.get_or_add_spcPts()
        spcPts.val = value


class CT_TextSpacingPercent(BaseOxmlElement):
    """`a:spcPct` element, specifying spacing in thousandths of a percent in its `val` attribute."""

    val: float = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "val", ST_TextSpacingPercentOrPercentString
    )


class CT_TextSpacingPoint(BaseOxmlElement):
    """`a:spcPts` element, specifying spacing in centipoints in its `val` attribute."""

    val: Length = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "val", ST_TextSpacingPoint
    )
