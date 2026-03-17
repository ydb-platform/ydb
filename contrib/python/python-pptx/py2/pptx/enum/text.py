# encoding: utf-8

"""
Enumerations used by text and related objects
"""

from __future__ import absolute_import

from .base import (
    alias,
    Enumeration,
    EnumMember,
    ReturnValueOnlyEnumMember,
    XmlEnumeration,
    XmlMappedEnumMember,
)


class MSO_AUTO_SIZE(Enumeration):
    """
    Determines the type of automatic sizing allowed.

    The following names can be used to specify the automatic sizing behavior
    used to fit a shape's text within the shape bounding box, for example::

        from pptx.enum.text import MSO_AUTO_SIZE

        shape.text_frame.auto_size = MSO_AUTO_SIZE.TEXT_TO_FIT_SHAPE

    The word-wrap setting of the text frame interacts with the auto-size
    setting to determine the specific auto-sizing behavior.

    Note that ``TextFrame.auto_size`` can also be set to |None|, which removes
    the auto size setting altogether. This causes the setting to be inherited,
    either from the layout placeholder, in the case of a placeholder shape, or
    from the theme.
    """

    NONE = 0
    SHAPE_TO_FIT_TEXT = 1
    TEXT_TO_FIT_SHAPE = 2

    __ms_name__ = "MsoAutoSize"

    __url__ = (
        "http://msdn.microsoft.com/en-us/library/office/ff865367(v=office.15" ").aspx"
    )

    __members__ = (
        EnumMember(
            "NONE",
            0,
            "No automatic sizing of the shape or text will be don"
            "e. Text can freely extend beyond the horizontal and vertical ed"
            "ges of the shape bounding box.",
        ),
        EnumMember(
            "SHAPE_TO_FIT_TEXT",
            1,
            "The shape height and possibly width are"
            " adjusted to fit the text. Note this setting interacts with the"
            " TextFrame.word_wrap property setting. If word wrap is turned o"
            "n, only the height of the shape will be adjusted; soft line bre"
            "aks will be used to fit the text horizontally.",
        ),
        EnumMember(
            "TEXT_TO_FIT_SHAPE",
            2,
            "The font size is reduced as necessary t"
            "o fit the text within the shape.",
        ),
        ReturnValueOnlyEnumMember(
            "MIXED",
            -2,
            "Return value only; indicates a combination of auto"
            "matic sizing schemes are used.",
        ),
    )


@alias("MSO_UNDERLINE")
class MSO_TEXT_UNDERLINE_TYPE(XmlEnumeration):
    """
    Indicates the type of underline for text. Used with
    :attr:`.Font.underline` to specify the style of text underlining.

    Alias: ``MSO_UNDERLINE``

    Example::

        from pptx.enum.text import MSO_UNDERLINE

        run.font.underline = MSO_UNDERLINE.DOUBLE_LINE
    """

    __ms_name__ = "MsoTextUnderlineType"

    __url__ = "http://msdn.microsoft.com/en-us/library/aa432699.aspx"

    __members__ = (
        XmlMappedEnumMember("NONE", 0, "none", "Specifies no underline."),
        XmlMappedEnumMember(
            "DASH_HEAVY_LINE", 8, "dashHeavy", "Specifies a dash underline."
        ),
        XmlMappedEnumMember("DASH_LINE", 7, "dash", "Specifies a dash line underline."),
        XmlMappedEnumMember(
            "DASH_LONG_HEAVY_LINE",
            10,
            "dashLongHeavy",
            "Specifies a long heavy line underline.",
        ),
        XmlMappedEnumMember(
            "DASH_LONG_LINE", 9, "dashLong", "Specifies a dashed long line underline."
        ),
        XmlMappedEnumMember(
            "DOT_DASH_HEAVY_LINE",
            12,
            "dotDashHeavy",
            "Specifies a dot dash heavy line underline.",
        ),
        XmlMappedEnumMember(
            "DOT_DASH_LINE", 11, "dotDash", "Specifies a dot dash line underline."
        ),
        XmlMappedEnumMember(
            "DOT_DOT_DASH_HEAVY_LINE",
            14,
            "dotDotDashHeavy",
            "Specifies a dot dot dash heavy line underline.",
        ),
        XmlMappedEnumMember(
            "DOT_DOT_DASH_LINE",
            13,
            "dotDotDash",
            "Specifies a dot dot dash line underline.",
        ),
        XmlMappedEnumMember(
            "DOTTED_HEAVY_LINE",
            6,
            "dottedHeavy",
            "Specifies a dotted heavy line underline.",
        ),
        XmlMappedEnumMember(
            "DOTTED_LINE", 5, "dotted", "Specifies a dotted line underline."
        ),
        XmlMappedEnumMember(
            "DOUBLE_LINE", 3, "dbl", "Specifies a double line underline."
        ),
        XmlMappedEnumMember(
            "HEAVY_LINE", 4, "heavy", "Specifies a heavy line underline."
        ),
        XmlMappedEnumMember(
            "SINGLE_LINE", 2, "sng", "Specifies a single line underline."
        ),
        XmlMappedEnumMember(
            "WAVY_DOUBLE_LINE", 17, "wavyDbl", "Specifies a wavy double line underline."
        ),
        XmlMappedEnumMember(
            "WAVY_HEAVY_LINE", 16, "wavyHeavy", "Specifies a wavy heavy line underline."
        ),
        XmlMappedEnumMember(
            "WAVY_LINE", 15, "wavy", "Specifies a wavy line underline."
        ),
        XmlMappedEnumMember("WORDS", 1, "words", "Specifies underlining words."),
        ReturnValueOnlyEnumMember("MIXED", -2, "Specifies a mixed of underline types."),
    )


@alias("MSO_ANCHOR")
class MSO_VERTICAL_ANCHOR(XmlEnumeration):
    """
    Specifies the vertical alignment of text in a text frame. Used with the
    ``.vertical_anchor`` property of the |TextFrame| object. Note that the
    ``vertical_anchor`` property can also have the value None, indicating
    there is no directly specified vertical anchor setting and its effective
    value is inherited from its placeholder if it has one or from the theme.
    |None| may also be assigned to remove an explicitly specified vertical
    anchor setting.
    """

    __ms_name__ = "MsoVerticalAnchor"

    __url__ = "http://msdn.microsoft.com/en-us/library/office/ff865255.aspx"

    __members__ = (
        XmlMappedEnumMember(
            None,
            None,
            None,
            "Text frame has no vertical anchor specified "
            "and inherits its value from its layout placeholder or theme.",
        ),
        XmlMappedEnumMember("TOP", 1, "t", "Aligns text to top of text frame"),
        XmlMappedEnumMember("MIDDLE", 3, "ctr", "Centers text vertically"),
        XmlMappedEnumMember("BOTTOM", 4, "b", "Aligns text to bottom of text frame"),
        ReturnValueOnlyEnumMember(
            "MIXED",
            -2,
            "Return value only; indicates a combination of the " "other states.",
        ),
    )


@alias("PP_ALIGN")
class PP_PARAGRAPH_ALIGNMENT(XmlEnumeration):
    """
    Specifies the horizontal alignment for one or more paragraphs.

    Alias: ``PP_ALIGN``

    Example::

        from pptx.enum.text import PP_ALIGN

        shape.paragraphs[0].alignment = PP_ALIGN.CENTER
    """

    __ms_name__ = "PpParagraphAlignment"

    __url__ = (
        "http://msdn.microsoft.com/en-us/library/office/ff745375(v=office.15" ").aspx"
    )

    __members__ = (
        XmlMappedEnumMember("CENTER", 2, "ctr", "Center align"),
        XmlMappedEnumMember(
            "DISTRIBUTE",
            5,
            "dist",
            "Evenly distributes e.g. Japanese chara"
            "cters from left to right within a line",
        ),
        XmlMappedEnumMember(
            "JUSTIFY",
            4,
            "just",
            "Justified, i.e. each line both begins and"
            " ends at the margin with spacing between words adjusted such th"
            "at the line exactly fills the width of the paragraph.",
        ),
        XmlMappedEnumMember(
            "JUSTIFY_LOW",
            7,
            "justLow",
            "Justify using a small amount of sp" "ace between words.",
        ),
        XmlMappedEnumMember("LEFT", 1, "l", "Left aligned"),
        XmlMappedEnumMember("RIGHT", 3, "r", "Right aligned"),
        XmlMappedEnumMember("THAI_DISTRIBUTE", 6, "thaiDist", "Thai distributed"),
        ReturnValueOnlyEnumMember(
            "MIXED",
            -2,
            "Return value only; indicates multiple paragraph al"
            "ignments are present in a set of paragraphs.",
        ),
    )
