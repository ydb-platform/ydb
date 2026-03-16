"""Enumerations used by DrawingML objects."""

from __future__ import annotations

from pptx.enum.base import BaseEnum, BaseXmlEnum


class MSO_COLOR_TYPE(BaseEnum):
    """
    Specifies the color specification scheme

    Example::

        from pptx.enum.dml import MSO_COLOR_TYPE

        assert shape.fill.fore_color.type == MSO_COLOR_TYPE.SCHEME

    MS API Name: "MsoColorType"

    http://msdn.microsoft.com/en-us/library/office/ff864912(v=office.15).aspx
    """

    RGB = (1, "Color is specified by an |RGBColor| value.")
    """Color is specified by an |RGBColor| value."""

    SCHEME = (2, "Color is one of the preset theme colors")
    """Color is one of the preset theme colors"""

    HSL = (101, "Color is specified using Hue, Saturation, and Luminosity values")
    """Color is specified using Hue, Saturation, and Luminosity values"""

    PRESET = (102, "Color is specified using a named built-in color")
    """Color is specified using a named built-in color"""

    SCRGB = (103, "Color is an scRGB color, a wide color gamut RGB color space")
    """Color is an scRGB color, a wide color gamut RGB color space"""

    SYSTEM = (
        104,
        "Color is one specified by the operating system, such as the window background color.",
    )
    """Color is one specified by the operating system, such as the window background color."""


class MSO_FILL_TYPE(BaseEnum):
    """
    Specifies the type of bitmap used for the fill of a shape.

    Alias: ``MSO_FILL``

    Example::

        from pptx.enum.dml import MSO_FILL

        assert shape.fill.type == MSO_FILL.SOLID

    MS API Name: `MsoFillType`

    http://msdn.microsoft.com/EN-US/library/office/ff861408.aspx
    """

    BACKGROUND = (
        5,
        "The shape is transparent, such that whatever is behind the shape shows through."
        " Often this is the slide background, but if a visible shape is behind, that will"
        " show through.",
    )
    """The shape is transparent, such that whatever is behind the shape shows through.

    Often this is the slide background, but if a visible shape is behind, that will show through.
    """

    GRADIENT = (3, "Shape is filled with a gradient")
    """Shape is filled with a gradient"""

    GROUP = (101, "Shape is part of a group and should inherit the fill properties of the group.")
    """Shape is part of a group and should inherit the fill properties of the group."""

    PATTERNED = (2, "Shape is filled with a pattern")
    """Shape is filled with a pattern"""

    PICTURE = (6, "Shape is filled with a bitmapped image")
    """Shape is filled with a bitmapped image"""

    SOLID = (1, "Shape is filled with a solid color")
    """Shape is filled with a solid color"""

    TEXTURED = (4, "Shape is filled with a texture")
    """Shape is filled with a texture"""


MSO_FILL = MSO_FILL_TYPE


class MSO_LINE_DASH_STYLE(BaseXmlEnum):
    """Specifies the dash style for a line.

    Alias: ``MSO_LINE``

    Example::

        from pptx.enum.dml import MSO_LINE

        shape.line.dash_style = MSO_LINE.DASH_DOT_DOT

    MS API name: `MsoLineDashStyle`

    https://learn.microsoft.com/en-us/office/vba/api/Office.MsoLineDashStyle
    """

    DASH = (4, "dash", "Line consists of dashes only.")
    """Line consists of dashes only."""

    DASH_DOT = (5, "dashDot", "Line is a dash-dot pattern.")
    """Line is a dash-dot pattern."""

    DASH_DOT_DOT = (6, "lgDashDotDot", "Line is a dash-dot-dot pattern.")
    """Line is a dash-dot-dot pattern."""

    LONG_DASH = (7, "lgDash", "Line consists of long dashes.")
    """Line consists of long dashes."""

    LONG_DASH_DOT = (8, "lgDashDot", "Line is a long dash-dot pattern.")
    """Line is a long dash-dot pattern."""

    ROUND_DOT = (3, "sysDot", "Line is made up of round dots.")
    """Line is made up of round dots."""

    SOLID = (1, "solid", "Line is solid.")
    """Line is solid."""

    SQUARE_DOT = (2, "sysDash", "Line is made up of square dots.")
    """Line is made up of square dots."""

    DASH_STYLE_MIXED = (-2, "", "Not supported.")
    """Return value only, indicating more than one dash style applies."""


MSO_LINE = MSO_LINE_DASH_STYLE


class MSO_PATTERN_TYPE(BaseXmlEnum):
    """Specifies the fill pattern used in a shape.

    Alias: ``MSO_PATTERN``

    Example::

        from pptx.enum.dml import MSO_PATTERN

        fill = shape.fill
        fill.patterned()
        fill.pattern = MSO_PATTERN.WAVE

    MS API Name: `MsoPatternType`

    https://learn.microsoft.com/en-us/office/vba/api/Office.MsoPatternType
    """

    CROSS = (51, "cross", "Cross")
    """Cross"""

    DARK_DOWNWARD_DIAGONAL = (15, "dkDnDiag", "Dark Downward Diagonal")
    """Dark Downward Diagonal"""

    DARK_HORIZONTAL = (13, "dkHorz", "Dark Horizontal")
    """Dark Horizontal"""

    DARK_UPWARD_DIAGONAL = (16, "dkUpDiag", "Dark Upward Diagonal")
    """Dark Upward Diagonal"""

    DARK_VERTICAL = (14, "dkVert", "Dark Vertical")
    """Dark Vertical"""

    DASHED_DOWNWARD_DIAGONAL = (28, "dashDnDiag", "Dashed Downward Diagonal")
    """Dashed Downward Diagonal"""

    DASHED_HORIZONTAL = (32, "dashHorz", "Dashed Horizontal")
    """Dashed Horizontal"""

    DASHED_UPWARD_DIAGONAL = (27, "dashUpDiag", "Dashed Upward Diagonal")
    """Dashed Upward Diagonal"""

    DASHED_VERTICAL = (31, "dashVert", "Dashed Vertical")
    """Dashed Vertical"""

    DIAGONAL_BRICK = (40, "diagBrick", "Diagonal Brick")
    """Diagonal Brick"""

    DIAGONAL_CROSS = (54, "diagCross", "Diagonal Cross")
    """Diagonal Cross"""

    DIVOT = (46, "divot", "Pattern Divot")
    """Pattern Divot"""

    DOTTED_DIAMOND = (24, "dotDmnd", "Dotted Diamond")
    """Dotted Diamond"""

    DOTTED_GRID = (45, "dotGrid", "Dotted Grid")
    """Dotted Grid"""

    DOWNWARD_DIAGONAL = (52, "dnDiag", "Downward Diagonal")
    """Downward Diagonal"""

    HORIZONTAL = (49, "horz", "Horizontal")
    """Horizontal"""

    HORIZONTAL_BRICK = (35, "horzBrick", "Horizontal Brick")
    """Horizontal Brick"""

    LARGE_CHECKER_BOARD = (36, "lgCheck", "Large Checker Board")
    """Large Checker Board"""

    LARGE_CONFETTI = (33, "lgConfetti", "Large Confetti")
    """Large Confetti"""

    LARGE_GRID = (34, "lgGrid", "Large Grid")
    """Large Grid"""

    LIGHT_DOWNWARD_DIAGONAL = (21, "ltDnDiag", "Light Downward Diagonal")
    """Light Downward Diagonal"""

    LIGHT_HORIZONTAL = (19, "ltHorz", "Light Horizontal")
    """Light Horizontal"""

    LIGHT_UPWARD_DIAGONAL = (22, "ltUpDiag", "Light Upward Diagonal")
    """Light Upward Diagonal"""

    LIGHT_VERTICAL = (20, "ltVert", "Light Vertical")
    """Light Vertical"""

    NARROW_HORIZONTAL = (30, "narHorz", "Narrow Horizontal")
    """Narrow Horizontal"""

    NARROW_VERTICAL = (29, "narVert", "Narrow Vertical")
    """Narrow Vertical"""

    OUTLINED_DIAMOND = (41, "openDmnd", "Outlined Diamond")
    """Outlined Diamond"""

    PERCENT_10 = (2, "pct10", "10% of the foreground color.")
    """10% of the foreground color."""

    PERCENT_20 = (3, "pct20", "20% of the foreground color.")
    """20% of the foreground color."""

    PERCENT_25 = (4, "pct25", "25% of the foreground color.")
    """25% of the foreground color."""

    PERCENT_30 = (5, "pct30", "30% of the foreground color.")
    """30% of the foreground color."""

    ERCENT_40 = (6, "pct40", "40% of the foreground color.")
    """40% of the foreground color."""

    PERCENT_5 = (1, "pct5", "5% of the foreground color.")
    """5% of the foreground color."""

    PERCENT_50 = (7, "pct50", "50% of the foreground color.")
    """50% of the foreground color."""

    PERCENT_60 = (8, "pct60", "60% of the foreground color.")
    """60% of the foreground color."""

    PERCENT_70 = (9, "pct70", "70% of the foreground color.")
    """70% of the foreground color."""

    PERCENT_75 = (10, "pct75", "75% of the foreground color.")
    """75% of the foreground color."""

    PERCENT_80 = (11, "pct80", "80% of the foreground color.")
    """80% of the foreground color."""

    PERCENT_90 = (12, "pct90", "90% of the foreground color.")
    """90% of the foreground color."""

    PLAID = (42, "plaid", "Plaid")
    """Plaid"""

    SHINGLE = (47, "shingle", "Shingle")
    """Shingle"""

    SMALL_CHECKER_BOARD = (17, "smCheck", "Small Checker Board")
    """Small Checker Board"""

    SMALL_CONFETTI = (37, "smConfetti", "Small Confetti")
    """Small Confetti"""

    SMALL_GRID = (23, "smGrid", "Small Grid")
    """Small Grid"""

    SOLID_DIAMOND = (39, "solidDmnd", "Solid Diamond")
    """Solid Diamond"""

    SPHERE = (43, "sphere", "Sphere")
    """Sphere"""

    TRELLIS = (18, "trellis", "Trellis")
    """Trellis"""

    UPWARD_DIAGONAL = (53, "upDiag", "Upward Diagonal")
    """Upward Diagonal"""

    VERTICAL = (50, "vert", "Vertical")
    """Vertical"""

    WAVE = (48, "wave", "Wave")
    """Wave"""

    WEAVE = (44, "weave", "Weave")
    """Weave"""

    WIDE_DOWNWARD_DIAGONAL = (25, "wdDnDiag", "Wide Downward Diagonal")
    """Wide Downward Diagonal"""

    WIDE_UPWARD_DIAGONAL = (26, "wdUpDiag", "Wide Upward Diagonal")
    """Wide Upward Diagonal"""

    ZIG_ZAG = (38, "zigZag", "Zig Zag")
    """Zig Zag"""

    MIXED = (-2, "", "Mixed pattern (read-only).")
    """Mixed pattern (read-only)."""


MSO_PATTERN = MSO_PATTERN_TYPE


class MSO_THEME_COLOR_INDEX(BaseXmlEnum):
    """An Office theme color, one of those shown in the color gallery on the formatting ribbon.

    Alias: ``MSO_THEME_COLOR``

    Example::

        from pptx.enum.dml import MSO_THEME_COLOR

        shape.fill.solid()
        shape.fill.fore_color.theme_color = MSO_THEME_COLOR.ACCENT_1

    MS API Name: `MsoThemeColorIndex`

    http://msdn.microsoft.com/en-us/library/office/ff860782(v=office.15).aspx
    """

    NOT_THEME_COLOR = (0, "", "Indicates the color is not a theme color.")
    """Indicates the color is not a theme color."""

    ACCENT_1 = (5, "accent1", "Specifies the Accent 1 theme color.")
    """Specifies the Accent 1 theme color."""

    ACCENT_2 = (6, "accent2", "Specifies the Accent 2 theme color.")
    """Specifies the Accent 2 theme color."""

    ACCENT_3 = (7, "accent3", "Specifies the Accent 3 theme color.")
    """Specifies the Accent 3 theme color."""

    ACCENT_4 = (8, "accent4", "Specifies the Accent 4 theme color.")
    """Specifies the Accent 4 theme color."""

    ACCENT_5 = (9, "accent5", "Specifies the Accent 5 theme color.")
    """Specifies the Accent 5 theme color."""

    ACCENT_6 = (10, "accent6", "Specifies the Accent 6 theme color.")
    """Specifies the Accent 6 theme color."""

    BACKGROUND_1 = (14, "bg1", "Specifies the Background 1 theme color.")
    """Specifies the Background 1 theme color."""

    BACKGROUND_2 = (16, "bg2", "Specifies the Background 2 theme color.")
    """Specifies the Background 2 theme color."""

    DARK_1 = (1, "dk1", "Specifies the Dark 1 theme color.")
    """Specifies the Dark 1 theme color."""

    DARK_2 = (3, "dk2", "Specifies the Dark 2 theme color.")
    """Specifies the Dark 2 theme color."""

    FOLLOWED_HYPERLINK = (12, "folHlink", "Specifies the theme color for a clicked hyperlink.")
    """Specifies the theme color for a clicked hyperlink."""

    HYPERLINK = (11, "hlink", "Specifies the theme color for a hyperlink.")
    """Specifies the theme color for a hyperlink."""

    LIGHT_1 = (2, "lt1", "Specifies the Light 1 theme color.")
    """Specifies the Light 1 theme color."""

    LIGHT_2 = (4, "lt2", "Specifies the Light 2 theme color.")
    """Specifies the Light 2 theme color."""

    TEXT_1 = (13, "tx1", "Specifies the Text 1 theme color.")
    """Specifies the Text 1 theme color."""

    TEXT_2 = (15, "tx2", "Specifies the Text 2 theme color.")
    """Specifies the Text 2 theme color."""

    MIXED = (
        -2,
        "",
        "Indicates multiple theme colors are used, such as in a group shape (read-only).",
    )
    """Indicates multiple theme colors are used, such as in a group shape (read-only)."""


MSO_THEME_COLOR = MSO_THEME_COLOR_INDEX
