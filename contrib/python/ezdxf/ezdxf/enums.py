#  Copyright (c) 2021, Manfred Moitzi
#  License: MIT License
from enum import IntEnum, IntFlag, Enum, auto
from ezdxf.lldxf import const


class TextHAlign(IntEnum):
    """Enumeration for DXF attribute: :attr:`ezdxf.entities.Text.dxf.halign`"""

    LEFT = const.LEFT
    CENTER = const.CENTER
    RIGHT = const.RIGHT
    ALIGNED = const.ALIGNED
    MIDDLE = 4
    FIT = const.FIT


class TextVAlign(IntEnum):
    """Enumeration for DXF attribute: :attr:`ezdxf.entities.Text.dxf.valign`"""

    BASELINE = const.BASELINE
    BOTTOM = const.BOTTOM
    MIDDLE = const.MIDDLE
    TOP = const.TOP


# noinspection PyArgumentList
class TextEntityAlignment(Enum):
    """Text alignment enum for the :class:`~ezdxf.entities.Text`,
    :class:`~ezdxf.entities.Attrib` and :class:`~ezdxf.entities.AttDef`
    entities.
    """

    LEFT = auto()
    CENTER = auto()
    RIGHT = auto()
    ALIGNED = auto()
    MIDDLE = auto()
    FIT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()
    MIDDLE_LEFT = auto()
    MIDDLE_CENTER = auto()
    MIDDLE_RIGHT = auto()
    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()


MAP_TEXT_ENUM_TO_ALIGN_FLAGS = {
    TextEntityAlignment.LEFT: (TextHAlign.LEFT, TextVAlign.BASELINE),
    TextEntityAlignment.CENTER: (TextHAlign.CENTER, TextVAlign.BASELINE),
    TextEntityAlignment.RIGHT: (TextHAlign.RIGHT, TextVAlign.BASELINE),
    TextEntityAlignment.ALIGNED: (TextHAlign.ALIGNED, TextVAlign.BASELINE),
    TextEntityAlignment.MIDDLE: (TextHAlign.MIDDLE, TextVAlign.BASELINE),
    TextEntityAlignment.FIT: (TextHAlign.FIT, TextVAlign.BASELINE),
    TextEntityAlignment.BOTTOM_LEFT: (TextHAlign.LEFT, TextVAlign.BOTTOM),
    TextEntityAlignment.BOTTOM_CENTER: (TextHAlign.CENTER, TextVAlign.BOTTOM),
    TextEntityAlignment.BOTTOM_RIGHT: (TextHAlign.RIGHT, TextVAlign.BOTTOM),
    TextEntityAlignment.MIDDLE_LEFT: (TextHAlign.LEFT, TextVAlign.MIDDLE),
    TextEntityAlignment.MIDDLE_CENTER: (TextHAlign.CENTER, TextVAlign.MIDDLE),
    TextEntityAlignment.MIDDLE_RIGHT: (TextHAlign.RIGHT, TextVAlign.MIDDLE),
    TextEntityAlignment.TOP_LEFT: (TextHAlign.LEFT, TextVAlign.TOP),
    TextEntityAlignment.TOP_CENTER: (TextHAlign.CENTER, TextVAlign.TOP),
    TextEntityAlignment.TOP_RIGHT: (TextHAlign.RIGHT, TextVAlign.TOP),
}
MAP_TEXT_ALIGN_FLAGS_TO_ENUM = dict(
    (flags, enum) for enum, flags in MAP_TEXT_ENUM_TO_ALIGN_FLAGS.items()
)

# Used by legacy add-ons MText and Table!
MAP_STRING_ALIGN_TO_FLAGS = {
    "LEFT": (TextHAlign.LEFT, TextVAlign.BASELINE),
    "CENTER": (TextHAlign.CENTER, TextVAlign.BASELINE),
    "RIGHT": (TextHAlign.RIGHT, TextVAlign.BASELINE),
    "ALIGNED": (TextHAlign.ALIGNED, TextVAlign.BASELINE),
    "MIDDLE": (TextHAlign.MIDDLE, TextVAlign.BASELINE),
    "FIT": (TextHAlign.FIT, TextVAlign.BASELINE),
    "BOTTOM_LEFT": (TextHAlign.LEFT, TextVAlign.BOTTOM),
    "BOTTOM_CENTER": (TextHAlign.CENTER, TextVAlign.BOTTOM),
    "BOTTOM_RIGHT": (TextHAlign.RIGHT, TextVAlign.BOTTOM),
    "MIDDLE_LEFT": (TextHAlign.LEFT, TextVAlign.MIDDLE),
    "MIDDLE_CENTER": (TextHAlign.CENTER, TextVAlign.MIDDLE),
    "MIDDLE_RIGHT": (TextHAlign.RIGHT, TextVAlign.MIDDLE),
    "TOP_LEFT": (TextHAlign.LEFT, TextVAlign.TOP),
    "TOP_CENTER": (TextHAlign.CENTER, TextVAlign.TOP),
    "TOP_RIGHT": (TextHAlign.RIGHT, TextVAlign.TOP),
}
MAP_FLAGS_TO_STRING_ALIGN = dict(
    (flags, name) for name, flags in MAP_STRING_ALIGN_TO_FLAGS.items()
)


class MTextEntityAlignment(IntEnum):
    """Text alignment enum for the :class:`~ezdxf.entities.MText` entity."""

    TOP_LEFT = const.MTEXT_TOP_LEFT
    TOP_CENTER = const.MTEXT_TOP_CENTER
    TOP_RIGHT = const.MTEXT_TOP_RIGHT
    MIDDLE_LEFT = const.MTEXT_MIDDLE_LEFT
    MIDDLE_CENTER = const.MTEXT_MIDDLE_CENTER
    MIDDLE_RIGHT = const.MTEXT_MIDDLE_RIGHT
    BOTTOM_LEFT = const.MTEXT_BOTTOM_LEFT
    BOTTOM_CENTER = const.MTEXT_BOTTOM_CENTER
    BOTTOM_RIGHT = const.MTEXT_BOTTOM_RIGHT


MAP_MTEXT_ALIGN_TO_FLAGS = {
    MTextEntityAlignment.TOP_LEFT: (TextHAlign.LEFT, TextVAlign.TOP),
    MTextEntityAlignment.TOP_CENTER: (TextHAlign.CENTER, TextVAlign.TOP),
    MTextEntityAlignment.TOP_RIGHT: (TextHAlign.RIGHT, TextVAlign.TOP),
    MTextEntityAlignment.MIDDLE_LEFT: (TextHAlign.LEFT, TextVAlign.MIDDLE),
    MTextEntityAlignment.MIDDLE_CENTER: (TextHAlign.CENTER, TextVAlign.MIDDLE),
    MTextEntityAlignment.MIDDLE_RIGHT: (TextHAlign.RIGHT, TextVAlign.MIDDLE),
    MTextEntityAlignment.BOTTOM_LEFT: (TextHAlign.LEFT, TextVAlign.BOTTOM),
    MTextEntityAlignment.BOTTOM_CENTER: (TextHAlign.CENTER, TextVAlign.BOTTOM),
    MTextEntityAlignment.BOTTOM_RIGHT: (TextHAlign.RIGHT, TextVAlign.BOTTOM),
}


class MTextParagraphAlignment(IntEnum):
    DEFAULT = 0
    LEFT = 1
    RIGHT = 2
    CENTER = 3
    JUSTIFIED = 4
    DISTRIBUTED = 5


class MTextFlowDirection(IntEnum):
    LEFT_TO_RIGHT = const.MTEXT_LEFT_TO_RIGHT
    TOP_TO_BOTTOM = const.MTEXT_TOP_TO_BOTTOM
    BY_STYLE = const.MTEXT_BY_STYLE


class MTextLineAlignment(IntEnum):  # exclusive state
    BOTTOM = 0
    MIDDLE = 1
    TOP = 2


class MTextStroke(IntFlag):
    """Combination of flags is supported: UNDERLINE + STRIKE_THROUGH"""

    UNDERLINE = 1
    STRIKE_THROUGH = 2
    OVERLINE = 4


class MTextLineSpacing(IntEnum):
    AT_LEAST = const.MTEXT_AT_LEAST
    EXACT = const.MTEXT_EXACT


class MTextBackgroundColor(IntEnum):
    OFF = const.MTEXT_BG_OFF
    COLOR = const.MTEXT_BG_COLOR
    WINDOW = const.MTEXT_BG_WINDOW_COLOR
    CANVAS = const.MTEXT_BG_CANVAS_COLOR


class InsertUnits(IntEnum):
    Unitless = 0
    Inches = 1
    Feet = 2
    Miles = 3
    Millimeters = 4
    Centimeters = 5
    Meters = 6
    Kilometers = 7
    Microinches = 8
    Mils = 9
    Yards = 10
    Angstroms = 11
    Nanometers = 12
    Microns = 13
    Decimeters = 14
    Decameters = 15
    Hectometers = 16
    Gigameters = 17
    AstronomicalUnits = 18
    Lightyears = 19
    Parsecs = 20
    USSurveyFeet = 21
    USSurveyInch = 22
    USSurveyYard = 23
    USSurveyMile = 24


class Measurement(IntEnum):
    Imperial = 0
    Metric = 1


class LengthUnits(IntEnum):
    Scientific = 1
    Decimal = 2
    Engineering = 3
    Architectural = 4
    Fractional = 5


class AngularUnits(IntEnum):
    DecimalDegrees = 0
    DegreesMinutesSeconds = 1
    Grad = 2
    Radians = 3


class SortEntities(IntFlag):
    DISABLE = 0
    SELECTION = 1  # 1 = Sorts for object selection
    SNAP = 2  # 2 = Sorts for object snap
    REDRAW = 4  # 4 = Sorts for redraws; obsolete
    MSLIDE = 8  # 8 = Sorts for MSLIDE command slide creation; obsolete
    REGEN = 16  # 16 = Sorts for REGEN commands
    PLOT = 32  # 32 = Sorts for plotting
    POSTSCRIPT = 64  # 64 = Sorts for PostScript output; obsolete


class ACI(IntEnum):
    """AutoCAD Color Index"""

    BYBLOCK = 0
    BYLAYER = 256
    BYOBJECT = 257
    RED = 1
    YELLOW = 2
    GREEN = 3
    CYAN = 4
    BLUE = 5
    MAGENTA = 6
    BLACK = 7
    WHITE = 7
    GRAY = 8
    LIGHT_GRAY = 9


class EndCaps(IntEnum):
    """ Lineweight end caps setting for new objects. """
    NONE = 0
    ROUND = 1
    ANGLE = 2
    SQUARE = 3


class JoinStyle(IntEnum):
    """ Lineweight joint setting for new objects. """
    NONE = 0
    ROUND = 1
    ANGLE = 2
    FLAT = 3
