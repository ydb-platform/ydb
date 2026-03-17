# Copyright (c) 2021-2024, Manfred Moitzi
# License: MIT License
# pylint: disable=consider-using-in
"""
Tools in this module should be as independent of DXF entities as possible!
"""
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    TYPE_CHECKING,
    Union,
    Optional,
    Callable,
    NamedTuple,
    Any,
)
import enum
import re
import math

from ezdxf.lldxf import validator, const
from ezdxf.enums import (
    TextEntityAlignment,
    TextHAlign,
    MTextParagraphAlignment,
    MTextLineAlignment,
    MTextStroke,
    MAP_MTEXT_ALIGN_TO_FLAGS,
)
from ezdxf.lldxf.const import (
    LEFT,
    CENTER,
    RIGHT,
    BASELINE,
    MIDDLE,
    TOP,
    MAX_STR_LEN,
)
from ezdxf.math import Vec3, Vec2, UVec
from ezdxf.colors import rgb2int, RGB, int2rgb


if TYPE_CHECKING:
    from ezdxf.entities import Text, MText, DXFEntity
    from ezdxf.lldxf.tags import Tags
    from ezdxf.fonts import fonts

X_MIDDLE = 4  # special case for overall alignment "MIDDLE"


class TextLine:
    """Helper class which represents a single line text entity
    (e.g. :class:`~ezdxf.entities.Text`).

    Args:
        text: content string
        font: ezdxf font definition like :class:`~ezdxf.fonts.fonts.MonospaceFont`
            or :class:`~ezdxf.fonts.fonts.TrueTypeFont`

    """

    def __init__(self, text: str, font: fonts.AbstractFont):
        self._font = font
        self._text_width: float = font.text_width(text)
        self._stretch_x: float = 1.0
        self._stretch_y: float = 1.0

    def stretch(self, alignment: TextEntityAlignment, p1: Vec3, p2: Vec3) -> None:
        """Set stretch factors for FIT and ALIGNED alignments to fit the
        text between `p1` and `p2`, only the distance between these points is
        important. Other given `alignment` values are ignore.

        """
        assert isinstance(alignment, TextEntityAlignment)
        sx: float = 1.0
        sy: float = 1.0
        if alignment in (TextEntityAlignment.FIT, TextEntityAlignment.ALIGNED):
            defined_length: float = (p2 - p1).magnitude
            if self._text_width > 1e-9:
                sx = defined_length / self._text_width
                if alignment == TextEntityAlignment.ALIGNED:
                    sy = sx
        self._stretch_x = sx
        self._stretch_y = sy

    @property
    def width(self) -> float:
        """Returns the final (stretched) text width."""
        return self._text_width * self._stretch_x

    @property
    def height(self) -> float:
        """Returns the final (stretched) text height."""
        return self._font.measurements.total_height * self._stretch_y

    def font_measurements(self) -> fonts.FontMeasurements:
        """Returns the scaled font measurements."""
        return self._font.measurements.scale(self._stretch_y)

    def baseline_vertices(
        self,
        insert: UVec,
        halign: int = 0,
        valign: int = 0,
        angle: float = 0,
        scale: tuple[float, float] = (1, 1),
    ) -> list[Vec3]:
        """Returns the left and the right baseline vertex of the text line.

        Args:
            insert: insertion location
            halign: horizontal alignment left=0, center=1, right=2
            valign: vertical alignment baseline=0, bottom=1, middle=2, top=3
            angle: text rotation in radians
            scale: scale in x- and y-axis as 2-tuple of float

        """
        fm = self.font_measurements()
        vertices = [
            Vec2(0, fm.baseline),
            Vec2(self.width, fm.baseline),
        ]
        shift = self._shift_vector(halign, valign, fm)
        # Oblique angle is deliberately not supported, the baseline should be
        # (near) the y-coordinate=0.
        return TextLine.transform_2d(vertices, insert, shift, angle, scale)

    def corner_vertices(
        self,
        insert: UVec,
        halign: int = 0,
        valign: int = 0,
        angle: float = 0,
        scale: tuple[float, float] = (1, 1),
        oblique: float = 0,
    ) -> list[Vec3]:
        """Returns the corner vertices of the text line in the order
        bottom left, bottom right, top right, top left.

        Args:
            insert: insertion location
            halign: horizontal alignment left=0, center=1, right=2
            valign: vertical alignment baseline=0, bottom=1, middle=2, top=3
            angle: text rotation in radians
            scale: scale in x- and y-axis as 2-tuple of float
            oblique: shear angle (slanting) in x-direction in radians

        """
        fm = self.font_measurements()
        vertices = [
            Vec2(0, fm.bottom),
            Vec2(self.width, fm.bottom),
            Vec2(self.width, fm.cap_top),
            Vec2(0, fm.cap_top),
        ]
        shift = self._shift_vector(halign, valign, fm)
        return TextLine.transform_2d(vertices, insert, shift, angle, scale, oblique)

    def _shift_vector(
        self, halign: int, valign: int, fm: fonts.FontMeasurements
    ) -> tuple[float, float]:
        return _shift_x(self.width, halign), _shift_y(fm, valign)

    @staticmethod
    def transform_2d(
        vertices: Iterable[UVec],
        insert: UVec = Vec3(0, 0, 0),
        shift: tuple[float, float] = (0, 0),
        rotation: float = 0,
        scale: tuple[float, float] = (1, 1),
        oblique: float = 0,
    ) -> list[Vec3]:
        """Transform any vertices from the text line located at the base
        location at (0, 0) and alignment LEFT.

        Args:
            vertices: iterable of vertices
            insert: insertion location
            shift: (shift-x, shift-y) as 2-tuple of float
            rotation: text rotation in radians
            scale: (scale-x, scale-y)  as 2-tuple of float
            oblique: shear angle (slanting) in x-direction in radians

        """
        # Building a transformation matrix vs. applying transformations in
        # individual steps:
        # Most text is horizontal, because people like to read horizontal text!
        # Operating in 2D is faster than building a full 3D transformation
        # matrix and a pure 2D transformation matrix is not implemented!
        # This function doesn't transform many vertices at the same time,
        # mostly only 4 vertices, therefore the matrix multiplication overhead
        # does not pay off.
        # The most expensive rotation transformation is the least frequently
        # used transformation.
        # IMPORTANT: these assumptions are not verified by profiling!

        # Use 2D vectors:
        vertices_: Iterable[Vec2] = Vec2.generate(vertices)

        # 1. slanting at the original location (very rare):
        if oblique:
            slant_x = math.tan(oblique)
            vertices_ = (Vec2(v.x + v.y * slant_x, v.y) for v in vertices_)

        # 2. apply alignment shifting (frequently):
        shift_vector = Vec2(shift)
        if shift_vector:
            vertices_ = (v + shift_vector for v in vertices_)

        # 3. scale (and mirror) at the aligned location (more often):
        scale_x, scale_y = scale
        if scale_x != 1 or scale_y != 1:
            vertices_ = (Vec2(v.x * scale_x, v.y * scale_y) for v in vertices_)

        # 4. apply rotation (rare):
        if rotation:
            vertices_ = (v.rotate(rotation) for v in vertices_)

        # 5. move to insert location in OCS/3D! (every time)
        insert = Vec3(insert)
        return [insert + v for v in vertices_]


def _shift_x(total_width: float, halign: int) -> float:
    if halign == CENTER:
        return -total_width / 2.0
    if halign == RIGHT:
        return -total_width
    return 0.0  # LEFT


def _shift_y(fm: fonts.FontMeasurements, valign: int) -> float:
    if valign == BASELINE:
        return fm.baseline
    if valign == MIDDLE:
        return -fm.cap_top + fm.cap_height / 2
    if valign == X_MIDDLE:
        return -fm.cap_top + fm.total_height / 2
    if valign == TOP:
        return -fm.cap_top
    return -fm.bottom


def unified_alignment(entity: Union[Text, MText]) -> tuple[int, int]:
    """Return unified horizontal and vertical alignment.

    horizontal alignment: left=0, center=1, right=2

    vertical alignment: baseline=0, bottom=1, middle=2, top=3

    Returns:
        tuple(halign, valign)

    """
    dxftype = entity.dxftype()
    if dxftype in ("TEXT", "ATTRIB", "ATTDEF"):
        halign = entity.dxf.halign
        valign = entity.dxf.valign
        if halign in (TextHAlign.ALIGNED, TextHAlign.FIT):
            # For the alignments ALIGNED and FIT the text stretching has to be
            # handles separately.
            halign = CENTER
            valign = BASELINE
        elif halign == TextHAlign.MIDDLE:  # MIDDLE is different to MIDDLE/CENTER
            halign = CENTER
            valign = X_MIDDLE
        return halign, valign
    if dxftype == "MTEXT":
        return MAP_MTEXT_ALIGN_TO_FLAGS.get(entity.dxf.attachment_point, (LEFT, TOP))
    raise TypeError(f"invalid DXF {dxftype}")


def plain_text(text: str) -> str:
    """Returns the plain text for :class:`~ezdxf.entities.Text`,
    :class:`~ezdxf.entities.Attrib` and :class:`~ezdxf.entities.Attdef` content.
    """
    # TEXT, ATTRIB and ATTDEF are short strings <= 255 in R12.
    # R2000 allows 2049 chars, but this limit is not often used in real world
    # applications.
    result = ""
    scanner = TextScanner(validator.fix_one_line_text(caret_decode(text)))
    while scanner.has_data:
        char = scanner.peek()
        if char == "%":  # special characters
            if scanner.peek(1) == "%":
                code = scanner.peek(2).lower()
                letter = const.SPECIAL_CHAR_ENCODING.get(code)
                if letter:
                    scanner.consume(3)  # %%?
                    result += letter
                    continue
                if code in "kou":
                    # formatting codes (%%k, %%o, %%u) will be ignored in
                    # TEXT, ATTRIB and ATTDEF:
                    scanner.consume(3)
                    continue
        scanner.consume(1)
        # slightly faster then "".join(chars)
        result += char
    return result


ONE_CHAR_COMMANDS = "PNLlOoKkX"


##################################################
# MTEXT inline codes
# \L	Start underline
# \l	Stop underline
# \O	Start overline
# \o	Stop overline
# \K	Start strike-through
# \k	Stop strike-through
# \P	New paragraph (new line)
# \N	New column
# \~    None breaking space
# ^I    Tabulator
# \	Escape character - e.g. \\ = "\", \{ = "{"
#
# \p    start paragraph properties until next ";"
# \pi#,l#,r#; paragraph indent
#   i#  indent first line left, relative to (l)!
#   l#  indent paragraph left
#   r#  indent paragraph right
#   q?  alignments:
#   ql  align text in paragraph: left
#   qr  align text in paragraph: right
#   qc  align text in paragraph: center
#   qj  align text in paragraph: justified
#   qd  align text in paragraph: distributed
#   x   unknown meaning
#   t#[,c#,r#...] define absolute tabulator stops 1,c2,r3...
#       without prefix is a left adjusted tab stop
#       prefix 'c' for center adjusted tab stop
#       prefix 'r' for right adjusted tab stop
#   ?*  reset command to default value
#
# Examples:
# \pi1,t[5,20,...]; define tab stops as comma separated list
# \pxt4,c8,r12,16,c20,r24; left, centered and right adjusted tab stops
# \pi*,l*,r*,q*,t; reset to default values
# \pi2,l0;  = first line  2 & paragraph left 0
# \pi-2,l2; = first line -2 & paragraph left 2
# \pi0,l2;  = first line  0 & paragraph left 2
#
# \X	Paragraph wrap on the dimension line (only in dimensions)
# \Q	Slanting (oblique) text by angle - e.g. \Q30;
# \H	Text height relative - e.g. \H3x;
# \H	Text height absolute - e.g. \H3;
# \W	Text width factor relative e.g. \W0.8x;
# \W	Text width factor absolute e.g. \W0.8;
# \F	Font selection
# \f	Font selection
#
#     e.g. \Fgdt;o - GDT-tolerance
#     e.g. \fArial|b0|i0|c238|p10; - font Arial, non-bold, non-italic,
#     codepage 238, pitch 10
#     codepage 0 = no change
#     pitch 0 = no change
#
# \S	Stacking, fractions
#
#     e.g. \SA^ B;
#     A
#     B
#     e.g. \SX/ Y;
#     X
#     -
#     Y
#     e.g. \S1# 4;
#     1/4
#
# \A	Alignment relative to current line
#
#     \A0; = bottom
#     \A1; = center
#     \A2; = top
#
# \C	Color change
#
#     \C1; = red
#     \C2; = yellow
#     \C3; = green
#     \C4; = cyan
#     \C5; = blue
#     \C6; = magenta
#     \C7; = white

#     \c255; = RED (255, 0, 0)
#     \c65280; = GREEN (0, 255, 0)
#     \c16711680; = BLUE (0, 0, 255)

#     RGB color = \c7528479;  = 31,224,114
#     ezdxf.rgb2int((31,224,114)) = 2089074 (r,g,b) wrong!
#     ezdxf.rgb2int((114,224,31)) = 7528479 (b,g,r) reversed order is correct!
#
# \T	Tracking, char spacing factor as absolute value e.g. \T2;
# \T	Tracking, char spacing factor as relative value e.g. \T2x;
# {}	Braces - define the text area influenced by the code
#       Multiple codes after the opening brace are valid until the closing
#       brace.  e.g. {\H0.4x;\A1;small centered text}
#
# Codes and braces can be nested up to 8 levels deep
#
# Column types in BricsCAD:
#   - dynamic auto height: all columns have the same height
#   - dynamic manual height: each column has an individual height
#   - no columns
#   - static: all columns have the same height, like dynamic auto height,
#     difference is only important for user interaction in CAD applications
#
# - All columns have the same width and gutter.
# - Paragraphs do overflow into the next column if required.


# pylint: disable-next=too-many-branches
def fast_plain_mtext(text: str, split=False) -> Union[list[str], str]:
    """Returns the plain MTEXT content as a single string or  a list of
    strings if `split` is ``True``. Replaces ``\\P`` by ``\\n`` and removes
    other controls chars and inline codes.

    This function is more than 4x faster than :func:`plain_mtext`, but does not
    remove single letter inline commands with arguments without a terminating
    semicolon like this ``"\\C1red text"``.

    .. note::

        Well behaved CAD applications and libraries always create inline codes
        for commands with arguments with a terminating semicolon like this
        ``"\\C1;red text"``!

    Args:
        text: MTEXT content string
        split: split content at line endings ``\\P``

    """
    chars = []
    # split text into chars, in reversed order for efficient pop()
    raw_chars = list(reversed(caret_decode(text)))
    # pylint: disable=too-many-nested-blocks
    while raw_chars:
        char = raw_chars.pop()
        if char == "\\":  # is a formatting command
            try:
                char = raw_chars.pop()
            except IndexError:
                break  # premature end of text - just ignore

            if char in "\\{}":
                chars.append(char)
            elif char in ONE_CHAR_COMMANDS:
                if char == "P":  # new line
                    chars.append("\n")
                elif char == "N":  # new column
                    # until columns are supported, better to at least remove the
                    # escape character
                    chars.append(" ")
                # else: discard other commands
            else:  # multiple character commands are terminated by ';'
                stacking = char == "S"  # stacking command surrounds user data
                first_char = char
                search_chars = raw_chars.copy()
                try:
                    while char != ";":  # end of format marker
                        char = search_chars.pop()
                        if stacking and char != ";":
                            # append user data of stacking command
                            chars.append(char)
                    raw_chars = search_chars
                except IndexError:
                    # premature end of text - just ignore
                    chars.append("\\")
                    chars.append(first_char)
        elif char in "{}":  # grouping
            pass  # discard group markers
        elif char == "%":  # special characters
            if raw_chars and raw_chars[-1] == "%":
                raw_chars.pop()  # discard next '%'
                if raw_chars:
                    code = raw_chars.pop()
                    letter = const.SPECIAL_CHAR_ENCODING.get(code.lower())
                    if letter:
                        chars.append(letter)
                    else:
                        chars.extend(("%", "%", code))
            else:  # char is just a single '%'
                chars.append(char)
        else:  # char is what it is, a character
            chars.append(char)

    result = "".join(chars)
    return result.split("\n") if split else result


def caret_decode(text: str) -> str:
    """DXF stores some special characters using caret notation. This function
    decodes this notation to normalize the representation of special characters
    in the string.

    see: https://en.wikipedia.org/wiki/Caret_notation

    """

    def replace_match(match: re.Match) -> str:
        c = ord(match.group(1))
        return chr((c - 64) % 126)

    return re.sub(r"\^(.)", replace_match, text)


def split_mtext_string(s: str, size: int = 250) -> list[str]:
    """Split the MTEXT content string into chunks of max `size`."""
    chunks = []
    pos = 0
    while True:
        chunk = s[pos : pos + size]
        if len(chunk):
            if len(chunk) < size:
                chunks.append(chunk)
                return chunks
            pos += size
            # do not split chunks at '^'
            if chunk[-1] == "^":
                chunk = chunk[:-1]
                pos -= 1
            chunks.append(chunk)
        else:
            return chunks


def plain_mtext(
    text: str,
    split=False,
    tabsize: int = 4,
) -> Union[list[str], str]:
    """Returns the plain MTEXT content as a single string or a list of
    strings if `split` is ``True``. Replaces ``\\P`` by ``\\n`` and removes
    other controls chars and inline codes.

    This function is much slower than :func:`fast_plain_mtext`, but removes all
    inline codes.

    Args:
        text: MTEXT content string
        split: split content at line endings ``\\P``
        tabsize: count of replacement spaces for tabulators ``^I``

    """
    content: list[str] = []
    paragraph: list[str] = []

    # localize enum to speed up inner loop
    (
        _,
        word,
        stack,
        space,
        nbsp,
        tabulator,
        new_paragraph,
        new_column,
        *_,
    ) = iter(TokenType)
    tab_replacement = " " * tabsize

    # pylint: disable=consider-using-in
    for token in MTextParser(text):
        t = token.type
        if t == word:
            paragraph.append(token.data)
        elif t == space or t == nbsp:
            paragraph.append(" ")
        elif t == new_paragraph or t == new_column:
            content.append("".join(paragraph))
            paragraph.clear()
        elif t == tabulator:
            paragraph.append(tab_replacement)
        elif t == stack:
            upr, lwr, divider = token.data
            paragraph.append(upr + divider + lwr)

    if paragraph:
        content.append("".join(paragraph))
    if split:
        return content
    return "\n".join(content)


def escape_dxf_line_endings(text: str) -> str:
    # replacing '\r\n' and '\n' by '\P' is required when exporting, else an
    # invalid DXF file would be created.
    return text.replace("\r", "").replace("\n", "\\P")


def replace_non_printable_characters(text: str, replacement: str = "▯") -> str:
    return "".join(replacement if is_non_printable_char(c) else c for c in text)


def is_non_printable_char(char: str) -> bool:
    return 0 <= ord(char) < 32 and char != "\t"


def text_wrap(
    text: str,
    box_width: Optional[float],
    get_text_width: Callable[[str], float],
) -> list[str]:
    """Wrap text at ``\\n`` and given `box_width`. This tool was developed for
    usage with the MTEXT entity. This isn't the most straightforward word
    wrapping algorithm, but it aims to match the behavior of AutoCAD.

    Args:
        text: text to wrap, included ``\\n`` are handled as manual line breaks
        box_width: wrapping length, ``None`` to just wrap at ``\\n``
        get_text_width: callable which returns the width of the given string

    """
    # Copyright (c) 2020-2021, Matthew Broadway
    # License: MIT License
    if not text or text.isspace():
        return []
    manual_lines = re.split(r"(\n)", text)  # includes \n as its own token
    tokens = [t for line in manual_lines for t in re.split(r"(\s+)", line) if t]
    lines: list[str] = []
    current_line: str = ""
    line_just_wrapped = False

    for t in tokens:
        on_first_line = not lines
        if t == "\n" and line_just_wrapped:
            continue
        line_just_wrapped = False
        if t == "\n":
            lines.append(current_line.rstrip())
            current_line = ""
        elif t.isspace():
            if current_line or on_first_line:
                current_line += t
        else:
            if box_width is not None and get_text_width(current_line + t) > box_width:
                if not current_line:
                    current_line += t
                else:
                    lines.append(current_line.rstrip())
                    current_line = t
                    line_just_wrapped = True
            else:
                current_line += t

    if current_line and not current_line.isspace():
        lines.append(current_line.rstrip())
    return lines


def is_text_vertical_stacked(text: DXFEntity) -> bool:
    """Returns ``True`` if the associated text :class:`~ezdxf.entities.Textstyle`
    is vertical stacked.
    """
    if not text.is_supported_dxf_attrib("style"):
        raise TypeError(f"{text.dxftype()} does not support the style attribute.")

    if text.doc:
        style = text.doc.styles.get(text.dxf.style)
        if style:
            return style.is_vertical_stacked
    return False


_alignment_char = {
    MTextParagraphAlignment.DEFAULT: "",
    MTextParagraphAlignment.LEFT: "l",
    MTextParagraphAlignment.RIGHT: "r",
    MTextParagraphAlignment.CENTER: "c",
    MTextParagraphAlignment.JUSTIFIED: "j",
    MTextParagraphAlignment.DISTRIBUTED: "d",
}

COMMA = ","
DIGITS = "01234567890"


def rstrip0(s: str) -> str:
    if isinstance(s, (int, float)):
        return f"{s:g}"
    return s


class ParagraphProperties(NamedTuple):
    """Stores all known MTEXT paragraph properties in a :class:`NamedTuple`.
    Indentations and tab stops are multiples of the default text height
    :attr:`MText.dxf.char_height`. E.g. :attr:`char_height` is 0.25 and
    :attr:`indent` is 4, the real indentation is 4 x 0.25 = 1 drawing unit.
    The default tabulator stops are 4, 8, 12, ... if no tabulator stops are
    explicit defined.

    Args:
         indent (float): left indentation of the first line, relative to :attr:`left`,
            which means an :attr:`indent` of 0 has always the same indentation
            as :attr:`left`
         left (float): left indentation of the paragraph except for the first line
         right (float): left indentation of the paragraph
         align: :class:`~ezdxf.lldxf.const.MTextParagraphAlignment` enum
         tab_stops: tuple of tabulator stops, as ``float`` or as ``str``,
            ``float`` values are left aligned tab stops, strings with prefix
            ``"c"`` are center aligned tab stops and strings with prefix ``"r"``
            are right aligned tab stops

    """

    # Reset: \pi*,l*,r*,q*,t;
    indent: float = 0  # relative to left!
    left: float = 0
    right: float = 0
    align: MTextParagraphAlignment = MTextParagraphAlignment.DEFAULT
    # tab stops without prefix or numbers are left adjusted
    # tab stops, e.g 2 or '2'
    # prefix 'c' defines a center adjusted tab stop e.g. 'c3.5'
    # prefix 'r' defines a right adjusted tab stop e.g. 'r2.7'
    # The tab stop in drawing units = n x char_height
    tab_stops: tuple = tuple()

    def tostring(self) -> str:
        """Returns the MTEXT paragraph properties as MTEXT inline code
        e.g. ``"\\pxi-2,l2;"``.

        """
        args = []
        if self.indent:
            args.append(f"i{self.indent:g}")
            args.append(COMMA)
        if self.left:
            args.append(f"l{self.left:g}")
            args.append(COMMA)
        if self.right:
            args.append(f"r{self.right:g}")
            args.append(COMMA)
        if self.align:
            args.append(f"q{_alignment_char[self.align]}")
            args.append(COMMA)
        if self.tab_stops:
            args.append(f"t{COMMA.join(map(rstrip0, self.tab_stops))}")
            args.append(COMMA)

        if args:
            if args[-1] == COMMA:
                args.pop()
            # exporting always "x" as second letter seems to be safe
            return "\\px" + "".join(args) + ";"
        return ""


# IMPORTANT for parsing MTEXT inline codes: "\\H0.1\\A1\\C1rot"
# Inline commands with a single argument, don't need a trailing ";"!


class MTextEditor:
    """The :class:`MTextEditor` is a helper class to build MTEXT content
    strings with support for inline codes to change color, font or
    paragraph properties. The result is always accessible by the :attr:`text`
    attribute or the magic :func:`__str__` function as
    :code:`str(MTextEditor("text"))`.

    All text building methods return `self` to implement a floating interface::

        e = MTextEditor("This example ").color("red").append("switches color to red.")
        mtext = msp.add_mtext(str(e))

    The initial text height, color, text style and so on is determined by the
    DXF attributes of the :class:`~ezdxf.entities.MText` entity.

    .. warning::

        The :class:`MTextEditor` assembles just the inline code, which has to be
        parsed and rendered by the target CAD application, `ezdxf` has no influence
        to that result.

        Keep inline formatting as simple as possible, don't test the limits of its
        capabilities, this will not work across different CAD applications and keep
        the formatting in a logic manner like, do not change paragraph properties
        in the middle of a paragraph.

        **There is no official documentation for the inline codes!**

    Args:
        text: init value of the MTEXT content string.

    """

    def __init__(self, text: str = ""):
        self.text = str(text)

    NEW_LINE = r"\P"
    NEW_PARAGRAPH = r"\P"
    NEW_COLUMN = r"\N"
    UNDERLINE_START = r"\L"
    UNDERLINE_STOP = r"\l"
    OVERSTRIKE_START = r"\O"
    OVERSTRIKE_STOP = r"\o"
    STRIKE_START = r"\K"
    STRIKE_STOP = r"\k"
    GROUP_START = "{"
    GROUP_END = "}"
    ALIGN_BOTTOM = r"\A0;"
    ALIGN_MIDDLE = r"\A1;"
    ALIGN_TOP = r"\A2;"
    NBSP = r"\~"  # non-breaking space
    TAB = "^I"

    def append(self, text: str) -> MTextEditor:
        """Append `text`."""
        self.text += text
        return self

    def __iadd__(self, text: str) -> MTextEditor:
        r"""
        Append `text`::

            e = MTextEditor("First paragraph.\P")
            e += "Second paragraph.\P")

        """
        self.text += text
        return self

    def __str__(self) -> str:
        """Returns the MTEXT content attribute :attr:`text`."""
        return self.text

    def clear(self):
        """Reset the content to an empty string."""
        self.text = ""

    def font(self, name: str, bold: bool = False, italic: bool = False) -> MTextEditor:
        """Set the text font by the font family name. Changing the font height
        should be done by the :meth:`height` or the :meth:`scale_height` method.
        The font family name is the name shown in font selection widgets in
        desktop applications: "Arial", "Times New Roman", "Comic Sans MS".
        Switching the codepage is not supported.

        Args:
            name: font family name
            bold: flag
            italic: flag

        """
        # c0 = current codepage
        # The current implementation of ezdxf writes everything in one
        # encoding, defined by $DWGCODEPAGE < DXF R2007 or utf8 for DXF R2007+
        # Switching codepage makes no sense!
        # p0 = current text size;
        # Text size should be changed by \H<factor>x;
        return self.append(rf"\f{name}|b{int(bold)}|i{int(italic)};")

    def scale_height(self, factor: float) -> MTextEditor:
        """Scale the text height by a `factor`. This scaling will accumulate,
        which means starting at height 2.5 and scaling by 2 and again by 3 will
        set the text height to 2.5 x 2 x 3 = 15. The current text height is not
        stored in the :class:`MTextEditor`, you have to track the text height by
        yourself! The initial text height is stored in the
        :class:`~ezdxf.entities.MText` entity as DXF attribute
        :class:`~ezdxf.entities.MText.dxf.char_height`.

        """
        return self.append(rf"\H{round(factor, 3)}x;")

    def height(self, height: float) -> MTextEditor:
        """Set the absolute text height in drawing units."""
        return self.append(rf"\H{round(height, 3)};")

    def width_factor(self, factor: float) -> MTextEditor:
        """Set the absolute text width factor."""
        return self.append(rf"\W{round(factor, 3)};")

    def char_tracking_factor(self, factor: float) -> MTextEditor:
        """Set the absolute character tracking factor."""
        return self.append(rf"\T{round(factor, 3)};")

    def oblique(self, angle: int) -> MTextEditor:
        """Set the text oblique angle in degrees, vertical is 0, a value of 15
        will lean the text 15 degree to the right.

        """
        return self.append(rf"\Q{int(angle)};")

    def color(self, name: str) -> MTextEditor:
        """Set the text color by color name: "red", "yellow", "green", "cyan",
        "blue", "magenta" or "white".

        """
        return self.aci(const.MTEXT_COLOR_INDEX[name.lower()])

    def aci(self, aci: int) -> MTextEditor:
        """Set the text color by :ref:`ACI` in range [0, 256]."""
        if 0 <= aci <= 256:
            return self.append(rf"\C{aci};")
        raise ValueError("aci not in range [0, 256]")

    def rgb(self, rgb: RGB) -> MTextEditor:
        """Set the text color as RGB value."""
        r, g, b = rgb
        return self.append(rf"\c{rgb2int((b, g, r))};")

    def stack(self, upr: str, lwr: str, t: str = "^") -> MTextEditor:
        r"""Append stacked text `upr` over `lwr`, argument `t` defines the
        kind of stacking, the space " " after the "^" will be added
        automatically to avoid caret decoding:

        .. code-block:: none

            "^": vertical stacked without divider line, e.g. \SA^ B:
                 A
                 B

            "/": vertical stacked with divider line,  e.g. \SX/Y:
                 X
                 -
                 Y

            "#": diagonal stacked, with slanting divider line, e.g. \S1#4:
                 1/4

        """
        if t not in "^/#":
            raise ValueError(f"invalid type symbol: {t}")
        # space " " after "^" is required to avoid caret decoding
        if t == "^":
            t += " "
        return self.append(rf"\S{upr}{t}{lwr};")

    def group(self, text: str) -> MTextEditor:
        """Group `text`, all properties changed inside a group are reverted at
        the end of the group. AutoCAD supports grouping up to 8 levels.

        """
        return self.append(f"{{{text}}}")

    def underline(self, text: str) -> MTextEditor:
        """Append `text` with a line below the text."""
        return self.append(rf"\L{text}\l")

    def overline(self, text: str) -> MTextEditor:
        """Append `text` with a line above the text."""
        return self.append(rf"\O{text}\o")

    def strike_through(self, text: str) -> MTextEditor:
        """Append `text` with a line through the text."""
        return self.append(rf"\K{text}\k")

    def paragraph(self, props: ParagraphProperties) -> MTextEditor:
        """Set paragraph properties by a :class:`ParagraphProperties` object."""
        return self.append(props.tostring())

    def bullet_list(
        self, indent: float, bullets: Iterable[str], content: Iterable[str]
    ) -> MTextEditor:
        """Build bulleted lists by utilizing paragraph indentation and a
        tabulator stop. Any string can be used as bullet. Indentation is
        a multiple of the initial MTEXT char height (see also docs about
        :class:`ParagraphProperties`), which means indentation in
        drawing units is :attr:`MText.dxf.char_height` x `indent`.

        Useful UTF bullets:

        - "bull" U+2022 = • (Alt Numpad 7)
        - "circle" U+25CB = ○ (Alt Numpad 9)

        For numbered lists just use numbers as bullets::

            MTextEditor.bullet_list(
                indent=2,
                bullets=["1.", "2."],
                content=["first", "second"],
            )

        Args:
            indent: content indentation as multiple of the initial MTEXT char height
            bullets: iterable of bullet strings, e.g. :code:`["-"] * 3`,
                for 3 dashes as bullet strings
            content: iterable of list item strings, one string per list item,
                list items should not contain new line or new paragraph commands.

        """
        items = MTextEditor().paragraph(
            ParagraphProperties(
                indent=-indent * 0.75,  # like BricsCAD
                left=indent,
                tab_stops=(indent,),
            )
        )
        items.append(
            "".join(
                b + self.TAB + c + self.NEW_PARAGRAPH for b, c in zip(bullets, content)
            )
        )
        return self.group(str(items))


class UnknownCommand(Exception):
    pass


class MTextContext:
    """Internal class to store the MTEXT context state."""

    def __init__(self) -> None:
        from ezdxf.fonts import fonts

        self._stroke: int = 0
        self.continue_stroke: bool = False
        self._aci = 7  # used if rgb is None
        self.rgb: Optional[RGB] = None  # overrules aci
        self.align = MTextLineAlignment.BOTTOM
        self.font_face = fonts.FontFace()  # is immutable
        self.cap_height: float = 1.0
        self.width_factor: float = 1.0
        self.char_tracking_factor: float = 1.0
        self.oblique: float = 0.0  # in degrees, where 0 is vertical (TEXT entity)
        self.paragraph = ParagraphProperties()

    def __copy__(self) -> MTextContext:
        p = MTextContext()
        p._stroke = self._stroke
        p.continue_stroke = self.continue_stroke
        p._aci = self._aci
        p.rgb = self.rgb
        p.align = self.align
        p.font_face = self.font_face  # is immutable
        p.cap_height = self.cap_height
        p.width_factor = self.width_factor
        p.char_tracking_factor = self.char_tracking_factor
        p.oblique = self.oblique
        p.paragraph = self.paragraph  # is immutable
        return p

    copy = __copy__

    def __hash__(self):
        return hash(
            (
                self._stroke,
                self.continue_stroke,
                self._aci,
                self.rgb,
                self.align,
                self.font_face,
                self.cap_height,
                self.width_factor,
                self.char_tracking_factor,
                self.oblique,
                self.paragraph,
            )
        )

    def __eq__(self, other) -> bool:
        return hash(self) == hash(other)

    @property
    def aci(self) -> int:
        return self._aci

    @aci.setter
    def aci(self, aci: int):
        if 0 <= aci <= 256:
            self._aci = aci
            self.rgb = None  # clear rgb
        else:
            raise ValueError("aci not in range[0,256]")

    def _set_stroke_state(self, stroke: MTextStroke, state: bool = True) -> None:
        """Set/clear binary `stroke` flag in `self._stroke`.

        Args:
            stroke: set/clear stroke flag
            state: ``True`` for setting, ``False`` for clearing

        """
        if state:
            self._stroke |= stroke
        else:
            self._stroke &= ~stroke

    @property
    def underline(self) -> bool:
        return bool(self._stroke & MTextStroke.UNDERLINE)

    @underline.setter
    def underline(self, value: bool) -> None:
        self._set_stroke_state(MTextStroke.UNDERLINE, value)

    @property
    def strike_through(self) -> bool:
        return bool(self._stroke & MTextStroke.STRIKE_THROUGH)

    @strike_through.setter
    def strike_through(self, value: bool) -> None:
        self._set_stroke_state(MTextStroke.STRIKE_THROUGH, value)

    @property
    def overline(self) -> bool:
        return bool(self._stroke & MTextStroke.OVERLINE)

    @overline.setter
    def overline(self, value: bool) -> None:
        self._set_stroke_state(MTextStroke.OVERLINE, value)

    @property
    def has_any_stroke(self) -> bool:
        return bool(self._stroke)


class TextScanner:
    __slots__ = ("_text", "_text_len", "_index")

    def __init__(self, text: str):
        self._text = str(text)
        self._text_len = len(self._text)
        self._index = 0

    @property
    def is_empty(self) -> bool:
        return self._index >= self._text_len

    @property
    def has_data(self) -> bool:
        return self._index < self._text_len

    def get(self) -> str:
        try:
            char = self._text[self._index]
            self._index += 1
        except IndexError:
            return ""
        return char

    def consume(self, count: int = 1) -> None:
        if count < 1:
            raise ValueError(count)
        self._index += count

    def fast_consume(self, count: int = 1) -> None:
        """consume() without safety check"""
        self._index += count

    def peek(self, offset: int = 0) -> str:
        if offset < 0:
            raise ValueError(offset)
        try:
            return self._text[self._index + offset]
        except IndexError:
            return ""

    def fast_peek(self, offset: int = 0) -> str:
        """peek() without safety check"""
        try:
            return self._text[self._index + offset]
        except IndexError:
            return ""

    def find(self, char: str, escape=False) -> int:
        """Return the index of the next `char`. If `escape` is True, backslash
        escaped `chars` will be ignored e.g. "\\;;" index of the next ";" is 1
        if `escape` is False and 2 if `escape` is True. Returns -1 if `char`
        was not found.

        Args:
            char: single letter as string
            escape: ignore backslash escaped chars if True.

        """

        scanner = self.__class__(self._text[self._index :])
        while scanner.has_data:
            c = scanner.peek()
            if escape and c == "\\" and scanner.peek(1) == char:
                scanner.consume(2)
                continue
            if c == char:
                return self._index + scanner._index  # pylint: disable=w0212
            scanner.consume(1)
        return -1

    def substr(self, stop: int) -> str:
        """Returns the substring from the current location until index < stop."""
        if stop < self._index:
            raise IndexError(stop)
        return self._text[self._index : stop]

    def tail(self) -> str:
        """Returns the unprocessed part of the content."""
        return self._text[self._index :]

    def index(self) -> int:
        return self._index

    def substr2(self, start: int, stop: int) -> str:
        return self._text[start:stop]


class TokenType(enum.IntEnum):
    NONE = 0
    WORD = 1  # data = str
    STACK = 2  # data = tuple[upr: str, lwr:str, type:str]
    SPACE = 3  # data = None
    NBSP = 4  # data = None
    TABULATOR = 5  # data = None
    NEW_PARAGRAPH = 6  # data = None
    NEW_COLUMN = 7  # data = None
    WRAP_AT_DIMLINE = 8  # data = None
    PROPERTIES_CHANGED = 9  # data = full command string e.g. "\H150;"


class MTextToken:
    __slots__ = ("type", "ctx", "data")

    def __init__(self, t: TokenType, ctx: MTextContext, data=None):
        self.type: TokenType = t
        self.ctx: MTextContext = ctx
        self.data = data


RE_FLOAT = re.compile(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")
RE_FLOAT_X = re.compile(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?([x]?)")

CHAR_TO_ALIGN = {
    "l": MTextParagraphAlignment.LEFT,
    "r": MTextParagraphAlignment.RIGHT,
    "c": MTextParagraphAlignment.CENTER,
    "j": MTextParagraphAlignment.JUSTIFIED,
    "d": MTextParagraphAlignment.DISTRIBUTED,
}


class MTextParser:
    """Parses the MText content string and yields the content as tokens and
    the current MText properties as MTextContext object. The context object is
    treated internally as immutable object and should be treated by the client
    the same way.

    The parser works as iterator and yields MTextToken objects.

    Args:
        content: MText content string
        ctx: initial MText context
        yield_property_commands: yield commands that change properties or context,
            default is ``False``

    """

    def __init__(
        self,
        content: str,
        ctx: Optional[MTextContext] = None,
        yield_property_commands=False,
    ):
        if ctx is None:
            ctx = MTextContext()
        self.ctx = ctx
        self.scanner = TextScanner(caret_decode(content))
        self._ctx_stack: list[MTextContext] = []
        self._continue_stroke = False
        self._yield_property_commands = bool(yield_property_commands)

    def __iter__(self) -> Iterator[MTextToken]:
        return self.parse()

    def push_ctx(self) -> None:
        self._ctx_stack.append(self.ctx)

    def pop_ctx(self) -> None:
        if self._ctx_stack:
            self.ctx = self._ctx_stack.pop()

    def parse(self) -> Iterator[MTextToken]:
        # pylint: disable=too-many-statements
        # localize method calls
        scanner = self.scanner
        consume = scanner.fast_consume
        peek = scanner.fast_peek
        followup_token: Optional[TokenType] = None
        space_token = TokenType.SPACE
        word_token = TokenType.WORD

        def word_and_token(word, token):
            nonlocal followup_token
            consume()
            if word:
                followup_token = token
                return word_token, word
            return token, None

        def next_token() -> tuple[TokenType, Any]:
            # pylint: disable=too-many-return-statements,too-many-branches,too-many-nested-blocks
            word: str = ""
            while scanner.has_data:
                escape = False
                letter = peek()
                cmd_start_index = scanner.index()
                if letter == "\\":
                    # known escape sequences: "\\", "\{", "\}"
                    if peek(1) in "\\{}":
                        escape = True
                        consume()  # leading backslash
                        letter = peek()
                    else:
                        # A non escaped backslash is always the end of a word.
                        if word:
                            # Do not consume backslash!
                            return word_token, word
                        consume()  # leading backslash
                        cmd = scanner.get()
                        if cmd == "~":
                            return TokenType.NBSP, None
                        if cmd == "P":
                            return TokenType.NEW_PARAGRAPH, None
                        if cmd == "N":
                            return TokenType.NEW_COLUMN, None
                        if cmd == "X":
                            return TokenType.WRAP_AT_DIMLINE, None
                        if cmd == "S":
                            return self.parse_stacking()
                        if cmd:
                            try:
                                self.parse_properties(cmd)
                            except UnknownCommand:
                                # print invalid escaped letters verbatim
                                word += letter + cmd
                            else:
                                if self._yield_property_commands:
                                    return (
                                        TokenType.PROPERTIES_CHANGED,
                                        scanner.substr2(
                                            cmd_start_index, scanner.index()
                                        ),
                                    )
                        continue

                # process control chars, caret decoding is already done!
                if letter < " ":
                    if letter == "\t":
                        return word_and_token(word, TokenType.TABULATOR)
                    if letter == "\n":  # LF
                        return word_and_token(word, TokenType.NEW_PARAGRAPH)
                    # replace other control chars by a space
                    letter = " "
                elif letter == "%" and peek(1) == "%":
                    code = peek(2).lower()
                    special_char = const.SPECIAL_CHAR_ENCODING.get(code)
                    if special_char:
                        consume(2)  # %%
                        letter = special_char

                if letter == " ":
                    return word_and_token(word, space_token)

                if not escape:
                    if letter == "{":
                        if word:
                            return word_token, word
                        else:
                            consume(1)
                            self.push_ctx()
                            continue
                    elif letter == "}":
                        if word:
                            return word_token, word
                        else:
                            consume(1)
                            self.pop_ctx()
                            continue

                # any unparsed unicode letter can be used in a word
                consume()
                if letter >= " ":
                    word += letter

            if word:
                return word_token, word
            else:
                return TokenType.NONE, None

        while True:
            type_, data = next_token()
            if type_:
                yield MTextToken(type_, self.ctx, data)
                if followup_token:
                    yield MTextToken(followup_token, self.ctx, None)
                    followup_token = None
            else:
                break

    def parse_stacking(self) -> tuple[TokenType, Any]:
        """Returns a tuple of strings: (numerator, denominator, type).
        The numerator and denominator is always a single and can contain spaces,
        which are not decoded as separate tokens. The type string is "^" for
        a limit style fraction without a line, "/" for a horizontal fraction
        and "#" for a diagonal fraction. If the expression does not contain
        any stacking type char, the type and denominator string are empty "".

        """

        def peek_char():
            c = stacking_scanner.peek()
            if ord(c) < 32:  # replace all control chars by space
                c = " "
            return c

        def get_next_char():
            escape = False
            c = peek_char()
            # escape sequences: remove backslash and return next char
            if c == "\\":
                escape = True
                stacking_scanner.consume(1)
                c = peek_char()
            stacking_scanner.consume(1)
            return c, escape

        def parse_numerator() -> tuple[str, str]:
            word = ""
            while stacking_scanner.has_data:
                c, escape = get_next_char()
                if not escape and c in "^/#":  # scan until stacking type char
                    return word, c
                word += c
            return word, ""

        def parse_denominator() -> str:
            word = ""
            while stacking_scanner.has_data:
                word += get_next_char()[0]
            return word

        stacking_scanner = TextScanner(self.extract_expression(escape=True))
        numerator, stacking_type = parse_numerator()
        denominator = parse_denominator() if stacking_type else ""
        return TokenType.STACK, (numerator, denominator, stacking_type)

    def parse_properties(self, cmd: str) -> None:
        # pylint: disable=too-many-branches
        # Treat the existing context as immutable, create a new one:
        new_ctx = self.ctx.copy()
        if cmd == "L":
            new_ctx.underline = True
            self._continue_stroke = True
        elif cmd == "l":
            new_ctx.underline = False
            if not new_ctx.has_any_stroke:
                self._continue_stroke = False
        elif cmd == "O":
            new_ctx.overline = True
            self._continue_stroke = True
        elif cmd == "o":
            new_ctx.overline = False
            if not new_ctx.has_any_stroke:
                self._continue_stroke = False
        elif cmd == "K":
            new_ctx.strike_through = True
            self._continue_stroke = True
        elif cmd == "k":
            new_ctx.strike_through = False
            if not new_ctx.has_any_stroke:
                self._continue_stroke = False
        elif cmd == "A":
            self.parse_align(new_ctx)
        elif cmd == "C":
            self.parse_aci_color(new_ctx)
        elif cmd == "c":
            self.parse_rgb_color(new_ctx)
        elif cmd == "H":
            self.parse_height(new_ctx)
        elif cmd == "W":
            self.parse_width(new_ctx)
        elif cmd == "Q":
            self.parse_oblique(new_ctx)
        elif cmd == "T":
            self.parse_char_tracking(new_ctx)
        elif cmd == "p":
            self.parse_paragraph_properties(new_ctx)
        elif cmd == "f" or cmd == "F":
            self.parse_font_properties(new_ctx)
        else:
            raise UnknownCommand(f"unknown command: {cmd}")
        new_ctx.continue_stroke = self._continue_stroke
        self.ctx = new_ctx

    def parse_align(self, ctx: MTextContext):
        char = self.scanner.get()  # always consume next char
        if char in "012":
            ctx.align = MTextLineAlignment(int(char))
        else:
            ctx.align = MTextLineAlignment.BOTTOM
        self.consume_optional_terminator()

    def parse_height(self, ctx: MTextContext):
        ctx.cap_height = self.parse_float_value_or_factor(ctx.cap_height)
        self.consume_optional_terminator()

    def parse_width(self, ctx: MTextContext):
        ctx.width_factor = self.parse_float_value_or_factor(ctx.width_factor)
        self.consume_optional_terminator()

    def parse_char_tracking(self, ctx: MTextContext):
        ctx.char_tracking_factor = self.parse_float_value_or_factor(
            ctx.char_tracking_factor
        )
        self.consume_optional_terminator()

    def parse_float_value_or_factor(self, value) -> float:
        expr = self.extract_float_expression(relative=True)
        if expr:
            if expr.endswith("x"):
                factor = float(expr[:-1])
                value *= abs(factor)
            else:
                value = abs(float(expr))
        return value

    def parse_oblique(self, ctx: MTextContext):
        oblique_expr = self.extract_float_expression(relative=False)
        if oblique_expr:
            ctx.oblique = float(oblique_expr)
        self.consume_optional_terminator()

    def parse_aci_color(self, ctx: MTextContext):
        aci_expr = self.extract_int_expression()
        if aci_expr:
            aci = int(aci_expr)
            if aci < 257:
                ctx.aci = aci
                ctx.rgb = None
        self.consume_optional_terminator()

    def parse_rgb_color(self, ctx: MTextContext):
        rgb_expr = self.extract_int_expression()
        if rgb_expr:
            # in reversed order!
            b, g, r = int2rgb(int(rgb_expr) & 0xFFFFFF)
            ctx.rgb = RGB(r, g, b)
        self.consume_optional_terminator()

    def extract_float_expression(self, relative=False) -> str:
        result = ""
        tail = self.scanner.tail()
        pattern = RE_FLOAT_X if relative else RE_FLOAT
        match = re.match(pattern, tail)
        if match:
            start, end = match.span()
            result = tail[start:end]
            self.scanner.consume(end)
        return result

    def extract_int_expression(self) -> str:
        result = ""
        tail = self.scanner.tail()
        match = re.match(r"\d+", tail)
        if match:
            start, end = match.span()
            result = tail[start:end]
            self.scanner.consume(end)
        return result

    def extract_expression(self, escape=False) -> str:
        """Returns the next expression from the current location until
        the terminating ";". The terminating semicolon is not included.
        Skips escaped "\\;" semicolons if `escape` is True.

        """
        stop = self.scanner.find(";", escape=escape)
        if stop < 0:  # ";" not found
            expr = self.scanner.tail()  # scan until end of content
        else:
            expr = self.scanner.substr(stop)  # exclude ";"
        # skip the expression in the main scanner
        self.scanner.consume(len(expr) + 1)  # include ";"
        return expr

    def parse_paragraph_properties(self, ctx: MTextContext):
        def parse_float() -> float:
            value = 0.0
            expr = parse_float_expr()
            if expr:
                value = float(expr)
            return value

        def parse_float_expr() -> str:
            expr = ""
            tail = paragraph_scanner.tail()
            match = re.match(RE_FLOAT, tail)
            if match:
                start, end = match.span()
                expr = tail[start:end]
                paragraph_scanner.consume(end)
                skip_commas()
            return expr

        def skip_commas():
            while paragraph_scanner.peek() == ",":
                paragraph_scanner.consume(1)

        paragraph_scanner = TextScanner(self.extract_expression())
        indent, left, right, align, tab_stops = ctx.paragraph  # NamedTuple
        while paragraph_scanner.has_data:
            cmd = paragraph_scanner.get()
            if cmd == "i":
                indent = parse_float()
            elif cmd == "l":
                left = parse_float()
            elif cmd == "r":
                right = parse_float()
            elif cmd == "x":
                pass  # ignore
            elif cmd == "q":
                adjustment = paragraph_scanner.get()
                align = CHAR_TO_ALIGN.get(adjustment, MTextParagraphAlignment.DEFAULT)
                skip_commas()
            elif cmd == "t":
                tab_stops = []  # type: ignore
                while paragraph_scanner.has_data:  # parse to end
                    type_ = paragraph_scanner.peek()
                    if type_ == "r" or type_ == "c":
                        paragraph_scanner.consume()
                        tab_stops.append(type_ + parse_float_expr())  # type: ignore
                    else:
                        float_expr = parse_float_expr()
                        if float_expr:
                            tab_stops.append(float(float_expr))  # type: ignore
                        else:
                            # invalid float expression, consume invalid letter
                            # and try again:
                            paragraph_scanner.consume()
        ctx.paragraph = ParagraphProperties(
            indent, left, right, align, tuple(tab_stops)
        )

    def parse_font_properties(self, ctx: MTextContext):
        from ezdxf.fonts import fonts

        parts = self.extract_expression().split("|")
        # an empty font family name does not change the font properties
        if parts and parts[0]:
            name = parts[0]
            style = "Regular"
            weight = 400
            # ignore codepage and pitch - it seems not to be used in newer
            # CAD applications.
            for part in parts[1:]:
                if part.startswith("b1"):
                    weight = 700
                elif part.startswith("i1"):
                    style = "Italic"
            ctx.font_face = fonts.FontFace(family=name, style=style, weight=weight)

    def consume_optional_terminator(self):
        if self.scanner.peek() == ";":
            self.scanner.consume(1)


def load_mtext_content(tags: Tags) -> str:
    tail = ""
    content = ""
    for code, value in tags:
        if code == 1:
            tail = value
        elif code == 3:
            content += value
    return escape_dxf_line_endings(content + tail)


def has_inline_formatting_codes(text: str) -> bool:
    """Returns `True` if `text` contains any MTEXT inline formatting codes."""
    # Each inline formatting code starts with a backslash "\".
    # Remove all special chars starting with a "\" and test if any backslashes
    # remain. Escaped backslashes "\\" may return false positive,
    # but they are rare.
    # Replacing multiple strings at once by "re" is much slower,
    # see profiling/string_replace.py
    return "\\" in text.replace(  # line breaks
        r"\P", ""
    ).replace(  # non breaking spaces
        r"\~", ""
    )


def is_upside_down_text_angle(angle: float, tol: float = 3.0) -> bool:
    """Returns ``True`` if the given text `angle` in degrees causes an upside
    down text in the :ref:`WCS`. The strict flip range is 90° < `angle` < 270°,
    the tolerance angle `tol` extends this range to: 90+tol < `angle` < 270-tol.
    The angle is normalized to [0, 360).

    Args:
        angle: text angle in degrees
        tol: tolerance range in which text flipping will be avoided

    """
    angle %= 360.0
    return 90.0 + tol < angle < 270.0 - tol


def upright_text_angle(angle: float, tol: float = 3.0) -> float:
    """Returns a readable (upright) text angle in the range `angle` <= 90+tol or
    `angle` >= 270-tol. The angle is normalized to [0, 360).

    Args:
        angle: text angle in degrees
        tol: tolerance range in which text flipping will be avoided

    """
    if is_upside_down_text_angle(angle, tol):
        angle += 180.0
    return angle % 360.0


def leading(cap_height: float, line_spacing: float = 1.0) -> float:
    """Returns the distance from baseline to baseline.

    Args:
        cap_height: cap height of the line
        line_spacing: line spacing factor as percentage of 3-on-5 spacing

    """
    # method "exact": 3-on-5 line spacing = 5/3 = 1.667
    # method "at least" is not supported
    return cap_height * 1.667 * line_spacing


def estimate_mtext_extents(mtext: MText) -> tuple[float, float]:
    """Estimate the width and height of a single column
    :class:`~ezdxf.entities.MText` entity.

    This function is faster than the :func:`~ezdxf.tools.text_size.mtext_size`
    function, but the result is very inaccurate if inline codes are used or
    line wrapping at the column border is involved!

    Returns:
        Tuple[width, height]

    """

    def _make_font() -> fonts.AbstractFont:
        from ezdxf.fonts import fonts

        cap_height: float = mtext.dxf.get_default("char_height")
        doc = mtext.doc
        if doc:
            style = doc.styles.get(mtext.dxf.get_default("style"))
            if style is not None:
                return style.make_font(cap_height)
        return fonts.make_font(const.DEFAULT_TTF, cap_height=cap_height)

    return estimate_mtext_content_extents(
        content=mtext.text,
        font=_make_font(),
        column_width=mtext.dxf.get("width", 0.0),
        line_spacing_factor=mtext.dxf.get_default("line_spacing_factor"),
    )


_SAFETY_FACTOR = 1.01


def set_estimation_safety_factor(factor: float) -> None:
    """Set the global safety factor for MTEXT size estimation."""
    global _SAFETY_FACTOR
    _SAFETY_FACTOR = factor


def reset_estimation_safety_factor() -> None:
    """Reset the global safety factor for MTEXT size estimation to the hard coded
    default value.
    """
    global _SAFETY_FACTOR
    _SAFETY_FACTOR = 1.01


def estimate_mtext_content_extents(
    content: str,
    font: fonts.AbstractFont,
    column_width: float = 0.0,
    line_spacing_factor: float = 1.0,
) -> tuple[float, float]:
    """Estimate the width and height of the :class:`~ezdxf.entities.MText`
    content string. The result is very inaccurate if inline codes are used or
    line wrapping at the column border is involved!
    Column breaks ``\\N`` will be ignored.

    Args:
        content: the :class:`~ezdxf.entities.MText` content string
        font: font abstraction based on :class:`ezdxf.tools.fonts.AbstractFont`
        column_width: :attr:`MText.dxf.width` or 0.0 for an unrestricted column
            width
        line_spacing_factor: :attr:`MText.dxf.line_spacing_factor`

    Returns:
        tuple[width, height]

    """
    max_width: float = 0.0
    height: float = 0.0
    cap_height: float = font.measurements.cap_height
    has_column_width: bool = column_width > 0.0
    lines: list[str] = fast_plain_mtext(content, split=True)  # type: ignore

    if any(lines):  # has any non-empty lines
        line_count: int = 0
        for line in lines:
            line_width = font.text_width(line)
            if line_width == 0 and line:
                # line contains only white space and text_width() returns 0
                # - MatplotlibFont returns 0 as text width
                # - MonospaceFont returns the correct width
                line_width = len(line) * font.space_width()
            if has_column_width:
                # naive line wrapping, does not care about line content
                line_count += math.ceil(line_width / column_width)
                line_width = min(line_width, column_width)
            else:
                line_count += 1
            # Note: max_width can be smaller than the column_width, if all lines
            # are shorter than column_width!
            max_width = max(max_width, line_width)

        spacing = leading(cap_height, line_spacing_factor) - cap_height
        height = cap_height * line_count + spacing * (line_count - 1)

    return max_width * _SAFETY_FACTOR, height


def safe_string(s: Optional[str], max_len: int = MAX_STR_LEN) -> str:
    """Returns a string with line breaks ``\\n`` replaced by ``\\P`` and the
    length limited to `max_len`.
    """
    if isinstance(s, str):
        return escape_dxf_line_endings(s)[:max_len]
    return ""


VALID_HEIGHT_CHARS = set("0123456789.")


def scale_mtext_inline_commands(content: str, factor: float) -> str:
    """Scale all inline commands which define an absolute value by a `factor`."""

    def _scale_leading_number(substr: str, prefix: str) -> str:
        index: int = 0
        try:
            while substr[index] in VALID_HEIGHT_CHARS:
                index += 1
            if substr[index] == "x":  # relative factor
                return f"{prefix}{substr}"
        except IndexError:  # end of string
            pass
        try:
            new_size = float(substr[:index]) * factor
            value = f"{new_size:.3g}"
        except ValueError:
            value = ""  # return a valid construct
        return rf"{prefix}{value}{substr[index:]}"

    # So far only the "\H<value>;" command will be scaled.
    # Fast check if scaling is required:
    if r"\H" not in content:
        return content

    factor = abs(factor)
    old_parts = content.split(r"\H")
    new_parts: list[str] = [old_parts[0]]
    for part in old_parts[1:]:
        new_parts.append(_scale_leading_number(part, r"\H"))

    return "".join(new_parts)
