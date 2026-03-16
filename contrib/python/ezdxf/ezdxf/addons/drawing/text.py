# Copyright (c) 2020-2023, Matthew Broadway
# License: MIT License
from __future__ import annotations
from typing import Union, Tuple, Iterable, Optional, Callable
from typing_extensions import TypeAlias
import enum
from math import radians

import ezdxf.lldxf.const as DXFConstants
from ezdxf.enums import (
    TextEntityAlignment,
    MAP_TEXT_ENUM_TO_ALIGN_FLAGS,
    MTextEntityAlignment,
)
from ezdxf.entities import MText, Text, Attrib, AttDef
from ezdxf.math import Matrix44, Vec3, sign
from ezdxf.fonts import fonts
from ezdxf.fonts.font_measurements import FontMeasurements
from ezdxf.tools.text import plain_text, text_wrap
from .text_renderer import TextRenderer

"""
Search google for 'typography' or 'font anatomy' for explanations of terms like 
'baseline' and 'x-height'

A Visual Guide to the Anatomy of Typography: https://visme.co/blog/type-anatomy/
Anatomy of a Character: https://www.fonts.com/content/learning/fontology/level-1/type-anatomy/anatomy
"""


@enum.unique
class HAlignment(enum.Enum):
    LEFT = 0
    CENTER = 1
    RIGHT = 2


@enum.unique
class VAlignment(enum.Enum):
    TOP = 0  # the top of capital letters or letters with ascenders (like 'b')
    LOWER_CASE_CENTER = 1  # the midpoint between the baseline and the x-height
    BASELINE = 2  # the line which text rests on, characters with descenders (like 'p') are partially below this line
    BOTTOM = 3  # the lowest point on a character with a descender (like 'p')
    UPPER_CASE_CENTER = 4  # the midpoint between the baseline and the cap-height


Alignment: TypeAlias = Tuple[HAlignment, VAlignment]
AnyText: TypeAlias = Union[Text, MText, Attrib, AttDef]

# multiple of cap_height between the baseline of the previous line and the
# baseline of the next line
DEFAULT_LINE_SPACING = 5 / 3

DXF_TEXT_ALIGNMENT_TO_ALIGNMENT: dict[TextEntityAlignment, Alignment] = {
    TextEntityAlignment.LEFT: (HAlignment.LEFT, VAlignment.BASELINE),
    TextEntityAlignment.CENTER: (HAlignment.CENTER, VAlignment.BASELINE),
    TextEntityAlignment.RIGHT: (HAlignment.RIGHT, VAlignment.BASELINE),
    TextEntityAlignment.ALIGNED: (HAlignment.CENTER, VAlignment.BASELINE),
    TextEntityAlignment.MIDDLE: (
        HAlignment.CENTER,
        VAlignment.LOWER_CASE_CENTER,
    ),
    TextEntityAlignment.FIT: (HAlignment.CENTER, VAlignment.BASELINE),
    TextEntityAlignment.BOTTOM_LEFT: (HAlignment.LEFT, VAlignment.BOTTOM),
    TextEntityAlignment.BOTTOM_CENTER: (HAlignment.CENTER, VAlignment.BOTTOM),
    TextEntityAlignment.BOTTOM_RIGHT: (HAlignment.RIGHT, VAlignment.BOTTOM),
    TextEntityAlignment.MIDDLE_LEFT: (
        HAlignment.LEFT,
        VAlignment.UPPER_CASE_CENTER,
    ),
    TextEntityAlignment.MIDDLE_CENTER: (
        HAlignment.CENTER,
        VAlignment.UPPER_CASE_CENTER,
    ),
    TextEntityAlignment.MIDDLE_RIGHT: (
        HAlignment.RIGHT,
        VAlignment.UPPER_CASE_CENTER,
    ),
    TextEntityAlignment.TOP_LEFT: (HAlignment.LEFT, VAlignment.TOP),
    TextEntityAlignment.TOP_CENTER: (HAlignment.CENTER, VAlignment.TOP),
    TextEntityAlignment.TOP_RIGHT: (HAlignment.RIGHT, VAlignment.TOP),
}
assert DXF_TEXT_ALIGNMENT_TO_ALIGNMENT.keys() == MAP_TEXT_ENUM_TO_ALIGN_FLAGS.keys()

DXF_MTEXT_ALIGNMENT_TO_ALIGNMENT: dict[int, Alignment] = {
    DXFConstants.MTEXT_TOP_LEFT: (HAlignment.LEFT, VAlignment.TOP),
    DXFConstants.MTEXT_TOP_CENTER: (HAlignment.CENTER, VAlignment.TOP),
    DXFConstants.MTEXT_TOP_RIGHT: (HAlignment.RIGHT, VAlignment.TOP),
    DXFConstants.MTEXT_MIDDLE_LEFT: (
        HAlignment.LEFT,
        VAlignment.LOWER_CASE_CENTER,
    ),
    DXFConstants.MTEXT_MIDDLE_CENTER: (
        HAlignment.CENTER,
        VAlignment.LOWER_CASE_CENTER,
    ),
    DXFConstants.MTEXT_MIDDLE_RIGHT: (
        HAlignment.RIGHT,
        VAlignment.LOWER_CASE_CENTER,
    ),
    DXFConstants.MTEXT_BOTTOM_LEFT: (HAlignment.LEFT, VAlignment.BOTTOM),
    DXFConstants.MTEXT_BOTTOM_CENTER: (HAlignment.CENTER, VAlignment.BOTTOM),
    DXFConstants.MTEXT_BOTTOM_RIGHT: (HAlignment.RIGHT, VAlignment.BOTTOM),
}
assert len(DXF_MTEXT_ALIGNMENT_TO_ALIGNMENT) == len(MTextEntityAlignment)


def _calc_aligned_rotation(text: Text) -> float:
    p1: Vec3 = text.dxf.insert
    p2: Vec3 = text.dxf.align_point
    if not p1.isclose(p2):
        return (p2 - p1).angle
    else:
        return radians(text.dxf.rotation)


def _get_rotation(text: AnyText) -> Matrix44:
    if isinstance(text, Text):  # Attrib and AttDef are sub-classes of Text
        if text.get_align_enum() in (
            TextEntityAlignment.FIT,
            TextEntityAlignment.ALIGNED,
        ):
            rotation = _calc_aligned_rotation(text)
        else:
            rotation = radians(text.dxf.rotation)
        return Matrix44.axis_rotate(text.dxf.extrusion, rotation)
    elif isinstance(text, MText):
        return Matrix44.axis_rotate(Vec3(0, 0, 1), radians(text.get_rotation()))
    else:
        raise TypeError(type(text))


def _get_alignment(text: AnyText) -> Alignment:
    if isinstance(text, Text):  # Attrib and AttDef are sub-classes of Text
        return DXF_TEXT_ALIGNMENT_TO_ALIGNMENT[text.get_align_enum()]
    elif isinstance(text, MText):
        return DXF_MTEXT_ALIGNMENT_TO_ALIGNMENT[text.dxf.attachment_point]
    else:
        raise TypeError(type(text))


def _get_cap_height(text: AnyText) -> float:
    if isinstance(text, (Text, Attrib, AttDef)):
        return text.dxf.height
    elif isinstance(text, MText):
        return text.dxf.char_height
    else:
        raise TypeError(type(text))


def _get_line_spacing(text: AnyText, cap_height: float) -> float:
    if isinstance(text, (Attrib, AttDef, Text)):
        return 0.0
    elif isinstance(text, MText):
        return cap_height * DEFAULT_LINE_SPACING * text.dxf.line_spacing_factor
    else:
        raise TypeError(type(text))


def _split_into_lines(
    entity: AnyText,
    box_width: Optional[float],
    get_text_width: Callable[[str], float],
) -> list[str]:
    if isinstance(entity, AttDef):
        # ATTDEF outside of an Insert renders the tag rather than the value
        text = plain_text(entity.dxf.tag)
    else:
        text = entity.plain_text()  # type: ignore
    if isinstance(entity, (Text, Attrib, AttDef)):
        assert "\n" not in text
        return [text]
    else:
        return text_wrap(text, box_width, get_text_width)


def _get_text_width(text: AnyText) -> Optional[float]:
    if isinstance(text, Text):  # Attrib and AttDef are sub-classes of Text
        return None
    elif isinstance(text, MText):
        width = text.dxf.width
        return None if width == 0.0 else width
    else:
        raise TypeError(type(text))


def _get_extra_transform(text: AnyText, line_width: float) -> Matrix44:
    extra_transform = Matrix44()
    if isinstance(text, Text):  # Attrib and AttDef are sub-classes of Text
        # 'width' is the width *scale factor* so 1.0 by default:
        scale_x = text.dxf.width
        scale_y = 1.0

        # Calculate text stretching for FIT and ALIGNED:
        alignment = text.get_align_enum()
        line_width = abs(line_width)
        if (
            alignment in (TextEntityAlignment.FIT, TextEntityAlignment.ALIGNED)
            and line_width > 1e-9
        ):
            defined_length = (text.dxf.align_point - text.dxf.insert).magnitude
            stretch_factor = defined_length / line_width
            scale_x = stretch_factor
            if alignment == TextEntityAlignment.ALIGNED:
                scale_y = stretch_factor

        if text.dxf.text_generation_flag & DXFConstants.MIRROR_X:
            scale_x *= -1.0
        if text.dxf.text_generation_flag & DXFConstants.MIRROR_Y:
            scale_y *= -1.0

        # Magnitude of extrusion does not have any effect.
        # An extrusion of (0, 0, 0) acts like (0, 0, 1)
        scale_x *= sign(text.dxf.extrusion.z)

        if scale_x != 1.0 or scale_y != 1.0:
            extra_transform = Matrix44.scale(scale_x, scale_y)

    elif isinstance(text, MText):
        # Not sure about the rationale behind this but it does match AutoCAD
        # behavior...
        scale_y = sign(text.dxf.extrusion.z)
        if scale_y != 1.0:
            extra_transform = Matrix44.scale(1.0, scale_y)

    return extra_transform


def _apply_alignment(
    alignment: Alignment,
    line_widths: list[float],
    line_spacing: float,
    box_width: Optional[float],
    font_measurements: FontMeasurements,
) -> tuple[tuple[float, float], list[float], list[float]]:
    if not line_widths:
        return (0, 0), [], []

    halign, valign = alignment
    line_ys = [
        -font_measurements.baseline - (font_measurements.cap_height + i * line_spacing)
        for i in range(len(line_widths))
    ]

    if box_width is None:
        box_width = max(line_widths)

    last_baseline = line_ys[-1]

    if halign == HAlignment.LEFT:
        anchor_x = 0.0
        line_xs = [0.0] * len(line_widths)
    elif halign == HAlignment.CENTER:
        anchor_x = box_width / 2
        line_xs = [anchor_x - w / 2 for w in line_widths]
    elif halign == HAlignment.RIGHT:
        anchor_x = box_width
        line_xs = [anchor_x - w for w in line_widths]
    else:
        raise ValueError(halign)

    if valign == VAlignment.TOP:
        anchor_y = 0.0
    elif valign == VAlignment.LOWER_CASE_CENTER:
        first_line_lower_case_top = line_ys[0] + font_measurements.x_height
        anchor_y = (first_line_lower_case_top + last_baseline) / 2
    elif valign == VAlignment.UPPER_CASE_CENTER:
        first_line_upper_case_top = line_ys[0] + font_measurements.cap_height
        anchor_y = (first_line_upper_case_top + last_baseline) / 2
    elif valign == VAlignment.BASELINE:
        anchor_y = last_baseline
    elif valign == VAlignment.BOTTOM:
        anchor_y = last_baseline - font_measurements.descender_height
    else:
        raise ValueError(valign)

    return (anchor_x, anchor_y), line_xs, line_ys


def _get_wcs_insert(text: AnyText) -> Vec3:
    if isinstance(text, Text):  # Attrib and AttDef are sub-classes of Text
        insert: Vec3 = text.dxf.insert
        align_point: Vec3 = text.dxf.align_point
        alignment: TextEntityAlignment = text.get_align_enum()
        if alignment == TextEntityAlignment.LEFT:
            # LEFT/BASELINE is always located at the insert point.
            pass
        elif alignment in (
            TextEntityAlignment.FIT,
            TextEntityAlignment.ALIGNED,
        ):
            # Interpolate insertion location between insert and align point:
            insert = insert.lerp(align_point, factor=0.5)
        else:
            # Everything else is located at the align point:
            insert = align_point
        return text.ocs().to_wcs(insert)
    else:
        return text.dxf.insert


# Simple but fast MTEXT renderer:
def simplified_text_chunks(
    text: AnyText,
    render_engine: TextRenderer,
    *,
    font_face: fonts.FontFace,
) -> Iterable[tuple[str, Matrix44, float]]:
    """Splits a complex text entity into simple chunks of text which can all be
    rendered the same way:
    render the string (which will not contain any newlines) with the given
    cap_height with (left, baseline) at (0, 0) then transform it with the given
    matrix to move it into place.
    """
    alignment = _get_alignment(text)
    box_width = _get_text_width(text)

    cap_height = _get_cap_height(text)
    lines = _split_into_lines(
        text,
        box_width,
        lambda s: render_engine.get_text_line_width(s, font_face, cap_height),
    )
    line_spacing = _get_line_spacing(text, cap_height)
    line_widths = [
        render_engine.get_text_line_width(line, font_face, cap_height) for line in lines
    ]
    font_measurements = render_engine.get_font_measurements(font_face, cap_height)
    anchor, line_xs, line_ys = _apply_alignment(
        alignment, line_widths, line_spacing, box_width, font_measurements
    )
    rotation = _get_rotation(text)

    # first_line_width is used for TEXT, ATTRIB and ATTDEF stretching
    if line_widths:
        first_line_width = line_widths[0]
    else:  # no text lines -> no output, value is not important
        first_line_width = 1.0

    extra_transform = _get_extra_transform(text, first_line_width)
    insert = _get_wcs_insert(text)

    whole_text_transform = (
        Matrix44.translate(-anchor[0], -anchor[1], 0)
        @ extra_transform
        @ rotation
        @ Matrix44.translate(*insert.xyz)
    )
    for i, (line, line_x, line_y) in enumerate(zip(lines, line_xs, line_ys)):
        transform = Matrix44.translate(line_x, line_y, 0) @ whole_text_transform
        yield line, transform, cap_height
