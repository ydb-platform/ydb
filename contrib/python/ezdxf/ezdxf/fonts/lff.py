#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
#
# Basic tools for the LibreCAD Font Format:
# https://github.com/Rallaz/LibreCAD/wiki/lff-definition
from __future__ import annotations
from typing import Sequence, Iterator, Iterable, Optional, no_type_check
from typing_extensions import TypeAlias
from ezdxf.math import Vec2, BoundingBox2d, Matrix44
from ezdxf import path
from .font_measurements import FontMeasurements
from .glyphs import GlyphPath, Glyphs

__all__ = ["loads", "LCFont", "Glyph", "GlyphCache"]


def loads(s: str) -> LCFont:
    lines = s.split("\n")
    name, letter_spacing, word_spacing = parse_properties(lines)
    lcf = LCFont(name, letter_spacing, word_spacing)
    for glyph, parent_code in parse_glyphs(lines):
        lcf.add(glyph, parent_code)
    return lcf


class LCFont:
    """Low level representation of LibreCAD fonts."""
    def __init__(
        self, name: str = "", letter_spacing: float = 0.0, word_spacing: float = 0.0
    ) -> None:
        self.name: str = name
        self.letter_spacing: float = letter_spacing
        self.word_spacing: float = word_spacing
        self._glyphs: dict[int, Glyph] = dict()

    def __len__(self) -> int:
        return len(self._glyphs)

    def __getitem__(self, item: int) -> Glyph:
        return self._glyphs[item]

    def add(self, glyph: Glyph, parent_code: int = 0) -> None:
        if parent_code:
            try:
                parent_glyph = self._glyphs[parent_code]
            except KeyError:
                return
            glyph = parent_glyph.extend(glyph)
        self._glyphs[glyph.code] = glyph

    def get(self, code: int) -> Optional[Glyph]:
        return self._glyphs.get(code, None)


Polyline: TypeAlias = Sequence[Sequence[float]]


class Glyph:
    """Low level representation of a LibreCAD glyph."""
    __slots__ = ("code", "polylines")

    def __init__(self, code: int, polylines: Sequence[Polyline]):
        self.code: int = code
        self.polylines: Sequence[Polyline] = tuple(polylines)

    def extend(self, glyph: Glyph) -> Glyph:
        polylines = list(self.polylines)
        polylines.extend(glyph.polylines)
        return Glyph(glyph.code, polylines)

    def to_path(self) -> GlyphPath:
        from ezdxf.math import OCS

        final_path = path.Path()
        ocs = OCS()
        for polyline in self.polylines:
            p = path.Path()  # empty path is required
            path.add_2d_polyline(
                p, convert_bulge_values(polyline), close=False, elevation=0, ocs=ocs
            )
            final_path.extend_multi_path(p)
        return GlyphPath(final_path)


def convert_bulge_values(polyline: Polyline) -> Iterator[Sequence[float]]:
    # In DXF the bulge value is always stored at the start vertex of the arc.
    last_index = len(polyline) - 1
    for index, vertex in enumerate(polyline):
        bulge = 0.0
        if index < last_index:
            next_vertex = polyline[index + 1]
            try:
                bulge = next_vertex[2]
            except IndexError:
                pass
        yield vertex[0], vertex[1], bulge


def parse_properties(lines: list[str]) -> tuple[str, float, float]:
    font_name = ""
    letter_spacing = 0.0
    word_spacing = 0.0
    for line in lines:
        line = line.strip()
        if not line.startswith("#"):
            continue
        try:
            name, value = line.split(":")
        except ValueError:
            continue

        name = name[1:].strip()
        if name == "Name":
            font_name = value.strip()
        elif name == "LetterSpacing":
            try:
                letter_spacing = float(value)
            except ValueError:
                continue
        elif name == "WordSpacing":
            try:
                word_spacing = float(value)
            except ValueError:
                continue
    return font_name, letter_spacing, word_spacing


def scan_glyphs(lines: Iterable[str]) -> Iterator[list[str]]:
    glyph: list[str] = []
    for line in lines:
        if line.startswith("["):
            if glyph:
                yield glyph
            glyph.clear()
        if line:
            glyph.append(line)
    if glyph:
        yield glyph


def strip_clutter(lines: list[str]) -> Iterator[str]:
    for line in lines:
        line = line.strip()
        if not line.startswith("#"):
            yield line


def scan_int_ex(s: str) -> int:
    from string import hexdigits

    if len(s) == 0:
        return 0
    try:
        end = s.index("]")
    except ValueError:
        end = len(s)
    s = s[1:end].lower()
    s = "".join(c for c in s if c in hexdigits)
    try:
        return int(s, 16)
    except ValueError:
        return 0


def parse_glyphs(lines: list[str]) -> Iterator[tuple[Glyph, int]]:
    code: int
    polylines: list[Polyline] = []
    for glyph in scan_glyphs(strip_clutter(lines)):
        parent_code: int = 0
        polylines.clear()
        line = glyph.pop(0)
        if line[0] != "[":
            continue
        try:
            code = int(line[1 : line.index("]")], 16)
        except ValueError:
            code = scan_int_ex(line)
        if code == 0:
            continue
        line = glyph[0]
        if line.startswith("C"):
            glyph.pop(0)
            try:
                parent_code = int(line[1:], 16)
            except ValueError:
                continue
        polylines = list(parse_polylines(glyph))
        yield Glyph(code, polylines), parent_code


def parse_polylines(lines: Iterable[str]) -> Iterator[Polyline]:
    polyline: list[Sequence[float]] = []
    for line in lines:
        polyline.clear()
        for vertex in line.split(";"):
            values = to_floats(vertex.split(","))
            if len(values) > 1:
                polyline.append(values[:3])
        yield tuple(polyline)


def to_floats(values: Iterable[str]) -> Sequence[float]:
    def strip(value: str) -> float:
        if value.startswith("A"):
            value = value[1:]
        try:
            return float(value)
        except ValueError:
            return 0.0

    return tuple(strip(value) for value in values)


class GlyphCache(Glyphs):
    """Text render engine for LibreCAD fonts with integrated glyph caching."""

    def __init__(self, font: LCFont) -> None:
        self.font: LCFont = font
        self._glyph_cache: dict[int, GlyphPath] = dict()
        self._advance_width_cache: dict[int, float] = dict()
        self.space_width: float = self.font.word_spacing
        self.empty_box: GlyphPath = self.get_empty_box()
        self.font_measurements: FontMeasurements = self._get_font_measurements()

    def get_scaling_factor(self, cap_height: float) -> float:
        try:
            return cap_height / self.font_measurements.cap_height
        except ZeroDivisionError:
            return 1.0

    def get_empty_box(self) -> GlyphPath:
        glyph_A = self.get_shape(65)
        box = BoundingBox2d(glyph_A.control_vertices())
        height = box.size.y
        width = box.size.x
        start = glyph_A.start
        p = path.Path(start)
        p.line_to(start + Vec2(width, 0))
        p.line_to(start + Vec2(width, height))
        p.line_to(start + Vec2(0, height))
        p.close()
        p.move_to(glyph_A.end)
        return GlyphPath(p)

    def _render_shape(self, shape_number) -> GlyphPath:
        try:
            glyph = self.font[shape_number]
        except KeyError:
            if shape_number > 32:
                return self.empty_box
            raise ValueError("space and non-printable characters are not glyphs")
        return glyph.to_path()

    def get_shape(self, shape_number: int) -> GlyphPath:
        if shape_number <= 32:
            raise ValueError("space and non-printable characters are not glyphs")
        try:
            return self._glyph_cache[shape_number].clone()
        except KeyError:
            pass
        glyph = self._render_shape(shape_number)
        self._glyph_cache[shape_number] = glyph
        advance_width = 0.0
        if len(glyph):
            box = glyph.bbox()
            assert box.extmax is not None
            advance_width = box.extmax.x + self.font.letter_spacing
        self._advance_width_cache[shape_number] = advance_width
        return glyph.clone()

    def get_advance_width(self, shape_number: int) -> float:
        if shape_number < 32:
            return 0.0
        if shape_number == 32:
            return self.space_width
        try:
            return self._advance_width_cache[shape_number]
        except KeyError:
            pass
        _ = self.get_shape(shape_number)
        return self._advance_width_cache[shape_number]

    @no_type_check
    def _get_font_measurements(self) -> FontMeasurements:
        # ignore last move_to command, which places the pen at the start of the
        # following glyph
        bbox = BoundingBox2d(self.get_shape(ord("x")).control_vertices())
        baseline = bbox.extmin.y
        x_height = bbox.extmax.y - baseline

        bbox = BoundingBox2d(self.get_shape(ord("A")).control_vertices())
        cap_height = bbox.extmax.y - baseline
        bbox = BoundingBox2d(self.get_shape(ord("p")).control_vertices())
        descender_height = baseline - bbox.extmin.y
        return FontMeasurements(
            baseline=baseline,
            cap_height=cap_height,
            x_height=x_height,
            descender_height=descender_height,
        )

    def get_text_length(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> float:
        scaling_factor = self.get_scaling_factor(cap_height) * width_factor
        return sum(self.get_advance_width(ord(c)) for c in text) * scaling_factor

    def get_text_glyph_paths(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> list[GlyphPath]:
        glyph_paths: list[GlyphPath] = []
        sy = self.get_scaling_factor(cap_height)
        sx = sy * width_factor
        m = Matrix44.scale(sx, sy, 1)
        current_location = 0.0
        for c in text:
            shape_number = ord(c)
            if shape_number > 32:
                glyph = self.get_shape(shape_number)
                m[3, 0] = current_location
                glyph.transform_inplace(m)
                glyph_paths.append(glyph)
            current_location += self.get_advance_width(shape_number) * sx
        return glyph_paths
