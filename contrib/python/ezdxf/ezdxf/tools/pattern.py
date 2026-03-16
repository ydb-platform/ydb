# Copyright (c) 2015-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, Tuple, Optional
from typing_extensions import TypeAlias
from ezdxf.math import Vec2
from ._iso_pattern import ISO_PATTERN

# Predefined hatch pattern prior to ezdxf v0.11 were scaled for imperial units,
# and were too small for ISO units by a factor of 1/25.4, to replicate this
# pattern scaling use load(measurement=0).

__all__ = [
    "load",
    "scale_pattern",
    "scale_all",
    "parse",
    "ISO_PATTERN",
    "IMPERIAL_PATTERN",
    "HatchPatternLineType",
    "HatchPatternType",
    "PatternAnalyser",
]
IMPERIAL_SCALE_FACTOR = 1.0 / 25.4
HatchPatternLineType: TypeAlias = Tuple[
    float, Sequence[float], Sequence[float], Sequence[float]
]
HatchPatternType: TypeAlias = Sequence[HatchPatternLineType]


def load(measurement: int = 1, factor: Optional[float] = None):
    """Load hatch pattern definition, default scaling is like the iso.pat of
    BricsCAD, set `measurement` to 0 to use the imperial (US) scaled pattern,
    which has a scaling factor of 1/25.4 = ~0.03937.

    Args:
        measurement: like the $MEASUREMENT header variable, 0 to user imperial
            scaled pattern, 1 to use ISO scaled pattern.
        factor: hatch pattern scaling factor, overrides `measurement`

    Returns: hatch pattern dict of scaled pattern

    """
    if factor is None:
        factor = 1.0 if measurement == 1 else IMPERIAL_SCALE_FACTOR
    pattern = ISO_PATTERN
    if factor != 1.0:
        pattern = scale_all(pattern, factor=factor)
    return pattern


def scale_pattern(
    pattern: HatchPatternType, factor: float = 1, angle: float = 0
) -> HatchPatternType:
    ndigits = 10

    def _scale(iterable) -> Sequence[float]:
        return [round(i * factor, ndigits) for i in iterable]

    def _scale_line(line) -> HatchPatternLineType:
        angle0, base_point, offset, dash_length_items = line
        if angle:
            base_point = Vec2(base_point).rotate_deg(angle)
            offset = Vec2(offset).rotate_deg(angle)
            angle0 = (angle0 + angle) % 360.0

        # noinspection PyTypeChecker
        return [  # type: ignore
            round(angle0, ndigits),
            tuple(_scale(base_point)),
            tuple(_scale(offset)),
            _scale(dash_length_items),
        ]

    return [_scale_line(line) for line in pattern]


def scale_all(pattern: dict, factor: float = 1, angle: float = 0):
    return {name: scale_pattern(p, factor, angle) for name, p in pattern.items()}


def parse(pattern: str) -> dict:
    try:
        comp = PatternFileCompiler(pattern)
        return comp.compile_pattern()
    except Exception:
        raise ValueError("Incompatible pattern definition.")


def _tokenize_pattern_line(line: str) -> list:
    return line.split(",", maxsplit=1 if line.startswith("*") else -1)


class PatternFileCompiler:
    def __init__(self, content: str):
        self._lines = [
            _tokenize_pattern_line(line)
            for line in (line.strip() for line in content.split("\n"))
            if line and line[0] != ";"
        ]

    def _parse_pattern(self):
        pattern = []
        for line in self._lines:
            if line[0].startswith("*"):
                if pattern:
                    yield pattern
                pattern = [[line[0][1:], line[1]]]  # name, description
            else:
                pattern.append([float(e) for e in line])  # list[floats]

        if pattern:
            yield pattern

    def compile_pattern(self, ndigits: int = 10) -> dict:
        pattern = dict()
        for p in self._parse_pattern():
            pat = []
            for line in p[1:]:
                # offset before rounding:
                offset = Vec2(line[3], line[4])

                # round all values:
                line = [round(e, ndigits) for e in line]
                pat_line = []

                angle = line[0]
                pat_line.append(angle)

                # base point:
                pat_line.append((line[1], line[2]))

                # rotate offset:
                offset = offset.rotate_deg(angle)
                pat_line.append((round(offset.x, ndigits), round(offset.y, ndigits)))

                # line dash pattern
                pat_line.append(line[5:])
                pat.append(pat_line)
            pattern[p[0][0]] = pat
        return pattern


IMPERIAL_PATTERN = load(measurement=0)


def is_solid(pattern: Sequence[float]) -> bool:
    return not bool(len(pattern))


def round_angle_15_deg(angle: float) -> int:
    return round((angle % 180) / 15) * 15


class PatternAnalyser:
    def __init__(self, pattern: HatchPatternType):
        # List of 2-tuples: (angle, is solid line pattern)
        # angle is rounded to a multiple of 15° in the range [0, 180)
        self._lines: list[tuple[int, bool]] = [
            (round_angle_15_deg(angle), is_solid(line_pattern))
            for angle, _, _, line_pattern in pattern
        ]

    def has_angle(self, angle: int) -> bool:
        return any(angle_ == angle for angle_, _ in self._lines)

    def all_angles(self, angle: int) -> bool:
        return all(angle_ == angle for angle_, _ in self._lines)

    def has_line(self, angle: int, solid: bool) -> bool:
        return any(
            angle_ == angle and solid_ == solid for angle_, solid_ in self._lines
        )

    def all_lines(self, angle: int, solid: bool) -> bool:
        return all(
            angle_ == angle and solid_ == solid for angle_, solid_ in self._lines
        )

    def has_solid_line(self) -> bool:
        return any(solid for _, solid in self._lines)

    def has_dashed_line(self) -> bool:
        return any(not solid for _, solid in self._lines)

    def all_solid_lines(self) -> bool:
        return all(solid for _, solid in self._lines)

    def all_dashed_lines(self) -> bool:
        return all(not solid for _, solid in self._lines)
