# Copyright (c) 2020-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Tuple, Iterable, Sequence
from typing_extensions import TypeAlias
import math
from ezdxf.math import Vec3, UVec

LineSegment: TypeAlias = Tuple[Vec3, Vec3]


class _LineTypeRenderer:
    """Renders a line segment as multiple short segments according to the given
    line pattern.

    In contrast to the DXF line pattern is this pattern simplified and follow
    the rule line-gap-line-gap-... a line of length 0 is a point.
    The pattern should end with a gap (even count) and the dash length is in
    drawing units.

    Args:
          dashes: sequence of floats, line-gap-line-gap-...

    """
    # Get the simplified line pattern by LineType.simplified_line_pattern()
    def __init__(self, dashes: Sequence[float]):
        self._dashes = dashes
        self._dash_count: int = len(dashes)
        self.is_solid: bool = True
        self._current_dash: int = 0
        self._current_dash_length: float = 0.0
        if self._dash_count > 1:
            self.is_solid = False
            self._current_dash_length = self._dashes[0]
            self._is_dash = True

    def line_segment(self, start: UVec, end: UVec) -> Iterable[LineSegment]:
        """Yields the line from `start` to `end` according to stored line
        pattern as short segments. Yields only the lines and points not the
        gaps.

        """
        _start = Vec3(start)
        _end = Vec3(end)
        if self.is_solid or _start.isclose(_end):
            yield _start, _end
            return

        segment_vec = _end - _start
        segment_length = segment_vec.magnitude
        segment_dir = segment_vec / segment_length  # normalize

        for is_dash, dash_length in self._render_dashes(segment_length):
            _end = _start + segment_dir * dash_length
            if is_dash:
                yield _start, _end
            _start = _end

    def _render_dashes(self, length: float) -> Iterable[tuple[bool, float]]:
        if length <= self._current_dash_length:
            self._current_dash_length -= length
            yield self._is_dash, length
            if math.isclose(self._current_dash_length, 0.0):
                self._cycle_dashes()
        else:
            # Avoid deep recursions!
            while length > self._current_dash_length:
                length -= self._current_dash_length
                yield from self._render_dashes(self._current_dash_length)
            if length > 0.0:
                yield from self._render_dashes(length)

    def _cycle_dashes(self):
        self._current_dash = (self._current_dash + 1) % self._dash_count
        self._current_dash_length = self._dashes[self._current_dash]
        self._is_dash = not self._is_dash
