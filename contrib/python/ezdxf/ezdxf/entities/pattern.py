# Copyright (c) 2019-2021 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, TYPE_CHECKING, Optional

from ezdxf.lldxf.tags import Tags, group_tags
from ezdxf.math import Vec2, UVec
from ezdxf.tools import pattern

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["Pattern", "PatternLine"]


class Pattern:
    def __init__(self, lines: Optional[Iterable[PatternLine]] = None):
        self.lines: list[PatternLine] = list(lines) if lines else []

    @classmethod
    def load_tags(cls, tags: Tags) -> Pattern:
        grouped_line_tags = group_tags(tags, splitcode=53)
        return cls(
            PatternLine.load_tags(line_tags) for line_tags in grouped_line_tags
        )

    def clear(self) -> None:
        """Delete all pattern definition lines."""
        self.lines = []

    def add_line(
        self,
        angle: float = 0,
        base_point: UVec = (0, 0),
        offset: UVec = (0, 0),
        dash_length_items: Optional[Iterable[float]] = None,
    ) -> None:
        """Create a new pattern definition line and add the line to the
        :attr:`Pattern.lines` attribute.

        """
        assert (
            dash_length_items is not None
        ), "argument 'dash_length_items' is None"
        self.lines.append(
            PatternLine(angle, base_point, offset, dash_length_items)
        )

    def export_dxf(self, tagwriter: AbstractTagWriter, force=False) -> None:
        if len(self.lines) or force:
            tagwriter.write_tag2(78, len(self.lines))
            for line in self.lines:
                line.export_dxf(tagwriter)

    def __str__(self) -> str:
        return "[" + ",".join(str(line) for line in self.lines) + "]"

    def as_list(self) -> list:
        return [line.as_list() for line in self.lines]

    def scale(self, factor: float = 1, angle: float = 0) -> None:
        """Scale and rotate pattern.

        Be careful, this changes the base pattern definition, maybe better use
        :meth:`Hatch.set_pattern_scale` or :meth:`Hatch.set_pattern_angle`.

        Args:
            factor: scaling factor
            angle: rotation angle in degrees

        """
        scaled_pattern = pattern.scale_pattern(
            self.as_list(), factor=factor, angle=angle
        )
        self.clear()
        for line in scaled_pattern:
            self.add_line(*line)


class PatternLine:
    def __init__(
        self,
        angle: float = 0,
        base_point: UVec = (0, 0),
        offset: UVec = (0, 0),
        dash_length_items: Optional[Iterable[float]] = None,
    ):
        self.angle: float = float(angle)  # in degrees
        self.base_point: Vec2 = Vec2(base_point)
        self.offset: Vec2 = Vec2(offset)
        self.dash_length_items: list[float] = (
            [] if dash_length_items is None else list(dash_length_items)
        )
        # dash_length_items = [item0, item1, ...]
        # item > 0 is line, < 0 is gap, 0.0 = dot;

    @staticmethod
    def load_tags(tags: Tags) -> PatternLine:
        p = {53: 0, 43: 0, 44: 0, 45: 0, 46: 0}
        dash_length_items = []
        for tag in tags:
            code, value = tag
            if code == 49:
                dash_length_items.append(value)
            else:
                p[code] = value
        return PatternLine(
            p[53], (p[43], p[44]), (p[45], p[46]), dash_length_items
        )

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        write_tag = tagwriter.write_tag2
        write_tag(53, self.angle)
        write_tag(43, self.base_point.x)
        write_tag(44, self.base_point.y)
        write_tag(45, self.offset.x)
        write_tag(46, self.offset.y)
        write_tag(79, len(self.dash_length_items))
        for item in self.dash_length_items:
            write_tag(49, item)

    def __str__(self):
        return (
            f"[{self.angle}, {self.base_point}, {self.offset}, "
            f"{self.dash_length_items}]"
        )

    def as_list(self) -> list:
        return [
            self.angle,
            self.base_point,
            self.offset,
            self.dash_length_items,
        ]
