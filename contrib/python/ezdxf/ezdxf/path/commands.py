# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
import enum
from typing import NamedTuple, Union

from ezdxf.math import Vec2, Vec3

__all__ = [
    "Command",
    "AnyCurve",
    "PathElement",
    "LineTo",
    "Curve3To",
    "Curve4To",
    "MoveTo",
]


@enum.unique
class Command(enum.IntEnum):
    LINE_TO = 1  # (LINE_TO, end vertex)
    CURVE3_TO = 2  # (CURVE3_TO, end vertex, ctrl) quadratic bezier
    CURVE4_TO = 3  # (CURVE4_TO, end vertex, ctrl1, ctrl2) cubic bezier
    MOVE_TO = 4  # (MOVE_TO, end vertex), creates a gap and starts a sub-path


class LineTo(NamedTuple):
    end: Vec2|Vec3

    @property
    def type(self):
        return Command.LINE_TO


class MoveTo(NamedTuple):
    end: Vec2|Vec3

    @property
    def type(self):
        return Command.MOVE_TO


class Curve3To(NamedTuple):
    end: Vec2|Vec3
    ctrl: Vec2|Vec3

    @property
    def type(self):
        return Command.CURVE3_TO


class Curve4To(NamedTuple):
    end: Vec2|Vec3
    ctrl1: Vec2|Vec3
    ctrl2: Vec2|Vec3

    @property
    def type(self):
        return Command.CURVE4_TO


AnyCurve = (Command.CURVE3_TO, Command.CURVE4_TO)
PathElement = Union[LineTo, Curve3To, Curve4To, MoveTo]
