#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Sequence
from .backend import Backend
from .deps import Vec2, Path
from .properties import Properties


class PolygonBuffer(Backend):
    def __init__(self):
        self.path = Path()
        self.start_new_sub_polygon = False

    def draw_polyline(self, properties: Properties, points: Sequence[Vec2]) -> None:
        if len(points) == 0:
            return
        index = 0
        if self.start_new_sub_polygon:
            self.start_new_sub_polygon = False
            count = len(points)
            while self.path.end.isclose(points[index]):
                index += 1
                if index == count:
                    return
            self.path.move_to(points[index])
        for p in points[index + 1 :]:
            self.path.line_to(p)

    def draw_paths(
        self, properties: Properties, paths: Sequence[Path], filled: bool
    ) -> None:
        if filled:
            raise NotImplementedError()
        for p in paths:
            if len(p) == 0:
                continue
            if self.start_new_sub_polygon:
                self.start_new_sub_polygon = False
                self.path.move_to(p.start)
            self.path.append_path(p)

    def get_paths(self) -> Sequence[Path]:
        return list(self.path.sub_paths())

    def close_path(self):
        if len(self.path):
            self.path.close_sub_path()
            self.start_new_sub_polygon = True

    def reset(self, location: Vec2) -> None:
        self.path = Path(location)
        self.start_new_sub_polygon = False
