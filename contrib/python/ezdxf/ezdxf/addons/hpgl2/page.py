#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
# 1 plot unit (plu) = 0.025mm
# 40 plu = 1 mm
# 1016 plu = 1 inch
# 3.39 plu = 1 dot @300 dpi

from __future__ import annotations
from typing import Sequence
import math
from .deps import Vec2, NULLVEC2

INCH_TO_PLU = 1016
MM_TO_PLU = 40


class Page:
    def __init__(self, size_x: int, size_y: int):
        self.size_x = int(size_x)  # in plotter units (plu)
        self.size_y = int(size_y)  # plu

        self.p1 = NULLVEC2  # plu
        self.p2 = Vec2(size_x, size_y)
        self.user_scaling = False
        self.user_scale_x: float = 1.0
        self.user_scale_y: float = 1.0
        self.user_origin = NULLVEC2  # plu

    def set_scaling_points(self, p1: Vec2, p2: Vec2) -> None:
        self.reset_scaling()
        self.p1 = Vec2(p1)
        self.p2 = Vec2(p2)

    def apply_scaling_factors(self, sx: float, sy: float) -> None:
        self.set_ucs(self.user_origin, self.user_scale_x * sx, self.user_scale_y * sy)

    def set_scaling_points_relative_1(self, xp1: float, yp1: float) -> None:
        size = self.p2 - self.p1
        p1 = Vec2(self.size_x * xp1, self.size_y * yp1)
        self.set_scaling_points(p1, p1 + size)

    def set_scaling_points_relative_2(
        self, xp1: float, yp1: float, xp2: float, yp2: float
    ) -> None:
        p1 = Vec2(self.size_x * xp1, self.size_y * yp1)
        p2 = Vec2(self.size_x * xp2, self.size_y * yp2)
        self.set_scaling_points(p1, p2)

    def reset_scaling(self) -> None:
        self.p1 = NULLVEC2
        self.p2 = Vec2(self.size_x, self.size_y)
        self.set_ucs(NULLVEC2)

    def set_isotropic_scaling(
        self,
        x_min: float,
        x_max: float,
        y_min: float,
        y_max: float,
        left=0.5,
        bottom=0.5,
    ) -> None:
        size = self.p2 - self.p1
        delta_x = x_max - x_min
        delta_y = y_max - y_min
        scale_x = 1.0
        if abs(delta_x) > 1e-9:
            scale_x = size.x / delta_x
        scale_y = 1.0
        if abs(delta_y) > 1e-9:
            scale_y = size.y / delta_y

        scale = min(abs(scale_x), abs(scale_y))
        scale_x = math.copysign(scale, scale_x)
        scale_y = math.copysign(scale, scale_y)
        offset_x = (size.x - delta_x * scale_x) * left
        offset_y = (size.y - delta_y * scale_y) * bottom
        origin_x = self.p1.x + offset_x - x_min * scale_x
        origin_y = self.p1.y + offset_y - y_min * scale_y
        self.set_ucs(Vec2(origin_x, origin_y), scale_x, scale_y)

    def set_anisotropic_scaling(
        self, x_min: float, x_max: float, y_min: float, y_max: float
    ) -> None:
        size = self.p2 - self.p1
        delta_x = x_max - x_min
        delta_y = y_max - y_min
        scale_x = 1.0
        if abs(delta_x) > 1e-9:
            scale_x = size.x / delta_x
        scale_y = 1.0
        if abs(delta_y) > 1e-9:
            scale_y = size.y / delta_y
        origin_x = self.p1.x - x_min * scale_x
        origin_y = self.p1.y - y_min * scale_y
        self.set_ucs(Vec2(origin_x, origin_y), scale_x, scale_y)

    def set_ucs(self, origin: Vec2, sx: float = 1.0, sy: float = 1.0):
        self.user_origin = Vec2(origin)
        self.user_scale_x = float(sx)
        self.user_scale_y = float(sy)
        if abs(self.user_scale_x) < 1e-6:
            self.user_scale_x = 1.0
        if abs(self.user_scale_y) < 1e-6:
            self.user_scale_y = 1.0
        if math.isclose(self.user_scale_x, 1.0) and math.isclose(
            self.user_scale_y, 1.0
        ):
            self.user_scaling = False
        else:
            self.user_scaling = True

    def page_point(self, x: float, y: float) -> Vec2:
        """Returns the page location as page point in plotter units."""
        return self.page_vector(x, y) + self.user_origin

    def page_vector(self, x: float, y: float) -> Vec2:
        """Returns the user vector in page vector in plotter units."""
        if self.user_scaling:
            x = self.user_scale_x * x
            y = self.user_scale_y * y
        return Vec2(x, y)

    def page_points(self, points: Sequence[Vec2]) -> list[Vec2]:
        """Returns all user points as page points in plotter units."""
        return [self.page_point(p.x, p.y) for p in points]

    def page_vectors(self, vectors: Sequence[Vec2]) -> list[Vec2]:
        """Returns all user vectors as page vectors in plotter units."""
        return [self.page_vector(p.x, p.y) for p in vectors]

    def scale_length(self, length: float) -> tuple[float, float]:
        """Scale a length in user units to plotter units, scaling can be non-uniform."""
        return length * self.user_scale_x, length * self.user_scale_y
