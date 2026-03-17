#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import NamedTuple
# A Visual Guide to the Anatomy of Typography: https://visme.co/blog/type-anatomy/
# Anatomy of a Character: https://www.fonts.com/content/learning/fontology/level-1/type-anatomy/anatomy


class FontMeasurements(NamedTuple):
    baseline: float
    cap_height: float
    x_height: float
    descender_height: float

    def scale(self, factor: float = 1.0) -> FontMeasurements:
        return FontMeasurements(
            self.baseline * factor,
            self.cap_height * factor,
            self.x_height * factor,
            self.descender_height * factor,
        )

    def shift(self, distance: float = 0.0) -> FontMeasurements:
        return FontMeasurements(
            self.baseline + distance,
            self.cap_height,
            self.x_height,
            self.descender_height,
        )

    def scale_from_baseline(self, desired_cap_height: float) -> FontMeasurements:
        factor = desired_cap_height / self.cap_height
        return FontMeasurements(
            self.baseline,
            desired_cap_height,
            self.x_height * factor,
            self.descender_height * factor,
        )

    @property
    def cap_top(self) -> float:
        return self.baseline + self.cap_height

    @property
    def x_top(self) -> float:
        return self.baseline + self.x_height

    @property
    def bottom(self) -> float:
        return self.baseline - self.descender_height

    @property
    def total_height(self) -> float:
        return self.cap_height + self.descender_height
