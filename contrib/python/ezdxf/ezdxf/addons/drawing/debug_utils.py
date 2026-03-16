# Copyright (c) 2020-2021, Matthew Broadway
# License: MIT License
from __future__ import annotations

from ezdxf.addons.drawing.backend import BackendInterface
from ezdxf.addons.drawing.type_hints import Color
from ezdxf.math import Vec3


def draw_rect(points: list[Vec3], color: Color, out: BackendInterface):
    from ezdxf.addons.drawing.properties import BackendProperties

    props = BackendProperties(color=color)
    for a, b in zip(points, points[1:]):
        out.draw_line(a, b, props)
