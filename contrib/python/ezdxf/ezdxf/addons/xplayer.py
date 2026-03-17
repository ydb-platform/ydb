#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
"""xplayer = cross backend player."""
from __future__ import annotations
from typing import Callable
from ezdxf.math import Vec2
from ezdxf.colors import RGB

from ezdxf.addons.drawing.backend import BackendInterface, BkPath2d
from ezdxf.addons.drawing.properties import BackendProperties
from ezdxf.addons.hpgl2 import api as hpgl2
from ezdxf.addons.hpgl2.backend import (
    Properties as HPGL2Properties,
    RecordType as HPGL2RecordType,
)


def hpgl2_to_drawing(
    player: hpgl2.Player,
    backend: BackendInterface,
    bg_color: str = "#ffffff",
    override: Callable[[BackendProperties], BackendProperties] | None = None,
) -> None:
    """Replays the recordings of the HPGL2 Recorder on a backend of the drawing add-on."""
    if bg_color:
        backend.set_background(bg_color)
    for record_type, properties, record_data in player.recordings():
        backend_properties = _make_drawing_backend_properties(properties)
        if override:
            backend_properties = override(backend_properties)
        if record_type == HPGL2RecordType.POLYLINE:
            points: list[Vec2] = record_data.vertices()
            size = len(points)
            if size == 1:
                backend.draw_point(points[0], backend_properties)
            elif size == 2:
                backend.draw_line(points[0], points[1], backend_properties)
            else:
                backend.draw_path(BkPath2d.from_vertices(points), backend_properties)
        elif record_type == HPGL2RecordType.FILLED_PATHS:
            backend.draw_filled_paths(record_data, backend_properties)
        elif record_type == HPGL2RecordType.OUTLINE_PATHS:
            for p in record_data:
                backend.draw_path(p, backend_properties)
    backend.finalize()


def _make_drawing_backend_properties(properties: HPGL2Properties) -> BackendProperties:
    """Make BackendProperties() for the drawing add-on."""
    return BackendProperties(
        color=properties.pen_color.to_hex(),
        lineweight=properties.pen_width,
        layer="0",
        pen=properties.pen_index,
        handle="",
    )


def map_color(color: str) -> Callable[[BackendProperties], BackendProperties]:
    def _map_color(properties: BackendProperties) -> BackendProperties:
        return BackendProperties(
            color=color,
            lineweight=properties.lineweight,
            layer=properties.layer,
            pen=properties.pen,
            handle=properties.handle,
        )

    return _map_color


def map_monochrome(dark_mode=True) -> Callable[[BackendProperties], BackendProperties]:
    def to_gray(color: str) -> str:
        gray = round(RGB.from_hex(color).luminance * 255)
        if dark_mode:
            gray = 255 - gray
        return RGB(gray, gray, gray).to_hex()

    def _map_color(properties: BackendProperties) -> BackendProperties:
        color = properties.color
        alpha = color[7:9]
        return BackendProperties(
            color=to_gray(color[:7]) + alpha,
            lineweight=properties.lineweight,
            layer=properties.layer,
            pen=properties.pen,
            handle=properties.handle,
        )

    return _map_color
