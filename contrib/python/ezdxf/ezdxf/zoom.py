#  Copyright (c) 2021-2022, Manfred Moitzi
#  License: MIT License
from typing import Iterable, cast

from ezdxf.math import UVec, Vec2, BoundingBox2d
from ezdxf.layouts import Layout, Paperspace
from ezdxf.entities import DXFEntity
from ezdxf import bbox

__all__ = ["center", "objects", "extents", "window"]


def center(layout: Layout, point: UVec, size: UVec):
    """Resets the active viewport center of `layout` to the given `point`,
    argument `size` defines the width and height of the viewport.
    Replaces the current viewport configuration by a single window
    configuration.

    """
    doc = layout.doc
    if doc:
        if layout.is_modelspace:
            height = guess_height(Vec2(size))
            doc.set_modelspace_vport(height, Vec2(point))
        elif layout.is_any_paperspace:
            psp = cast(Paperspace, layout)
            psp.reset_main_viewport(Vec2(point), Vec2(size))
        else:
            raise TypeError("unsupported layout type")


def guess_height(size):
    width = size.x
    height = size.y
    # expected aspect ratio: 16:10
    return max(width / 2.0, height)


def zoom_to_entities(layout: Layout, entities: Iterable[DXFEntity], factor):
    if isinstance(layout, Paperspace):  # filter main viewport
        main_viewport = layout.main_viewport()
        if main_viewport is not None:
            entities = (e for e in entities if e is not main_viewport)
    extents = bbox.extents(entities, fast=True)
    if extents.has_data:
        center(layout, extents.center, extents.size * factor)


def objects(layout: Layout, entities: Iterable[DXFEntity], factor: float = 1):
    """Resets the active viewport limits of `layout` to the extents of the
    given `entities`. Only entities in the given `layout` are taken into
    account. The argument `factor` scales the viewport limits.
    Replaces the current viewport configuration by a single window
    configuration.

    """
    owner = layout.layout_key
    content = (e for e in entities if e.dxf.owner == owner)
    zoom_to_entities(layout, content, factor)


def extents(layout: Layout, factor: float = 1):
    """Resets the active viewport limits of `layout` to the extents of all
    entities in this `layout`. The argument `factor` scales the viewport limits.
    Replaces the current viewport configuration by a single window
    configuration.

    """
    zoom_to_entities(layout, layout, factor)


def window(layout: Layout, p1: UVec, p2: UVec):
    """Resets the active viewport limits of `layout` to the lower left corner
    `p1` and the upper right corner `p2`.
    Replaces the current viewport configuration by a single window
    configuration.

    """
    extents = BoundingBox2d([p1, p2])
    center(layout, extents.center, extents.size)
