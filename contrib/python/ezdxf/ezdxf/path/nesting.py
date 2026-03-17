# Copyright (c) 2020-2023, Manfred Moitzi
# License: MIT License
"""
This module provides "nested Polygon" detection for multiple paths.

Terminology
-----------

exterior
    creates a filled area, has counter-clockwise (ccw) winding
    exterior := Path

hole
    creates an unfilled area, has clockwise winding (cw),
    hole := Polygon

polygon
    list of nested paths:
    polygon without a hole: [path]
    polygon with 1 hole: [path, [path]]
    polygon with 2 separated holes: [path, [path], [path]]
    polygon with 2 nested holes: [path, [path, [path]]]

    polygon := [exterior, *hole]

The result is a list of polygons:

1 polygon returns: [[ext-path]]
2 separated polygons returns: [[ext-path], [ext-path, [hole-path]]]

A hole is just another polygon, some render backends may require a distinct winding
order for nested paths like: ccw-cw-ccw-cw...

[Exterior-ccw,
    [Hole-Exterior-cw,
        [Sub-Hole-ccw],
        [Sub-Hole-ccw],
    ],
    [Hole-Exterior-cw],
    [Hole-Exterior-cw],
]

The implementation has to do some expensive tests, like check if a path is
inside of another path or if paths do overlap. A goal is to reduce this costs
by using proxy objects:

Bounding Box Proxy
------------------

This implementation uses the bounding box of the path as proxy object, this is very fast
but not accurate, but can handle most of the real world scenarios, in the assumption
that most HATCHES are created from non-overlapping boundary paths.
Overlap detection and resolving is not possible.

The input paths have to implement the SupportsBoundingBox protocol, which requires
only a method bbox() that returns a BoundingBox2d instance for the path.

Sort by Area
------------

It is not possible for a path to contain another path with a larger area.

"""
from __future__ import annotations
from typing import (
    Tuple,
    Optional,
    List,
    Iterable,
    Sequence,
    Iterator,
    TypeVar,
)
from typing_extensions import TypeAlias
from collections import namedtuple
from ezdxf.math import AbstractBoundingBox
from ezdxf.protocols import SupportsBoundingBox


__all__ = [
    "make_polygon_structure",
    "winding_deconstruction",
    "group_paths",
    "flatten_polygons",
]


T = TypeVar("T", bound=SupportsBoundingBox)

Polygon: TypeAlias = Tuple[T, Optional[List["Polygon"]]]
BoxStruct = namedtuple("BoxStruct", "bbox, path")


def make_polygon_structure(paths: Iterable[T]) -> list[Polygon]:
    """Returns a recursive polygon structure from iterable `paths`, uses 2D
    bounding boxes as fast detection objects.

    """

    # Implements fast bounding box construction and fast inside check.
    def area(item: BoxStruct) -> float:
        size = item.bbox.size
        return size.x * size.y

    def separate(
        exterior: AbstractBoundingBox, candidates: list[BoxStruct]
    ) -> tuple[list[BoxStruct], list[BoxStruct]]:
        holes: list[BoxStruct] = []
        outside: list[BoxStruct] = []
        for candidate in candidates:
            # Fast inside check:
            (holes if exterior.inside(candidate.bbox.center) else outside).append(
                candidate
            )
        return holes, outside

    def polygon_structure(outside: list[BoxStruct]) -> list[list]:
        polygons = []
        while outside:
            exterior = outside.pop()  # path with the largest area
            # Get holes inside of exterior and returns the remaining paths
            # outside of exterior:
            holes, outside = separate(exterior.bbox, outside)
            if holes:
                # build nested hole structure:
                # the largest hole could contain the smaller holes,
                # and so on ...
                holes = polygon_structure(holes)  # type: ignore
            polygons.append([exterior, *holes])
        return polygons

    def as_nested_paths(polygons) -> list:
        return [
            polygon.path if isinstance(polygon, BoxStruct) else as_nested_paths(polygon)
            for polygon in polygons
        ]

    boxed_paths = []
    for path in paths:
        bbox = path.bbox()
        if bbox.has_data:
            boxed_paths.append(BoxStruct(bbox, path))
    boxed_paths.sort(key=area)
    return as_nested_paths(polygon_structure(boxed_paths))


def winding_deconstruction(
    polygons: list[Polygon],
) -> tuple[list[T], list[T]]:
    """Flatten the nested polygon structure in a tuple of two lists,
    the first list contains the paths which should be counter-clockwise oriented
    and the second list contains the paths which should be clockwise oriented.

    The paths are not converted to this orientation.

    """

    def deconstruct(polygons_, level):
        for polygon in polygons_:
            if isinstance(polygon, Sequence):
                deconstruct(polygon, level + 1)
            else:
                # level 0 is the list of polygons
                # level 1 = ccw, 2 = cw, 3 = ccw, 4 = cw, ...
                (ccw_paths if (level % 2) else cw_paths).append(polygon)

    cw_paths: list[T] = []
    ccw_paths: list[T] = []
    deconstruct(polygons, 0)
    return ccw_paths, cw_paths


def flatten_polygons(polygons: Polygon) -> Iterator[T]:
    """Yield a flat representation of the given nested polygons."""
    for polygon in polygons:
        if isinstance(polygon, Sequence):
            yield from flatten_polygons(polygon)  # type: ignore
        else:
            yield polygon  # type: ignore  # T


def group_paths(paths: Iterable[T]) -> list[list[T]]:
    """Group separated paths and their inner holes as flat lists."""
    polygons = make_polygon_structure(paths)
    return [list(flatten_polygons(polygon)) for polygon in polygons]
