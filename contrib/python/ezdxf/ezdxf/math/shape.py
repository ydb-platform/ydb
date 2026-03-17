# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Optional, Iterator, TYPE_CHECKING, overload
import math
from ezdxf.math import Vec2, UVec, Matrix44


__all__ = ["Shape2d"]

if TYPE_CHECKING:
    from ezdxf.math import BoundingBox2d


class Shape2d:
    """Construction tools for 2D shapes.

    A 2D geometry object as list of :class:`Vec2` objects, vertices can be
    moved, rotated and scaled.

    Args:
        vertices: iterable of :class:`Vec2` compatible objects.

    """

    def __init__(self, vertices: Optional[Iterable[UVec]] = None):
        from ezdxf.npshapes import NumpyPoints2d

        _vec2: Iterator[Vec2] | None = None
        if vertices is not None:
            _vec2 = Vec2.generate(vertices)
        self.np_vertices = NumpyPoints2d(_vec2)

    @property
    def vertices(self) -> list[Vec2]:
        return self.np_vertices.vertices()

    @property
    def bounding_box(self) -> BoundingBox2d:
        """Returns the bounding box of the shape."""
        return self.np_vertices.bbox()

    def copy(self) -> Shape2d:
        clone = self.__class__()
        clone.np_vertices = self.np_vertices.clone()
        return clone

    __copy__ = copy

    def translate(self, vector: UVec) -> None:
        """Translate shape about `vector`."""
        offset = Vec2(vector)
        self.np_vertices.transform_inplace(Matrix44.translate(offset.x, offset.y, 0))

    def scale(self, sx: float = 1.0, sy: float = 1.0) -> None:
        """Scale shape about `sx` in x-axis and `sy` in y-axis."""
        self.np_vertices.transform_inplace(Matrix44.scale(sx, sy, 1))

    def scale_uniform(self, scale: float) -> None:
        """Scale shape uniform about `scale` in x- and y-axis."""
        self.np_vertices.transform_inplace(Matrix44.scale(scale, scale, 1))

    def rotate(self, angle: float, center: Optional[UVec] = None) -> None:
        """Rotate shape around rotation `center` about `angle` in degrees."""
        self.rotate_rad(math.radians(angle), center)

    def rotate_rad(self, angle: float, center: Optional[UVec] = None) -> None:
        """Rotate shape around rotation `center` about `angle` in radians."""
        m = Matrix44.z_rotate(angle)
        if center is not None:
            p = Vec2(center)
            m = (
                Matrix44.translate(-p.x, -p.y, 0)
                @ m
                @ Matrix44.translate(p.x, p.y, 0)
            )
        self.np_vertices.transform_inplace(m)

    def offset(self, offset: float, closed: bool = False) -> Shape2d:
        """Returns a new offset shape, for more information see also
        :func:`ezdxf.math.offset_vertices_2d` function.

        Args:
            offset: line offset perpendicular to direction of shape segments
                defined by vertices order, offset > ``0`` is 'left' of line
                segment, offset < ``0`` is 'right' of line segment
            closed: ``True`` to handle as closed shape

        """
        from ezdxf.math.offset2d import offset_vertices_2d

        return self.__class__(
            offset_vertices_2d(
                self.np_vertices.vertices(), offset=offset, closed=closed
            )
        )

    def convex_hull(self) -> Shape2d:
        """Returns convex hull as new shape."""
        from ezdxf.math.construct2d import convex_hull_2d

        return self.__class__(convex_hull_2d(self.np_vertices.vertices()))

    # Sequence interface
    def __len__(self) -> int:
        """Returns `count` of vertices."""
        return len(self.np_vertices)

    @overload
    def __getitem__(self, item: int) -> Vec2: ...
    @overload
    def __getitem__(self, item: slice) -> list[Vec2]: ...
    def __getitem__(self, item: int | slice) -> Vec2|list[Vec2]:
        """Get vertex by index `item`, supports ``list`` like slicing."""
        np_vertices = self.np_vertices.np_vertices()
        if isinstance(item, int):
            return Vec2(np_vertices[item])
        else:
            return Vec2.list(np_vertices[item])

    def append(self, vertex: UVec) -> None:
        """Append single `vertex`.

        Args:
             vertex: vertex as :class:`Vec2` compatible object

        """
        self.extend((vertex,))

    def extend(self, vertices: Iterable[UVec]) -> None:
        """Append multiple `vertices`.

        Args:
             vertices: iterable of vertices as :class:`Vec2` compatible objects

        """
        from ezdxf.npshapes import NumpyPoints2d

        new_vertices = self.np_vertices.vertices() + Vec2.list(vertices)
        self.np_vertices = NumpyPoints2d(new_vertices)
