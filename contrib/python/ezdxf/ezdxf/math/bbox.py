# Copyright (c) 2019-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Optional, Iterator, Sequence, TypeVar, Generic
import abc
import math
import numpy as np

from ezdxf.math import Vec3, Vec2, UVec

T = TypeVar("T", Vec2, Vec3)

__all__ = ["BoundingBox2d", "BoundingBox", "AbstractBoundingBox"]


class AbstractBoundingBox(Generic[T], abc.ABC):
    extmin: T
    extmax: T

    @abc.abstractmethod
    def __init__(self, vertices: Optional[Iterable[UVec]] = None):
        ...

    def copy(self):
        box = self.__class__()
        box.extmin = self.extmin
        box.extmax = self.extmax
        return box

    def __str__(self) -> str:
        return f"[{self.extmin}, {self.extmax}]"

    def __repr__(self) -> str:
        name = self.__class__.__name__
        if self.has_data:
            return f"{name}({self.__str__()})"
        else:
            return f"{name}()"

    def __iter__(self) -> Iterator[T]:
        if self.has_data:
            yield self.extmin
            yield self.extmax

    @abc.abstractmethod
    def extend(self, vertices: Iterable[UVec]) -> None:
        ...

    @property
    @abc.abstractmethod
    def is_empty(self) -> bool:
        ...

    @abc.abstractmethod
    def inside(self, vertex: UVec) -> bool:
        ...

    @abc.abstractmethod
    def has_intersection(self, other: AbstractBoundingBox[T]) -> bool:
        ...

    @abc.abstractmethod
    def has_overlap(self, other: AbstractBoundingBox[T]) -> bool:
        ...

    @abc.abstractmethod
    def intersection(self, other: AbstractBoundingBox[T]) -> AbstractBoundingBox[T]:
        ...

    def contains(self, other: AbstractBoundingBox[T]) -> bool:
        """Returns ``True`` if the `other` bounding box is completely inside
        this bounding box.

        """
        return self.inside(other.extmin) and self.inside(other.extmax)

    def any_inside(self, vertices: Iterable[UVec]) -> bool:
        """Returns ``True`` if any vertex is inside this bounding box.

        Vertices at the box border are inside!
        """
        if self.has_data:
            return any(self.inside(v) for v in vertices)
        return False

    def all_inside(self, vertices: Iterable[UVec]) -> bool:
        """Returns ``True`` if all vertices are inside this bounding box.

        Vertices at the box border are inside!
        """
        if self.has_data:
            # all() returns True for an empty set of vertices
            has_any = False
            for v in vertices:
                has_any = True
                if not self.inside(v):
                    return False
            return has_any
        return False

    @property
    def has_data(self) -> bool:
        """Returns ``True`` if the bonding box has known limits."""
        return math.isfinite(self.extmin.x)

    @property
    def size(self) -> T:
        """Returns size of bounding box."""
        return self.extmax - self.extmin

    @property
    def center(self) -> T:
        """Returns center of bounding box."""
        return self.extmin.lerp(self.extmax)

    def union(self, other: AbstractBoundingBox[T]) -> AbstractBoundingBox[T]:
        """Returns a new bounding box as union of this and `other` bounding
        box.
        """
        vertices: list[T] = []
        if self.has_data:
            vertices.extend(self)
        if other.has_data:
            vertices.extend(other)
        return self.__class__(vertices)

    def rect_vertices(self) -> Sequence[Vec2]:
        """Returns the corners of the bounding box in the xy-plane as
        :class:`Vec2` objects.
        """
        if self.has_data:
            x0, y0, *_ = self.extmin
            x1, y1, *_ = self.extmax
            return Vec2(x0, y0), Vec2(x1, y0), Vec2(x1, y1), Vec2(x0, y1)
        else:
            raise ValueError("empty bounding box")

    def grow(self, value: float) -> None:
        """Grow or shrink the bounding box by an uniform value in x, y and
        z-axis. A negative value shrinks the bounding box.
        Raises :class:`ValueError` for shrinking the size of the bounding box to
        zero or below in any dimension.
        """
        if self.has_data:
            if value < 0.0:
                min_ext = min(self.size)
                if -value >= min_ext / 2.0:
                    raise ValueError("shrinking one or more dimensions <= 0")
            self.extmax += Vec3(value, value, value)
            self.extmin += Vec3(-value, -value, -value)


class BoundingBox(AbstractBoundingBox[Vec3]):
    """3D bounding box.

    Args:
        vertices: iterable of ``(x, y, z)`` tuples or :class:`Vec3` objects

    """

    __slots__ = ("extmin", "extmax")

    def __init__(self, vertices: Optional[Iterable[UVec]] = None):
        self.extmin = Vec3(math.inf, math.inf, math.inf)
        self.extmax = self.extmin
        if vertices is not None:
            try:
                self.extmin, self.extmax = extents3d(vertices)
            except ValueError:
                # No or invalid data creates an empty BoundingBox
                pass

    @property
    def is_empty(self) -> bool:
        """Returns ``True`` if the bounding box is empty or the bounding box
        has a size of 0 in any or all dimensions or is undefined.

        """
        if self.has_data:
            sx, sy, sz = self.size
            return sx * sy * sz == 0.0
        return True

    def extend(self, vertices: Iterable[UVec]) -> None:
        """Extend bounds by `vertices`.

        Args:
            vertices: iterable of vertices

        """
        v = list(vertices)
        if not v:
            return
        if self.has_data:
            v.extend([self.extmin, self.extmax])
        self.extmin, self.extmax = extents3d(v)

    def inside(self, vertex: UVec) -> bool:
        """Returns ``True`` if `vertex` is inside this bounding box.

        Vertices at the box border are inside!
        """
        if not self.has_data:
            return False
        x, y, z = Vec3(vertex).xyz
        xmin, ymin, zmin = self.extmin.xyz
        xmax, ymax, zmax = self.extmax.xyz
        return (xmin <= x <= xmax) and (ymin <= y <= ymax) and (zmin <= z <= zmax)

    def has_intersection(self, other: AbstractBoundingBox[T]) -> bool:
        """Returns ``True`` if this bounding box intersects with `other` but does
        not include touching bounding boxes, see also :meth:`has_overlap`::

            bbox1 = BoundingBox([(0, 0, 0), (1, 1, 1)])
            bbox2 = BoundingBox([(1, 1, 1), (2, 2, 2)])
            assert bbox1.has_intersection(bbox2) is False

        """
        # Source: https://gamemath.com/book/geomtests.html#intersection_two_aabbs
        # Check for a separating axis:
        if not self.has_data or not other.has_data:
            return False
        o_min = Vec3(other.extmin)  # could be a 2D bounding box
        o_max = Vec3(other.extmax)  # could be a 2D bounding box

        # Check for a separating axis:
        if self.extmin.x >= o_max.x:
            return False
        if self.extmax.x <= o_min.x:
            return False
        if self.extmin.y >= o_max.y:
            return False
        if self.extmax.y <= o_min.y:
            return False
        if self.extmin.z >= o_max.z:
            return False
        if self.extmax.z <= o_min.z:
            return False
        return True

    def has_overlap(self, other: AbstractBoundingBox[T]) -> bool:
        """Returns ``True`` if this bounding box intersects with `other` but
        in contrast to :meth:`has_intersection` includes touching bounding boxes too::

            bbox1 = BoundingBox([(0, 0, 0), (1, 1, 1)])
            bbox2 = BoundingBox([(1, 1, 1), (2, 2, 2)])
            assert bbox1.has_overlap(bbox2) is True

        """
        # Source: https://gamemath.com/book/geomtests.html#intersection_two_aabbs
        # Check for a separating axis:
        if not self.has_data or not other.has_data:
            return False
        o_min = Vec3(other.extmin)  # could be a 2D bounding box
        o_max = Vec3(other.extmax)  # could be a 2D bounding box
        # Check for a separating axis:
        if self.extmin.x > o_max.x:
            return False
        if self.extmax.x < o_min.x:
            return False
        if self.extmin.y > o_max.y:
            return False
        if self.extmax.y < o_min.y:
            return False
        if self.extmin.z > o_max.z:
            return False
        if self.extmax.z < o_min.z:
            return False
        return True

    def cube_vertices(self) -> Sequence[Vec3]:
        """Returns the 3D corners of the bounding box as :class:`Vec3` objects."""
        if self.has_data:
            x0, y0, z0 = self.extmin
            x1, y1, z1 = self.extmax
            return (
                Vec3(x0, y0, z0),
                Vec3(x1, y0, z0),
                Vec3(x1, y1, z0),
                Vec3(x0, y1, z0),
                Vec3(x0, y0, z1),
                Vec3(x1, y0, z1),
                Vec3(x1, y1, z1),
                Vec3(x0, y1, z1),
            )
        else:
            raise ValueError("empty bounding box")

    def intersection(self, other: AbstractBoundingBox[T]) -> BoundingBox:
        """Returns the bounding box of the intersection cube of both
        3D bounding boxes. Returns an empty bounding box if the intersection
        volume is 0.

        """
        new_bbox = self.__class__()
        if not self.has_intersection(other):
            return new_bbox
        s_min_x, s_min_y, s_min_z = Vec3(self.extmin)
        o_min_x, o_min_y, o_min_z = Vec3(other.extmin)
        s_max_x, s_max_y, s_max_z = Vec3(self.extmax)
        o_max_x, o_max_y, o_max_z = Vec3(other.extmax)
        new_bbox.extend(
            [
                (
                    max(s_min_x, o_min_x),
                    max(s_min_y, o_min_y),
                    max(s_min_z, o_min_z),
                ),
                (
                    min(s_max_x, o_max_x),
                    min(s_max_y, o_max_y),
                    min(s_max_z, o_max_z),
                ),
            ]
        )
        return new_bbox


class BoundingBox2d(AbstractBoundingBox[Vec2]):
    """2D bounding box.

    Args:
        vertices: iterable of ``(x, y[, z])`` tuples or :class:`Vec3` objects

    """

    __slots__ = ("extmin", "extmax")

    def __init__(self, vertices: Optional[Iterable[UVec]] = None):
        self.extmin = Vec2(math.inf, math.inf)
        self.extmax = self.extmin
        if vertices is not None:
            try:
                self.extmin, self.extmax = extents2d(vertices)
            except ValueError:
                # No or invalid data creates an empty BoundingBox
                pass

    @property
    def is_empty(self) -> bool:
        """Returns ``True`` if the bounding box is empty. The bounding box has a
        size of 0 in any or all dimensions or is undefined.
        """
        if self.has_data:
            sx, sy = self.size
            return sx * sy == 0.0
        return True

    def extend(self, vertices: Iterable[UVec]) -> None:
        """Extend bounds by `vertices`.

        Args:
            vertices: iterable of vertices

        """
        v = list(vertices)
        if not v:
            return
        if self.has_data:
            v.extend([self.extmin, self.extmax])
        self.extmin, self.extmax = extents2d(v)

    def inside(self, vertex: UVec) -> bool:
        """Returns ``True`` if `vertex` is inside this bounding box.

        Vertices at the box border are inside!
        """
        if not self.has_data:
            return False
        v = Vec2(vertex)
        min_ = self.extmin
        max_ = self.extmax
        return (min_.x <= v.x <= max_.x) and (min_.y <= v.y <= max_.y)

    def has_intersection(self, other: AbstractBoundingBox[T]) -> bool:
        """Returns ``True`` if this bounding box intersects with `other` but does
        not include touching bounding boxes, see also :meth:`has_overlap`::

            bbox1 = BoundingBox2d([(0, 0), (1, 1)])
            bbox2 = BoundingBox2d([(1, 1), (2, 2)])
            assert bbox1.has_intersection(bbox2) is False

        """
        # Source: https://gamemath.com/book/geomtests.html#intersection_two_aabbs
        if not self.has_data or not other.has_data:
            return False
        # Check for a separating axis:
        if self.extmin.x >= other.extmax.x:
            return False
        if self.extmax.x <= other.extmin.x:
            return False
        if self.extmin.y >= other.extmax.y:
            return False
        if self.extmax.y <= other.extmin.y:
            return False
        return True

    def intersection(self, other: AbstractBoundingBox[T]) -> BoundingBox2d:
        """Returns the bounding box of the intersection rectangle of both
        2D bounding boxes. Returns an empty bounding box if the intersection
        area is 0.
        """
        new_bbox = self.__class__()
        if not self.has_intersection(other):
            return new_bbox
        s_min_x, s_min_y = Vec2(self.extmin)
        o_min_x, o_min_y = Vec2(other.extmin)
        s_max_x, s_max_y = Vec2(self.extmax)
        o_max_x, o_max_y = Vec2(other.extmax)
        new_bbox.extend(
            [
                (max(s_min_x, o_min_x), max(s_min_y, o_min_y)),
                (min(s_max_x, o_max_x), min(s_max_y, o_max_y)),
            ]
        )
        return new_bbox

    def has_overlap(self, other: AbstractBoundingBox[T]) -> bool:
        """Returns ``True`` if this bounding box intersects with `other` but
        in contrast to :meth:`has_intersection` includes touching bounding boxes too::

            bbox1 = BoundingBox2d([(0, 0), (1, 1)])
            bbox2 = BoundingBox2d([(1, 1), (2, 2)])
            assert bbox1.has_overlap(bbox2) is True

        """
        # Source: https://gamemath.com/book/geomtests.html#intersection_two_aabbs
        if not self.has_data or not other.has_data:
            return False
        # Check for a separating axis:
        if self.extmin.x > other.extmax.x:
            return False
        if self.extmax.x < other.extmin.x:
            return False
        if self.extmin.y > other.extmax.y:
            return False
        if self.extmax.y < other.extmin.y:
            return False
        return True


def extents3d(vertices: Iterable[UVec]) -> tuple[Vec3, Vec3]:
    """Returns the extents of the bounding box as tuple (extmin, extmax)."""
    vertices = np.array([Vec3(v).xyz for v in vertices], dtype=np.float64)
    if len(vertices):
        return Vec3(vertices.min(0)), Vec3(vertices.max(0))
    else:
        raise ValueError("no vertices given")


def extents2d(vertices: Iterable[UVec]) -> tuple[Vec2, Vec2]:
    """Returns the extents of the bounding box as tuple (extmin, extmax)."""
    vertices = np.array([(x, y) for x, y, *_ in vertices], dtype=np.float64)
    if len(vertices):
        return Vec2(vertices.min(0)), Vec2(vertices.max(0))
    else:
        raise ValueError("no vertices given")
