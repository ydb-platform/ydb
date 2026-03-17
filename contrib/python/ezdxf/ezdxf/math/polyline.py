# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Tuple,
    Iterator,
    Sequence,
    Dict,
)
import abc
from typing_extensions import Protocol, TypeAlias
import numpy as np

from ezdxf.math import (
    Vec2,
    Vec3,
    UVec,
    NULLVEC,
    intersection_line_line_2d,
    BoundingBox2d,
    intersection_line_line_3d,
    BoundingBox,
    AbstractBoundingBox,
)


import bisect


__all__ = [
    "ConstructionPolyline",
    "ApproxParamT",
    "intersect_polylines_2d",
    "intersect_polylines_3d",
]

REL_TOL = 1e-9


class ConstructionPolyline(Sequence):
    """Construction tool for 3D polylines.

    A polyline construction tool to measure, interpolate and divide anything
    that can be approximated or flattened into vertices.
    This is an immutable data structure which supports the :class:`Sequence`
    interface.

    Args:
        vertices: iterable of polyline vertices
        close: ``True`` to close the polyline (first vertex == last vertex)
        rel_tol: relative tolerance for floating point comparisons

    Example to measure or divide a SPLINE entity::

        import ezdxf
        from ezdxf.math import ConstructionPolyline

        doc = ezdxf.readfile("your.dxf")
        msp = doc.modelspace()
        spline = msp.query("SPLINE").first
        if spline is not None:
            polyline = ConstructionPolyline(spline.flattening(0.01))
            print(f"Entity {spline} has an approximated length of {polyline.length}")
            # get dividing points with a distance of 1.0 drawing unit to each other
            points = list(polyline.divide_by_length(1.0))

    """

    def __init__(
        self,
        vertices: Iterable[UVec],
        close: bool = False,
        rel_tol: float = REL_TOL,
    ):
        self._rel_tol = float(rel_tol)
        v3list: list[Vec3] = Vec3.list(vertices)
        self._vertices: list[Vec3] = v3list
        if close and len(v3list) > 2:
            if not v3list[0].isclose(v3list[-1], rel_tol=self._rel_tol):
                v3list.append(v3list[0])

        self._distances: list[float] = _distances(v3list)

    def __len__(self) -> int:
        """len(self)"""
        return len(self._vertices)

    def __iter__(self) -> Iterator[Vec3]:
        """iter(self)"""
        return iter(self._vertices)

    def __getitem__(self, item):
        """vertex = self[item]"""
        if isinstance(item, int):
            return self._vertices[item]
        else:  # slice
            return self.__class__(self._vertices[item], rel_tol=self._rel_tol)

    @property
    def length(self) -> float:
        """Returns the overall length of the polyline."""
        if self._distances:
            return self._distances[-1]
        return 0.0

    @property
    def is_closed(self) -> bool:
        """Returns ``True`` if the polyline is closed
        (first vertex == last vertex).
        """
        if len(self._vertices) > 2:
            return self._vertices[0].isclose(
                self._vertices[-1], rel_tol=self._rel_tol
            )
        return False

    def data(self, index: int) -> tuple[float, float, Vec3]:
        """Returns the tuple (distance from start, distance from previous vertex,
        vertex). All distances measured along the polyline.
        """
        vertices = self._vertices
        if not vertices:
            raise ValueError("empty polyline")
        distances = self._distances

        if index == 0:
            return 0.0, 0.0, vertices[0]
        prev_distance = distances[index - 1]
        current_distance = distances[index]
        vertex = vertices[index]
        return current_distance, current_distance - prev_distance, vertex

    def index_at(self, distance: float) -> int:
        """Returns the data index of the exact or next data entry for the given
        `distance`. Returns the index of last entry if `distance` > :attr:`length`.

        """
        if distance <= 0.0:
            return 0
        if distance >= self.length:
            return max(0, len(self) - 1)
        return self._index_at(distance)

    def _index_at(self, distance: float) -> int:
        # fast method without any checks
        return bisect.bisect_left(self._distances, distance)

    def vertex_at(self, distance: float) -> Vec3:
        """Returns the interpolated vertex at the given `distance` from the
        start of the polyline.
        """
        if distance < 0.0 or distance > self.length:
            raise ValueError("distance out of range")
        if len(self._vertices) < 2:
            raise ValueError("not enough vertices for interpolation")
        return self._vertex_at(distance)

    def _vertex_at(self, distance: float) -> Vec3:
        # fast method without any checks
        vertices = self._vertices
        distances = self._distances
        index1 = self._index_at(distance)
        if index1 == 0:
            return vertices[0]
        index0 = index1 - 1
        distance1 = distances[index1]
        distance0 = distances[index0]
        # skip coincident vertices:
        while index0 > 0 and distance0 == distance1:
            index0 -= 1
            distance0 = distances[index0]
        if distance0 == distance1:
            raise ArithmeticError("internal interpolation error")

        factor = (distance - distance0) / (distance1 - distance0)
        return vertices[index0].lerp(vertices[index1], factor=factor)

    def divide(self, count: int) -> Iterator[Vec3]:
        """Returns `count` interpolated vertices along the polyline.
        Argument `count` has to be greater than 2 and the start- and end
        vertices are always included.

        """
        if count < 2:
            raise ValueError(f"invalid count: {count}")
        vertex_at = self._vertex_at
        for distance in np.linspace(0.0, self.length, count):
            yield vertex_at(distance)

    def divide_by_length(
        self, length: float, force_last: bool = False
    ) -> Iterator[Vec3]:
        """Returns interpolated vertices along the polyline. Each vertex has a
        fix distance `length` from its predecessor. Yields the last vertex if
        argument `force_last` is ``True`` even if the last distance is not equal
        to `length`.

        """
        if length <= 0.0:
            raise ValueError(f"invalid length: {length}")
        if len(self._vertices) < 2:
            raise ValueError("not enough vertices for interpolation")

        total_length: float = self.length
        vertex_at = self._vertex_at
        distance: float = 0.0

        vertex: Vec3 = NULLVEC
        while distance <= total_length:
            vertex = vertex_at(distance)
            yield vertex
            distance += length

        if force_last and not vertex.isclose(self._vertices[-1]):
            yield self._vertices[-1]


def _distances(vertices: Iterable[Vec3]) -> list[float]:
    # distance from start vertex of the polyline to the vertex
    current_station: float = 0.0
    distances: list[float] = []
    prev_vertex = Vec3()
    for vertex in vertices:
        if distances:
            distant_vec = vertex - prev_vertex
            current_station += distant_vec.magnitude
            distances.append(current_station)
        else:
            distances.append(current_station)
        prev_vertex = vertex
    return distances


class SupportsPointMethod(Protocol):
    def point(self, t: float) -> UVec:
        ...


class ApproxParamT:
    """Approximation tool for parametrized curves.

    - approximate parameter `t` for a given distance from the start of the curve
    - approximate the distance for a given parameter `t` from the start of the curve

    These approximations can be applied to all parametrized curves which provide
    a :meth:`point` method, like :class:`Bezier4P`, :class:`Bezier3P` and
    :class:`BSpline`.

    The approximation is based on equally spaced parameters from 0 to `max_t`
    for a given segment count.
    The :meth:`flattening` method can not be used for the curve approximation,
    because the required parameter `t` is not logged by the flattening process.

    Args:
        curve: curve object, requires a method :meth:`point`
        max_t: the max. parameter value
        segments: count of approximation segments

    """

    def __init__(
        self,
        curve: SupportsPointMethod,
        *,
        max_t: float = 1.0,
        segments: int = 100,
    ):
        assert hasattr(curve, "point")
        assert segments > 0
        self._polyline = ConstructionPolyline(
            curve.point(t) for t in np.linspace(0.0, max_t, segments + 1)
        )
        self._max_t = max_t
        self._step = max_t / segments

    @property
    def max_t(self) -> float:
        return self._max_t

    @property
    def polyline(self) -> ConstructionPolyline:
        return self._polyline

    def param_t(self, distance: float):
        """Approximate parameter t for the given `distance` from the start of
        the curve.
        """
        poly = self._polyline
        if distance >= poly.length:
            return self._max_t

        t_step = self._step
        i = poly.index_at(distance)
        station, d0, _ = poly.data(i)
        t = t_step * i  # t for station
        if d0 > 1e-12:
            t -= t_step * (station - distance) / d0
        return min(self._max_t, t)

    def distance(self, t: float) -> float:
        """Approximate the distance from the start of the curve to the point
        `t` on the curve.
        """
        if t <= 0.0:
            return 0.0
        poly = self._polyline
        if t >= self._max_t:
            return poly.length

        step = self._step
        index = int(t / step) + 1
        station, d0, _ = poly.data(index)
        return station - d0 * (step * index - t) / step


def intersect_polylines_2d(
    p1: Sequence[Vec2], p2: Sequence[Vec2], abs_tol=1e-10
) -> list[Vec2]:
    """Returns the intersection points for two polylines as list of :class:`Vec2`
    objects, the list is empty if no intersection points exist.
    Does not return self intersection points of `p1` or `p2`.
    Duplicate intersection points are removed from the result list, but the list
    does not have a particular order! You can sort the result list by
    :code:`result.sort()` to introduce an order.

    Args:
        p1: first polyline as sequence of :class:`Vec2` objects
        p2: second polyline as sequence of :class:`Vec2` objects
        abs_tol: absolute tolerance for comparisons

    """
    intersect = _PolylineIntersection2d(p1, p2, abs_tol)
    intersect.execute()
    return intersect.intersections


def intersect_polylines_3d(
    p1: Sequence[Vec3], p2: Sequence[Vec3], abs_tol=1e-10
) -> list[Vec3]:
    """Returns the intersection points for two polylines as list of :class:`Vec3`
    objects, the list is empty if no intersection points exist.
    Does not return self intersection points of `p1` or `p2`.
    Duplicate intersection points are removed from the result list, but the list
    does not have a particular order! You can sort the result list by
    :code:`result.sort()` to introduce an order.

    Args:
        p1: first polyline as sequence of :class:`Vec3` objects
        p2: second polyline as sequence of :class:`Vec3` objects
        abs_tol: absolute tolerance for comparisons

    """
    intersect = _PolylineIntersection3d(p1, p2, abs_tol)
    intersect.execute()
    return intersect.intersections


def divide(a: int, b: int) -> tuple[int, int, int, int]:
    m = (a + b) // 2
    return a, m, m, b


TCache: TypeAlias = Dict[Tuple[int, int, int], AbstractBoundingBox]


class _PolylineIntersection:
    p1: Sequence
    p2: Sequence

    def __init__(self) -> None:
        # At each recursion level the bounding box for each half of the
        # polyline will be created two times, using a cache is an advantage:
        self.bbox_cache: TCache = {}

    @abc.abstractmethod
    def bbox(self, points: Sequence) -> AbstractBoundingBox:
        ...

    @abc.abstractmethod
    def line_intersection(self, s1: int, e1: int, s2: int, e2: int) -> None:
        ...

    def execute(self) -> None:
        l1: int = len(self.p1)
        l2: int = len(self.p2)
        if l1 < 2 or l2 < 2:  # polylines with only one vertex
            return
        self.intersect(0, l1 - 1, 0, l2 - 1)

    def overlap(self, s1: int, e1: int, s2: int, e2: int) -> bool:
        e1 += 1
        e2 += 1
        # If one part of the polylines has less than 2 vertices no intersection
        # calculation is required:
        if e1 - s1 < 2 or e2 - s2 < 2:
            return False

        cache = self.bbox_cache
        key1 = (1, s1, e1)
        bbox1 = cache.get(key1)
        if bbox1 is None:
            bbox1 = self.bbox(self.p1[s1:e1])
            cache[key1] = bbox1

        key2 = (2, s2, e2)
        bbox2 = cache.get(key2)
        if bbox2 is None:
            bbox2 = self.bbox(self.p2[s2:e2])
            cache[key2] = bbox2
        return bbox1.has_overlap(bbox2)

    def intersect(self, s1: int, e1: int, s2: int, e2: int) -> None:
        assert e1 > s1 and e2 > s2
        if e1 - s1 == 1 and e2 - s2 == 1:
            self.line_intersection(s1, e1, s2, e2)
            return
        s1_a, e1_b, s1_c, e1_d = divide(s1, e1)
        s2_a, e2_b, s2_c, e2_d = divide(s2, e2)

        if self.overlap(s1_a, e1_b, s2_a, e2_b):
            self.intersect(s1_a, e1_b, s2_a, e2_b)
        if self.overlap(s1_a, e1_b, s2_c, e2_d):
            self.intersect(s1_a, e1_b, s2_c, e2_d)
        if self.overlap(s1_c, e1_d, s2_a, e2_b):
            self.intersect(s1_c, e1_d, s2_a, e2_b)
        if self.overlap(s1_c, e1_d, s2_c, e2_d):
            self.intersect(s1_c, e1_d, s2_c, e2_d)


class _PolylineIntersection2d(_PolylineIntersection):
    def __init__(self, p1: Sequence[Vec2], p2: Sequence[Vec2], abs_tol=1e-10):
        super().__init__()
        self.p1 = p1
        self.p2 = p2
        self.intersections: list[Vec2] = []
        self.abs_tol = abs_tol

    def bbox(self, points: Sequence) -> AbstractBoundingBox:
        return BoundingBox2d(points)

    def line_intersection(self, s1: int, e1: int, s2: int, e2: int) -> None:
        line1 = self.p1[s1], self.p1[e1]
        line2 = self.p2[s2], self.p2[e2]
        p = intersection_line_line_2d(
            line1, line2, virtual=False, abs_tol=self.abs_tol
        )
        if p is not None and not any(
            p.isclose(ip, abs_tol=self.abs_tol) for ip in self.intersections
        ):
            self.intersections.append(p)


class _PolylineIntersection3d(_PolylineIntersection):
    def __init__(self, p1: Sequence[Vec3], p2: Sequence[Vec3], abs_tol=1e-10):
        super().__init__()
        self.p1 = p1
        self.p2 = p2
        self.intersections: list[Vec3] = []
        self.abs_tol = abs_tol

    def bbox(self, points: Sequence) -> AbstractBoundingBox:
        return BoundingBox(points)

    def line_intersection(self, s1: int, e1: int, s2: int, e2: int) -> None:
        line1 = self.p1[s1], self.p1[e1]
        line2 = self.p2[s2], self.p2[e2]
        p = intersection_line_line_3d(
            line1, line2, virtual=False, abs_tol=self.abs_tol
        )
        if p is not None and not any(
            p.isclose(ip, abs_tol=self.abs_tol) for ip in self.intersections
        ):
            self.intersections.append(p)
