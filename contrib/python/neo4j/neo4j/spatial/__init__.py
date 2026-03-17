# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Spatial data types for interchange with the DBMS."""

from __future__ import annotations as _

from threading import Lock as _Lock

from .. import _typing as _t


__all__ = [
    "CartesianPoint",
    "Point",
    "WGS84Point",
]


# SRID to subclass mappings
_srid_table: dict[int, tuple[type[Point], int]] = {}
_srid_table_lock = _Lock()


class Point(tuple[float, ...]):
    """
    Base-class for spatial data.

    A point within a geometric space. This type is generally used via its
    subclasses and should not be instantiated directly unless there is no
    subclass defined for the required SRID.

    :param iterable:
        An iterable of coordinates.
        All items will be converted to :class:`float`.
    :type iterable: Iterable[float]
    """

    #: The SRID (spatial reference identifier) of the spatial data.
    #: A number that identifies the coordinate system the spatial type is to
    #: be interpreted in.
    srid: int | None

    if _t.TYPE_CHECKING:

        @property
        def x(self) -> float: ...

        @property
        def y(self) -> float: ...

        @property
        def z(self) -> float: ...

    def __new__(cls, iterable: _t.Iterable[float]) -> Point:
        return tuple.__new__(cls, map(float, iterable))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(map(repr, self))})"

    def __eq__(self, other: object) -> bool:
        try:
            return type(self) is type(other) and tuple(self) == tuple(
                _t.cast(Point, other)
            )
        except (AttributeError, TypeError):
            return NotImplemented

    def __hash__(self):
        return hash(type(self)) ^ hash(tuple(self))


def _point_type(
    name: str, fields: tuple[str, str, str], srid_map: dict[int, int]
) -> type[Point]:
    """Dynamically create a Point subclass."""

    def srid(self):
        try:
            return srid_map[len(self)]
        except KeyError:
            return None

    attributes = {"srid": property(srid)}

    for index, subclass_field in enumerate(fields):

        def accessor(self, i=index, f=subclass_field):
            try:
                return self[i]
            except IndexError:
                raise AttributeError(f) from None

        for field_alias in (subclass_field, "xyz"[index]):
            attributes[field_alias] = property(accessor)

    cls = _t.cast(type[Point], type(name, (Point,), attributes))

    with _srid_table_lock:
        for dim, srid_ in srid_map.items():
            _srid_table[srid_] = (cls, dim)

    return cls


# Point subclass definitions
if _t.TYPE_CHECKING:

    class CartesianPoint(Point): ...
else:
    CartesianPoint = _point_type(
        "CartesianPoint", ("x", "y", "z"), {2: 7203, 3: 9157}
    )

if _t.TYPE_CHECKING:

    class WGS84Point(Point):
        @property
        def longitude(self) -> float: ...

        @property
        def latitude(self) -> float: ...

        @property
        def height(self) -> float: ...
else:
    WGS84Point = _point_type(
        "WGS84Point", ("longitude", "latitude", "height"), {2: 4326, 3: 4979}
    )
