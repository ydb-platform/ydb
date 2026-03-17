"""
Adapters for PostGIS geometries
"""

from __future__ import annotations

from functools import cache

from .. import postgres
from ..pq import Format
from ..abc import AdaptContext, Buffer
from ..adapt import Dumper, Loader
from .._typeinfo import TypeInfo

try:
    import shapely

except ImportError:
    raise ImportError(
        "The module psycopg.types.shapely requires the package 'Shapely'"
        " to be installed"
    )

from shapely.geometry.base import BaseGeometry

shapely_version = tuple(int(s) for s in shapely.__version__.split(".") if s.isdigit())

if shapely_version >= (2, 0):
    from shapely import from_wkb, to_wkb
else:
    from shapely.wkb import dumps as to_wkb  # type: ignore[no-redef]
    from shapely.wkb import loads as from_wkb  # type: ignore[no-redef]


class GeometryBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> BaseGeometry:
        return from_wkb(bytes(data))


class GeometryLoader(Loader):
    def load(self, data: Buffer) -> BaseGeometry:
        # it's a hex string in binary
        return from_wkb(bytes(data))


class BaseGeometryBinaryDumper(Dumper):
    format = Format.BINARY

    def dump(self, obj: BaseGeometry) -> Buffer | None:
        return to_wkb(obj, include_srid=True)


class BaseGeometryDumper(Dumper):
    def dump(self, obj: BaseGeometry) -> Buffer | None:
        return to_wkb(obj, True, include_srid=True).encode()


def register_shapely(info: TypeInfo, context: AdaptContext | None = None) -> None:
    """Register Shapely dumper and loaders."""

    # A friendly error warning instead of an AttributeError in case fetch()
    # failed and it wasn't noticed.
    if not info:
        raise TypeError("no info passed. Is the 'postgis' extension loaded?")

    info.register(context)
    adapters = context.adapters if context else postgres.adapters

    adapters.register_loader(info.oid, GeometryBinaryLoader)
    adapters.register_loader(info.oid, GeometryLoader)
    # Default binary dump
    adapters.register_dumper(BaseGeometry, _make_dumper(info.oid))
    adapters.register_dumper(BaseGeometry, _make_binary_dumper(info.oid))


# Cache all dynamically-generated types to avoid leaks in case the types
# cannot be GC'd.


@cache
def _make_dumper(oid_in: int) -> type[BaseGeometryDumper]:
    class GeometryDumper(BaseGeometryDumper):
        oid = oid_in

    return GeometryDumper


@cache
def _make_binary_dumper(oid_in: int) -> type[BaseGeometryBinaryDumper]:
    class GeometryBinaryDumper(BaseGeometryBinaryDumper):
        oid = oid_in

    return GeometryBinaryDumper
