"""geojson-pydantic."""

from .features import Feature, FeatureCollection  # noqa
from .geometries import (  # noqa
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

__version__ = "2.1.0"

__all__ = [
    "Feature",
    "FeatureCollection",
    "GeometryCollection",
    "LineString",
    "MultiLineString",
    "MultiPoint",
    "MultiPolygon",
    "Point",
    "Polygon",
]
