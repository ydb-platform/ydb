#  Copyright (c) 2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence, no_type_check, Any, Callable, Dict, List, Tuple
from typing_extensions import TypeAlias, override
import abc
import json

from ezdxf.math import Vec2, world_mercator_to_gps
from ezdxf.path import Command, nesting
from ezdxf.npshapes import orient_paths, single_paths

from .type_hints import Color
from .backend import BackendInterface, BkPath2d, BkPoints2d, ImageData
from .config import Configuration
from .properties import BackendProperties


__all__ = ["CustomJSONBackend", "GeoJSONBackend"]

CUSTOM_JSON_SPECS = """
JSON content = [entity, entity, ...]

entity = {
    "type": point | lines | path | filled-paths | filled-polygon,
    "properties": {
        "color": "#RRGGBBAA",
        "stroke-width": 0.25, # in mm
        "layer": "name"
    },
    "geometry": depends on "type"
}
DXF linetypes (DASH, DOT, ...) are resolved into solid lines.

A single point:
point = {
    "type": "point",
    "properties": {...},
    "geometry": [x, y]
}

Multiple lines with common properties:
lines = {
    "type": "lines",
    "properties": {...},
    "geometry": [
        (x0, y0, x1, y1),  # 1. line
        (x0, y0, x1, y1),  # 2. line
        ....
    ]
}
Lines can contain points where x0 == x1 and y0 == y1!

A single linear path without filling:
path = {
    "type": "path",
    "properties": {...},
    "geometry": [path-command, ...]
}

SVG-like path structure:
- The first path-command is always an absolute move to "M"
- The "M" command does not appear inside a path, each path is a continuouse geometry 
  (no multi-paths).

path-command = 
    ("M", x, y) = absolute move to
    ("L", x, y) = absolute line to
    ("Q", x0, y0, x1, y1) = absolute quadratice Bezier curve to
    - (x0, y0) = control point
    - (x1, y1) = end point
    ("C", x0, y0, x1, y1, x2, y2) = absolute cubic Bezier curve to
    - (x0, y0) = control point 1
    - (x1, y1) = control point 2
    - (x2, y2) = end point
    ("Z",) = close path

Multiple filled paths:

Exterior paths and holes are mixed and NOT oriented by default (clockwise or 
counter-clockwise) - PyQt and SVG have no problem with that structure but matplotlib 
requires oriented paths.  When oriented paths are required the CustomJSONBackend can 
orient the paths on demand.

filled-paths = {
    "type": "filled-paths",
    "properties": {...},
    "geometry": [
        [path-command, ...],  # 1. path
        [path-command, ...],  # 2. path  
        ...
    ]
}

A single filled polygon:
A polygon is explicitly closed, so first vertex == last vertex is guaranteed.
filled-polygon = {
    "type": "filled-polygon",
    "properties": {...},
    "geometry": [
        (x0, y0),
        (x1, y1),
        (x2, y2),
        ...
    ]
}

"""


class _JSONBackend(BackendInterface):
    def __init__(self) -> None:
        self._entities: list[dict[str, Any]] = []
        self.max_sagitta = 0.01  # set by configure()
        self.min_lineweight = 0.05  # in mm, set by configure()
        self.lineweight_scaling = 1.0  # set by configure()
        # set fixed lineweight for all strokes:
        # set Configuration.min_lineweight to the desired lineweight in 1/300 inch!
        # set Configuration.lineweight_scaling to 0
        self.fixed_lineweight = 0.0

    @abc.abstractmethod
    def get_json_data(self) -> Any: ...
    def get_string(self, *, indent: int | str = 2) -> str:
        """Returns the result as a JSON string."""
        return json.dumps(self.get_json_data(), indent=indent)

    @override
    def configure(self, config: Configuration) -> None:
        if config.min_lineweight:
            # config.min_lineweight in 1/300 inch!
            min_lineweight_mm = config.min_lineweight * 25.4 / 300
            self.min_lineweight = max(0.05, min_lineweight_mm)
        self.lineweight_scaling = config.lineweight_scaling
        if self.lineweight_scaling == 0.0:
            # use a fixed lineweight for all strokes defined by min_lineweight
            self.fixed_lineweight = self.min_lineweight
        self.max_sagitta = config.max_flattening_distance

    @override
    def clear(self) -> None:
        self._entities.clear()

    @override
    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        pass

    @override
    def set_background(self, color: Color) -> None:
        pass

    @override
    def finalize(self) -> None:
        pass

    @override
    def enter_entity(self, entity, properties) -> None:
        pass

    @override
    def exit_entity(self, entity) -> None:
        pass


MOVE_TO_ABS = "M"
LINE_TO_ABS = "L"
QUAD_TO_ABS = "Q"
CUBIC_TO_ABS = "C"
CLOSE_PATH = "Z"


class CustomJSONBackend(_JSONBackend):
    """Creates a JSON-like output with a custom JSON scheme.  This scheme supports
    curved shapes by a SVG-path like structure and coordinates are not limited in
    any way.  This backend can be used to send geometries from a web-backend to a
    frontend.

    The JSON scheme is documented in the source code:

    https://github.com/mozman/ezdxf/blob/master/src/ezdxf/addons/drawing/json.py

    Args:
        orient_paths: orient exterior and hole paths on demand, exterior paths have
            counter-clockwise orientation and holes have clockwise orientation.

    **Class Methods**

    .. automethod:: get_json_data

    .. automethod:: get_string

    .. versionadded:: 1.3.0

    """

    def __init__(self, orient_paths=False) -> None:
        super().__init__()
        self.orient_paths = orient_paths

    @override
    def get_json_data(self) -> list[dict[str, Any]]:
        """Returns the result as a JSON-like data structure."""
        return self._entities

    def add_entity(
        self, entity_type: str, geometry: Sequence[Any], properties: BackendProperties
    ):
        if not geometry:
            return
        self._entities.append(
            {
                "type": entity_type,
                "properties": self.make_properties_dict(properties),
                "geometry": geometry,
            }
        )

    def make_properties_dict(self, properties: BackendProperties) -> dict[str, Any]:
        if self.fixed_lineweight:
            stroke_width = self.fixed_lineweight
        else:
            stroke_width = max(
                self.min_lineweight, properties.lineweight * self.lineweight_scaling
            )
        return {
            "color": properties.color,
            "stroke-width": round(stroke_width, 2),
            "layer": properties.layer,
        }

    @override
    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        self.add_entity("point", [pos.x, pos.y], properties)

    @override
    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        self.add_entity("lines", [(start.x, start.y, end.x, end.y)], properties)

    @override
    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        lines = list(lines)
        if len(lines) == 0:
            return
        self.add_entity("lines", [(s.x, s.y, e.x, e.y) for s, e in lines], properties)

    @override
    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        self.add_entity("path", make_json_path(path), properties)

    @override
    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        paths = list(paths)
        if len(paths) == 0:
            return
        if self.orient_paths:
            paths = orient_paths(paths)  # returns single paths
        else:
            # Just single paths allowed, no multi paths!
            paths = single_paths(paths)
        json_paths: list[Any] = []
        for path in paths:
            if len(path):
                json_paths.append(make_json_path(path, close=True))
        if json_paths:
            self.add_entity("filled-paths", json_paths, properties)

    @override
    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        vertices: list[Vec2] = points.vertices()
        if len(vertices) < 3:
            return
        if not vertices[0].isclose(vertices[-1]):
            vertices.append(vertices[0])
        self.add_entity("filled-polygon", [(v.x, v.y) for v in vertices], properties)


@no_type_check
def make_json_path(path: BkPath2d, close=False) -> list[Any]:
    if len(path) == 0:
        return []
    end: Vec2 = path.start
    commands: list = [(MOVE_TO_ABS, end.x, end.y)]
    for cmd in path.commands():
        end = cmd.end
        if cmd.type == Command.MOVE_TO:
            commands.append((MOVE_TO_ABS, end.x, end.y))
        elif cmd.type == Command.LINE_TO:
            commands.append((LINE_TO_ABS, end.x, end.y))
        elif cmd.type == Command.CURVE3_TO:
            c1 = cmd.ctrl
            commands.append((QUAD_TO_ABS, c1.x, c1.y, end.x, end.y))
        elif cmd.type == Command.CURVE4_TO:
            c1 = cmd.ctrl1
            c2 = cmd.ctrl2
            commands.append((CUBIC_TO_ABS, c1.x, c1.y, c2.x, c2.y, end.x, end.y))
    if close:
        commands.append(CLOSE_PATH)
    return commands


# dict and list not allowed here for Python < 3.10
PropertiesMaker: TypeAlias = Callable[[str, float, str], Dict[str, Any]]
TransformFunc: TypeAlias = Callable[[Vec2], Tuple[float, float]]
# GeoJSON ring
Ring: TypeAlias = List[Tuple[float, float]]

# The first ring is the exterior path followed by optional holes, nested polygons are
# not supported by the GeoJSON specification.
GeoJsonPolygon: TypeAlias = List[Ring]


def properties_maker(color: str, stroke_width: float, layer: str) -> dict[str, Any]:
    """Returns the property dict::

        {
            "color": color,
            "stroke-width": stroke_width,
            "layer": layer,
        }

    Returning an empty dict prevents properties in the GeoJSON output and also avoids
    wraping entities into "Feature" objects.
    """
    return {
        "color": color,
        "stroke-width": round(stroke_width, 2),
        "layer": layer,
    }


def no_transform(location: Vec2) -> tuple[float, float]:
    """Dummy transformation function.  Does not apply any transformations and
    just returns the input coordinates.
    """
    return (location.x, location.y)


def make_world_mercator_to_gps_function(tol: float = 1e-6) -> TransformFunc:
    """Returns a function to transform WGS84 World Mercator `EPSG:3395 <https://epsg.io/3395>`_
    location given as cartesian 2D coordinates x, y in meters into WGS84 decimal
    degrees as longitude and latitude `EPSG:4326 <https://epsg.io/4326>`_ as
    used by GPS.

    Args:
        tol: accuracy for latitude calculation

    """

    def _transform(location: Vec2) -> tuple[float, float]:
        """Transforms WGS84 World Mercator EPSG:3395 coordinates to WGS84 EPSG:4326."""
        return world_mercator_to_gps(location.x, location.y, tol)
    return _transform


class GeoJSONBackend(_JSONBackend):
    """Creates a JSON-like output according the `GeoJSON`_ scheme.
    GeoJSON uses a geographic coordinate reference system, World Geodetic
    System 1984 `EPSG:4326 <https://epsg.io/4326>`_, and units of decimal degrees.

    - Latitude: -90 to +90 (South/North)
    - Longitude: -180 to +180 (East/West)

    So most DXF files will produce invalid coordinates and it is the job of the
    **package-user** to provide a function to transfrom the input coordinates to
    EPSG:4326!  The :class:`~ezdxf.addons.drawing.recorder.Recorder` and
    :class:`~ezdxf.addons.drawing.recorder.Player` classes can help to detect the
    extents of the DXF content.

    Default implementation:

    .. autofunction:: no_transform

    Factory function to make a transform function from WGS84 World Mercator
    `EPSG:3395 <https://epsg.io/3395>`_  coordinates to WGS84 (GPS) 
    `EPSG:4326 <https://epsg.io/4326>`_.

    .. autofunction:: make_world_mercator_to_gps_function

    The GeoJSON format supports only straight lines so curved shapes are flattened to
    polylines and polygons.

    The properties are handled as a foreign member feature and is therefore not defined
    in the GeoJSON specs.  It is possible to provide a custom function to create these
    property objects.

    Default implementation:

    .. autofunction:: properties_maker


    Args:
        properties_maker: function to create a properties dict.

    **Class Methods**

    .. automethod:: get_json_data

    .. automethod:: get_string

    .. versionadded:: 1.3.0
    
    .. _GeoJSON: https://geojson.org/
    """

    def __init__(
        self,
        properties_maker: PropertiesMaker = properties_maker,
        transform_func: TransformFunc = no_transform,
    ) -> None:
        super().__init__()
        self._properties_dict_maker = properties_maker
        self._transform_function = transform_func

    @override
    def get_json_data(self) -> dict[str, Any]:
        """Returns the result as a JSON-like data structure according the GeoJSON specs."""
        if len(self._entities) == 0:
            return {}
        using_features = self._entities[0]["type"] == "Feature"
        if using_features:
            return {"type": "FeatureCollection", "features": self._entities}
        else:
            return {"type": "GeometryCollection", "geometries": self._entities}

    def add_entity(self, entity: dict[str, Any], properties: BackendProperties):
        if not entity:
            return
        properties_dict: dict[str, Any] = self._properties_dict_maker(
            *self.make_properties(properties)
        )
        if properties_dict:
            self._entities.append(
                {
                    "type": "Feature",
                    "properties": properties_dict,
                    "geometry": entity,
                }
            )
        else:
            self._entities.append(entity)

    def make_properties(self, properties: BackendProperties) -> tuple[str, float, str]:
        if self.fixed_lineweight:
            stroke_width = self.fixed_lineweight
        else:
            stroke_width = max(
                self.min_lineweight, properties.lineweight * self.lineweight_scaling
            )
        return (properties.color, round(stroke_width, 2), properties.layer)

    @override
    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        self.add_entity(
            geojson_object("Point", list(self._transform_function(pos))), properties
        )

    @override
    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        tf = self._transform_function
        self.add_entity(
            geojson_object("LineString", [tf(start), tf(end)]),
            properties,
        )

    @override
    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        lines = list(lines)
        if len(lines) == 0:
            return
        tf = self._transform_function
        json_lines = [(tf(s), tf(e)) for s, e in lines]
        self.add_entity(geojson_object("MultiLineString", json_lines), properties)

    @override
    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        if len(path) == 0:
            return
        tf = self._transform_function
        vertices = [tf(v) for v in path.flattening(distance=self.max_sagitta)]
        self.add_entity(geojson_object("LineString", vertices), properties)

    @override
    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        paths = list(paths)
        if len(paths) == 0:
            return

        polygons: list[GeoJsonPolygon] = []
        for path in paths:
            if len(path):
                polygons.extend(
                    geojson_polygons(
                        path, max_sagitta=self.max_sagitta, tf=self._transform_function
                    )
                )
        if polygons:
            self.add_entity(geojson_object("MultiPolygon", polygons), properties)

    @override
    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        vertices: list[Vec2] = points.vertices()
        if len(vertices) < 3:
            return
        if not vertices[0].isclose(vertices[-1]):
            vertices.append(vertices[0])
        # exterior ring, without holes
        tf = self._transform_function
        self.add_entity(
            geojson_object("Polygon", [[tf(v) for v in vertices]]), properties
        )


def geojson_object(name: str, coordinates: Any) -> dict[str, Any]:
    return {"type": name, "coordinates": coordinates}


def geojson_ring(
    path: BkPath2d, is_hole: bool, max_sagitta: float, tf: TransformFunc
) -> Ring:
    """Returns a linear ring according to the GeoJSON specs.

    -  A linear ring is a closed LineString with four or more positions.
    -  The first and last positions are equivalent, and they MUST contain
       identical values; their representation SHOULD also be identical.
    -  A linear ring is the boundary of a surface or the boundary of a
       hole in a surface.
    -  A linear ring MUST follow the right-hand rule with respect to the
       area it bounds, i.e., exterior rings are counterclockwise, and
       holes are clockwise.

    """
    if path.has_sub_paths:
        raise TypeError("multi-paths not allowed")
    vertices: Ring = [tf(v) for v in path.flattening(max_sagitta)]
    if not path.is_closed:
        start = path.start
        vertices.append(tf(start))
    clockwise = path.has_clockwise_orientation()
    if (is_hole and not clockwise) or (not is_hole and clockwise):
        vertices.reverse()
    return vertices


def geojson_polygons(
    path: BkPath2d, max_sagitta: float, tf: TransformFunc
) -> list[GeoJsonPolygon]:
    """Returns a list of polygons, where each polygon is a list of an exterior path and
    optional holes e.g. [[ext0, hole0, hole1], [ext1], [ext2, hole0], ...].

    """
    sub_paths: list[BkPath2d] = path.sub_paths()
    if len(sub_paths) == 0:
        return []
    if len(sub_paths) == 1:
        return [[geojson_ring(sub_paths[0], False, max_sagitta, tf)]]

    polygons = nesting.make_polygon_structure(sub_paths)
    geojson_polygons: list[GeoJsonPolygon] = []
    for polygon in polygons:
        geojson_polygon: GeoJsonPolygon = [
            geojson_ring(polygon[0], False, max_sagitta, tf)
        ]  # exterior ring
        if len(polygon) > 1:
            # GeoJSON has no support for nested hole structures, so the sub polygons of
            # holes (hole[1]) are ignored yet!
            holes = polygon[1]
            if isinstance(holes, BkPath2d):  # single hole
                geojson_polygon.append(geojson_ring(holes, True, max_sagitta, tf))
                continue
            if isinstance(holes, (tuple, list)):  # multiple holes
                for hole in holes:
                    if isinstance(hole, (tuple, list)):  # nested polygon
                        # TODO: add sub polygons of holes as separated polygons
                        hole = hole[0]  # exterior path
                    geojson_polygon.append(geojson_ring(hole, True, max_sagitta, tf))

        geojson_polygons.append(geojson_polygon)
    return geojson_polygons
