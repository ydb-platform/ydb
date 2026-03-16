#  Copyright (c) 2020-2024, Manfred Moitzi
#  License: MIT License
"""
Implementation of the `__geo_interface__`: https://gist.github.com/sgillies/2217756

Which is also supported by Shapely: https://pypi.org/project/Shapely/

Type definitions see GeoJson Standard: https://tools.ietf.org/html/rfc7946
and examples : https://tools.ietf.org/html/rfc7946#appendix-A

GeoJSON Linter: https://geojsonlint.com/

"""
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    Union,
    cast,
    Callable,
    Sequence,
    Optional,
    Any,
    MutableMapping,
)
from typing_extensions import TypeAlias, Self
import numbers
import copy
import math
import enum
from ezdxf.math import (
    Vec3,
    has_clockwise_orientation,
    Matrix44,
    world_mercator_to_gps,
    gps_to_world_mercator,
)
from ezdxf.path import make_path, from_hatch_boundary_path, make_polygon_structure
from ezdxf.entities import DXFGraphic, LWPolyline, Point, Polyline, Line
from ezdxf.entities.polygon import DXFPolygon
from ezdxf.lldxf import const
from ezdxf.entities import factory


__all__ = ["proxy", "dxf_entities", "gfilter", "GeoProxy", "PolygonConversion"]

TYPE = "type"
COORDINATES = "coordinates"
POINT = "Point"
MULTI_POINT = "MultiPoint"
LINE_STRING = "LineString"
MULTI_LINE_STRING = "MultiLineString"
POLYGON = "Polygon"
MULTI_POLYGON = "MultiPolygon"
GEOMETRY_COLLECTION = "GeometryCollection"
GEOMETRIES = "geometries"
GEOMETRY = "geometry"
FEATURES = "features"
FEATURE = "Feature"
PROPERTIES = "properties"
FEATURE_COLLECTION = "FeatureCollection"
MAX_FLATTENING_DISTANCE = 0.1
SUPPORTED_DXF_TYPES = {
    "POINT",
    "LINE",
    "LWPOLYLINE",
    "POLYLINE",
    "HATCH",
    "MPOLYGON",
    "SOLID",
    "TRACE",
    "3DFACE",
    "CIRCLE",
    "ARC",
    "ELLIPSE",
    "SPLINE",
}

GeoMapping: TypeAlias = MutableMapping[str, Any]
PostProcessFunc: TypeAlias = Callable[[DXFGraphic, GeoMapping], None]


class PolygonConversion(enum.IntEnum):
    """Polygon conversion types as :class:`IntEnum`.

    Attributes:
        HATCH:
        POLYLINE:
        HATCH_AND_POLYLINE:
        MPOLYGON:

    """

    HATCH = 1
    POLYLINE = 2
    HATCH_AND_POLYLINE = 3
    MPOLYGON = 4


def proxy(
    entity: Union[DXFGraphic, Iterable[DXFGraphic]],
    distance: float = MAX_FLATTENING_DISTANCE,
    force_line_string: bool = False,
) -> GeoProxy:
    """Returns a :class:`GeoProxy` object.

    Args:
        entity: a single DXF entity or iterable of DXF entities
        distance: maximum flattening distance for curve approximations
        force_line_string: by default this function returns Polygon objects for
            closed geometries like CIRCLE, SOLID, closed POLYLINE and so on,
            by setting argument `force_line_string` to ``True``, this entities
            will be returned as LineString objects.

    """
    return GeoProxy.from_dxf_entities(entity, distance, force_line_string)


def dxf_entities(
    geo_mapping: GeoMapping,
    polygon=PolygonConversion.HATCH,
    dxfattribs=None,
    *,
    post_process: Optional[PostProcessFunc] = None,
) -> Iterator[DXFGraphic]:
    """Returns ``__geo_interface__`` mappings as DXF entities.

    The enum `polygon` determines the method to convert polygons,
    use :attr:`PolygonConversion.HATCH` for :class:`~ezdxf.entities.Hatch` entity,
    :attr:`PolygonConversion.POLYLINE` for :class:`~ezdxf.entities.LWPolyline` or
    :attr:`PolygonConversion.HATCH_AND_POLYLINE` for both.
    Option :attr:`PolygonConversion.POLYLINE` returns for the exterior path and each hole
    a separated :class:`LWPolyline` entity. The :class:`Hatch` entity supports holes,
    but has no explicit borderline.

    Yields :class:`Hatch` always before :class:`LWPolyline` entities.

    :attr:`PolygonConversion.MPOLYGON` support was added in v0.16.6, which is
    like a :class:`~ezdxf.entities.Hatch` entity  with additional borderlines,
    but the MPOLYGON entity is not a core DXF entity and DXF viewers,
    applications and libraries my not support this entity. The DXF attribute
    `color` defines the borderline color and `fill_color` the color of the
    solid filling.

    The returned DXF entities can be added to a layout by the
    :meth:`Layout.add_entity` method.

    Args:
        geo_mapping: ``__geo__interface__`` mapping as :class:`dict` or a Python
            object with a :attr:`__geo__interface__` property
        polygon: see :class:`PolygonConversion`
        dxfattribs: dict with additional DXF attributes
        post_process: post process function of type :class:`PostProcessFunc` that get the
            created DXF entity and the geo mapping as input, see reference implementation
            :func:`assign_layers`

    """
    return GeoProxy.parse(geo_mapping).to_dxf_entities(
        polygon, dxfattribs, post_process=post_process
    )


def gfilter(entities: Iterable[DXFGraphic]) -> Iterator[DXFGraphic]:
    """Filter DXF entities from iterable `entities`, which are incompatible to
    the ``__geo_reference__`` interface.
    """
    for e in entities:
        if isinstance(e, Polyline):
            if e.is_2d_polyline or e.is_3d_polyline:
                yield e
        elif e.dxftype() in SUPPORTED_DXF_TYPES:
            yield e


TFunc: TypeAlias = Callable[[Vec3], Vec3]


class GeoProxy:
    """Stores the ``__geo_interface__`` mapping in a parsed and compiled form.

    Stores coordinates as :class:`Vec3` objects and represents "Polygon"
    always as tuple (exterior, holes) even without holes.

    The GeoJSON specification recommends 6 decimal places for latitude and
    longitude which equates to roughly 10cm of precision. You may need
    slightly more for certain applications, 9 decimal places would be
    sufficient for professional survey-grade GPS coordinates.

    Args:
        geo_mapping: parsed and compiled ``__geo_interface__`` mapping
        places: decimal places to round for ``__geo_interface__`` export

    """

    def __init__(self, geo_mapping: GeoMapping, places: int = 6):
        self._root = geo_mapping
        self.places = places

    @classmethod
    def parse(cls, geo_mapping: GeoMapping) -> Self:
        """Parse and compile a ``__geo_interface__`` mapping as :class:`dict`
        or a Python object with a ``__geo_interface__`` property, does some
        basic syntax checks, converts all coordinates into :class:`Vec3`
        objects, represents "Polygon" always as tuple (exterior, holes) even
        without holes.

        """
        if hasattr(geo_mapping, "__geo_interface__"):
            geo_mapping = geo_mapping.__geo_interface__
        return cls(parse(geo_mapping))

    @property
    def root(self) -> GeoMapping:
        return self._root

    @property
    def geotype(self):
        """Property returns the top level entity type or ``None``."""
        return self._root.get("type")

    def __copy__(self) -> GeoProxy:
        """Returns a deep copy."""
        return copy.deepcopy(self)

    copy = __copy__

    @property
    def __geo_interface__(self) -> GeoMapping:
        """Returns the ``__geo_interface__`` compatible mapping as
        :class:`dict`.
        """
        return _rebuild(self._root, self.places)

    def __iter__(self) -> Iterator[GeoMapping]:
        """Iterate over all geometry entities.

        Yields only "Point", "LineString", "Polygon", "MultiPoint",
        "MultiLineString" and "MultiPolygon" objects, returns the content of
        "GeometryCollection", "FeatureCollection" and "Feature" as geometry
        objects ("Point", ...).

        """

        def _iter(node: GeoMapping) -> Iterator[GeoMapping]:
            type_ = node[TYPE]
            if type_ == FEATURE_COLLECTION:
                for feature in node[FEATURES]:
                    yield from _iter(feature)
            elif type_ == GEOMETRY_COLLECTION:
                for geometry in node[GEOMETRIES]:
                    yield from _iter(geometry)
            elif type_ == FEATURE:
                geometry = node[GEOMETRY]
                if geometry[TYPE] == GEOMETRY_COLLECTION:
                    yield from _iter(geometry)
                else:
                    yield geometry
            else:
                yield node

        yield from _iter(self._root)

    def filter(self, func: Callable[[GeoProxy], bool]) -> None:
        """Removes all mappings for which `func()` returns ``False``.
        The function only has to handle Point, LineString and Polygon entities,
        other entities like MultiPolygon are divided into separate entities
        also any collection.

        """

        def multi_entity(root, type_) -> bool:
            coordinates = []
            for entity in root[COORDINATES]:
                if func(GeoProxy({TYPE: type_, COORDINATES: entity})):
                    coordinates.append(entity)
            root[COORDINATES] = coordinates
            return bool(len(coordinates))

        def check(root) -> bool:
            type_ = root[TYPE]
            if type_ == FEATURE_COLLECTION:
                root[FEATURES] = [
                    feature for feature in root[FEATURES] if check(feature)
                ]
                return bool(len(root[FEATURES]))
            elif type_ == GEOMETRY_COLLECTION:
                root[GEOMETRIES] = [
                    geometry for geometry in root[GEOMETRIES] if check(geometry)
                ]
                return bool(len(root[GEOMETRIES]))
            elif type_ == FEATURE:
                root[GEOMETRY] = root[GEOMETRY] if check(root[GEOMETRY]) else {}
                return bool(root[GEOMETRY])
            elif type_ == MULTI_POINT:
                return multi_entity(root, POINT)
            elif type_ == MULTI_LINE_STRING:
                return multi_entity(root, LINE_STRING)
            elif type_ == MULTI_POLYGON:
                return multi_entity(root, POLYGON)
            else:
                return func(GeoProxy(root))

        if not check(self._root):
            self._root = {}

    def globe_to_map(self, func: Optional[TFunc] = None) -> None:
        """Transform all coordinates recursive from globe representation
        in longitude and latitude in decimal degrees into 2D map representation
        in meters.

        Default is WGS84 `EPSG:4326 <https://epsg.io/4326>`_ (GPS) to WGS84
        `EPSG:3395 <https://epsg.io/3395>`_ World Mercator function
        :func:`wgs84_4326_to_3395`.

        Use the `pyproj <https://pypi.org/project/pyproj/>`_ package to write
        a custom projection function as needed.

        Args:
            func: custom transformation function, which takes one
                :class:`Vec3` object as argument and returns the result as
                a :class:`Vec3` object.

        """
        if func is None:
            func = wgs84_4326_to_3395
        self.apply(func)

    def map_to_globe(self, func: Optional[TFunc] = None) -> None:
        """Transform all coordinates recursive from 2D map representation in
        meters into globe representation as longitude and latitude in decimal
        degrees.

        Default is WGS84 `EPSG:3395 <https://epsg.io/3395>`_ World Mercator
        to WGS84 `EPSG:4326 <https://epsg.io/4326>`_ GPS function
        :func:`wgs84_3395_to_4326`.

        Use the `pyproj <https://pypi.org/project/pyproj/>`_ package to write
        a custom projection function as needed.

        Args:
            func: custom transformation function, which takes one
                :class:`Vec3` object as argument and returns the result as
                a :class:`Vec3` object.

        """
        if func is None:
            func = wgs84_3395_to_4326
        self.apply(func)

    def crs_to_wcs(self, crs: Matrix44) -> None:
        """Transform all coordinates recursive from CRS into
        :ref:`WCS` coordinates by transformation matrix `crs` inplace,
        see also :meth:`GeoProxy.wcs_to_crs`.

        Args:
            crs: transformation matrix of type :class:`~ezdxf.math.Matrix44`

        """
        self.apply(crs.ucs_vertex_from_wcs)

    def wcs_to_crs(self, crs: Matrix44) -> None:
        """Transform all coordinates recursive from :ref:`WCS` coordinates into
        Coordinate Reference System (CRS) by transformation matrix `crs`
        inplace.

        The CRS is defined by the :class:`~ezdxf.entities.GeoData` entity,
        get the :class:`GeoData` entity from the modelspace by method
        :meth:`~ezdxf.layouts.Modelspace.get_geodata`.
        The CRS transformation matrix can be acquired form the :class:`GeoData`
        object by :meth:`~ezdxf.entities.GeoData.get_crs_transformation` method:

        .. code:: Python

            doc = ezdxf.readfile('file.dxf')
            msp = doc.modelspace()
            geodata = msp.get_geodata()
            if geodata:
                matrix, axis_ordering = geodata.get_crs_transformation()

        If `axis_ordering` is ``False`` the CRS is not compatible with the
        ``__geo_interface__`` or GeoJSON (see chapter 3.1.1).

        Args:
            crs: transformation matrix of type :class:`~ezdxf.math.Matrix44`

        """

        self.apply(crs.transform)

    def apply(self, func: TFunc) -> None:
        """Apply the transformation function `func` recursive to all
        coordinates.

        Args:
            func: transformation function as Callable[[Vec3], Vec3]

        """

        def process(entity: GeoMapping):
            def transform(coords):
                if isinstance(coords, Vec3):
                    return func(coords)
                else:
                    return [transform(c) for c in coords]

            entity[COORDINATES] = transform(entity[COORDINATES])

        for entity in self.__iter__():
            process(entity)

    @classmethod
    def from_dxf_entities(
        cls,
        entity: Union[DXFGraphic, Iterable[DXFGraphic]],
        distance: float = MAX_FLATTENING_DISTANCE,
        force_line_string: bool = False,
    ) -> GeoProxy:
        """Constructor from a single DXF entity or an iterable of DXF entities.

        Args:
            entity: DXF entity or entities
            distance: maximum flattening distance for curve approximations
            force_line_string: by default this function returns Polygon objects for
                closed geometries like CIRCLE, SOLID, closed POLYLINE and so on,
                by setting argument `force_line_string` to ``True``, this entities
                will be returned as LineString objects.

        """
        if isinstance(entity, DXFGraphic):
            m = mapping(entity, distance, force_line_string)
        else:
            m = collection(entity, distance, force_line_string)
        return cls(m)

    def to_dxf_entities(
        self,
        polygon=PolygonConversion.HATCH,
        dxfattribs=None,
        *,
        post_process: Optional[PostProcessFunc] = None,
    ) -> Iterator[DXFGraphic]:
        """Returns stored ``__geo_interface__`` mappings as DXF entities.

        The `polygon` argument determines the method to convert polygons,
        use 1 for :class:`~ezdxf.entities.Hatch` entity, 2 for
        :class:`~ezdxf.entities.LWPolyline` or 3 for both.
        Option 2 returns for the exterior path and each hole a separated
        :class:`LWPolyline` entity. The :class:`Hatch` entity supports holes,
        but has no explicit borderline.

        Yields :class:`Hatch` always before :class:`LWPolyline` entities.

        :class:`~ezdxf.entities.MPolygon` support was added in v0.16.6, which is
        like a :class:`~ezdxf.entities.Hatch` entity  with additional borderlines,
        but the MPOLYGON entity is not a core DXF entity and DXF viewers,
        applications and libraries my not support this entity. The DXF attribute
        `color` defines the borderline color and `fill_color` the color of the
        solid filling.

        The returned DXF entities can be added to a layout by the
        :meth:`Layout.add_entity` method.

        Args:
            polygon: see :class:`PolygonConversion`
            dxfattribs: dict with additional DXF attributes
            post_process: post process function of type :class:`PostProcesFunc` that get the
                created DXF entity and the geo mapping as input, see reference implementation
                :func:`assign_layers`

        """

        def point(vertex: Sequence) -> Point:
            point = cast(Point, factory.new("POINT", dxfattribs=dxfattribs))
            point.dxf.location = vertex
            return point

        def lwpolyline(vertices: Sequence) -> LWPolyline:
            polyline = cast(
                LWPolyline, factory.new("LWPOLYLINE", dxfattribs=dxfattribs)
            )
            polyline.append_points(vertices, format="xy")
            return polyline

        def polygon_(exterior: list, holes: list) -> Iterator[DXFGraphic]:
            if polygon == PolygonConversion.MPOLYGON:
                yield mpolygon_(exterior, holes)
                # the following DXF entities do not support the
                # "fill_color" attribute
                return
            if polygon & PolygonConversion.HATCH:
                yield hatch_(exterior, holes)
            if polygon & PolygonConversion.POLYLINE:
                for path in [exterior] + holes:
                    yield lwpolyline(path)

        def dxf_polygon_(
            dxftype: str, exterior: Sequence, holes: Sequence
        ) -> DXFPolygon:
            dxf_polygon = cast(DXFPolygon, factory.new(dxftype, dxfattribs=dxfattribs))
            dxf_polygon.dxf.hatch_style = const.HATCH_STYLE_OUTERMOST
            dxf_polygon.paths.add_polyline_path(
                exterior, flags=const.BOUNDARY_PATH_EXTERNAL
            )
            for hole in holes:
                dxf_polygon.paths.add_polyline_path(
                    hole, flags=const.BOUNDARY_PATH_OUTERMOST
                )
            return dxf_polygon

        def hatch_(exterior: Sequence, holes: Sequence) -> DXFPolygon:
            return dxf_polygon_("HATCH", exterior, holes)

        def mpolygon_(exterior: Sequence, holes: Sequence) -> DXFPolygon:
            return dxf_polygon_("MPOLYGON", exterior, holes)

        def entity(type_, coordinates) -> Iterator[DXFGraphic]:
            if type_ == POINT:
                yield point(coordinates)
            elif type_ == LINE_STRING:
                yield lwpolyline(coordinates)
            elif type_ == POLYGON:
                exterior, holes = coordinates
                yield from polygon_(exterior, holes)
            elif type_ == MULTI_POINT:
                for data in coordinates:
                    yield point(data)
            elif type_ == MULTI_LINE_STRING:
                for data in coordinates:
                    yield lwpolyline(data)
            elif type_ == MULTI_POLYGON:
                for data in coordinates:
                    exterior, holes = data
                    yield from polygon_(exterior, holes)

        if polygon < 1 or polygon > 4:
            raise ValueError(f"invalid value for polygon: {polygon}")

        dxfattribs = dict(dxfattribs or {})
        for feature, geometry in iter_features(self._root):
            type_ = geometry.get(TYPE)
            for e in entity(type_, geometry.get(COORDINATES)):
                if post_process:
                    post_process(e, feature)
                yield e


def iter_features(geo_mapping: GeoMapping) -> Iterator[tuple[GeoMapping, GeoMapping]]:
    """Yields all geometries of a ``__geo_mapping__`` as (`feature`, `geometry`) tuples.

    If no feature is defined the `feature` value is an empty ``dict``. When a `feature`
    contains `GeometryCollections`, the function yields for each sub-geometry a separate
    (`feature`, `geometry`) tuple.

    """
    current_feature: GeoMapping = {}

    def features(node: GeoMapping) -> Iterator[tuple[GeoMapping, GeoMapping]]:
        nonlocal current_feature

        type_ = node[TYPE]
        if type_ == FEATURE_COLLECTION:
            for feature in node[FEATURES]:
                yield from features(feature)
        elif type_ == GEOMETRY_COLLECTION:
            for geometry in node[GEOMETRIES]:
                yield from features(geometry)
        elif type_ == FEATURE:
            current_feature = node
            geometry = node[GEOMETRY]
            if geometry[TYPE] == GEOMETRY_COLLECTION:
                yield from features(geometry)
            else:
                yield current_feature, geometry
        else:
            yield current_feature, node

    yield from features(geo_mapping)


def parse(geo_mapping: GeoMapping) -> GeoMapping:
    """Parse ``__geo_interface__`` convert all coordinates into
    :class:`Vec3` objects, Polygon['coordinates'] is always a
    tuple (exterior, holes), holes maybe an empty list.

    """
    geo_mapping = copy.deepcopy(geo_mapping)
    type_ = geo_mapping.get(TYPE)
    if type_ is None:
        raise ValueError(f'Required key "{TYPE}" not found.')

    if type_ == FEATURE_COLLECTION:
        # It is possible for this array to be empty.
        features = geo_mapping.get(FEATURES)
        if features:
            geo_mapping[FEATURES] = [parse(f) for f in features]
        else:
            raise ValueError(f'Missing key "{FEATURES}" in FeatureCollection.')
    elif type_ == GEOMETRY_COLLECTION:
        # It is possible for this array to be empty.
        geometries = geo_mapping.get(GEOMETRIES)
        if geometries:
            geo_mapping[GEOMETRIES] = [parse(g) for g in geometries]
        else:
            raise ValueError(f'Missing key "{GEOMETRIES}" in GeometryCollection.')
    elif type_ == FEATURE:
        # The value of the geometry member SHALL be either a Geometry object
        # or, in the case that the Feature is unlocated, a JSON null value.
        if GEOMETRY in geo_mapping:
            geometry = geo_mapping.get(GEOMETRY)
            geo_mapping[GEOMETRY] = parse(geometry) if geometry else None
        else:
            raise ValueError(f'Missing key "{GEOMETRY}" in Feature.')
    elif type_ in {
        POINT,
        LINE_STRING,
        POLYGON,
        MULTI_POINT,
        MULTI_LINE_STRING,
        MULTI_POLYGON,
    }:
        coordinates = geo_mapping.get(COORDINATES)
        if coordinates is None:
            raise ValueError(f'Missing key "{COORDINATES}" in {type_}.')
        if type_ == POINT:
            coordinates = Vec3(coordinates)
        elif type_ in (LINE_STRING, MULTI_POINT):
            coordinates = Vec3.list(coordinates)
        elif type_ == POLYGON:
            coordinates = _parse_polygon(coordinates)
        elif type_ == MULTI_LINE_STRING:
            coordinates = [Vec3.list(v) for v in coordinates]
        elif type_ == MULTI_POLYGON:
            coordinates = [_parse_polygon(v) for v in coordinates]
        geo_mapping[COORDINATES] = coordinates
    else:
        raise TypeError(f'Invalid type "{type_}".')
    return geo_mapping


def _is_coordinate_sequence(coordinates: Sequence) -> bool:
    """Returns ``True`` for a sequence of coordinates like [(0, 0), (1, 0)]
    and ``False`` for a sequence of sequences:
    [[(0, 0), (1, 0)], [(2, 0), (3, 0)]]
    """
    if not isinstance(coordinates, Sequence):
        raise ValueError("Invalid coordinate sequence.")
    if len(coordinates) == 0:
        raise ValueError("Invalid coordinate sequence.")
    first_item = coordinates[0]
    if len(first_item) == 0:
        raise ValueError("Invalid coordinate sequence.")
    return isinstance(first_item[0], numbers.Real)


def _parse_polygon(coordinates: Sequence) -> Sequence:
    """Returns polygon definition as tuple (exterior, [holes])."""
    if _is_coordinate_sequence(coordinates):
        exterior = coordinates
        holes: Sequence = []
    else:
        exterior = coordinates[0]
        holes = coordinates[1:]
    return Vec3.list(exterior), [Vec3.list(h) for h in holes]


def _rebuild(geo_mapping: GeoMapping, places: int = 6) -> GeoMapping:
    """Returns ``__geo_interface__`` compatible mapping as :class:`dict` from
    compiled internal representation.

    """

    def vertex_2d(v: Vec3) -> tuple[float, float]:
        return round(v.x, places), round(v.y, places)

    def vertex_3d(v: Vec3) -> tuple[float, float, float]:
        return round(v.x, places), round(v.y, places), round(v.z, places)

    def vertices(
        coords: Sequence[Vec3],
    ) -> list[tuple[float, float] | tuple[float, float, float]]:
        if any(v.z for v in coords):
            return [vertex_3d(v) for v in coords]
        return [vertex_2d(v) for v in coords]

    def _polygon(exterior, holes):
        # For type "Polygon", the "coordinates" member MUST be an array of
        # linear ring coordinate arrays.
        return [vertices(ring) for ring in [exterior] + holes]

    geo_interface = dict(geo_mapping)
    type_ = geo_interface[TYPE]
    if type_ == FEATURE_COLLECTION:
        geo_interface[FEATURES] = [_rebuild(f) for f in geo_interface[FEATURES]]
    elif type_ == GEOMETRY_COLLECTION:
        geo_interface[GEOMETRIES] = [_rebuild(g) for g in geo_interface[GEOMETRIES]]
    elif type_ == FEATURE:
        geo_interface[GEOMETRY] = _rebuild(geo_interface[GEOMETRY])
    elif type_ == POINT:
        v = geo_interface[COORDINATES]
        geo_interface[COORDINATES] = vertex_3d(v) if v.z else vertex_2d(v)
    elif type_ in (LINE_STRING, MULTI_POINT):
        coordinates = geo_interface[COORDINATES]
        geo_interface[COORDINATES] = vertices(coordinates)
    elif type_ == MULTI_LINE_STRING:
        coordinates = []
        for line in geo_interface[COORDINATES]:
            coordinates.append(vertices(line))
        geo_interface[COORDINATES] = coordinates
    elif type_ == POLYGON:
        geo_interface[COORDINATES] = _polygon(*geo_interface[COORDINATES])
    elif type_ == MULTI_POLYGON:
        geo_interface[COORDINATES] = [
            _polygon(exterior, holes) for exterior, holes in geo_interface[COORDINATES]
        ]
    return geo_interface


def mapping(
    entity: DXFGraphic,
    distance: float = MAX_FLATTENING_DISTANCE,
    force_line_string: bool = False,
) -> GeoMapping:
    """Create the compiled ``__geo_interface__`` mapping as :class:`dict`
    for the given DXF `entity`, all coordinates are :class:`Vec3` objects and
    represents "Polygon" always as tuple (exterior, holes) even without holes.


    Internal API - result is **not** a valid ``_geo_interface__`` mapping!

    Args:
        entity: DXF entity
        distance: maximum flattening distance for curve approximations
        force_line_string: by default this function returns Polygon objects for
            closed geometries like CIRCLE, SOLID, closed POLYLINE and so on,
            by setting argument `force_line_string` to ``True``, this entities
            will be returned as LineString objects.

    """

    dxftype = entity.dxftype()
    if isinstance(entity, Point):
        return {TYPE: POINT, COORDINATES: entity.dxf.location}
    elif isinstance(entity, Line):
        return line_string_mapping([entity.dxf.start, entity.dxf.end])
    elif isinstance(entity, Polyline):
        if entity.is_3d_polyline or entity.is_2d_polyline:
            # May contain arcs as bulge values:
            path = make_path(entity)
            points = list(path.flattening(distance))
            return _line_string_or_polygon_mapping(points, force_line_string)
        else:
            raise TypeError("Polymesh and Polyface not supported.")
    elif isinstance(entity, LWPolyline):
        # May contain arcs as bulge values:
        path = make_path(entity)
        points = list(path.flattening(distance))
        return _line_string_or_polygon_mapping(points, force_line_string)
    elif dxftype in {"CIRCLE", "ARC", "ELLIPSE", "SPLINE"}:
        return _line_string_or_polygon_mapping(
            list(entity.flattening(distance)), force_line_string  # type: ignore
        )
    elif dxftype in {"SOLID", "TRACE", "3DFACE"}:
        return _line_string_or_polygon_mapping(
            entity.wcs_vertices(close=True), force_line_string  # type: ignore
        )
    elif isinstance(entity, DXFPolygon):
        return _hatch_as_polygon(entity, distance, force_line_string)
    else:
        raise TypeError(dxftype)


def _line_string_or_polygon_mapping(points: list[Vec3], force_line_string: bool):
    len_ = len(points)
    if len_ < 2:
        raise ValueError("Invalid vertex count.")
    if len_ == 2 or force_line_string:
        return line_string_mapping(points)
    else:
        if is_linear_ring(points):
            return polygon_mapping(points, [])
        else:
            return line_string_mapping(points)


def _hatch_as_polygon(
    hatch: DXFPolygon, distance: float, force_line_string: bool
) -> GeoMapping:
    def boundary_to_vertices(boundary) -> list[Vec3]:
        path = from_hatch_boundary_path(boundary, ocs, elevation)
        return path_to_vertices(path)

    def path_to_vertices(path) -> list[Vec3]:
        path.close()
        return list(path.flattening(distance))

    # Path vertex winding order can be ignored here, validation and
    # correction is done in polygon_mapping().

    elevation = hatch.dxf.elevation.z
    ocs = hatch.ocs()
    hatch_style = hatch.dxf.hatch_style

    # Returns boundaries in EXTERNAL, OUTERMOST and DEFAULT order and filters
    # unused boundaries according the hatch style:
    boundaries = list(hatch.paths.rendering_paths(hatch_style))
    count = len(boundaries)
    if count == 0:
        raise ValueError(f"{hatch.dxftype()} without any boundary path.")
    # Take first path as exterior path, multiple EXTERNAL paths are possible
    exterior = boundaries[0]
    if count == 1 or hatch_style == const.HATCH_STYLE_IGNORE:
        points = boundary_to_vertices(exterior)
        return _line_string_or_polygon_mapping(points, force_line_string)
    else:
        if force_line_string:
            # Build a MultiString collection:
            points = boundary_to_vertices(exterior)
            geometries = [_line_string_or_polygon_mapping(points, force_line_string)]
            # All other boundary paths are treated as holes
            for hole in boundaries[1:]:
                points = boundary_to_vertices(hole)
                geometries.append(
                    _line_string_or_polygon_mapping(points, force_line_string)
                )
            return join_multi_single_type_mappings(geometries)
        else:
            # Multiple separated polygons are possible in one HATCH entity:
            polygons = []
            for exterior, holes in _boundaries_to_polygons(boundaries, ocs, elevation):
                points = path_to_vertices(exterior)
                polygons.append(
                    polygon_mapping(points, [path_to_vertices(hole) for hole in holes])
                )
            if len(polygons) > 1:
                return join_multi_single_type_mappings(polygons)
            return polygons[0]


def _boundaries_to_polygons(boundaries, ocs, elevation):
    paths = (
        from_hatch_boundary_path(boundary, ocs, elevation) for boundary in boundaries
    )
    for polygon in make_polygon_structure(paths):
        exterior = polygon[0]
        # only take exterior path of level 1 holes, nested holes are ignored
        yield exterior, [hole[0] for hole in polygon[1:]]


def collection(
    entities: Iterable[DXFGraphic],
    distance: float = MAX_FLATTENING_DISTANCE,
    force_line_string: bool = False,
) -> GeoMapping:
    """Create the ``__geo_interface__`` mapping as :class:`dict` for the
    given DXF `entities`, see https://gist.github.com/sgillies/2217756

    Returns a "MultiPoint", "MultiLineString" or "MultiPolygon" collection if
    all entities return the same GeoJSON type ("Point", "LineString", "Polygon")
    else a "GeometryCollection".

    Internal API - result is **not** a valid ``_geo_interface__`` mapping!

    Args:
        entities: iterable of DXF entities
        distance: maximum flattening distance for curve approximations
        force_line_string: by default this function returns "Polygon" objects for
            closed geometries like CIRCLE, SOLID, closed POLYLINE and so on,
            by setting argument `force_line_string` to ``True``, this entities
            will be returned as "LineString" objects.
    """
    m = [mapping(e, distance, force_line_string) for e in entities]
    types = set(g[TYPE] for g in m)
    if len(types) > 1:
        return geometry_collection_mapping(m)
    else:
        return join_multi_single_type_mappings(m)


def line_string_mapping(points: list[Vec3]) -> GeoMapping:
    """Returns a "LineString" mapping.

    .. code::

        {
            "type": "LineString",
            "coordinates": [
                (100.0, 0.0),
                (101.0, 1.0)
            ]
        }
    """

    return {TYPE: LINE_STRING, COORDINATES: points}


def is_linear_ring(points: list[Vec3]):
    return points[0].isclose(points[-1])


# GeoJSON : A linear ring MUST follow the right-hand rule with respect
# to the area it bounds, i.e., exterior rings are counterclockwise, and
# holes are clockwise.
def linear_ring(points: list[Vec3], ccw=True) -> list[Vec3]:
    """Return `points` as linear ring (last vertex == first vertex),
    argument `ccw` defines the winding orientation, ``True`` for counter-clock
    wise and ``False`` for clock wise.

    """
    if len(points) < 3:
        raise ValueError(f"Invalid vertex count: {len(points)}")
    if not points[0].isclose(points[-1]):
        points.append(points[0])

    if has_clockwise_orientation(points):
        if ccw:
            points.reverse()
    else:
        if not ccw:
            points.reverse()

    return points


def polygon_mapping(points: list[Vec3], holes: list[list[Vec3]]) -> GeoMapping:
    """Returns a "Polygon" mapping.

    .. code::

        {
            "type": "Polygon",
            "coordinates": [
                 [
                     (100.0, 0.0),
                     (101.0, 0.0),
                     (101.0, 1.0),
                     (100.0, 1.0),
                     (100.0, 0.0)
                 ],
                 [
                     (100.8, 0.8),
                     (100.8, 0.2),
                     (100.2, 0.2),
                     (100.2, 0.8),
                     (100.8, 0.8)
                 ]
            ]
        }
    """

    exterior = linear_ring(points, ccw=True)
    if holes:
        holes = [linear_ring(hole, ccw=False) for hole in holes]
        rings = exterior, holes
    else:
        rings = exterior, []
    return {
        TYPE: POLYGON,
        COORDINATES: rings,
    }


def join_multi_single_type_mappings(geometries: Iterable[GeoMapping]) -> GeoMapping:
    """Returns multiple geometries as a "MultiPoint", "MultiLineString" or
    "MultiPolygon" mapping.
    """
    types = set()
    data = list()
    for g in geometries:
        types.add(g[TYPE])
        data.append(g[COORDINATES])

    if len(types) > 1:
        raise TypeError(f"Type mismatch: {str(types)}")
    elif len(types) == 0:
        return dict()
    else:
        return {TYPE: "Multi" + tuple(types)[0], COORDINATES: data}


def geometry_collection_mapping(geometries: Iterable[GeoMapping]) -> GeoMapping:
    """Returns multiple geometries as a "GeometryCollection" mapping."""
    return {TYPE: GEOMETRY_COLLECTION, GEOMETRIES: list(geometries)}


# Values stored in GeoData RSS tag are not precise enough to match
# control calculation at epsg.io:
# Semi Major Axis: 6.37814e+06
# Semi Minor Axis: 6.35675e+06

WGS84_SEMI_MAJOR_AXIS = 6378137
WGS84_SEMI_MINOR_AXIS = 6356752.3142
WGS84_ELLIPSOID_ECCENTRIC = math.sqrt(
    1.0 - WGS84_SEMI_MINOR_AXIS**2 / WGS84_SEMI_MAJOR_AXIS**2
)
CONST_E2 = math.e / 2.0
CONST_PI_2 = math.pi / 2.0
CONST_PI_4 = math.pi / 4.0


def wgs84_4326_to_3395(location: Vec3) -> Vec3:
    """Transform WGS84 `EPSG:4326 <https://epsg.io/4326>`_ location given as
    latitude and longitude in decimal degrees as used by GPS into World Mercator
    cartesian 2D coordinates in meters `EPSG:3395 <https://epsg.io/3395>`_.

    Args:
        location: :class:`Vec3` object, x-attribute represents the longitude
            value (East-West) in decimal degrees and the y-attribute
            represents the latitude value (North-South) in decimal degrees.
    """
    return Vec3(gps_to_world_mercator(location.x, location.y))


def wgs84_3395_to_4326(location: Vec3, tol: float = 1e-6) -> Vec3:
    """Transform WGS84 World Mercator `EPSG:3395 <https://epsg.io/3395>`_
    location given as cartesian 2D coordinates x, y in meters into WGS84 decimal
    degrees as longitude and latitude `EPSG:4326 <https://epsg.io/4326>`_ as
    used by GPS.

    Args:
        location: :class:`Vec3` object, z-axis is ignored
        tol: accuracy for latitude calculation

    """
    return Vec3(world_mercator_to_gps(location.x, location.y, tol))


def dms2dd(d: float, m: float = 0, s: float = 0) -> float:
    """Convert degree, minutes, seconds into decimal degrees."""
    dd = d + float(m) / 60 + float(s) / 3600
    return dd


def dd2dms(dd: float) -> tuple[float, float, float]:
    """Convert decimal degrees into degree, minutes, seconds."""
    m, s = divmod(dd * 3600, 60)
    d, m = divmod(m, 60)
    return d, m, s


def assign_layers(entity: DXFGraphic, mapping: GeoMapping) -> None:
    """Reference implementation for a :func:`post_process` function.

    .. seealso::

        :func:`dxf_entities`

    """
    properties = mapping.get(PROPERTIES)
    if properties is None:
        return
    layer = properties.get("layer")
    if layer:
        entity.dxf.layer = layer
