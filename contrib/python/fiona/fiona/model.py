"""Fiona data model"""

from binascii import hexlify
from collections.abc import MutableMapping
from enum import Enum
import itertools
from json import JSONEncoder
import reprlib
from warnings import warn

from fiona.errors import FionaDeprecationWarning

_model_repr = reprlib.Repr()
_model_repr.maxlist = 1
_model_repr.maxdict = 5


class OGRGeometryType(Enum):
    Unknown = 0
    Point = 1
    LineString = 2
    Polygon = 3
    MultiPoint = 4
    MultiLineString = 5
    MultiPolygon = 6
    GeometryCollection = 7
    CircularString = 8
    CompoundCurve = 9
    CurvePolygon = 10
    MultiCurve = 11
    MultiSurface = 12
    Curve = 13
    Surface = 14
    PolyhedralSurface = 15
    TIN = 16
    Triangle = 17
    NONE = 100
    LinearRing = 101
    CircularStringZ = 1008
    CompoundCurveZ = 1009
    CurvePolygonZ = 1010
    MultiCurveZ = 1011
    MultiSurfaceZ = 1012
    CurveZ = 1013
    SurfaceZ = 1014
    PolyhedralSurfaceZ = 1015
    TINZ = 1016
    TriangleZ = 1017
    PointM = 2001
    LineStringM = 2002
    PolygonM = 2003
    MultiPointM = 2004
    MultiLineStringM = 2005
    MultiPolygonM = 2006
    GeometryCollectionM = 2007
    CircularStringM = 2008
    CompoundCurveM = 2009
    CurvePolygonM = 2010
    MultiCurveM = 2011
    MultiSurfaceM = 2012
    CurveM = 2013
    SurfaceM = 2014
    PolyhedralSurfaceM = 2015
    TINM = 2016
    TriangleM = 2017
    PointZM = 3001
    LineStringZM = 3002
    PolygonZM = 3003
    MultiPointZM = 3004
    MultiLineStringZM = 3005
    MultiPolygonZM = 3006
    GeometryCollectionZM = 3007
    CircularStringZM = 3008
    CompoundCurveZM = 3009
    CurvePolygonZM = 3010
    MultiCurveZM = 3011
    MultiSurfaceZM = 3012
    CurveZM = 3013
    SurfaceZM = 3014
    PolyhedralSurfaceZM = 3015
    TINZM = 3016
    TriangleZM = 3017
    Point25D = 0x80000001
    LineString25D = 0x80000002
    Polygon25D = 0x80000003
    MultiPoint25D = 0x80000004
    MultiLineString25D = 0x80000005
    MultiPolygon25D = 0x80000006
    GeometryCollection25D = 0x80000007


# Mapping of OGR integer geometry types to GeoJSON type names.
_GEO_TYPES = {
    OGRGeometryType.Unknown.value: "Unknown",
    OGRGeometryType.Point.value: "Point",
    OGRGeometryType.LineString.value: "LineString",
    OGRGeometryType.Polygon.value: "Polygon",
    OGRGeometryType.MultiPoint.value: "MultiPoint",
    OGRGeometryType.MultiLineString.value: "MultiLineString",
    OGRGeometryType.MultiPolygon.value: "MultiPolygon",
    OGRGeometryType.GeometryCollection.value: "GeometryCollection"
}

GEOMETRY_TYPES = {
    **_GEO_TYPES,
    OGRGeometryType.NONE.value: "None",
    OGRGeometryType.LinearRing.value: "LinearRing",
    OGRGeometryType.Point25D.value: "3D Point",
    OGRGeometryType.LineString25D.value: "3D LineString",
    OGRGeometryType.Polygon25D.value: "3D Polygon",
    OGRGeometryType.MultiPoint25D.value: "3D MultiPoint",
    OGRGeometryType.MultiLineString25D.value: "3D MultiLineString",
    OGRGeometryType.MultiPolygon25D.value: "3D MultiPolygon",
    OGRGeometryType.GeometryCollection25D.value: "3D GeometryCollection",
}


class Object(MutableMapping):
    """Base class for CRS, geometry, and feature objects

    In Fiona 2.0, the implementation of those objects will change.  They
    will no longer be dicts or derive from dict, and will lose some
    features like mutability and default JSON serialization.

    Object will be used for these objects in Fiona 1.9. This class warns
    about future deprecation of features.
    """

    _delegated_properties = []

    def __init__(self, **kwds):
        self._data = dict(**kwds)

    def _props(self):
        return {
            k: getattr(self._delegate, k)
            for k in self._delegated_properties
            if k is not None  # getattr(self._delegate, k) is not None
        }

    def __getitem__(self, item):
        if item in self._delegated_properties:
            return getattr(self._delegate, item)
        else:
            props = {
                k: (dict(v) if isinstance(v, Object) else v)
                for k, v in self._props().items()
            }
            props.update(**self._data)
            return props[item]

    def __iter__(self):
        props = self._props()
        return itertools.chain(iter(props), iter(self._data))

    def __len__(self):
        props = self._props()
        return len(props) + len(self._data)

    def __repr__(self):
        kvs = [
            f"{k}={v!r}"
            for k, v in itertools.chain(self._props().items(), self._data.items())
        ]
        return "fiona.{}({})".format(self.__class__.__name__, ", ".join(kvs))

    def __setitem__(self, key, value):
        warn(
            "instances of this class -- CRS, geometry, and feature objects -- will become immutable in fiona version 2.0",
            FionaDeprecationWarning,
            stacklevel=2,
        )
        if key in self._delegated_properties:
            setattr(self._delegate, key, value)
        else:
            self._data[key] = value

    def __delitem__(self, key):
        warn(
            "instances of this class -- CRS, geometry, and feature objects -- will become immutable in fiona version 2.0",
            FionaDeprecationWarning,
            stacklevel=2,
        )
        if key in self._delegated_properties:
            setattr(self._delegate, key, None)
        else:
            del self._data[key]

    def __eq__(self, other):
        return dict(**self) == dict(**other)


class _Geometry:
    def __init__(self, coordinates=None, type=None, geometries=None):
        self.coordinates = coordinates
        self.type = type
        self.geometries = geometries


class Geometry(Object):
    """A GeoJSON-like geometry

    Notes
    -----
    Delegates coordinates and type properties to an instance of
    _Geometry, which will become an extension class in Fiona 2.0.

    """

    _delegated_properties = ["coordinates", "type", "geometries"]

    def __init__(self, coordinates=None, type=None, geometries=None, **data):
        self._delegate = _Geometry(
            coordinates=coordinates, type=type, geometries=geometries
        )
        super().__init__(**data)

    def __repr__(self):
        kvs = [f"{k}={_model_repr.repr(v)}" for k, v in self.items() if v is not None]
        return "fiona.Geometry({})".format(", ".join(kvs))

    @classmethod
    def from_dict(cls, ob=None, **kwargs):
        if ob is not None:
            data = dict(getattr(ob, "__geo_interface__", ob))
            data.update(kwargs)
        else:
            data = kwargs

        if "geometries" in data and data["type"] == "GeometryCollection":
            _ = data.pop("coordinates", None)
            _ = data.pop("type", None)
            return Geometry(
                type="GeometryCollection",
                geometries=[
                    Geometry.from_dict(part) for part in data.pop("geometries")
                ],
                **data
            )
        else:
            _ = data.pop("geometries", None)
            return Geometry(
                type=data.pop("type", None),
                coordinates=data.pop("coordinates", []),
                **data
            )

    @property
    def coordinates(self):
        """The geometry's coordinates

        Returns
        -------
        Sequence

        """
        return self._delegate.coordinates

    @property
    def type(self):
        """The geometry's type

        Returns
        -------
        str

        """
        return self._delegate.type

    @property
    def geometries(self):
        """A collection's geometries.

        Returns
        -------
        list

        """
        return self._delegate.geometries

    @property
    def __geo_interface__(self):
        return ObjectEncoder().default(self)


class _Feature:
    def __init__(self, geometry=None, id=None, properties=None):
        self.geometry = geometry
        self.id = id
        self.properties = properties


class Feature(Object):
    """A GeoJSON-like feature

    Notes
    -----
    Delegates geometry and properties to an instance of _Feature, which
    will become an extension class in Fiona 2.0.

    """

    _delegated_properties = ["geometry", "id", "properties"]

    def __init__(self, geometry=None, id=None, properties=None, **data):
        if properties is None:
            properties = Properties()
        self._delegate = _Feature(geometry=geometry, id=id, properties=properties)
        super().__init__(**data)

    @classmethod
    def from_dict(cls, ob=None, **kwargs):
        if ob is not None:
            data = dict(getattr(ob, "__geo_interface__", ob))
            data.update(kwargs)
        else:
            data = kwargs
        geom_data = data.pop("geometry", None)

        if isinstance(geom_data, Geometry):
            geom = geom_data
        else:
            geom = Geometry.from_dict(geom_data) if geom_data is not None else None

        props_data = data.pop("properties", None)

        if isinstance(props_data, Properties):
            props = props_data
        else:
            props = Properties(**props_data) if props_data is not None else None

        fid = data.pop("id", None)
        return Feature(geometry=geom, id=fid, properties=props, **data)

    def __eq__(self, other):
        return (
            self.geometry == other.geometry
            and self.id == other.id
            and self.properties == other.properties
        )

    @property
    def geometry(self):
        """The feature's geometry object

        Returns
        -------
        Geometry

        """
        return self._delegate.geometry

    @property
    def id(self):
        """The feature's id

        Returns
        ------
        object

        """
        return self._delegate.id

    @property
    def properties(self):
        """The feature's properties

        Returns
        -------
        object

        """
        return self._delegate.properties

    @property
    def type(self):
        """The Feature's type

        Returns
        -------
        str

        """
        return "Feature"

    @property
    def __geo_interface__(self):
        return ObjectEncoder().default(self)


class Properties(Object):
    """A GeoJSON-like feature's properties"""

    def __init__(self, **kwds):
        super().__init__(**kwds)

    @classmethod
    def from_dict(cls, mapping=None, **kwargs):
        if mapping:
            return Properties(**mapping, **kwargs)
        return Properties(**kwargs)


class ObjectEncoder(JSONEncoder):
    """Encodes Geometry, Feature, and Properties."""

    def default(self, o):
        if isinstance(o, Object):
            o_dict = {
                k: self.default(v)
                for k, v in itertools.chain(o._props().items(), o._data.items())
            }
            if isinstance(o, Geometry):
                if o.type == "GeometryCollection":
                    _ = o_dict.pop("coordinates", None)
                else:
                    _ = o_dict.pop("geometries", None)
            elif isinstance(o, Feature):
                o_dict["type"] = "Feature"
            return o_dict
        elif isinstance(o, bytes):
            return hexlify(o)
        else:
            return o


def decode_object(obj):
    """A json.loads object_hook

    Parameters
    ----------
    obj : dict
        A decoded dict.

    Returns
    -------
    Feature, Geometry, or dict

    """
    if isinstance(obj, Object):
        return obj
    else:
        obj = obj.get("__geo_interface__", obj)

        _type = obj.get("type", None)
        if (_type == "Feature") or "geometry" in obj:
            return Feature.from_dict(obj)
        elif _type in _GEO_TYPES.values():
            return Geometry.from_dict(obj)
        else:
            return obj


def to_dict(val):
    """Converts an object to a dict"""
    try:
        obj = ObjectEncoder().default(val)
    except TypeError:
        return val
    else:
        return obj
