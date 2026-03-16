"""Operations on GeoJSON feature and geometry objects."""

from collections import UserDict
from functools import wraps
import itertools
from typing import Generator, Iterable, Mapping, Union

from fiona.transform import transform_geom  # type: ignore
import shapely  # type: ignore
import shapely.ops  # type: ignore
from shapely.geometry import mapping, shape  # type: ignore
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry  # type: ignore

from .errors import ReduceError
from ._vendor import snuggs

# Patch snuggs's func_map, extending it with Python builtins, geometry
# methods and attributes, and functions exported in the shapely module
# (such as set_precision).


class FuncMapper(UserDict, Mapping):
    """Resolves functions from names in pipeline expressions."""

    def __getitem__(self, key):
        """Get a function by its name."""
        if key in self.data:
            return self.data[key]
        elif key in __builtins__ and not key.startswith("__"):
            return __builtins__[key]
        elif key in dir(shapely):
            return lambda g, *args, **kwargs: getattr(shapely, key)(g, *args, **kwargs)
        elif key in dir(shapely.ops):
            return lambda g, *args, **kwargs: getattr(shapely.ops, key)(
                g, *args, **kwargs
            )
        else:
            return (
                lambda g, *args, **kwargs: getattr(g, key)(*args, **kwargs)
                if callable(getattr(g, key))
                else getattr(g, key)
            )


def collect(geoms: Iterable) -> object:
    """Turn a sequence of geometries into a single GeometryCollection.

    Parameters
    ----------
    geoms : Iterable
        A sequence of geometry objects.

    Returns
    -------
    Geometry

    """
    return shapely.GeometryCollection(list(geoms))


def dump(geom: Union[BaseGeometry, BaseMultipartGeometry]) -> Generator:
    """Get the individual parts of a geometry object.

    If the given geometry object has a single part, e.g., is an
    instance of LineString, Point, or Polygon, this function yields a
    single result, the geometry itself.

    Parameters
    ----------
    geom : a shapely geometry object.

    Yields
    ------
    A shapely geometry object.

    """
    if hasattr(geom, "geoms"):
        parts = geom.geoms
    else:
        parts = [geom]
    for part in parts:
        yield part


def identity(obj: object) -> object:
    """Get back the given argument.

    To help in making expression lists, where the first item must be a
    callable object.

    Parameters
    ----------
    obj : objeect

    Returns
    -------
    obj

    """
    return obj


def vertex_count(obj: object) -> int:
    """Count the vertices of a GeoJSON-like geometry object.

    Parameters
    ----------
    obj: object
        A GeoJSON-like mapping or an object that provides
        __geo_interface__.

    Returns
    -------
    int

    """
    shp = shape(obj)
    if hasattr(shp, "geoms"):
        return sum(vertex_count(part) for part in shp.geoms)
    elif hasattr(shp, "exterior"):
        return vertex_count(shp.exterior) + sum(
            vertex_count(ring) for ring in shp.interiors
        )
    else:
        return len(shp.coords)


def binary_projectable_property_wrapper(func):
    """Project func's geometry args before computing a property.

    Parameters
    ----------
    func : callable
        Signature is func(geom1, geom2, *args, **kwargs)

    Returns
    -------
    callable
        Signature is func(geom1, geom2, projected=True, *args, **kwargs)

    """

    @wraps(func)
    def wrapper(geom1, geom2, *args, projected=True, **kwargs):
        if projected:
            geom1 = shape(transform_geom("OGC:CRS84", "EPSG:6933", mapping(geom1)))
            geom2 = shape(transform_geom("OGC:CRS84", "EPSG:6933", mapping(geom2)))

        return func(geom1, geom2, *args, **kwargs)

    return wrapper


def unary_projectable_property_wrapper(func):
    """Project func's geometry arg before computing a property.

    Parameters
    ----------
    func : callable
        Signature is func(geom1, *args, **kwargs)

    Returns
    -------
    callable
        Signature is func(geom1, projected=True, *args, **kwargs)

    """

    @wraps(func)
    def wrapper(geom, *args, projected=True, **kwargs):
        if projected:
            geom = shape(transform_geom("OGC:CRS84", "EPSG:6933", mapping(geom)))

        return func(geom, *args, **kwargs)

    return wrapper


def unary_projectable_constructive_wrapper(func):
    """Project func's geometry arg before constructing a new geometry.

    Parameters
    ----------
    func : callable
        Signature is func(geom1, *args, **kwargs)

    Returns
    -------
    callable
        Signature is func(geom1, projected=True, *args, **kwargs)

    """

    @wraps(func)
    def wrapper(geom, *args, projected=True, **kwargs):
        if projected:
            geom = shape(transform_geom("OGC:CRS84", "EPSG:6933", mapping(geom)))
            product = func(geom, *args, **kwargs)
            return shape(transform_geom("EPSG:6933", "OGC:CRS84", mapping(product)))
        else:
            return func(geom, *args, **kwargs)

    return wrapper


area = unary_projectable_property_wrapper(shapely.area)
buffer = unary_projectable_constructive_wrapper(shapely.buffer)
distance = binary_projectable_property_wrapper(shapely.distance)
set_precision = unary_projectable_constructive_wrapper(shapely.set_precision)
simplify = unary_projectable_constructive_wrapper(shapely.simplify)
length = unary_projectable_property_wrapper(shapely.length)

snuggs.func_map = FuncMapper(
    area=area,
    buffer=buffer,
    collect=collect,
    distance=distance,
    dump=dump,
    identity=identity,
    length=length,
    simplify=simplify,
    set_precision=set_precision,
    vertex_count=vertex_count,
    **{
        k: getattr(itertools, k)
        for k in dir(itertools)
        if not k.startswith("_") and callable(getattr(itertools, k))
    },
)


def map_feature(
    expression: str, feature: Mapping, dump_parts: bool = False
) -> Generator:
    """Map a pipeline expression to a feature.

    Yields one or more values.

    Parameters
    ----------
    expression : str
        A snuggs expression. The outermost parentheses are optional.
    feature : dict
        A Fiona feature object.
    dump_parts : bool, optional (default: False)
        If True, the parts of the feature's geometry are turned into
        new features.

    Yields
    ------
    object

    """
    if not (expression.startswith("(") and expression.endswith(")")):
        expression = f"({expression})"

    try:
        geom = shape(feature.get("geometry", None))
        if dump_parts and hasattr(geom, "geoms"):
            parts = geom.geoms
        else:
            parts = [geom]
    except (AttributeError, KeyError):
        parts = [None]

    for part in parts:
        result = snuggs.eval(expression, g=part, f=feature)
        if isinstance(result, (str, float, int, Mapping)):
            yield result
        elif isinstance(result, (BaseGeometry, BaseMultipartGeometry)):
            yield mapping(result)
        else:
            try:
                for item in result:
                    if isinstance(item, (BaseGeometry, BaseMultipartGeometry)):
                        item = mapping(item)
                    yield item
            except TypeError:
                yield result


def reduce_features(expression: str, features: Iterable[Mapping]) -> Generator:
    """Reduce a collection of features to a single value.

    The pipeline is a string that, when evaluated by snuggs, produces
    a new value. The name of the input feature collection in the
    context of the pipeline is "c".

    Parameters
    ----------
    pipeline : str
        Geometry operation pipeline such as "(unary_union c)".
    features : iterable
        A sequence of Fiona feature objects.

    Yields
    ------
    object

    Raises
    ------
    ReduceError

    """
    if not (expression.startswith("(") and expression.endswith(")")):
        expression = f"({expression})"

    collection = (shape(feat["geometry"]) for feat in features)
    result = snuggs.eval(expression, c=collection)

    if isinstance(result, (str, float, int, Mapping)):
        yield result
    elif isinstance(result, (BaseGeometry, BaseMultipartGeometry)):
        yield mapping(result)
    else:
        raise ReduceError("Expression failed to reduce to a single value.")
