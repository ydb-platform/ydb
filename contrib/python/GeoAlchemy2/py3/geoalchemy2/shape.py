"""This module provides utility functions for integrating with Shapely.

.. note::

    As GeoAlchemy 2 itself has no dependency on `Shapely`, applications using
    functions of this module have to ensure that `Shapely` is available.
"""

from contextlib import contextmanager
from typing import List
from typing import Optional
from typing import Union

try:
    import shapely.wkb
    import shapely.wkt
    from shapely.wkb import dumps

    HAS_SHAPELY = True
    _shapely_exc = None
except ImportError as exc:
    HAS_SHAPELY = False
    _shapely_exc = exc

from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement


@contextmanager
def check_shapely():
    if not HAS_SHAPELY:
        raise ImportError(
            "This feature needs the optional Shapely dependency. "
            "Please install it with 'pip install geoalchemy2[shapely]'."
        ) from _shapely_exc
    yield


@check_shapely()
def to_shape(element: Union[WKBElement, WKTElement]):
    """Function to convert a :class:`geoalchemy2.types.SpatialElement` to a Shapely geometry.

    Args:
        element: The element to convert into a ``Shapely`` object.

    Example::

        lake = Session.query(Lake).get(1)
        polygon = to_shape(lake.geom)
    """
    if isinstance(element, WKBElement):
        data, hex = (
            (element.data, True) if isinstance(element.data, str) else (bytes(element.data), False)
        )
        return shapely.wkb.loads(data, hex=hex)
    elif isinstance(element, WKTElement):
        if element.extended:
            return shapely.wkt.loads(element.data.split(";", 1)[1])
        else:
            return shapely.wkt.loads(element.data)
    else:
        raise TypeError("Only WKBElement and WKTElement objects are supported")


@check_shapely()
def from_shape(shape, srid: int = -1, extended: Optional[bool] = False) -> WKBElement:
    """Function to convert a Shapely geometry to a :class:`geoalchemy2.types.WKBElement`.

    Args:
        shape: The shape to convert.
        srid: An integer representing the spatial reference system. E.g. ``4326``.
            Default value is ``-1``, which means no/unknown reference system.
        extended: A boolean to switch between WKB and EWKB.
            Default value is False.

    Example::

        from shapely.geometry import Point
        wkb_element = from_shape(Point(5, 45), srid=4326)
        ewkb_element = from_shape(Point(5, 45), srid=4326, extended=True)
    """
    return WKBElement(
        memoryview(dumps(shape, srid=srid if extended else None)),
        srid=srid,
        extended=extended,
    )


__all__: List[str] = [
    "from_shape",
    "to_shape",
]


def __dir__() -> List[str]:
    return __all__
