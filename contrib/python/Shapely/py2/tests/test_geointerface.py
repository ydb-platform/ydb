from . import unittest

from shapely.geometry import asShape
from shapely.geometry.multipoint import MultiPointAdapter
from shapely.geometry.linestring import LineStringAdapter
from shapely.geometry.multilinestring import MultiLineStringAdapter
from shapely.geometry.polygon import Polygon, PolygonAdapter
from shapely.geometry.multipolygon import MultiPolygonAdapter
from shapely import wkt


class GeoThing(object):
    def __init__(self, d):
        self.__geo_interface__ = d


class GeoInterfaceTestCase(unittest.TestCase):

    def test_geointerface(self):
        # Adapt a dictionary
        d = {"type": "Point", "coordinates": (0.0, 0.0)}
        shape = asShape(d)
        self.assertEqual(shape.geom_type, 'Point')
        self.assertEqual(tuple(shape.coords), ((0.0, 0.0),))

        # Adapt an object that implements the geo protocol
        shape = None
        thing = GeoThing({"type": "Point", "coordinates": (0.0, 0.0)})
        shape = asShape(thing)
        self.assertEqual(shape.geom_type, 'Point')
        self.assertEqual(tuple(shape.coords), ((0.0, 0.0),))

        # Check line string
        shape = asShape(
            {'type': 'LineString', 'coordinates': ((-1.0, -1.0), (1.0, 1.0))})
        self.assertIsInstance(shape, LineStringAdapter)
        self.assertEqual(tuple(shape.coords), ((-1.0, -1.0), (1.0, 1.0)))

        # polygon
        shape = asShape(
            {'type': 'Polygon',
             'coordinates':
                (((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (2.0, -1.0), (0.0, 0.0)),
                 ((0.1, 0.1), (0.1, 0.2), (0.2, 0.2), (0.2, 0.1), (0.1, 0.1)))}
        )
        self.assertIsInstance(shape, PolygonAdapter)
        self.assertEqual(
            tuple(shape.exterior.coords),
            ((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (2.0, -1.0), (0.0, 0.0)))
        self.assertEqual(len(shape.interiors), 1)

        # multi point
        shape = asShape({'type': 'MultiPoint',
                         'coordinates': ((1.0, 2.0), (3.0, 4.0))})
        self.assertIsInstance(shape, MultiPointAdapter)
        self.assertEqual(len(shape.geoms), 2)

        # multi line string
        shape = asShape({'type': 'MultiLineString',
                         'coordinates': (((0.0, 0.0), (1.0, 2.0)),)})
        self.assertIsInstance(shape, MultiLineStringAdapter)
        self.assertEqual(len(shape.geoms), 1)

        # multi polygon
        shape = asShape(
            {'type': 'MultiPolygon',
             'coordinates':
                [(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)),
                  ((0.1, 0.1), (0.1, 0.2), (0.2, 0.2), (0.2, 0.1), (0.1, 0.1))
                  )]})
        self.assertIsInstance(shape, MultiPolygonAdapter)
        self.assertEqual(len(shape.geoms), 1)


def test_empty_wkt_polygon():
    """Confirm fix for issue #450"""
    g = wkt.loads('POLYGON EMPTY')
    assert g.__geo_interface__['type'] == 'Polygon'
    assert g.__geo_interface__['coordinates'] == ()


def test_empty_polygon():
    """Confirm fix for issue #450"""
    g = Polygon()
    assert g.__geo_interface__['type'] == 'Polygon'
    assert g.__geo_interface__['coordinates'] == ()


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(GeoInterfaceTestCase)
