from . import unittest
from shapely.geometry.base import BaseGeometry, EmptyGeometry
import shapely.geometry as sgeom
from shapely.geometry.polygon import LinearRing

from shapely.geometry import MultiPolygon, mapping, shape, asShape


empty_generator = lambda: iter([])

class EmptinessTestCase(unittest.TestCase):

    def test_empty_class(self):
        g = EmptyGeometry()
        self.assertTrue(g._is_empty)

    def test_empty_base(self):
        g = BaseGeometry()
        self.assertTrue(g._is_empty)

    def test_emptying_point(self):
        p = sgeom.Point(0, 0)
        self.assertFalse(p._is_empty)
        p.empty()
        self.assertTrue(p._is_empty)

    def test_none_geom(self):
        p = BaseGeometry()
        p._geom = None
        self.assertTrue(p.is_empty)

    def test_empty_point(self):
        self.assertTrue(sgeom.Point().is_empty)

    def test_empty_multipoint(self):
        self.assertTrue(sgeom.MultiPoint().is_empty)

    def test_empty_geometry_collection(self):
        self.assertTrue(sgeom.GeometryCollection().is_empty)

    def test_empty_linestring(self):
        self.assertTrue(sgeom.LineString().is_empty)
        self.assertTrue(sgeom.LineString(None).is_empty)
        self.assertTrue(sgeom.LineString([]).is_empty)
        self.assertTrue(sgeom.LineString(empty_generator()).is_empty)

    def test_empty_multilinestring(self):
        self.assertTrue(sgeom.MultiLineString([]).is_empty)

    def test_empty_polygon(self):
        self.assertTrue(sgeom.Polygon().is_empty)
        self.assertTrue(sgeom.Polygon(None).is_empty)
        self.assertTrue(sgeom.Polygon([]).is_empty)
        self.assertTrue(sgeom.Polygon(empty_generator()).is_empty)

    def test_empty_multipolygon(self):
        self.assertTrue(sgeom.MultiPolygon([]).is_empty)

    def test_empty_linear_ring(self):
        self.assertTrue(LinearRing().is_empty)
        self.assertTrue(LinearRing(None).is_empty)
        self.assertTrue(LinearRing([]).is_empty)
        self.assertTrue(LinearRing(empty_generator()).is_empty)


def test_asshape_empty():
    empty_mp = MultiPolygon()
    empty_json = mapping(empty_mp)
    empty_asShape = asShape(empty_json)
    assert empty_asShape.is_empty


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(EmptinessTestCase)

if __name__ == '__main__':
    unittest.main()
