from . import unittest
from shapely import geometry


class BufferSingleSidedCase(unittest.TestCase):
    """ Test Buffer Point/Line/Polygon with and without single_sided params """

    def test_empty(self):
        g = geometry.Point(0,0)
        h = g.buffer(0)
        assert h.is_empty

    def test_point(self):
        g = geometry.Point(0, 0)
        h = g.buffer(1, resolution=1)
        self.assertEqual(h.geom_type, 'Polygon')
        expected_coord = [(1.0, 0.0), (0, -1.0), (-1.0, 0), (0, 1.0), (1.0, 0.0)]
        for index, coord in enumerate(h.exterior.coords):
            self.assertAlmostEqual(coord[0], expected_coord[index][0])
            self.assertAlmostEqual(coord[1], expected_coord[index][1])

    def test_point_single_sidedd(self):
        g = geometry.Point(0, 0)
        h = g.buffer(1, resolution=1, single_sided=True)
        self.assertEqual(h.geom_type, 'Polygon')
        expected_coord = [(1.0, 0.0), (0, -1.0), (-1.0, 0), (0, 1.0), (1.0, 0.0)]
        for index, coord in enumerate(h.exterior.coords):
            self.assertAlmostEqual(coord[0], expected_coord[index][0])
            self.assertAlmostEqual(coord[1], expected_coord[index][1])

    def test_line(self):
        g = geometry.LineString([[0, 0], [0, 1]])
        h = g.buffer(1, resolution=1)
        self.assertEqual(h.geom_type, 'Polygon')
        expected_coord = [(-1.0, 1.0), (0, 2.0), (1.0, 1.0), (1.0, 0.0), (0, -1.0), (-1.0, 0.0),
                          (-1.0, 1.0)]
        for index, coord in enumerate(h.exterior.coords):
            self.assertAlmostEqual(coord[0], expected_coord[index][0])
            self.assertAlmostEqual(coord[1], expected_coord[index][1])

    def test_line_single_sideded_left(self):
        g = geometry.LineString([[0, 0], [0, 1]])
        h = g.buffer(1, resolution=1, single_sided=True)
        self.assertEqual(h.geom_type, 'Polygon')
        expected_coord = [(0.0, 1.0), (0.0, 0.0), (-1.0, 0.0), (-1.0, 1.0), (0.0, 1.0)]
        for index, coord in enumerate(h.exterior.coords):
            self.assertAlmostEqual(coord[0], expected_coord[index][0])
            self.assertAlmostEqual(coord[1], expected_coord[index][1])

    def test_line_single_sideded_right(self):
        g = geometry.LineString([[0, 0], [0, 1]])
        h = g.buffer(-1, resolution=1, single_sided=True)
        self.assertEqual(h.geom_type, 'Polygon')
        expected_coord = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]
        for index, coord in enumerate(h.exterior.coords):
            self.assertAlmostEqual(coord[0], expected_coord[index][0])
            self.assertAlmostEqual(coord[1], expected_coord[index][1])

    def test_polygon(self):
        g = geometry.Polygon([[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]])
        h = g.buffer(1, resolution=1)
        self.assertEqual(h.geom_type, 'Polygon')
        expected_coord = [(-1.0, 0.0), (-1.0, 1.0), (0.0, 2.0), (1.0, 2.0), (2.0, 1.0), (2.0, 0.0),
                          (1.0, -1.0), (0.0, -1.0), (-1.0, 0.0)]
        for index, coord in enumerate(h.exterior.coords):
            self.assertAlmostEqual(coord[0], expected_coord[index][0])
            self.assertAlmostEqual(coord[1], expected_coord[index][1])

    def test_polygon_single_sideded(self):
        g = geometry.Polygon([[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]])
        h = g.buffer(1, resolution=1, single_sided=True)
        self.assertEqual(h.geom_type, 'Polygon')
        expected_coord = [(-1.0, 0.0), (-1.0, 1.0), (0.0, 2.0), (1.0, 2.0), (2.0, 1.0), (2.0, 0.0),
                          (1.0, -1.0), (0.0, -1.0), (-1.0, 0.0)]
        for index, coord in enumerate(h.exterior.coords):
            self.assertAlmostEqual(coord[0], expected_coord[index][0])
            self.assertAlmostEqual(coord[1], expected_coord[index][1])

def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(BufferSingleSidedCase)
