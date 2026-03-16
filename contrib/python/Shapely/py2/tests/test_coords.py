from . import unittest, numpy
from shapely import geometry


class CoordsTestCase(unittest.TestCase):
    """
    Shapely assumes contiguous C-order float64 data for internal ops.
    Data should be converted to contiguous float64 if numpy exists.
    c9a0707 broke this a little bit.
    """

    @unittest.skipIf(not numpy, 'Numpy required')
    def test_data_promotion(self):
        coords = numpy.array([[ 12, 34 ], [ 56, 78 ]], dtype=numpy.float32)
        processed_coords = numpy.array(
            geometry.LineString(coords).coords
        )

        self.assertEqual(
            coords.tolist(),
            processed_coords.tolist()
        )

    @unittest.skipIf(not numpy, 'Numpy required')
    def test_data_destriding(self):
        coords = numpy.array([[ 12, 34 ], [ 56, 78 ]], dtype=numpy.float32)

        # Easy way to introduce striding: reverse list order
        processed_coords = numpy.array(
            geometry.LineString(coords[::-1]).coords
        )

        self.assertEqual(
            coords[::-1].tolist(),
            processed_coords.tolist()
        )


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(CoordsTestCase)
