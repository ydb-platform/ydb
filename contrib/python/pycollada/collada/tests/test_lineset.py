import numpy
from numpy.testing import assert_array_equal
import unittest

import collada
from collada.xmlutil import etree
from collada.common import DaeIncompleteError

fromstring = etree.fromstring
tostring = etree.tostring


class TestLineset(unittest.TestCase):

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)

    def test_lineset_construction(self):
        # An empty sources dict should raise an exception.
        with self.assertRaises(DaeIncompleteError) as e:
            collada.lineset.LineSet({}, None, None)
        self.assertIn("at least one", e.exception.msg)

        # As should passing one without VERTEX defined as a key.
        with self.assertRaises(DaeIncompleteError) as e:
            collada.lineset.LineSet({"a": []}, None, None)
        self.assertIn("requires vertex", e.exception.msg)

        # Adding an input list with vertex defined, but empty should raise an error.
        with self.assertRaises(DaeIncompleteError) as e:
            collada.lineset.LineSet({'VERTEX': []}, None, None)
        self.assertIn("requires vertex", e.exception.msg)

    def test_empty_lineset_saving(self):
        linefloats = [1, 1, -1, 1, -1, -1, -1, -0.9999998, -1, -0.9999997, 1, -1, 1, 0.9999995, 1, 0.9999994, -1.000001, 1]
        linefloatsrc = collada.source.FloatSource("mylinevertsource", numpy.array(linefloats), ('X', 'Y', 'Z'))
        geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mygeometry", [linefloatsrc])
        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#mylinevertsource")
        indices = numpy.array([0, 1, 1, 2, 2, 3, 3, 4, 4, 5])
        lineset = geometry.createLineSet(indices, input_list, "mymaterial")

        # Check the initial values for the lineset.
        self.assertIsNotNone(str(lineset))
        assert_array_equal(lineset.index, indices)
        self.assertEqual(lineset.nlines, len(indices))
        self.assertEqual(lineset.material, "mymaterial")

        # Serialize and deserialize.
        lineset.save()
        loaded_lineset = collada.lineset.LineSet.load(collada, {'mylinevertsource': linefloatsrc}, fromstring(tostring(lineset.xmlnode)))

        # Check that the deserialized version has all of the same properties.
        self.assertEqual(loaded_lineset.sources, lineset.sources)
        self.assertEqual(loaded_lineset.material, lineset.material)
        assert_array_equal(loaded_lineset.index, lineset.index)
        assert_array_equal(loaded_lineset.indices, lineset.indices)
        self.assertEqual(loaded_lineset.nindices, lineset.nindices)
        self.assertEqual(loaded_lineset.nlines, lineset.nlines)


if __name__ == '__main__':
    unittest.main()
