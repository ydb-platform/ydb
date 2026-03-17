import numpy
import unittest

import collada
from collada.xmlutil import etree

fromstring = etree.fromstring
tostring = etree.tostring


class TestSource(unittest.TestCase):

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)

    def test_float_source_saving(self):
        floatsource = collada.source.FloatSource("myfloatsource", numpy.array([0.1, 0.2, 0.3]), ('X', 'Y', 'X'))
        self.assertEqual(floatsource.id, "myfloatsource")
        self.assertEqual(len(floatsource), 1)
        self.assertTupleEqual(floatsource.components, ('X', 'Y', 'X'))
        self.assertIsNotNone(str(floatsource))
        floatsource.id = "yourfloatsource"
        floatsource.components = ('S', 'T')
        floatsource.data = numpy.array([0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
        floatsource.save()
        loaded_floatsource = collada.source.Source.load(self.dummy, {}, fromstring(tostring(floatsource.xmlnode)))
        self.assertTrue(isinstance(loaded_floatsource, collada.source.FloatSource))
        self.assertEqual(floatsource.id, "yourfloatsource")
        self.assertEqual(len(floatsource), 3)
        self.assertTupleEqual(floatsource.components, ('S', 'T'))

    def test_idref_source_saving(self):
        idrefsource = collada.source.IDRefSource("myidrefsource",
                                                 numpy.array(['Ref1', 'Ref2'], dtype=numpy.bytes_),
                                                 ('MORPH_TARGET',))
        self.assertEqual(idrefsource.id, "myidrefsource")
        self.assertEqual(len(idrefsource), 2)
        self.assertTupleEqual(idrefsource.components, ('MORPH_TARGET',))
        self.assertIsNotNone(str(idrefsource))
        idrefsource.id = "youridrefsource"
        idrefsource.components = ('JOINT_TARGET', 'WHATEVER_TARGET')
        idrefsource.data = numpy.array(['Ref5', 'Ref6', 'Ref7', 'Ref8', 'Ref9', 'Ref10'], dtype=numpy.bytes_)
        idrefsource.save()
        loaded_idrefsource = collada.source.Source.load(self.dummy, {}, fromstring(tostring(idrefsource.xmlnode)))
        self.assertTrue(isinstance(loaded_idrefsource, collada.source.IDRefSource))
        self.assertEqual(loaded_idrefsource.id, "youridrefsource")
        self.assertEqual(len(loaded_idrefsource), 3)
        self.assertTupleEqual(loaded_idrefsource.components, ('JOINT_TARGET', 'WHATEVER_TARGET'))

    def test_name_source_saving(self):
        namesource = collada.source.NameSource("mynamesource",
                                               numpy.array(['Name1', 'Name2'], dtype=numpy.bytes_),
                                               ('JOINT',))
        self.assertEqual(namesource.id, "mynamesource")
        self.assertEqual(len(namesource), 2)
        self.assertTupleEqual(namesource.components, ('JOINT',))
        self.assertIsNotNone(str(namesource))
        namesource.id = "yournamesource"
        namesource.components = ('WEIGHT', 'WHATEVER')
        namesource.data = numpy.array(['Name1', 'Name2', 'Name3', 'Name4', 'Name5', 'Name6'], dtype=numpy.bytes_)
        namesource.save()
        loaded_namesource = collada.source.Source.load(self.dummy, {}, fromstring(tostring(namesource.xmlnode)))
        self.assertTrue(isinstance(loaded_namesource, collada.source.NameSource))
        self.assertEqual(loaded_namesource.id, "yournamesource")
        self.assertEqual(len(loaded_namesource), 3)
        self.assertTupleEqual(loaded_namesource.components, ('WEIGHT', 'WHATEVER'))


if __name__ == '__main__':
    unittest.main()
