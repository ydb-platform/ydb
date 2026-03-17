import numpy
import unittest

import collada
from collada.xmlutil import etree

fromstring = etree.fromstring
tostring = etree.tostring


class TestGeometry(unittest.TestCase):

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)

    def test_empty_geometry_saving(self):
        floatsource = collada.source.FloatSource("myfloatsource", numpy.array([0.1, 0.2, 0.3]), ('X', 'Y', 'Z'))
        geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mygeometry", {"myfloatsource": floatsource})
        self.assertEqual(geometry.id, "geometry0")
        self.assertEqual(geometry.name, "mygeometry")
        self.assertEqual(len(geometry.primitives), 0)
        self.assertDictEqual(geometry.sourceById, {"myfloatsource": floatsource})
        self.assertIsNotNone(str(geometry))

        geometry.id = "geometry1"
        geometry.name = "yourgeometry"
        othersource1 = collada.source.FloatSource("yourfloatsource", numpy.array([0.4, 0.5, 0.6]), ('X', 'Y', 'Z'))
        othersource2 = collada.source.FloatSource("hisfloatsource", numpy.array([0.7, 0.8, 0.9]), ('X', 'Y', 'Z'))
        geometry.sourceById[othersource1.id] = othersource1
        geometry.sourceById[othersource2.id] = othersource2
        del geometry.sourceById[floatsource.id]
        geometry.save()

        loaded_geometry = collada.geometry.Geometry.load(collada, {}, fromstring(tostring(geometry.xmlnode)))
        self.assertEqual(loaded_geometry.id, "geometry1")
        self.assertEqual(loaded_geometry.name, "yourgeometry")
        self.assertEqual(len(loaded_geometry.primitives), 0)
        self.assertIn(othersource1.id, loaded_geometry.sourceById)
        self.assertIn(othersource2.id, loaded_geometry.sourceById)
        self.assertNotIn(floatsource.id, loaded_geometry.sourceById)

    def test_geometry_lineset_adding_with_material(self):
        linefloats = [1, 1, -1, 1, -1, -1, -1, -0.9999998, -1, -0.9999997, 1, -1, 1, 0.9999995, 1, 0.9999994, -1.000001, 1]
        linefloatsrc = collada.source.FloatSource("mylinevertsource", numpy.array(linefloats), ('X', 'Y', 'Z'))
        geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mygeometry", [linefloatsrc])
        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#mylinevertsource")
        indices = numpy.array([0, 1, 1, 2, 2, 3, 3, 4, 4, 5])
        lineset1 = geometry.createLineSet(indices, input_list, "mymaterial")
        lineset2 = geometry.createLineSet(indices, input_list, "mymaterial")
        geometry.primitives.append(lineset1)
        geometry.primitives.append(lineset2)
        self.assertEqual(len(geometry.primitives), 2)
        self.assertIsNotNone(str(lineset1))
        self.assertIsNotNone(str(input_list))
        geometry.save()

        loaded_geometry = collada.geometry.Geometry.load(self.dummy, {}, fromstring(tostring(geometry.xmlnode)))
        self.assertEqual(len(loaded_geometry.primitives), 2)

        loaded_geometry.primitives.pop(0)
        lineset3 = loaded_geometry.createLineSet(indices, input_list, "mymaterial")

        loaded_lineset = collada.lineset.LineSet.load(self.dummy, geometry.sourceById, fromstring(tostring(lineset3.xmlnode)))
        self.assertEqual(len(loaded_lineset), 5)

        loaded_geometry.primitives.append(lineset3)
        loaded_geometry.save()
        loaded_geometry2 = collada.geometry.Geometry.load(self.dummy, {}, fromstring(tostring(loaded_geometry.xmlnode)))

        self.assertEqual(len(loaded_geometry2.primitives), 2)
        self.assertEqual(loaded_geometry2.primitives[0].material, lineset2.material)
        self.assertEqual(loaded_geometry2.primitives[1].material, lineset3.material)

    def test_geometry_lineset_adding_without_material(self):
        linefloats = [1, 1, -1, 1, -1, -1, -1, -0.9999998, -1, -0.9999997, 1, -1, 1, 0.9999995, 1, 0.9999994, -1.000001, 1]
        linefloatsrc = collada.source.FloatSource("mylinevertsource", numpy.array(linefloats), ('X', 'Y', 'Z'))
        geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mygeometry", [linefloatsrc])
        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#mylinevertsource")
        indices = numpy.array([0, 1, 1, 2, 2, 3, 3, 4, 4, 5])
        lineset1 = geometry.createLineSet(indices, input_list)
        lineset2 = geometry.createLineSet(indices, input_list)
        geometry.primitives.append(lineset1)
        geometry.primitives.append(lineset2)
        self.assertEqual(len(geometry.primitives), 2)
        self.assertIsNotNone(str(lineset1))
        self.assertIsNotNone(str(input_list))
        geometry.save()

        loaded_geometry = collada.geometry.Geometry.load(self.dummy, {}, fromstring(tostring(geometry.xmlnode)))
        self.assertEqual(len(loaded_geometry.primitives), 2)

        loaded_geometry.primitives.pop(0)
        lineset3 = loaded_geometry.createLineSet(indices, input_list)

        loaded_lineset = collada.lineset.LineSet.load(self.dummy, geometry.sourceById, fromstring(tostring(lineset3.xmlnode)))
        self.assertEqual(len(loaded_lineset), 5)

        loaded_geometry.primitives.append(lineset3)
        loaded_geometry.save()
        loaded_geometry2 = collada.geometry.Geometry.load(self.dummy, {}, fromstring(tostring(loaded_geometry.xmlnode)))

        self.assertEqual(len(loaded_geometry2.primitives), 2)

    def test_geometry_triangleset_adding(self):
        vert_floats = [-50, 50, 50, 50, 50, 50, -50, -50, 50, 50, -50, 50, -50, 50, -50, 50, 50, -50, -50, -50, -50, 50, -50, -50]
        normal_floats = [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, -1, 0, 0,
                         -1, 0, 0, -1, 0, 0, -1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1]
        vert_src = collada.source.FloatSource("cubeverts-array", numpy.array(vert_floats), ('X', 'Y', 'Z'))
        normal_src = collada.source.FloatSource("cubenormals-array", numpy.array(normal_floats), ('X', 'Y', 'Z'))
        self.assertEqual(len(vert_src), 8)
        self.assertEqual(len(normal_src), 24)

        geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mycube", [vert_src, normal_src], [])

        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#cubeverts-array")
        input_list.addInput(1, 'NORMAL', "#cubenormals-array")

        indices = numpy.array([0, 0, 2, 1, 3, 2, 0, 0, 3, 2, 1, 3, 0, 4, 1, 5, 5, 6, 0, 4, 5, 6, 4, 7, 6, 8, 7, 9, 3, 10, 6, 8, 3, 10, 2, 11, 0, 12,
                               4, 13, 6, 14, 0, 12, 6, 14, 2, 15, 3, 16, 7, 17, 5, 18, 3, 16, 5, 18, 1, 19, 5, 20, 7, 21, 6, 22, 5, 20, 6, 22, 4, 23])
        triangleset = geometry.createTriangleSet(indices, input_list)
        self.assertIsNotNone(str(triangleset))

        loaded_triangleset = collada.triangleset.TriangleSet.load(self.dummy, geometry.sourceById, fromstring(tostring(triangleset.xmlnode)))
        self.assertEqual(len(loaded_triangleset), 12)

        geometry.primitives.append(triangleset)
        geometry.save()

        loaded_geometry = collada.geometry.Geometry.load(self.dummy, {}, fromstring(tostring(geometry.xmlnode)))
        self.assertEqual(len(loaded_geometry.primitives), 1)
        self.assertEqual(len(loaded_geometry.primitives[0]), 12)

    def test_geometry_polylist_adding(self):
        vert_floats = [-50, 50, 50, 50, 50, 50, -50, -50, 50, 50, -50, 50, -50, 50, -50, 50, 50, -50, -50, -50, -50, 50, -50, -50]
        normal_floats = [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, -1, 0, 0,
                         -1, 0, 0, -1, 0, 0, -1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1]
        vert_src = collada.source.FloatSource("cubeverts-array", numpy.array(vert_floats), ('X', 'Y', 'Z'))
        normal_src = collada.source.FloatSource("cubenormals-array", numpy.array(normal_floats), ('X', 'Y', 'Z'))

        geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mycube", [vert_src, normal_src], [])

        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#cubeverts-array")
        input_list.addInput(1, 'NORMAL', "#cubenormals-array")

        vcounts = numpy.array([4, 4, 4, 4, 4, 4])
        indices = numpy.array([0, 0, 2, 1, 3, 2, 1, 3, 0, 4, 1, 5, 5, 6, 4, 7, 6, 8, 7, 9, 3, 10, 2, 11, 0, 12, 4, 13, 6, 14, 2,
                               15, 3, 16, 7, 17, 5, 18, 1, 19, 5, 20, 7, 21, 6, 22, 4, 23])
        polylist = geometry.createPolylist(indices, vcounts, input_list)
        self.assertIsNotNone(str(polylist))

        loaded_polylist = collada.polylist.Polylist.load(self.dummy, geometry.sourceById, fromstring(tostring(polylist.xmlnode)))
        self.assertEqual(len(loaded_polylist), 6)

        geometry.primitives.append(polylist)
        geometry.save()

        loaded_geometry = collada.geometry.Geometry.load(self.dummy, {}, fromstring(tostring(geometry.xmlnode)))

        self.assertEqual(len(loaded_geometry.primitives), 1)
        self.assertEqual(len(loaded_geometry.primitives[0]), 6)

    def test_geometry_polygons_adding(self):
        vert_floats = [-50, 50, 50, 50, 50, 50, -50, -50, 50, 50, -50, 50, -50, 50, -50, 50, 50, -50, -50, -50, -50, 50, -50, -50]
        normal_floats = [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, -1, 0, 0,
                         -1, 0, 0, -1, 0, 0, -1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1]
        vert_src = collada.source.FloatSource("cubeverts-array", numpy.array(vert_floats), ('X', 'Y', 'Z'))
        normal_src = collada.source.FloatSource("cubenormals-array", numpy.array(normal_floats), ('X', 'Y', 'Z'))

        geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mycube", [vert_src, normal_src], [])

        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#cubeverts-array")
        input_list.addInput(1, 'NORMAL', "#cubenormals-array")

        indices = []
        indices.append(numpy.array([0, 0, 2, 1, 3, 2, 1, 3], dtype=numpy.int32))
        indices.append(numpy.array([0, 4, 1, 5, 5, 6, 4, 7], dtype=numpy.int32))
        indices.append(numpy.array([6, 8, 7, 9, 3, 10, 2, 11], dtype=numpy.int32))
        indices.append(numpy.array([0, 12, 4, 13, 6, 14, 2, 15], dtype=numpy.int32))
        indices.append(numpy.array([3, 16, 7, 17, 5, 18, 1, 19], dtype=numpy.int32))
        indices.append(numpy.array([5, 20, 7, 21, 6, 22, 4, 23], dtype=numpy.int32))

        polygons = geometry.createPolygons(indices, input_list)
        self.assertIsNotNone(str(polygons))

        loaded_polygons = collada.polygons.Polygons.load(self.dummy, geometry.sourceById, fromstring(tostring(polygons.xmlnode)))
        self.assertEqual(len(loaded_polygons), 6)

        geometry.primitives.append(polygons)
        geometry.save()

        loaded_geometry = collada.geometry.Geometry.load(self.dummy, {}, fromstring(tostring(geometry.xmlnode)))

        self.assertEqual(len(loaded_geometry.primitives), 1)
        self.assertEqual(len(loaded_geometry.primitives[0]), 6)


if __name__ == '__main__':
    unittest.main()
