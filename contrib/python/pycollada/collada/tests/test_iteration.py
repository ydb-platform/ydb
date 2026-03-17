import collada
import numpy
import unittest
from collada.xmlutil import etree
fromstring = etree.fromstring
tostring = etree.tostring


class TestIteration(unittest.TestCase):

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)

    def test_triangle_iterator_vert_normals(self):
        mesh = collada.Collada(validate_output=True)

        vert_floats = [-50, 50, 50, 50, 50, 50, -50, -50, 50, 50, -50, 50, -50, 50, -50, 50, 50, -50, -50, -50, -50, 50, -50, -50]
        normal_floats = [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, -1, 0, 0,
                         -1, 0, 0, -1, 0, 0, -1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1]
        vert_src = collada.source.FloatSource("cubeverts-array", numpy.array(vert_floats), ('X', 'Y', 'Z'))
        normal_src = collada.source.FloatSource("cubenormals-array", numpy.array(normal_floats), ('X', 'Y', 'Z'))
        geometry = collada.geometry.Geometry(mesh, "geometry0", "mycube", [vert_src, normal_src], [])

        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#cubeverts-array")
        input_list.addInput(1, 'NORMAL', "#cubenormals-array")

        indices = numpy.array([0, 0, 2, 1, 3, 2, 0, 0, 3, 2, 1, 3, 0, 4, 1, 5, 5, 6, 0, 4, 5, 6, 4, 7, 6, 8, 7, 9, 3, 10, 6, 8, 3, 10, 2, 11, 0, 12,
                               4, 13, 6, 14, 0, 12, 6, 14, 2, 15, 3, 16, 7, 17, 5, 18, 3, 16, 5, 18, 1, 19, 5, 20, 7, 21, 6, 22, 5, 20, 6, 22, 4, 23])
        triangleset = geometry.createTriangleSet(indices, input_list, "cubematerial")
        geometry.primitives.append(triangleset)
        mesh.geometries.append(geometry)

        geomnode = collada.scene.GeometryNode(geometry, [])
        mynode = collada.scene.Node('mynode6', children=[geomnode], transforms=[])
        scene = collada.scene.Scene('myscene', [mynode])
        mesh.scenes.append(scene)
        mesh.scene = scene

        mesh.save()

        geoms = list(mesh.scene.objects('geometry'))
        self.assertEqual(len(geoms), 1)

        prims = list(geoms[0].primitives())
        self.assertEqual(len(prims), 1)

        tris = list(prims[0])
        self.assertEqual(len(tris), 12)

        self.assertEqual(list(tris[0].vertices[0]), [-50.0, 50.0, 50.0])
        self.assertEqual(list(tris[0].vertices[1]), [-50.0, -50.0, 50.0])
        self.assertEqual(list(tris[0].vertices[2]), [50.0, -50.0, 50.0])
        self.assertEqual(list(tris[0].normals[0]), [0.0, 0.0, 1.0])
        self.assertEqual(list(tris[0].normals[1]), [0.0, 0.0, 1.0])
        self.assertEqual(list(tris[0].normals[2]), [0.0, 0.0, 1.0])
        self.assertEqual(tris[0].texcoords, [])
        self.assertEqual(tris[0].material, None)
        self.assertEqual(list(tris[0].indices), [0, 2, 3])
        self.assertEqual(list(tris[0].normal_indices), [0, 1, 2])
        self.assertEqual(tris[0].texcoord_indices, [])

    def test_polylist_iterator_vert_normals(self):
        mesh = collada.Collada(validate_output=True)

        vert_floats = [-50, 50, 50, 50, 50, 50, -50, -50, 50, 50, -50, 50, -50, 50, -50, 50, 50, -50, -50, -50, -50, 50, -50, -50]
        normal_floats = [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, -1, 0, 0,
                         -1, 0, 0, -1, 0, 0, -1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1]
        vert_src = collada.source.FloatSource("cubeverts-array", numpy.array(vert_floats), ('X', 'Y', 'Z'))
        normal_src = collada.source.FloatSource("cubenormals-array", numpy.array(normal_floats), ('X', 'Y', 'Z'))

        geometry = collada.geometry.Geometry(mesh, "geometry0", "mycube", [vert_src, normal_src], [])

        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#cubeverts-array")
        input_list.addInput(1, 'NORMAL', "#cubenormals-array")

        vcounts = numpy.array([4, 4, 4, 4, 4, 4])
        indices = numpy.array([0, 0, 2, 1, 3, 2, 1, 3, 0, 4, 1, 5, 5, 6, 4, 7, 6, 8, 7, 9, 3, 10, 2, 11, 0, 12, 4, 13, 6, 14, 2,
                               15, 3, 16, 7, 17, 5, 18, 1, 19, 5, 20, 7, 21, 6, 22, 4, 23])
        polylist = geometry.createPolylist(indices, vcounts, input_list, "cubematerial")

        geometry.primitives.append(polylist)
        mesh.geometries.append(geometry)

        geomnode = collada.scene.GeometryNode(geometry, [])
        mynode = collada.scene.Node('mynode6', children=[geomnode], transforms=[])
        scene = collada.scene.Scene('myscene', [mynode])
        mesh.scenes.append(scene)
        mesh.scene = scene

        mesh.save()

        geoms = list(mesh.scene.objects('geometry'))
        self.assertEqual(len(geoms), 1)

        prims = list(geoms[0].primitives())
        self.assertEqual(len(prims), 1)

        poly = list(prims[0])
        self.assertEqual(len(poly), 6)

        self.assertEqual(list(poly[0].vertices[0]), [-50.0, 50.0, 50.0])
        self.assertEqual(list(poly[0].vertices[1]), [-50.0, -50.0, 50.0])
        self.assertEqual(list(poly[0].vertices[2]), [50.0, -50.0, 50.0])
        self.assertEqual(list(poly[0].normals[0]), [0.0, 0.0, 1.0])
        self.assertEqual(list(poly[0].normals[1]), [0.0, 0.0, 1.0])
        self.assertEqual(list(poly[0].normals[2]), [0.0, 0.0, 1.0])
        self.assertEqual(poly[0].texcoords, [])
        self.assertEqual(poly[0].material, None)
        self.assertEqual(list(poly[0].indices), [0, 2, 3, 1])
        self.assertEqual(list(poly[0].normal_indices), [0, 1, 2, 3])
        self.assertEqual(poly[0].texcoord_indices, [])

        tris = list(poly[0].triangles())

        self.assertEqual(list(tris[0].vertices[0]), [-50.0, 50.0, 50.0])
        self.assertEqual(list(tris[0].vertices[1]), [-50.0, -50.0, 50.0])
        self.assertEqual(list(tris[0].vertices[2]), [50.0, -50.0, 50.0])
        self.assertEqual(list(tris[0].normals[0]), [0.0, 0.0, 1.0])
        self.assertEqual(list(tris[0].normals[1]), [0.0, 0.0, 1.0])
        self.assertEqual(list(tris[0].normals[2]), [0.0, 0.0, 1.0])
        self.assertEqual(tris[0].texcoords, [])
        self.assertEqual(tris[0].material, None)
        self.assertEqual(list(tris[0].indices), [0, 2, 3])
        self.assertEqual(list(tris[0].normal_indices), [0, 1, 2])
        self.assertEqual(tris[0].texcoord_indices, [])


if __name__ == '__main__':
    unittest.main()
