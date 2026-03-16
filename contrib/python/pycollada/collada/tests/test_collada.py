import io
import os
import numpy
import dateutil.parser
import unittest

import collada
from collada.xmlutil import etree

fromstring = etree.fromstring
tostring = etree.tostring


class TestCollada(unittest.TestCase):

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)
        self.datadir = os.path.join(os.getcwd(), "data")

    def test_collada_duck_tris(self):
        f = os.path.join(self.datadir, "duck_triangles.dae")
        mesh = collada.Collada(f, validate_output=True)

        self.assertEqual(mesh.assetInfo.contributors[0].author, 'gcorson')
        self.assertEqual(mesh.assetInfo.contributors[0].authoring_tool, 'Maya 8.0 | ColladaMaya v3.02 | FCollada v3.2')
        self.assertEqual(mesh.assetInfo.contributors[0].source_data,
                         'file:///C:/vs2005/sample_data/Complete_Packages/SCEA_Private/Maya_MoonLander/Moonlander/untitled')
        self.assertEqual(len(mesh.assetInfo.contributors[0].copyright), 595)
        self.assertEqual(len(mesh.assetInfo.contributors[0].comments), 449)

        self.assertEqual(mesh.assetInfo.unitmeter, 0.01)
        self.assertEqual(mesh.assetInfo.unitname, 'centimeter')
        self.assertEqual(mesh.assetInfo.upaxis, collada.asset.UP_AXIS.Y_UP)
        self.assertIsNone(mesh.assetInfo.title)
        self.assertIsNone(mesh.assetInfo.subject)
        self.assertIsNone(mesh.assetInfo.revision)
        self.assertIsNone(mesh.assetInfo.keywords)
        self.assertEqual(mesh.assetInfo.created, dateutil.parser.parse('2006-08-23T22:29:59Z'))
        self.assertEqual(mesh.assetInfo.modified, dateutil.parser.parse('2007-02-21T22:52:44Z'))

        self.assertEqual(mesh.scene.id, 'VisualSceneNode')
        self.assertIn('LOD3spShape-lib', mesh.geometries)
        self.assertIn('directionalLightShape1-lib', mesh.lights)
        self.assertIn('cameraShape1', mesh.cameras)
        self.assertIn('file2', mesh.images)
        self.assertIn('blinn3-fx', mesh.effects)
        self.assertIn('blinn3', mesh.materials)
        self.assertEqual(len(mesh.nodes), 0)
        self.assertIn('VisualSceneNode', mesh.scenes)

        triset = mesh.geometries[0].primitives[0]
        input_list = triset.getInputList().getList()
        self.assertEqual(3, len(input_list))

        self.assertIsNotNone(str(list(mesh.scene.objects('geometry'))))
        self.assertIsNotNone(str(list(mesh.scene.objects('light'))))
        self.assertIsNotNone(str(list(mesh.scene.objects('camera'))))

        s = io.BytesIO()
        mesh.write(s)
        out = s.getvalue()
        t = io.BytesIO(out)
        mesh = collada.Collada(t, validate_output=True)

        self.assertEqual(mesh.assetInfo.contributors[0].author, 'gcorson')
        self.assertEqual(mesh.assetInfo.contributors[0].authoring_tool, 'Maya 8.0 | ColladaMaya v3.02 | FCollada v3.2')
        self.assertEqual(mesh.assetInfo.contributors[0].source_data,
                         'file:///C:/vs2005/sample_data/Complete_Packages/SCEA_Private/Maya_MoonLander/Moonlander/untitled')
        self.assertEqual(len(mesh.assetInfo.contributors[0].copyright), 595)
        self.assertEqual(len(mesh.assetInfo.contributors[0].comments), 449)

        self.assertEqual(mesh.assetInfo.unitmeter, 0.01)
        self.assertEqual(mesh.assetInfo.unitname, 'centimeter')
        self.assertEqual(mesh.assetInfo.upaxis, collada.asset.UP_AXIS.Y_UP)
        self.assertIsNone(mesh.assetInfo.title)
        self.assertIsNone(mesh.assetInfo.subject)
        self.assertIsNone(mesh.assetInfo.revision)
        self.assertIsNone(mesh.assetInfo.keywords)
        self.assertEqual(mesh.assetInfo.created, dateutil.parser.parse('2006-08-23T22:29:59Z'))
        self.assertEqual(mesh.assetInfo.modified, dateutil.parser.parse('2007-02-21T22:52:44Z'))

        self.assertEqual(mesh.scene.id, 'VisualSceneNode')
        self.assertIn('LOD3spShape-lib', mesh.geometries)
        self.assertIn('directionalLightShape1-lib', mesh.lights)
        self.assertIn('cameraShape1', mesh.cameras)
        self.assertIn('file2', mesh.images)
        self.assertIn('blinn3-fx', mesh.effects)
        self.assertIn('blinn3', mesh.materials)
        self.assertEqual(len(mesh.nodes), 0)
        self.assertIn('VisualSceneNode', mesh.scenes)

        self.assertIsNotNone(str(list(mesh.scene.objects('geometry'))))
        self.assertIsNotNone(str(list(mesh.scene.objects('light'))))
        self.assertIsNotNone(str(list(mesh.scene.objects('camera'))))

    def test_collada_duck_poly(self):
        f = os.path.join(self.datadir, "duck_polylist.dae")
        mesh = collada.Collada(f, validate_output=True)
        self.assertEqual(mesh.scene.id, 'VisualSceneNode')
        self.assertIn('LOD3spShape-lib', mesh.geometries)
        self.assertIn('directionalLightShape1-lib', mesh.lights)
        self.assertIn('cameraShape1', mesh.cameras)
        self.assertIn('file2', mesh.images)
        self.assertIn('blinn3-fx', mesh.effects)
        self.assertIn('blinn3', mesh.materials)
        self.assertEqual(len(mesh.nodes), 0)
        self.assertIn('VisualSceneNode', mesh.scenes)

        s = io.BytesIO()
        mesh.write(s)
        out = s.getvalue()
        t = io.BytesIO(out)
        mesh = collada.Collada(t, validate_output=True)

        self.assertEqual(mesh.scene.id, 'VisualSceneNode')
        self.assertIn('LOD3spShape-lib', mesh.geometries)
        self.assertIn('directionalLightShape1-lib', mesh.lights)
        self.assertIn('cameraShape1', mesh.cameras)
        self.assertIn('file2', mesh.images)
        self.assertIn('blinn3-fx', mesh.effects)
        self.assertIn('blinn3', mesh.materials)
        self.assertEqual(len(mesh.nodes), 0)
        self.assertIn('VisualSceneNode', mesh.scenes)

    def test_collada_duck_zip(self):
        f = os.path.join(self.datadir, "duck.zip")
        mesh = collada.Collada(f, validate_output=True)
        self.assertEqual(mesh.scene.id, 'VisualSceneNode')
        self.assertIn('LOD3spShape-lib', mesh.geometries)
        self.assertIn('directionalLightShape1-lib', mesh.lights)
        self.assertIn('cameraShape1', mesh.cameras)
        self.assertIn('file2', mesh.images)
        self.assertIn('blinn3-fx', mesh.effects)
        self.assertIn('blinn3', mesh.materials)
        self.assertEqual(len(mesh.nodes), 0)
        self.assertIn('VisualSceneNode', mesh.scenes)

    def test_collada_saving(self):
        mesh = collada.Collada(validate_output=True)

        self.assertEqual(len(mesh.geometries), 0)
        self.assertEqual(len(mesh.controllers), 0)
        self.assertEqual(len(mesh.lights), 0)
        self.assertEqual(len(mesh.cameras), 0)
        self.assertEqual(len(mesh.images), 0)
        self.assertEqual(len(mesh.effects), 0)
        self.assertEqual(len(mesh.materials), 0)
        self.assertEqual(len(mesh.nodes), 0)
        self.assertEqual(len(mesh.scenes), 0)
        self.assertEqual(mesh.scene, None)
        self.assertIsNotNone(str(mesh))

        floatsource = collada.source.FloatSource("myfloatsource", numpy.array([0.1, 0.2, 0.3]), ('X', 'Y', 'Z'))
        geometry1 = collada.geometry.Geometry(mesh, "geometry1", "mygeometry1", {"myfloatsource": floatsource})
        mesh.geometries.append(geometry1)

        linefloats = [1, 1, -1, 1, -1, -1, -1, -0.9999998, -1, -0.9999997, 1, -1, 1, 0.9999995, 1, 0.9999994, -1.000001, 1]
        linefloatsrc = collada.source.FloatSource("mylinevertsource", numpy.array(linefloats), ('X', 'Y', 'Z'))
        geometry2 = collada.geometry.Geometry(mesh, "geometry2", "mygeometry2", [linefloatsrc])
        input_list = collada.source.InputList()
        input_list.addInput(0, 'VERTEX', "#mylinevertsource")
        indices = numpy.array([0, 1, 1, 2, 2, 3, 3, 4, 4, 5])
        lineset1 = geometry2.createLineSet(indices, input_list, "mymaterial2")
        geometry2.primitives.append(lineset1)
        mesh.geometries.append(geometry2)

        ambientlight = collada.light.AmbientLight("myambientlight", (1, 1, 1))
        pointlight = collada.light.PointLight("mypointlight", (1, 1, 1))
        mesh.lights.append(ambientlight)
        mesh.lights.append(pointlight)

        camera1 = collada.camera.PerspectiveCamera("mycam1", 45.0, 0.01, 1000.0)
        camera2 = collada.camera.PerspectiveCamera("mycam2", 45.0, 0.01, 1000.0)
        mesh.cameras.append(camera1)
        mesh.cameras.append(camera2)

        cimage1 = collada.material.CImage("mycimage1", "./whatever.tga", mesh)
        cimage2 = collada.material.CImage("mycimage2", "./whatever.tga", mesh)
        mesh.images.append(cimage1)
        mesh.images.append(cimage2)

        effect1 = collada.material.Effect("myeffect1", [], "phong")
        effect2 = collada.material.Effect("myeffect2", [], "phong")
        mesh.effects.append(effect1)
        mesh.effects.append(effect2)

        mat1 = collada.material.Material("mymaterial1", "mymat1", effect1)
        mat2 = collada.material.Material("mymaterial2", "mymat2", effect2)
        mesh.materials.append(mat1)
        mesh.materials.append(mat2)

        rotate = collada.scene.RotateTransform(0.1, 0.2, 0.3, 90)
        scale = collada.scene.ScaleTransform(0.1, 0.2, 0.3)
        mynode1 = collada.scene.Node('mynode1', children=[], transforms=[rotate, scale])
        mynode2 = collada.scene.Node('mynode2', children=[], transforms=[])
        mesh.nodes.append(mynode1)
        mesh.nodes.append(mynode2)

        geomnode = collada.scene.GeometryNode(geometry2)
        mynode3 = collada.scene.Node('mynode3', children=[geomnode], transforms=[])
        mynode4 = collada.scene.Node('mynode4', children=[], transforms=[])
        scene1 = collada.scene.Scene('myscene1', [mynode3])
        scene2 = collada.scene.Scene('myscene2', [mynode4])
        mesh.scenes.append(scene1)
        mesh.scenes.append(scene2)

        mesh.scene = scene1

        out = io.BytesIO()
        mesh.write(out)

        toload = io.BytesIO(out.getvalue())

        loaded_mesh = collada.Collada(toload, validate_output=True)
        self.assertEqual(len(loaded_mesh.geometries), 2)
        self.assertEqual(len(loaded_mesh.controllers), 0)
        self.assertEqual(len(loaded_mesh.lights), 2)
        self.assertEqual(len(loaded_mesh.cameras), 2)
        self.assertEqual(len(loaded_mesh.images), 2)
        self.assertEqual(len(loaded_mesh.effects), 2)
        self.assertEqual(len(loaded_mesh.materials), 2)
        self.assertEqual(len(loaded_mesh.nodes), 2)
        self.assertEqual(len(loaded_mesh.scenes), 2)
        self.assertEqual(loaded_mesh.scene.id, scene1.id)

        self.assertIn('geometry1', loaded_mesh.geometries)
        self.assertIn('geometry2', loaded_mesh.geometries)
        self.assertIn('mypointlight', loaded_mesh.lights)
        self.assertIn('myambientlight', loaded_mesh.lights)
        self.assertIn('mycam1', loaded_mesh.cameras)
        self.assertIn('mycam2', loaded_mesh.cameras)
        self.assertIn('mycimage1', loaded_mesh.images)
        self.assertIn('mycimage2', loaded_mesh.images)
        self.assertIn('myeffect1', loaded_mesh.effects)
        self.assertIn('myeffect2', loaded_mesh.effects)
        self.assertIn('mymaterial1', loaded_mesh.materials)
        self.assertIn('mymaterial2', loaded_mesh.materials)
        self.assertIn('mynode1', loaded_mesh.nodes)
        self.assertIn('mynode2', loaded_mesh.nodes)
        self.assertIn('myscene1', loaded_mesh.scenes)
        self.assertIn('myscene2', loaded_mesh.scenes)

        linefloatsrc2 = collada.source.FloatSource("mylinevertsource2", numpy.array(linefloats), ('X', 'Y', 'Z'))
        geometry3 = collada.geometry.Geometry(mesh, "geometry3", "mygeometry3", [linefloatsrc2])
        loaded_mesh.geometries.pop(0)
        loaded_mesh.geometries.append(geometry3)

        dirlight = collada.light.DirectionalLight("mydirlight", (1, 1, 1))
        loaded_mesh.lights.pop(0)
        loaded_mesh.lights.append(dirlight)

        camera3 = collada.camera.PerspectiveCamera("mycam3", 45.0, 0.01, 1000.0)
        loaded_mesh.cameras.pop(0)
        loaded_mesh.cameras.append(camera3)

        cimage3 = collada.material.CImage("mycimage3", "./whatever.tga", loaded_mesh)
        loaded_mesh.images.pop(0)
        loaded_mesh.images.append(cimage3)

        effect3 = collada.material.Effect("myeffect3", [], "phong")
        loaded_mesh.effects.pop(0)
        loaded_mesh.effects.append(effect3)

        mat3 = collada.material.Material("mymaterial3", "mymat3", effect3)
        loaded_mesh.materials.pop(0)
        loaded_mesh.materials.append(mat3)

        mynode5 = collada.scene.Node('mynode5', children=[], transforms=[])
        loaded_mesh.nodes.pop(0)
        loaded_mesh.nodes.append(mynode5)

        mynode6 = collada.scene.Node('mynode6', children=[], transforms=[])
        scene3 = collada.scene.Scene('myscene3', [mynode6])
        loaded_mesh.scenes.pop(0)
        loaded_mesh.scenes.append(scene3)

        loaded_mesh.scene = scene3

        loaded_mesh.save()

        strdata = tostring(loaded_mesh.xmlnode.getroot())
        indata = io.BytesIO(strdata)
        loaded_mesh2 = collada.Collada(indata, validate_output=True)

        self.assertEqual(loaded_mesh2.scene.id, scene3.id)
        self.assertIn('geometry3', loaded_mesh2.geometries)
        self.assertIn('geometry2', loaded_mesh2.geometries)
        self.assertIn('mydirlight', loaded_mesh2.lights)
        self.assertIn('mypointlight', loaded_mesh2.lights)
        self.assertIn('mycam3', loaded_mesh2.cameras)
        self.assertIn('mycam2', loaded_mesh2.cameras)
        self.assertIn('mycimage3', loaded_mesh2.images)
        self.assertIn('mycimage2', loaded_mesh2.images)
        self.assertIn('myeffect3', loaded_mesh2.effects)
        self.assertIn('myeffect2', loaded_mesh2.effects)
        self.assertIn('mymaterial3', loaded_mesh2.materials)
        self.assertIn('mymaterial2', loaded_mesh2.materials)
        self.assertIn('mynode5', loaded_mesh2.nodes)
        self.assertIn('mynode2', loaded_mesh2.nodes)
        self.assertIn('myscene3', loaded_mesh2.scenes)
        self.assertIn('myscene2', loaded_mesh2.scenes)

    def test_collada_attribute_replace(self):
        mesh = collada.Collada(validate_output=True)
        self.assertIsInstance(mesh.geometries, collada.util.IndexedList)
        self.assertIsInstance(mesh.controllers, collada.util.IndexedList)
        self.assertIsInstance(mesh.animations, collada.util.IndexedList)
        self.assertIsInstance(mesh.lights, collada.util.IndexedList)
        self.assertIsInstance(mesh.cameras, collada.util.IndexedList)
        self.assertIsInstance(mesh.images, collada.util.IndexedList)
        self.assertIsInstance(mesh.effects, collada.util.IndexedList)
        self.assertIsInstance(mesh.materials, collada.util.IndexedList)
        self.assertIsInstance(mesh.nodes, collada.util.IndexedList)
        self.assertIsInstance(mesh.scenes, collada.util.IndexedList)

        mesh.geometries = []
        mesh.controllers = []
        mesh.animations = []
        mesh.lights = []
        mesh.cameras = []
        mesh.images = []
        mesh.effects = []
        mesh.materials = []
        mesh.nodes = []
        mesh.scenes = []

        self.assertIsInstance(mesh.geometries, collada.util.IndexedList)
        self.assertIsInstance(mesh.controllers, collada.util.IndexedList)
        self.assertIsInstance(mesh.animations, collada.util.IndexedList)
        self.assertIsInstance(mesh.lights, collada.util.IndexedList)
        self.assertIsInstance(mesh.cameras, collada.util.IndexedList)
        self.assertIsInstance(mesh.images, collada.util.IndexedList)
        self.assertIsInstance(mesh.effects, collada.util.IndexedList)
        self.assertIsInstance(mesh.materials, collada.util.IndexedList)
        self.assertIsInstance(mesh.nodes, collada.util.IndexedList)
        self.assertIsInstance(mesh.scenes, collada.util.IndexedList)

    def test_collada_tristrips(self):
        f = os.path.join(self.datadir, "tristrips.dae")
        mesh = collada.Collada(f, validate_output=True)
        triangles = mesh.geometries[0].primitives[0]
        self.assertEqual(20, len(triangles))

    def test_collada_trifans(self):
        f = os.path.join(self.datadir, "trifans.dae")
        mesh = collada.Collada(f, validate_output=True)
        triangles = mesh.geometries[0].primitives[0]
        self.assertEqual(6, len(triangles))

    def test_collada_empty_triangles(self):
        f = os.path.join(self.datadir, "empty_triangles.dae")
        mesh = collada.Collada(f, validate_output=True)
        triangles = mesh.geometries[0].primitives[0]
        self.assertEqual(0, len(triangles))

    def test_collada_empty_triangles_with_multiple_ns(self):
        f = os.path.join(self.datadir, "empty_triangles_with_multiple_ns.dae")
        mesh = collada.Collada(f, validate_output=True)
        triangles = mesh.geometries[0].primitives[0]
        self.assertEqual(0, len(triangles))

    def test_namespace(self):
        """
        Test loading a file with a different namespace.

        Default namespace is:
        http://www.collada.org/2005/11/COLLADASchema

        This test file has namespace:
        http://www.collada.org/2008/03/COLLADASchema
        """

        # a 1.5 spec collada file with a different namespace
        # check both the zipped ("zae") and plain text ("dae") versions
        for name in ['wam.zae', 'wam.dae']:
            # full path to test file
            file_name = os.path.join(self.datadir, name)
            # load the scene
            mesh = collada.Collada(file_name, validate_output=True)
            # scene should have 8 geometries
            self.assertEqual(len(mesh.geometries), 8)
            # scene should have one root node
            self.assertEqual(len(mesh.scene.nodes), 1)


if __name__ == '__main__':
    unittest.main()
