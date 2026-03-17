import numpy
import unittest

import collada
from collada.xmlutil import etree

fromstring = etree.fromstring
tostring = etree.tostring


class TestScene(unittest.TestCase):

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)

        self.yourcam = collada.camera.PerspectiveCamera("yourcam", 45.0, 0.01, 1000.0)
        self.dummy.cameras.append(self.yourcam)

        self.yourdirlight = collada.light.DirectionalLight("yourdirlight", (1, 1, 1))
        self.dummy.lights.append(self.yourdirlight)

        cimage = collada.material.CImage("mycimage", "./whatever.tga", self.dummy)
        surface = collada.material.Surface("mysurface", cimage)
        sampler2d = collada.material.Sampler2D("mysampler2d", surface)
        mymap = collada.material.Map(sampler2d, "TEX0")
        self.effect = collada.material.Effect("myeffect", [surface, sampler2d], "phong",
                                              emission=(0.1, 0.2, 0.3),
                                              ambient=(0.4, 0.5, 0.6),
                                              diffuse=mymap,
                                              specular=(0.3, 0.2, 0.1))
        self.effect2 = collada.material.Effect("youreffect", [], "phong",
                                               emission=(0.1, 0.2, 0.3),
                                               ambient=(0.4, 0.5, 0.6),
                                               specular=(0.3, 0.2, 0.1))
        self.dummy.materials.append(self.effect)
        self.dummy.materials.append(self.effect2)

        self.floatsource = collada.source.FloatSource("myfloatsource", numpy.array([0.1, 0.2, 0.3]), ('X', 'Y', 'Z'))
        self.geometry = collada.geometry.Geometry(self.dummy, "geometry0", "mygeometry", {"myfloatsource": self.floatsource})
        self.geometry2 = collada.geometry.Geometry(self.dummy, "geometry1", "yourgeometry", {"myfloatsource": self.floatsource})
        self.dummy.geometries.append(self.geometry)
        self.dummy.geometries.append(self.geometry2)

    def test_scene_light_node_saving(self):
        dirlight = collada.light.DirectionalLight("mydirlight", (1, 1, 1))
        lightnode = collada.scene.LightNode(dirlight)
        bindtest = list(lightnode.objects('light'))
        self.assertEqual(lightnode.light, dirlight)
        self.assertEqual(len(bindtest), 1)
        self.assertEqual(bindtest[0].original, dirlight)
        self.assertIsNotNone(str(lightnode))

        lightnode.light = self.yourdirlight
        lightnode.save()

        loadedlightnode = collada.scene.LightNode.load(self.dummy, fromstring(tostring(lightnode.xmlnode)))
        self.assertEqual(loadedlightnode.light.id, 'yourdirlight')

    def test_scene_camera_node_saving(self):
        cam = collada.camera.PerspectiveCamera("mycam", 45.0, 0.01, 1000.0)
        camnode = collada.scene.CameraNode(cam)
        bindtest = list(camnode.objects('camera'))
        self.assertEqual(camnode.camera, cam)
        self.assertEqual(len(bindtest), 1)
        self.assertEqual(bindtest[0].original, cam)
        self.assertIsNotNone(str(camnode))

        camnode.camera = self.yourcam
        camnode.save()

        loadedcamnode = collada.scene.CameraNode.load(self.dummy, fromstring(tostring(camnode.xmlnode)))
        self.assertEqual(loadedcamnode.camera.id, 'yourcam')

    def test_scene_translate_node(self):
        translate = collada.scene.TranslateTransform(0.1, 0.2, 0.3)
        self.assertAlmostEqual(translate.x, 0.1)
        self.assertAlmostEqual(translate.y, 0.2)
        self.assertAlmostEqual(translate.z, 0.3)
        self.assertIsNotNone(str(translate))
        loaded_translate = collada.scene.TranslateTransform.load(self.dummy, fromstring(tostring(translate.xmlnode)))
        self.assertAlmostEqual(loaded_translate.x, 0.1)
        self.assertAlmostEqual(loaded_translate.y, 0.2)
        self.assertAlmostEqual(loaded_translate.z, 0.3)

    def test_scene_rotate_node(self):
        rotate = collada.scene.RotateTransform(0.1, 0.2, 0.3, 90)
        self.assertAlmostEqual(rotate.x, 0.1)
        self.assertAlmostEqual(rotate.y, 0.2)
        self.assertAlmostEqual(rotate.z, 0.3)
        self.assertAlmostEqual(rotate.angle, 90)
        self.assertIsNotNone(str(rotate))
        loaded_rotate = collada.scene.RotateTransform.load(self.dummy, fromstring(tostring(rotate.xmlnode)))
        self.assertAlmostEqual(loaded_rotate.x, 0.1)
        self.assertAlmostEqual(loaded_rotate.y, 0.2)
        self.assertAlmostEqual(loaded_rotate.z, 0.3)
        self.assertAlmostEqual(loaded_rotate.angle, 90)

    def test_scene_scale_node(self):
        scale = collada.scene.ScaleTransform(0.1, 0.2, 0.3)
        self.assertAlmostEqual(scale.x, 0.1)
        self.assertAlmostEqual(scale.y, 0.2)
        self.assertAlmostEqual(scale.z, 0.3)
        self.assertIsNotNone(str(scale))
        loaded_scale = collada.scene.ScaleTransform.load(self.dummy, fromstring(tostring(scale.xmlnode)))
        self.assertAlmostEqual(loaded_scale.x, 0.1)
        self.assertAlmostEqual(loaded_scale.y, 0.2)
        self.assertAlmostEqual(loaded_scale.z, 0.3)

    def test_scene_matrix_node(self):
        matrix = collada.scene.MatrixTransform(numpy.array([1.0, 0, 0, 2, 0, 1, 0, 3, 0, 0, 1, 4, 0, 0, 0, 1]))
        self.assertAlmostEqual(matrix.matrix[0][0], 1.0)
        self.assertIsNotNone(str(matrix))
        loaded_matrix = collada.scene.MatrixTransform.load(self.dummy, fromstring(tostring(matrix.xmlnode)))
        self.assertAlmostEqual(loaded_matrix.matrix[0][0], 1.0)

    def test_scene_lookat_node(self):
        eye = numpy.array([2.0, 0, 3])
        interest = numpy.array([0.0, 0, 0])
        upvector = numpy.array([0.0, 1, 0])
        lookat = collada.scene.LookAtTransform(eye, interest, upvector)
        self.assertListEqual(list(lookat.eye), list(eye))
        self.assertListEqual(list(lookat.interest), list(interest))
        self.assertListEqual(list(lookat.upvector), list(upvector))
        self.assertIsNotNone(str(lookat))
        loaded_lookat = collada.scene.LookAtTransform.load(self.dummy, fromstring(tostring(lookat.xmlnode)))
        self.assertListEqual(list(loaded_lookat.eye), list(eye))
        self.assertListEqual(list(loaded_lookat.interest), list(interest))
        self.assertListEqual(list(loaded_lookat.upvector), list(upvector))

    def test_scene_node_combos(self):
        emptynode = collada.scene.Node('myemptynode')
        self.assertEqual(len(emptynode.children), 0)
        self.assertEqual(len(emptynode.transforms), 0)
        self.assertIsNotNone(str(emptynode))
        loadedempty = collada.scene.Node.load(self.dummy, fromstring(tostring(emptynode.xmlnode)), {})
        self.assertEqual(len(loadedempty.children), 0)
        self.assertEqual(len(loadedempty.transforms), 0)

        justchildren = collada.scene.Node('myjustchildrennode', children=[emptynode])
        self.assertEqual(len(justchildren.children), 1)
        self.assertEqual(len(justchildren.transforms), 0)
        self.assertEqual(justchildren.children[0], emptynode)
        loadedjustchildren = collada.scene.Node.load(self.dummy, fromstring(tostring(justchildren.xmlnode)), {})
        self.assertEqual(len(loadedjustchildren.children), 1)
        self.assertEqual(len(loadedjustchildren.transforms), 0)

        scale = collada.scene.ScaleTransform(0.1, 0.2, 0.3)
        justtransform = collada.scene.Node('myjusttransformnode', transforms=[scale])
        self.assertEqual(len(justtransform.children), 0)
        self.assertEqual(len(justtransform.transforms), 1)
        self.assertEqual(justtransform.transforms[0], scale)
        loadedjusttransform = collada.scene.Node.load(self.dummy, fromstring(tostring(justtransform.xmlnode)), {})
        self.assertEqual(len(loadedjusttransform.children), 0)
        self.assertEqual(len(loadedjusttransform.transforms), 1)

        both = collada.scene.Node('mybothnode', children=[justchildren, justtransform], transforms=[scale])
        self.assertEqual(len(both.children), 2)
        self.assertEqual(len(both.transforms), 1)
        self.assertEqual(both.transforms[0], scale)
        self.assertEqual(both.children[0], justchildren)
        self.assertEqual(both.children[1], justtransform)
        collada.scene.Node.load(self.dummy, fromstring(tostring(both.xmlnode)), {})
        self.assertEqual(len(both.children), 2)
        self.assertEqual(len(both.transforms), 1)

    def test_scene_node_saving(self):
        myemptynode = collada.scene.Node('myemptynode')
        rotate = collada.scene.RotateTransform(0.1, 0.2, 0.3, 90)
        scale = collada.scene.ScaleTransform(0.1, 0.2, 0.3)
        mynode = collada.scene.Node('mynode', children=[myemptynode], transforms=[rotate, scale])
        self.assertEqual(mynode.id, 'mynode')
        self.assertEqual(mynode.name, 'mynode')
        self.assertEqual(mynode.children[0], myemptynode)
        self.assertEqual(mynode.transforms[0], rotate)
        self.assertEqual(mynode.transforms[1], scale)

        translate = collada.scene.TranslateTransform(0.1, 0.2, 0.3)
        mynode.transforms.append(translate)
        mynode.transforms.pop(0)
        youremptynode = collada.scene.Node('youremptynode')
        mynode.children.append(youremptynode)
        mynode.id = 'yournode'
        mynode.save()

        yournode = collada.scene.Node.load(self.dummy, fromstring(tostring(mynode.xmlnode)), {})
        self.assertEqual(yournode.id, 'yournode')
        self.assertEqual(yournode.name, 'mynode')
        self.assertEqual(len(yournode.children), 2)
        self.assertEqual(len(yournode.transforms), 2)
        self.assertEqual(yournode.children[0].id, 'myemptynode')
        self.assertEqual(yournode.children[1].id, 'youremptynode')
        self.assertTrue(isinstance(yournode.transforms[0], collada.scene.ScaleTransform))
        self.assertTrue(isinstance(yournode.transforms[1], collada.scene.TranslateTransform))

        translate = collada.scene.TranslateTransform(0.1, 0.2, 0.3)
        mynode.transforms.append(translate)
        mynode.transforms.pop(0)
        youremptynode = collada.scene.Node('youremptynode')
        mynode.children.append(youremptynode)
        mynode.id = 'yournode'
        mynode.name = 'yourname'
        mynode.save()

        yournode = collada.scene.Node.load(self.dummy, fromstring(tostring(mynode.xmlnode)), {})
        self.assertEqual(yournode.id, 'yournode')
        self.assertEqual(yournode.name, 'yourname')

        myemptynode = collada.scene.Node('myemptynode')
        rotate = collada.scene.RotateTransform(0.1, 0.2, 0.3, 90)
        scale = collada.scene.ScaleTransform(0.1, 0.2, 0.3)
        mynode = collada.scene.Node('mynode', children=[myemptynode], transforms=[rotate, scale])
        self.assertEqual(mynode.id, 'mynode')
        self.assertEqual(mynode.name, 'mynode')
        mynode = collada.scene.Node('othernode', children=[myemptynode], transforms=[rotate, scale], name='othername')
        self.assertEqual(mynode.id, 'othernode')
        self.assertEqual(mynode.name, 'othername')

    def test_scene_material_node(self):
        binding1 = ("TEX0", "TEXCOORD", "0")
        binding2 = ("TEX1", "TEXCOORD", "1")
        binding3 = ("TEX2", "TEXCOORD", "2")
        matnode = collada.scene.MaterialNode("mygeommatref", self.effect, [binding1, binding2])

        self.assertEqual(matnode.target, self.effect)
        self.assertEqual(matnode.symbol, "mygeommatref")
        self.assertListEqual(matnode.inputs, [binding1, binding2])
        self.assertIsNotNone(str(matnode))
        matnode.save()
        self.assertEqual(matnode.target, self.effect)
        self.assertEqual(matnode.symbol, "mygeommatref")
        self.assertListEqual(matnode.inputs, [binding1, binding2])

        matnode.symbol = 'yourgeommatref'
        matnode.target = self.effect2
        matnode.inputs.append(binding3)
        matnode.inputs.pop(0)
        matnode.save()

        loaded_matnode = collada.scene.MaterialNode.load(self.dummy, fromstring(tostring(matnode.xmlnode)))
        self.assertEqual(loaded_matnode.target.id, self.effect2.id)
        self.assertEqual(loaded_matnode.symbol, "yourgeommatref")
        self.assertListEqual(loaded_matnode.inputs, [binding2, binding3])

    def test_scene_geometry_node(self):
        binding = ("TEX0", "TEXCOORD", "0")
        matnode = collada.scene.MaterialNode("mygeommatref", self.effect, [binding])
        geomnode = collada.scene.GeometryNode(self.geometry, [matnode])

        bindtest = list(geomnode.objects('geometry'))
        self.assertEqual(len(bindtest), 1)
        self.assertEqual(bindtest[0].original, self.geometry)
        self.assertEqual(geomnode.geometry, self.geometry)
        self.assertListEqual(geomnode.materials, [matnode])
        self.assertIsNotNone(str(geomnode))
        geomnode.save()
        bindtest = list(geomnode.objects('geometry'))
        self.assertEqual(len(bindtest), 1)
        self.assertEqual(bindtest[0].original, self.geometry)
        self.assertEqual(geomnode.geometry, self.geometry)
        self.assertListEqual(geomnode.materials, [matnode])

        matnode2 = collada.scene.MaterialNode("yourgeommatref", self.effect, [binding])
        geomnode.materials.append(matnode2)
        geomnode.materials.pop(0)
        geomnode.geometry = self.geometry2
        geomnode.save()

        loaded_geomnode = collada.scene.loadNode(self.dummy, fromstring(tostring(geomnode.xmlnode)), {})
        self.assertEqual(loaded_geomnode.geometry.id, self.geometry2.id)
        self.assertEqual(len(loaded_geomnode.materials), 1)
        self.assertEqual(loaded_geomnode.materials[0].target, matnode2.target)
        self.assertEqual(loaded_geomnode.materials[0].symbol, "yourgeommatref")
        self.assertListEqual(loaded_geomnode.materials[0].inputs, [binding])

    def test_scene_node_with_instances(self):
        binding = ("TEX0", "TEXCOORD", "0")
        matnode = collada.scene.MaterialNode("mygeommatref", self.effect, [binding])
        geomnode = collada.scene.GeometryNode(self.geometry, [matnode])
        camnode = collada.scene.CameraNode(self.yourcam)
        lightnode = collada.scene.LightNode(self.yourdirlight)
        myemptynode = collada.scene.Node('myemptynode')
        rotate = collada.scene.RotateTransform(0.1, 0.2, 0.3, 90)
        scale = collada.scene.ScaleTransform(0.1, 0.2, 0.3)
        mynode = collada.scene.Node('mynode',
                                    children=[myemptynode, geomnode, camnode, lightnode],
                                    transforms=[rotate, scale])

        self.assertEqual(len(mynode.children), 4)
        self.assertEqual(mynode.children[0], myemptynode)
        self.assertEqual(mynode.children[1], geomnode)
        self.assertEqual(mynode.children[2], camnode)
        self.assertEqual(mynode.children[3], lightnode)
        self.assertEqual(mynode.transforms[0], rotate)
        self.assertEqual(mynode.transforms[1], scale)

        mynode.id = 'yournode'
        mynode.children.pop(0)
        mynode.save()

        yournode = collada.scene.Node.load(self.dummy, fromstring(tostring(mynode.xmlnode)), {})
        self.assertEqual(yournode.id, 'yournode')
        self.assertEqual(len(yournode.children), 3)
        self.assertEqual(len(yournode.transforms), 2)
        self.assertEqual(yournode.children[0].geometry.id, self.geometry.id)
        self.assertEqual(yournode.children[1].camera.id, self.yourcam.id)
        self.assertEqual(yournode.children[2].light.id, self.yourdirlight.id)
        self.assertTrue(isinstance(yournode.transforms[0], collada.scene.RotateTransform))
        self.assertTrue(isinstance(yournode.transforms[1], collada.scene.ScaleTransform))

    def test_scene_with_nodes(self):
        rotate = collada.scene.RotateTransform(0.1, 0.2, 0.3, 90)
        scale = collada.scene.ScaleTransform(0.1, 0.2, 0.3)
        mynode = collada.scene.Node('mynode', children=[], transforms=[rotate, scale])
        yournode = collada.scene.Node('yournode', children=[], transforms=[])
        othernode = collada.scene.Node('othernode', children=[], transforms=[])
        scene = collada.scene.Scene('myscene', [mynode, yournode, othernode])

        self.assertEqual(scene.id, 'myscene')
        self.assertEqual(len(scene.nodes), 3)
        self.assertEqual(scene.nodes[0], mynode)
        self.assertEqual(scene.nodes[1], yournode)
        self.assertEqual(scene.nodes[2], othernode)

        scene.id = 'yourscene'
        scene.nodes.pop(1)
        anothernode = collada.scene.Node('anothernode')
        scene.nodes.append(anothernode)
        scene.save()

        loaded_scene = collada.scene.Scene.load(self.dummy, fromstring(tostring(scene.xmlnode)))

        self.assertEqual(loaded_scene.id, 'yourscene')
        self.assertEqual(len(loaded_scene.nodes), 3)
        self.assertEqual(loaded_scene.nodes[0].id, 'mynode')
        self.assertEqual(loaded_scene.nodes[1].id, 'othernode')
        self.assertEqual(loaded_scene.nodes[2].id, 'anothernode')


if __name__ == '__main__':
    unittest.main()
