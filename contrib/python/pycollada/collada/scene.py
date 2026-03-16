####################################################################
#                                                                  #
# THIS FILE IS PART OF THE pycollada LIBRARY SOURCE CODE.          #
# USE, DISTRIBUTION AND REPRODUCTION OF THIS LIBRARY SOURCE IS     #
# GOVERNED BY A BSD-STYLE SOURCE LICENSE INCLUDED WITH THIS SOURCE #
# IN 'COPYING'. PLEASE READ THESE TERMS BEFORE DISTRIBUTING.       #
#                                                                  #
# THE pycollada SOURCE CODE IS (C) COPYRIGHT 2011                  #
# by Jeff Terrace and contributors                                 #
#                                                                  #
####################################################################

"""This module contains several classes related to the scene graph.

Supported scene nodes are:
  * <node> which is loaded as a Node
  * <instance_camera> which is loaded as a CameraNode
  * <instance_light> which is loaded as a LightNode
  * <instance_material> which is loaded as a MaterialNode
  * <instance_geometry> which is loaded as a GeometryNode
  * <instance_controller> which is loaded as a ControllerNode
  * <scene> which is loaded as a Scene

"""

import collada
import numpy

from collada.common import DaeBrokenRefError
from collada.common import DaeError
from collada.common import DaeMalformedError
from collada.common import DaeObject
from collada.common import DaeUnsupportedError
from collada.common import E
from collada.common import tag
from collada.util import toUnitVec
from collada.xmlutil import etree as ElementTree

# Pre-allocated identity matrix for reuse (read-only, never modify directly)
_IDENTITY_MATRIX = numpy.identity(4, dtype=numpy.float32)
_IDENTITY_MATRIX.flags.writeable = False


class DaeInstanceNotLoadedError(Exception):
    """Raised when an instance_node refers to a node that isn't loaded yet. Will always be caught"""

    def __init__(self, msg):
        super(DaeInstanceNotLoadedError, self).__init__()
        self.msg = msg


class SceneNode(DaeObject):
    """Abstract base class for all nodes within a scene."""

    def objects(self, tipo, matrix=None):
        """Iterate through all objects under this node that match `tipo`.
        The objects will be bound and transformed via the scene transformations.

        :param str tipo:
          A string for the desired object type. This can be one of 'geometry',
          'camera', 'light', or 'controller'.
        :param numpy.array matrix:
          An optional transformation matrix

        :rtype: generator that yields the type specified

        """


def makeRotationMatrix(x, y, z, angle):
    """Build and return a transform 4x4 matrix to rotate `angle` radians
    around (`x`,`y`,`z`) axis."""
    c = numpy.cos(angle)
    s = numpy.sin(angle)
    t = (1 - c)
    return numpy.array([[t * x * x + c, t * x * y - s * z, t * x * z + s * y, 0],
                        [t * x * y + s * z, t * y * y + c, t * y * z - s * x, 0],
                        [t * x * z - s * y, t * y * z + s * x, t * z * z + c, 0],
                        [0, 0, 0, 1]],
                       dtype=numpy.float32)


class Transform(DaeObject):
    """Base class for all transformation types"""

    def save(self):
        pass


class TranslateTransform(Transform):
    """Contains a translation transformation as defined in the collada <translate> tag."""

    def __init__(self, x, y, z, xmlnode=None):
        """Creates a translation transformation

        :param float x:
          x coordinate
        :param float y:
          y coordinate
        :param float z:
          z coordinate
        :param xmlnode:
           When loaded, the xmlnode it comes from

        """
        self.x = x
        """x coordinate"""
        self.y = y
        """y coordinate"""
        self.z = z
        """z coordinate"""
        self.matrix = numpy.identity(4, dtype=numpy.float32)
        """The resulting transformation matrix. This will be a numpy.array of size 4x4."""
        self.matrix[:3, 3] = [x, y, z]
        self.xmlnode = xmlnode
        """ElementTree representation of the transform."""
        if xmlnode is None:
            self.xmlnode = E.translate(' '.join([str(x), str(y), str(z)]))

    @staticmethod
    def load(collada, node):
        try:
            floats = numpy.fromstring(node.text, dtype=numpy.float32, sep=' ')
        except ValueError:
            raise DaeMalformedError("Failed to parse translate transform floats")
        if len(floats) != 3:
            raise DaeMalformedError("Translate node requires three float values")
        return TranslateTransform(floats[0], floats[1], floats[2], node)

    def __str__(self):
        return '<TranslateTransform (%s, %s, %s)>' % (self.x, self.y, self.z)

    def __repr__(self):
        return str(self)


class RotateTransform(Transform):
    """Contains a rotation transformation as defined in the collada <rotate> tag."""

    def __init__(self, x, y, z, angle, xmlnode=None):
        """Creates a rotation transformation

        :param float x:
          x coordinate
        :param float y:
          y coordinate
        :param float z:
          z coordinate
        :param float angle:
          angle of rotation, in degrees
        :param xmlnode:
           When loaded, the xmlnode it comes from

        """
        self.x = x
        """x coordinate"""
        self.y = y
        """y coordinate"""
        self.z = z
        """z coordinate"""
        self.angle = angle
        """angle of rotation, in degrees"""
        self.matrix = makeRotationMatrix(x, y, z, angle * numpy.pi / 180.0)
        """The resulting transformation matrix. This will be a numpy.array of size 4x4."""
        self.xmlnode = xmlnode
        """ElementTree representation of the transform."""
        if xmlnode is None:
            self.xmlnode = E.rotate(' '.join([str(x), str(y), str(z), str(angle)]))

    @staticmethod
    def load(collada, node):
        try:
            floats = numpy.fromstring(node.text, dtype=numpy.float32, sep=' ')
        except ValueError:
            raise DaeMalformedError("Failed to parse rotate transform floats")
        if len(floats) != 4:
            raise DaeMalformedError("Rotate node requires four float values")
        return RotateTransform(floats[0], floats[1], floats[2], floats[3], node)

    def __str__(self):
        return '<RotateTransform (%s, %s, %s) angle=%s>' % (self.x, self.y, self.z, self.angle)

    def __repr__(self):
        return str(self)


class ScaleTransform(Transform):
    """Contains a scale transformation as defined in the collada <scale> tag."""

    def __init__(self, x, y, z, xmlnode=None):
        """Creates a scale transformation

        :param float x:
          x coordinate
        :param float y:
          y coordinate
        :param float z:
          z coordinate
        :param xmlnode:
           When loaded, the xmlnode it comes from

        """
        self.x = x
        """x coordinate"""
        self.y = y
        """y coordinate"""
        self.z = z
        """z coordinate"""
        self.matrix = numpy.identity(4, dtype=numpy.float32)
        """The resulting transformation matrix. This will be a numpy.array of size 4x4."""
        self.matrix[0, 0] = x
        self.matrix[1, 1] = y
        self.matrix[2, 2] = z
        self.xmlnode = xmlnode
        """ElementTree representation of the transform."""
        if xmlnode is None:
            self.xmlnode = E.scale(' '.join([str(x), str(y), str(z)]))

    @staticmethod
    def load(collada, node):
        try:
            floats = numpy.fromstring(node.text, dtype=numpy.float32, sep=' ')
        except ValueError:
            raise DaeMalformedError("Failed to parse scale transform floats")
        if len(floats) != 3:
            raise DaeMalformedError("Scale node requires three float values")
        return ScaleTransform(floats[0], floats[1], floats[2], node)

    def __str__(self):
        return '<ScaleTransform (%s, %s, %s)>' % (self.x, self.y, self.z)

    def __repr__(self):
        return str(self)


class MatrixTransform(Transform):
    """Contains a matrix transformation as defined in the collada <matrix> tag."""

    def __init__(self, matrix, xmlnode=None):
        """Creates a matrix transformation

        :param numpy.array matrix:
          This should be an unshaped numpy array of floats of length 16
        :param xmlnode:
           When loaded, the xmlnode it comes from

        """
        self.matrix = matrix
        """The resulting transformation matrix. This will be a numpy.array of size 4x4."""
        if len(self.matrix) != 16:
            raise DaeMalformedError('Corrupted matrix transformation node')
        self.matrix.shape = (4, 4)
        self.xmlnode = xmlnode
        """ElementTree representation of the transform."""
        if xmlnode is None:
            self.xmlnode = E.matrix(' '.join(map(str, self.matrix.flat)))

    @staticmethod
    def load(collada, node):
        try:
            floats = numpy.fromstring(node.text, dtype=numpy.float32, sep=' ')
        except ValueError:
            raise DaeMalformedError("Failed to parse matrix transform floats")
        return MatrixTransform(floats, node)

    def __str__(self):
        return '<MatrixTransform>'

    def __repr__(self):
        return str(self)


class LookAtTransform(Transform):
    """Contains a transformation for aiming a camera as defined in the collada <lookat> tag."""

    def __init__(self, eye, interest, upvector, xmlnode=None):
        """Creates a lookat transformation

        :param numpy.array eye:
          An unshaped numpy array of floats of length 3 containing the position of the eye
        :param numpy.array interest:
          An unshaped numpy array of floats of length 3 containing the point of interest
        :param numpy.array upvector:
          An unshaped numpy array of floats of length 3 containing the up-axis direction
        :param xmlnode:
          When loaded, the xmlnode it comes from

        """
        self.eye = eye
        """A numpy array of length 3 containing the position of the eye"""
        self.interest = interest
        """A numpy array of length 3 containing the point of interest"""
        self.upvector = upvector
        """A numpy array of length 3 containing the up-axis direction"""

        if len(eye) != 3 or len(interest) != 3 or len(upvector) != 3:
            raise DaeMalformedError('Corrupted lookat transformation node')

        self.matrix = numpy.identity(4, dtype=numpy.float32)
        """The resulting transformation matrix. This will be a numpy.array of size 4x4."""

        front = toUnitVec(numpy.subtract(eye, interest))
        side = numpy.multiply(-1, toUnitVec(numpy.cross(front, upvector)))
        self.matrix[0, 0:3] = side
        self.matrix[1, 0:3] = upvector
        self.matrix[2, 0:3] = front
        self.matrix[3, 0:3] = eye

        self.xmlnode = xmlnode
        """ElementTree representation of the transform."""
        if xmlnode is None:
            self.xmlnode = E.lookat(' '.join(map(str,
                                                 numpy.concatenate((self.eye, self.interest, self.upvector)))))

    @staticmethod
    def load(collada, node):
        try:
            floats = numpy.fromstring(node.text, dtype=numpy.float32, sep=' ')
        except ValueError:
            raise DaeMalformedError("Failed to parse lookat transform floats")
        if len(floats) != 9:
            raise DaeMalformedError("Lookat node requires 9 float values")
        return LookAtTransform(floats[0:3], floats[3:6], floats[6:9], node)

    def __str__(self):
        return '<LookAtTransform>'

    def __repr__(self):
        return str(self)


class Node(SceneNode):
    """Represents a node object, which is a point on the scene graph, as defined in the collada <node> tag.

    Contains the list of transformations effecting the node as well as any children.
    """

    def __init__(self, id, children=None, transforms=None, xmlnode=None, name=None):
        """Create a node in the scene graph.

        :param str id:
          A unique string identifier for the node
        :param list children:
          A list of child nodes of this node. This can contain any
          object that inherits from :class:`collada.scene.SceneNode`
        :param list transforms:
          A list of transformations effecting the node. This can
          contain any object that inherits from :class:`collada.scene.Transform`
        :param xmlnode:
          When loaded, the xmlnode it comes from
        :param name:
          If given, sets the node name property, if missing (None) then then
          node name property is set from the id value (if not None)

        """
        self.id = id
        """The unique string identifier for the node"""
        self.name = id if name is None else name
        """The optional string node name (not required to be unique)for the node, by default set to the id value"""
        self.children = []
        """A list of child nodes of this node. This can contain any
          object that inherits from :class:`collada.scene.SceneNode`"""
        if children is not None:
            self.children = children
        self.transforms = []
        if transforms is not None:
            self.transforms = transforms
        """A list of transformations effecting the node. This can
          contain any object that inherits from :class:`collada.scene.Transform`"""

        # Use cached identity matrix when no transforms, otherwise compute the combined matrix
        if len(self.transforms) == 0:
            self.matrix = _IDENTITY_MATRIX
        elif len(self.transforms) == 1:
            self.matrix = self.transforms[0].matrix.copy()
        else:
            self.matrix = numpy.identity(4, dtype=numpy.float32)
            for t in self.transforms:
                self.matrix = numpy.dot(self.matrix, t.matrix)
        """A numpy.array of size 4x4 containing a transformation matrix that
        combines all the transformations in :attr:`transforms`. This will only
        be updated after calling :meth:`save`."""

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the transform."""
        else:
            self.xmlnode = E.node(id=self.id, name=self.name)
            for t in self.transforms:
                self.xmlnode.append(t.xmlnode)
            for c in self.children:
                self.xmlnode.append(c.xmlnode)

    def objects(self, tipo, matrix=None):
        """Iterate through all objects under this node that match `tipo`.
        The objects will be bound and transformed via the scene transformations.

        :param str tipo:
          A string for the desired object type. This can be one of 'geometry',
          'camera', 'light', or 'controller'.
        :param numpy.array matrix:
          An optional transformation matrix

        :rtype: generator that yields the type specified

        """
        if matrix is not None:
            M = numpy.dot(matrix, self.matrix)
        else:
            M = self.matrix
        for node in self.children:
            for obj in node.objects(tipo, M):
                yield obj

    def save(self):
        """Saves the geometry back to :attr:`xmlnode`. Also updates
        :attr:`matrix` if :attr:`transforms` has been modified."""
        if len(self.transforms) == 0:
            self.matrix = _IDENTITY_MATRIX
        else:
            self.matrix = numpy.identity(4, dtype=numpy.float32)
            for t in self.transforms:
                self.matrix = numpy.dot(self.matrix, t.matrix)

        for child in self.children:
            child.save()

        if self.id is not None:
            self.xmlnode.set('id', self.id)
        if self.name is not None:
            self.xmlnode.set('name', self.name)

        for t in self.transforms:
            if t.xmlnode not in self.xmlnode:
                self.xmlnode.append(t.xmlnode)
        for c in self.children:
            if c.xmlnode not in self.xmlnode:
                self.xmlnode.append(c.xmlnode)
        xmlnodes = [c.xmlnode for c in self.children]
        xmlnodes.extend([t.xmlnode for t in self.transforms])
        for n in self.xmlnode:
            if n not in xmlnodes:
                self.xmlnode.remove(n)

    @staticmethod
    def load(collada, node, localscope):
        id = node.get('id')
        name = node.get('name')
        children = []
        transforms = []

        dispatch = getattr(collada, '_node_dispatch', None)
        if dispatch is None:
            dispatch = _build_node_dispatch(collada)

        for subnode in node:
            entry = dispatch.get(subnode.tag)
            if entry is None:
                collada.handleError(DaeUnsupportedError('Unknown scene node %s' % str(subnode.tag)))
                continue

            load_type, loader_class, is_transform = entry
            if load_type == 'none':
                continue

            try:
                if load_type == 'node':
                    n = loader_class.load(collada, subnode, localscope)
                else:
                    n = loader_class.load(collada, subnode)

                if n is not None:
                    if is_transform:
                        transforms.append(n)
                    else:
                        children.append(n)
            except DaeError as ex:
                collada.handleError(ex)

        return Node(id, children, transforms, xmlnode=node, name=name)

    def __str__(self):
        return '<Node transforms=%d, children=%d>' % (len(self.transforms), len(self.children))

    def __repr__(self):
        return str(self)


class NodeNode(Node):
    """Represents a node being instantiated in a scene, as defined in the collada <instande_node> tag."""

    def __init__(self, node, xmlnode=None):
        """Creates a node node

        :param collada.scene.Node node:
          A node to instantiate in the scene
        :param xmlnode:
          When loaded, the xmlnode it comes from

        """
        self.node = node
        """An object of type :class:`collada.scene.Node` representing the node to bind in the scene"""

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the node node."""
        else:
            self.xmlnode = E.instance_node(url="#%s" % self.node.id)

    def objects(self, tipo, matrix=None):
        for obj in self.node.objects(tipo, matrix):
            yield obj

    id = property(lambda s: s.node.id)
    children = property(lambda s: s.node.children)
    matrix = property(lambda s: s.node.matrix)

    @staticmethod
    def load(collada, node, localscope):
        url = node.get('url')
        if not url.startswith('#'):
            raise DaeMalformedError('Invalid url in node instance %s' % url)
        referred_node = localscope.get(url[1:])
        if not referred_node:
            referred_node = collada.nodes.get(url[1:])
        if not referred_node:
            raise DaeInstanceNotLoadedError('Node %s not found in library' % url)
        return NodeNode(referred_node, xmlnode=node)

    def save(self):
        """Saves the node node back to :attr:`xmlnode`"""
        self.xmlnode.set('url', "#%s" % self.node.id)

    def __str__(self):
        return '<NodeNode node=%s>' % (self.node.id,)

    def __repr__(self):
        return str(self)


class GeometryNode(SceneNode):
    """Represents a geometry instance in a scene, as defined in the collada <instance_geometry> tag."""

    def __init__(self, geometry, materials=None, xmlnode=None):
        """Creates a geometry node

        :param collada.geometry.Geometry geometry:
          A geometry to instantiate in the scene
        :param list materials:
          A list containing items of type :class:`collada.scene.MaterialNode`.
          Each of these represents a material that the geometry should be
          bound to.
        :param xmlnode:
          When loaded, the xmlnode it comes from

        """
        self.geometry = geometry
        """An object of type :class:`collada.geometry.Geometry` representing the
        geometry to bind in the scene"""
        self.materials = []
        """A list containing items of type :class:`collada.scene.MaterialNode`.
          Each of these represents a material that the geometry is bound to."""
        if materials is not None:
            self.materials = materials
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the geometry node."""
        else:
            self.xmlnode = E.instance_geometry(url="#%s" % self.geometry.id)
            if len(self.materials) > 0:
                self.xmlnode.append(E.bind_material(
                    E.technique_common(
                        *[mat.xmlnode for mat in self.materials]
                    )
                ))

    def objects(self, tipo, matrix=None):
        """Yields a :class:`collada.geometry.BoundGeometry` if ``tipo=='geometry'``"""
        if tipo == 'geometry':
            if matrix is None:
                matrix = _IDENTITY_MATRIX
            materialnodesbysymbol = {mat.symbol: mat for mat in self.materials}
            yield self.geometry.bind(matrix, materialnodesbysymbol)

    @staticmethod
    def load(collada, node):
        url = node.get('url')
        if not url.startswith('#'):
            raise DaeMalformedError('Invalid url in geometry instance %s' % url)
        geometry = collada.geometries.get(url[1:])
        if not geometry:
            raise DaeBrokenRefError('Geometry %s not found in library' % url)
        # Use cached xpath or build and cache it
        mat_xpath = getattr(collada, '_geomnode_mat_xpath', None)
        if mat_xpath is None:
            mat_xpath = f"{collada.tag('bind_material')}/{collada.tag('technique_common')}/{collada.tag('instance_material')}"
            collada._geomnode_mat_xpath = mat_xpath
        matnodes = node.findall(mat_xpath)
        materials = [MaterialNode.load(collada, matnode) for matnode in matnodes]
        return GeometryNode(geometry, materials, xmlnode=node)

    def save(self):
        """Saves the geometry node back to :attr:`xmlnode`"""
        self.xmlnode.set('url', "#%s" % self.geometry.id)

        for m in self.materials:
            m.save()

        matparent = self.xmlnode.find(f"{tag('bind_material')}/{tag('technique_common')}")
        if matparent is None and len(self.materials) == 0:
            return
        elif matparent is None:
            matparent = E.technique_common()
            self.xmlnode.append(E.bind_material(matparent))
        elif len(self.materials) == 0 and matparent is not None:
            bindnode = self.xmlnode.find('%s' % tag('bind_material'))
            self.xmlnode.remove(bindnode)
            return

        for m in self.materials:
            if m.xmlnode not in matparent:
                matparent.append(m.xmlnode)
        xmlnodes = [m.xmlnode for m in self.materials]
        for n in matparent:
            if n not in xmlnodes:
                matparent.remove(n)

    def __str__(self):
        return '<GeometryNode geometry=%s>' % (self.geometry.id,)

    def __repr__(self):
        return str(self)


class ControllerNode(SceneNode):
    """Represents a controller instance in a scene, as defined in the collada <instance_controller> tag. **This class is highly
    experimental. More support will be added in version 0.4.**"""

    def __init__(self, controller, materials, xmlnode=None):
        """Creates a controller node

        :param collada.controller.Controller controller:
          A controller to instantiate in the scene
        :param list materials:
          A list containing items of type :class:`collada.scene.MaterialNode`.
          Each of these represents a material that the controller should be
          bound to.
        :param xmlnode:
          When loaded, the xmlnode it comes from

        """
        self.controller = controller
        """ An object of type :class:`collada.controller.Controller` representing
        the controller being instantiated in the scene"""
        self.materials = materials
        """A list containing items of type :class:`collada.scene.MaterialNode`.
          Each of these represents a material that the controller is bound to."""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the controller node."""
        else:
            self.xmlnode = ElementTree.Element(collada.tag('instance_controller'))
            bindnode = ElementTree.Element(collada.tag('bind_material'))
            technode = ElementTree.Element(collada.tag('technique_common'))
            bindnode.append(technode)
            self.xmlnode.append(bindnode)
            for mat in materials:
                technode.append(mat.xmlnode)

    def objects(self, tipo, matrix=None):
        """Yields a :class:`collada.controller.BoundController` if ``tipo=='controller'``"""
        if tipo == 'controller':
            if matrix is None:
                matrix = _IDENTITY_MATRIX
            materialnodesbysymbol = {mat.symbol: mat for mat in self.materials}
            yield self.controller.bind(matrix, materialnodesbysymbol)

    @staticmethod
    def load(collada, node):
        url = node.get('url')
        if not url.startswith('#'):
            raise DaeMalformedError('Invalid url in controller instance %s' % url)
        controller = collada.controllers.get(url[1:])
        if not controller:
            raise DaeBrokenRefError('Controller %s not found in library' % url)
        # Use cached xpath (same as GeometryNode) or build and cache it
        mat_xpath = getattr(collada, '_geomnode_mat_xpath', None)
        if mat_xpath is None:
            mat_xpath = f"{collada.tag('bind_material')}/{collada.tag('technique_common')}/{collada.tag('instance_material')}"
            collada._geomnode_mat_xpath = mat_xpath
        matnodes = node.findall(mat_xpath)
        materials = [MaterialNode.load(collada, matnode) for matnode in matnodes]
        return ControllerNode(controller, materials, xmlnode=node)

    def save(self):
        """Saves the controller node back to :attr:`xmlnode`"""
        self.xmlnode.set('url', '#' + self.controller.id)
        for mat in self.materials:
            mat.save()

    def __str__(self):
        return '<ControllerNode controller=%s>' % (self.controller.id,)

    def __repr__(self):
        return str(self)


class MaterialNode(SceneNode):
    """Represents a material being instantiated in a scene, as defined in the collada <instance_material> tag."""

    def __init__(self, symbol, target, inputs, xmlnode=None):
        """Creates a material node

        :param str symbol:
          The symbol within a geometry this material should be bound to
        :param collada.material.Material target:
          The material object being bound to
        :param list inputs:
          A list of tuples of the form ``(semantic, input_semantic, set)`` mapping
          texcoords or other inputs to material input channels, e.g.
          ``('TEX0', 'TEXCOORD', '0')`` would map the effect parameter ``'TEX0'``
          to the ``'TEXCOORD'`` semantic of the geometry, using texture coordinate
          set ``0``.
        :param xmlnode:
          When loaded, the xmlnode it comes from

        """
        self.symbol = symbol
        """The symbol within a geometry this material should be bound to"""
        self.target = target
        """An object of type :class:`collada.material.Material` representing the material object being bound to"""
        self.inputs = inputs
        """A list of tuples of the form ``(semantic, input_semantic, set)`` mapping
          texcoords or other inputs to material input channels, e.g.
          ``('TEX0', 'TEXCOORD', '0')`` would map the effect parameter ``'TEX0'``
          to the ``'TEXCOORD'`` semantic of the geometry, using texture coordinate
          set ``0``."""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the material node."""
        else:
            self.xmlnode = E.instance_material(
                *[E.bind_vertex_input(semantic=sem, input_semantic=input_sem, input_set=set)
                  for sem, input_sem, set in self.inputs], **{'symbol': self.symbol, 'target': "#%s" % self.target.id})

    @staticmethod
    def load(collada, node):
        inputs = []
        for inputnode in node.findall(collada.tag('bind_vertex_input')):
            inputs.append((inputnode.get('semantic'), inputnode.get('input_semantic'), inputnode.get('input_set')))
        targetid = node.get('target')
        if not targetid.startswith('#'):
            raise DaeMalformedError('Incorrect target id in material ' + targetid)
        target = collada.materials.get(targetid[1:])
        if not target:
            raise DaeBrokenRefError('Material %s not found' % targetid)
        return MaterialNode(node.get('symbol'), target, inputs, xmlnode=node)

    def objects(self):
        pass

    def save(self):
        """Saves the material node back to :attr:`xmlnode`"""
        self.xmlnode.set('symbol', self.symbol)
        self.xmlnode.set('target', "#%s" % self.target.id)

        inputs_in = []
        for i in self.xmlnode.findall(tag('bind_vertex_input')):
            input_tuple = (i.get('semantic'), i.get('input_semantic'), i.get('input_set'))
            if input_tuple not in self.inputs:
                self.xmlnode.remove(i)
            else:
                inputs_in.append(input_tuple)
        for i in self.inputs:
            if i not in inputs_in:
                self.xmlnode.append(E.bind_vertex_input(semantic=i[0], input_semantic=i[1], input_set=i[2]))

    def __str__(self):
        return '<MaterialNode symbol=%s targetid=%s>' % (self.symbol, self.target.id)

    def __repr__(self):
        return str(self)


class CameraNode(SceneNode):
    """Represents a camera being instantiated in a scene, as defined in the collada <instance_camera> tag."""

    def __init__(self, camera, xmlnode=None):
        """Create a camera instance

        :param collada.camera.Camera camera:
          The camera being instantiated
        :param xmlnode:
          When loaded, the xmlnode it comes from

        """
        self.camera = camera
        """An object of type :class:`collada.camera.Camera` representing the instantiated camera"""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the camera node."""
        else:
            self.xmlnode = E.instance_camera(url="#%s" % camera.id)

    def objects(self, tipo, matrix=None):
        """Yields a :class:`collada.camera.BoundCamera` if ``tipo=='camera'``"""
        if tipo == 'camera':
            if matrix is None:
                matrix = _IDENTITY_MATRIX
            yield self.camera.bind(matrix)

    @staticmethod
    def load(collada, node):
        url = node.get('url')
        if not url.startswith('#'):
            raise DaeMalformedError('Invalid url in camera instance %s' % url)
        camera = collada.cameras.get(url[1:])
        if not camera:
            raise DaeBrokenRefError('Camera %s not found in library' % url)
        return CameraNode(camera, xmlnode=node)

    def save(self):
        """Saves the camera node back to :attr:`xmlnode`"""
        self.xmlnode.set('url', '#' + self.camera.id)

    def __str__(self):
        return '<CameraNode camera=%s>' % (self.camera.id,)

    def __repr__(self):
        return str(self)


class LightNode(SceneNode):
    """Represents a light being instantiated in a scene, as defined in the collada <instance_light> tag."""

    def __init__(self, light, xmlnode=None):
        """Create a light instance

        :param collada.light.Light light:
          The light being instantiated
        :param xmlnode:
          When loaded, the xmlnode it comes from

        """
        self.light = light
        """An object of type :class:`collada.light.Light` representing the instantiated light"""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the light node."""
        else:
            self.xmlnode = E.instance_light(url="#%s" % light.id)

    def objects(self, tipo, matrix=None):
        """Yields a :class:`collada.light.BoundLight` if ``tipo=='light'``"""
        if tipo == 'light':
            if matrix is None:
                matrix = _IDENTITY_MATRIX
            yield self.light.bind(matrix)

    @staticmethod
    def load(collada, node):
        url = node.get('url')
        if not url.startswith('#'):
            raise DaeMalformedError('Invalid url in light instance %s' % url)
        light = collada.lights.get(url[1:])
        if not light:
            raise DaeBrokenRefError('Light %s not found in library' % url)
        return LightNode(light, xmlnode=node)

    def save(self):
        """Saves the light node back to :attr:`xmlnode`"""
        self.xmlnode.set('url', '#' + self.light.id)

    def __str__(self):
        return '<LightNode light=%s>' % (self.light.id,)

    def __repr__(self):
        return str(self)


class ExtraNode(SceneNode):
    """Represents extra information in a scene, as defined in a collada <extra> tag."""

    def __init__(self, xmlnode):
        """Create an extra node which stores arbitrary xml

        :param xmlnode:
          Should be an ElementTree instance of tag type <extra>

        """
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the extra node."""
        else:
            self.xmlnode = E.extra()

    def objects(self, tipo, matrix=None):
        if tipo == 'extra':
            for e in self.xmlnode.findall(tag(tipo)):
                yield e

    @staticmethod
    def load(collada, node):
        return ExtraNode(node)

    def save(self):
        pass


def _build_node_dispatch(collada):
    """Build the node dispatch table and cache it on the collada object."""
    dispatch = {
        collada.tag('node'): ('node', Node, False),
        collada.tag('translate'): ('simple', TranslateTransform, True),
        collada.tag('rotate'): ('simple', RotateTransform, True),
        collada.tag('scale'): ('simple', ScaleTransform, True),
        collada.tag('matrix'): ('simple', MatrixTransform, True),
        collada.tag('lookat'): ('simple', LookAtTransform, True),
        collada.tag('instance_geometry'): ('simple', GeometryNode, False),
        collada.tag('instance_camera'): ('simple', CameraNode, False),
        collada.tag('instance_light'): ('simple', LightNode, False),
        collada.tag('instance_controller'): ('simple', ControllerNode, False),
        collada.tag('instance_node'): ('node', NodeNode, False),
        collada.tag('extra'): ('simple', ExtraNode, False),
        collada.tag('asset'): ('none', None, False),
    }
    collada._node_dispatch = dispatch
    return dispatch


def loadNode(collada, node, localscope):
    """Generic scene node loading from an xml `node` and a `collada` object.

    Knowing the supported nodes, create the appropriate class for the given node
    and return it.

    """
    # Inline check, call builder only if needed
    dispatch = getattr(collada, '_node_dispatch', None)
    if dispatch is None:
        dispatch = _build_node_dispatch(collada)

    entry = dispatch.get(node.tag)
    if entry is None:
        raise DaeUnsupportedError('Unknown scene node %s' % str(node.tag))
    load_type, loader_class, _ = entry
    if load_type == 'node':
        return loader_class.load(collada, node, localscope)
    elif load_type == 'simple':
        return loader_class.load(collada, node)
    else:  # 'none'
        return None


class Scene(DaeObject):
    """The root object for a scene, as defined in a collada <scene> tag"""

    def __init__(self, id, nodes, xmlnode=None, collada=None):
        """Create a scene

        :param str id:
          A unique string identifier for the scene
        :param list nodes:
          A list of type :class:`collada.scene.Node` representing the nodes in the scene
        :param xmlnode:
          When loaded, the xmlnode it comes from
        :param collada:
          The collada instance this is part of

        """
        self.id = id
        """The unique string identifier for the scene"""
        self.nodes = nodes
        """A list of type :class:`collada.scene.Node` representing the nodes in the scene"""
        self.collada = collada
        """The collada instance this is part of"""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the scene node."""
        else:
            self.xmlnode = E.visual_scene(id=self.id)
            for node in nodes:
                self.xmlnode.append(node.xmlnode)

    def objects(self, tipo):
        """Iterate through all objects in the scene that match `tipo`.
        The objects will be bound and transformed via the scene transformations.

        :param str tipo:
          A string for the desired object type. This can be one of 'geometry',
          'camera', 'light', or 'controller'.

        :rtype: generator that yields the type specified

        """
        matrix = None
        for node in self.nodes:
            for obj in node.objects(tipo, matrix):
                yield obj

    @staticmethod
    def load(collada, node):
        id = node.get('id')
        nodes = []
        tried_loading = []
        succeeded = False
        localscope = {}
        for nodenode in node.findall(collada.tag('node')):
            try:
                N = loadNode(collada, nodenode, localscope)
            except DaeInstanceNotLoadedError as ex:
                tried_loading.append((nodenode, ex))
            except DaeError as ex:
                collada.handleError(ex)
            else:
                if N is not None:
                    nodes.append(N)
                    if N.id and N.id not in localscope:
                        localscope[N.id] = N
                    succeeded = True
        while len(tried_loading) > 0 and succeeded:
            succeeded = False
            next_tried = []
            for nodenode, ex in tried_loading:
                try:
                    N = loadNode(collada, nodenode, localscope)
                except DaeInstanceNotLoadedError as ex:
                    next_tried.append((nodenode, ex))
                except DaeError as ex:
                    collada.handleError(ex)
                else:
                    if N is not None:
                        nodes.append(N)
                        succeeded = True
            tried_loading = next_tried
        if len(tried_loading) > 0:
            for nodenode, ex in tried_loading:
                raise DaeBrokenRefError(ex.msg)

        return Scene(id, nodes, xmlnode=node, collada=collada)

    def save(self):
        """Saves the scene back to :attr:`xmlnode`"""
        self.xmlnode.set('id', self.id)
        for node in self.nodes:
            node.save()
            if node.xmlnode not in self.xmlnode:
                self.xmlnode.append(node.xmlnode)
        xmlnodes = [n.xmlnode for n in self.nodes]
        for node in self.xmlnode:
            if node not in xmlnodes:
                self.xmlnode.remove(node)

    def __str__(self):
        return '<Scene id=%s nodes=%d>' % (self.id, len(self.nodes))

    def __repr__(self):
        return str(self)
