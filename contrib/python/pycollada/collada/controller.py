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

"""Contains objects representing controllers. Currently has partial
    support for loading Skin and Morph. **This module is highly
    experimental. More support will be added in version 0.4.**"""

import numpy

from collada import source
from collada.common import DaeObject
from collada.common import DaeIncompleteError, DaeBrokenRefError, \
    DaeMalformedError, DaeUnsupportedError
from collada.geometry import Geometry
from collada.util import checkSource


class Controller(DaeObject):
    """Base controller class holding data from <controller> tags."""

    def bind(self, matrix, materialnodebysymbol):
        pass

    @staticmethod
    def load(collada, localscope, node):
        controller = node.find(collada.tag('skin'))
        if controller is None:
            controller = node.find(collada.tag('morph'))
        if controller is None:
            raise DaeUnsupportedError('Unknown controller node')

        sourcebyid = {}
        sourcenodes = node.findall(f"{controller.tag}/{collada.tag('source')}")
        for sourcenode in sourcenodes:
            ch = source.Source.load(collada, {}, sourcenode)
            sourcebyid[ch.id] = ch

        if controller.tag == collada.tag('skin'):
            return Skin.load(collada, sourcebyid, controller, node)
        else:
            return Morph.load(collada, sourcebyid, controller, node)


class BoundController(object):
    """Base class for a controller bound to a transform matrix and materials mapping."""


class Skin(Controller):
    """Class containing data collada holds in the <skin> tag"""

    def __init__(self, sourcebyid, bind_shape_matrix, joint_source, joint_matrix_source,
                 weight_source, weight_joint_source, vcounts, vertex_weight_index,
                 offsets, geometry, controller_node=None, skin_node=None):
        """Create a skin.

        :Parameters:
          sourceById
            A dict mapping id's to a collada source
          bind_shape_matrix
            A numpy array of floats (pre-shape)
          joint_source
            The string id for the joint source
          joint_matrix_source
            The string id for the joint matrix source
          weight_source
            The string id for the weight source
          weight_joint_source
            The string id for the joint source of weights
          vcounts
            A list with the number of influences on each vertex
          vertex_weight_index
            An array with the indexes as they come from <v> array
          offsets
            A list with the offsets in the weight index array for each source
            in (joint, weight)
          geometry
            The source geometry this should be applied to (geometry.Geometry)
          controller_node
            XML node of the <controller> tag which is the parent of this
          skin_node
            XML node of the <skin> tag if this is from there

        """
        self.sourcebyid = sourcebyid
        self.bind_shape_matrix = bind_shape_matrix
        self.joint_source = joint_source
        self.joint_matrix_source = joint_matrix_source
        self.weight_source = weight_source
        self.weight_joint_source = weight_joint_source
        self.vcounts = vcounts
        self.vertex_weight_index = vertex_weight_index
        self.offsets = offsets
        self.geometry = geometry
        self.controller_node = controller_node
        self.skin_node = skin_node
        self.xmlnode = controller_node

        if not isinstance(self.geometry, Geometry):
            raise DaeMalformedError('Invalid reference geometry in skin')

        self.id = controller_node.get('id')
        if self.id is None:
            raise DaeMalformedError('Controller node requires an ID')

        self.nindices = max(self.offsets) + 1

        if len(bind_shape_matrix) != 16:
            raise DaeMalformedError('Corrupted bind shape matrix in skin')
        self.bind_shape_matrix.shape = (4, 4)

        if not (joint_source in sourcebyid and joint_matrix_source in sourcebyid):
            raise DaeBrokenRefError("Input in joints not found")
        if not (isinstance(sourcebyid[joint_source], source.NameSource) or isinstance(sourcebyid[joint_source], source.IDRefSource)):
            raise DaeIncompleteError("Could not find joint name input for skin")
        if not isinstance(sourcebyid[joint_matrix_source], source.FloatSource):
            raise DaeIncompleteError("Could not find joint matrix source for skin")
        joint_names = [j for j in sourcebyid[joint_source]]
        joint_matrices = sourcebyid[joint_matrix_source].data
        joint_matrices.shape = (-1, 4, 4)
        if len(joint_names) != len(joint_matrices):
            raise DaeMalformedError("Skin joint and matrix inputs must be same length")
        self.joint_matrices = dict(zip(joint_names, joint_matrices))

        if not (weight_source in sourcebyid and weight_joint_source in sourcebyid):
            raise DaeBrokenRefError("Weights input in joints not found")
        if not isinstance(sourcebyid[weight_source], source.FloatSource):
            raise DaeIncompleteError("Could not find weight inputs for skin")
        if not (isinstance(sourcebyid[weight_joint_source], source.NameSource) or isinstance(sourcebyid[weight_joint_source], source.IDRefSource)):
            raise DaeIncompleteError("Could not find weight joint source input for skin")
        self.weights = sourcebyid[weight_source]
        self.weight_joints = sourcebyid[weight_joint_source]

        try:
            newshape = []
            at = 0
            for ct in self.vcounts:
                this_set = self.vertex_weight_index[self.nindices * at:self.nindices * (at + ct)]
                this_set.shape = (ct, self.nindices)
                newshape.append(numpy.array(this_set))
                at += ct
            self.index = newshape
        except BaseException:
            raise DaeMalformedError('Corrupted vcounts or index in skin weights')

        try:
            self.joint_index = [influence[:, self.offsets[0]] for influence in self.index]
            self.weight_index = [influence[:, self.offsets[1]] for influence in self.index]
        except BaseException:
            raise DaeMalformedError('Corrupted joint or weight index in skin')

        self.max_joint_index = numpy.max([numpy.max(joint) if len(joint) > 0 else 0 for joint in self.joint_index])
        self.max_weight_index = numpy.max([numpy.max(weight) if len(weight) > 0 else 0 for weight in self.weight_index])
        checkSource(self.weight_joints, ('JOINT',), self.max_joint_index)
        checkSource(self.weights, ('WEIGHT',), self.max_weight_index)

    def __len__(self):
        return len(self.index)

    def __getitem__(self, i):
        return self.index[i]

    def bind(self, matrix, materialnodebysymbol):
        """Create a bound morph from this one, transform and material mapping"""
        return BoundSkin(self, matrix, materialnodebysymbol)

    @staticmethod
    def load(collada, localscope, skinnode, controllernode):
        if len(localscope) < 3:
            raise DaeMalformedError('Not enough sources in skin')

        geometry_source = skinnode.get('source')
        if geometry_source is None or len(geometry_source) < 2 \
                or geometry_source[0] != '#':
            raise DaeBrokenRefError('Invalid source attribute of skin node')
        if not geometry_source[1:] in collada.geometries:
            raise DaeBrokenRefError('Source geometry for skin node not found')
        geometry = collada.geometries[geometry_source[1:]]

        bind_shape_mat = skinnode.find(collada.tag('bind_shape_matrix'))
        if bind_shape_mat is None:
            bind_shape_mat = numpy.identity(4, dtype=numpy.float32)
            bind_shape_mat.shape = (-1,)
        else:
            try:
                values = [float(v) for v in bind_shape_mat.text.split()]
            except ValueError:
                raise DaeMalformedError('Corrupted bind shape matrix in skin')
            bind_shape_mat = numpy.array(values, dtype=numpy.float32)

        inputnodes = skinnode.findall(f"{collada.tag('joints')}/{collada.tag('input')}")
        if inputnodes is None or len(inputnodes) < 2:
            raise DaeIncompleteError("Not enough inputs in skin joints")

        try:
            inputs = [(i.get('semantic'), i.get('source')) for i in inputnodes]
        except ValueError:
            raise DaeMalformedError('Corrupted inputs in skin')

        joint_source = None
        matrix_source = None
        for i in inputs:
            if len(i[1]) < 2 or i[1][0] != '#':
                raise DaeBrokenRefError('Input in skin node %s not found' % i[1])
            if i[0] == 'JOINT':
                joint_source = i[1][1:]
            elif i[0] == 'INV_BIND_MATRIX':
                matrix_source = i[1][1:]

        weightsnode = skinnode.find(collada.tag('vertex_weights'))
        if weightsnode is None:
            raise DaeIncompleteError("No vertex_weights found in skin")
        indexnode = weightsnode.find(collada.tag('v'))
        if indexnode is None:
            raise DaeIncompleteError('Missing indices in skin vertex weights')
        vcountnode = weightsnode.find(collada.tag('vcount'))
        if vcountnode is None:
            raise DaeIncompleteError('Missing vcount in skin vertex weights')
        inputnodes = weightsnode.findall(collada.tag('input'))

        try:
            index = numpy.array([float(v)
                                 for v in indexnode.text.split()], dtype=numpy.int32)
            vcounts = numpy.array([int(v)
                                   for v in vcountnode.text.split()], dtype=numpy.int32)
            inputs = [(i.get('semantic'), i.get('source'), int(i.get('offset')))
                      for i in inputnodes]
        except ValueError:
            raise DaeMalformedError('Corrupted index or offsets in skin vertex weights')

        weight_joint_source = None
        weight_source = None
        offsets = [0, 0]
        for i in inputs:
            if len(i[1]) < 2 or i[1][0] != '#':
                raise DaeBrokenRefError('Input in skin node %s not found' % i[1])
            if i[0] == 'JOINT':
                weight_joint_source = i[1][1:]
                offsets[0] = i[2]
            elif i[0] == 'WEIGHT':
                weight_source = i[1][1:]
                offsets[1] = i[2]

        if joint_source is None or weight_source is None:
            raise DaeMalformedError('Not enough inputs for vertex weights in skin')

        return Skin(localscope, bind_shape_mat, joint_source, matrix_source,
                    weight_source, weight_joint_source, vcounts, index, offsets,
                    geometry, controllernode, skinnode)


class BoundSkin(BoundController):
    """A skin bound to a transform matrix and materials mapping."""

    def __init__(self, skin, matrix, materialnodebysymbol):
        self.matrix = matrix
        self.materialnodebysymbol = materialnodebysymbol
        self.skin = skin
        self.id = skin.id
        self.index = skin.index
        self.joint_matrices = skin.joint_matrices
        self.geometry = skin.geometry.bind(numpy.dot(matrix, skin.bind_shape_matrix), materialnodebysymbol)

    def __len__(self):
        return len(self.index)

    def __getitem__(self, i):
        return self.index[i]

    def getJoint(self, i):
        return self.skin.weight_joints[i]

    def getWeight(self, i):
        return self.skin.weights[i]

    def primitives(self):
        for prim in self.geometry.primitives():
            bsp = BoundSkinPrimitive(prim, self)
            yield bsp


class BoundSkinPrimitive(object):
    """A bound skin bound to a primitive."""

    def __init__(self, primitive, boundskin):
        self.primitive = primitive
        self.boundskin = boundskin

    def __len__(self):
        return len(self.primitive)

    def shapes(self):
        for shape in self.primitive.shapes():
            yield shape


class Morph(Controller):
    """Class containing data collada holds in the <morph> tag"""

    def __init__(self, source_geometry, target_list, xmlnode=None):
        """Create a morph instance

        :Parameters:
          source_geometry
            The source geometry (Geometry)
          targets
            A list of tuples where each tuple (g,w) contains
            a Geometry (g) and a float weight value (w)
          xmlnode
            When loaded, the xmlnode it comes from

        """
        self.id = xmlnode.get('id')
        if self.id is None:
            raise DaeMalformedError('Controller node requires an ID')
        self.source_geometry = source_geometry
        """The source geometry (Geometry)"""
        self.target_list = target_list
        """A list of tuples where each tuple (g,w) contains
            a Geometry (g) and a float weight value (w)"""

        self.xmlnode = xmlnode
        # TODO

    def __len__(self):
        return len(self.target_list)

    def __getitem__(self, i):
        return self.target_list[i]

    def bind(self, matrix, materialnodebysymbol):
        """Create a bound morph from this one, transform and material mapping"""
        return BoundMorph(self, matrix, materialnodebysymbol)

    @staticmethod
    def load(collada, localscope, morphnode, controllernode):
        baseid = morphnode.get('source')
        if len(baseid) < 2 or baseid[0] != '#' or \
                not baseid[1:] in collada.geometries:
            raise DaeBrokenRefError('Base source of morph %s not found' % baseid)
        basegeom = collada.geometries[baseid[1:]]

        method = morphnode.get('method')
        if method is None:
            method = 'NORMALIZED'
        if not (method == 'NORMALIZED' or method == 'RELATIVE'):
            raise DaeMalformedError("Morph method must be either NORMALIZED or RELATIVE. Found '%s'" % method)

        inputnodes = morphnode.findall(f"{collada.tag('targets')}/{collada.tag('input')}")
        if inputnodes is None or len(inputnodes) < 2:
            raise DaeIncompleteError("Not enough inputs in a morph")

        try:
            inputs = [(i.get('semantic'), i.get('source')) for i in inputnodes]
        except ValueError:
            raise DaeMalformedError('Corrupted inputs in morph')

        target_source = None
        weight_source = None
        for i in inputs:
            if len(i[1]) < 2 or i[1][0] != '#' or not i[1][1:] in localscope:
                raise DaeBrokenRefError('Input in morph node %s not found' % i[1])
            if i[0] == 'MORPH_TARGET':
                target_source = localscope[i[1][1:]]
            elif i[0] == 'MORPH_WEIGHT':
                weight_source = localscope[i[1][1:]]

        if not isinstance(target_source, source.IDRefSource) or \
                not isinstance(weight_source, source.FloatSource):
            raise DaeIncompleteError("Not enough inputs in targets of morph")

        if len(target_source) != len(weight_source):
            raise DaeMalformedError("Morph inputs must be of same length")

        target_list = []
        for target, weight in zip(target_source, weight_source):
            if len(target) < 1 or not (target in collada.geometries):
                raise DaeBrokenRefError("Targeted geometry %s in morph not found" % target)
            target_list.append((collada.geometries[target], weight[0]))

        return Morph(basegeom, target_list, controllernode)

    def save(self):
        # TODO
        pass


class BoundMorph(BoundController):
    """A morph bound to a transform matrix and materials mapping."""

    def __init__(self, morph, matrix, materialnodebysymbol):
        self.matrix = matrix
        self.materialnodebysymbol = materialnodebysymbol
        self.original = morph

    def __len__(self):
        return len(self.original)

    def __getitem__(self, i):
        return self.original[i]
