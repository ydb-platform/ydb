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

"""Module containing classes and functions for the <polylist> primitive."""

import numpy

from collada import primitive
from collada import triangleset
from collada.common import E
from collada.common import DaeIncompleteError, DaeMalformedError
from collada.util import checkSource


class Polygon(object):
    """Single polygon representation. Represents a polygon of N points."""

    def __init__(self, indices, vertices, normal_indices, normals, texcoord_indices, texcoords, material):
        """A Polygon should not be created manually."""

        self.vertices = vertices
        """A (N, 3) float array containing the points in the polygon."""
        self.normals = normals
        """A (N, 3) float array with the normals for points in the polygon. Can be None."""
        self.texcoords = texcoords
        """A tuple where entries are numpy float arrays of size (N, 2) containing
        the texture coordinates for the points in the polygon for each texture
        coordinate set. Can be length 0 if there are no texture coordinates."""
        self.material = material
        """If coming from an unbound :class:`collada.polylist.Polylist`, contains a
        string with the material symbol. If coming from a bound
        :class:`collada.polylist.BoundPolylist`, contains the actual
        :class:`collada.material.Effect` the line is bound to."""
        self.indices = indices
        """A (N,) int array containing the indices for the vertices
           of the N points in the polygon."""
        self.normal_indices = normal_indices
        """A (N,) int array containing the indices for the normals of
           the N points in the polygon"""
        self.texcoord_indices = texcoord_indices
        """A (N,2) int array with texture coordinate indexes for the
           texcoords of the N points in the polygon"""

    def triangles(self):
        """This triangulates the polygon using a simple fanning method.

        :rtype: generator of :class:`collada.polylist.Polygon`
        """

        npts = len(self.vertices)

        for i in range(npts - 2):

            tri_indices = numpy.array([
                self.indices[0], self.indices[i + 1], self.indices[i + 2]
            ], dtype=numpy.float32)

            tri_vertices = numpy.array([
                self.vertices[0], self.vertices[i + 1], self.vertices[i + 2]
            ], dtype=numpy.float32)

            if self.normals is None:
                tri_normals = None
                normal_indices = None
            else:
                tri_normals = numpy.array([
                    self.normals[0], self.normals[i + 1], self.normals[i + 2]
                ], dtype=numpy.float32)
                normal_indices = numpy.array([
                    self.normal_indices[0],
                    self.normal_indices[i + 1],
                    self.normal_indices[i + 2]
                ], dtype=numpy.float32)

            tri_texcoords = []
            tri_texcoord_indices = []
            for texcoord, texcoord_indices in zip(
                    self.texcoords, self.texcoord_indices):
                tri_texcoords.append(numpy.array([
                    texcoord[0],
                    texcoord[i + 1],
                    texcoord[i + 2]
                ], dtype=numpy.float32))
                tri_texcoord_indices.append(numpy.array([
                    texcoord_indices[0],
                    texcoord_indices[i + 1],
                    texcoord_indices[i + 2]
                ], dtype=numpy.float32))

            tri = triangleset.Triangle(
                tri_indices, tri_vertices,
                normal_indices, tri_normals,
                tri_texcoord_indices, tri_texcoords,
                self.material)
            yield tri

    def __repr__(self):
        return '<Polygon vertices=%d>' % len(self.vertices)

    def __str__(self):
        return repr(self)


class Polylist(primitive.Primitive):
    """Class containing the data COLLADA puts in a <polylist> tag, a collection of
    polygons. The Polylist object is read-only. To modify a Polylist, create a new
    instance using :meth:`collada.geometry.Geometry.createPolylist`.

    * If ``P`` is an instance of :class:`collada.polylist.Polylist`, then ``len(P)``
      returns the number of polygons in the set. ``P[i]`` returns the i\\ :sup:`th`
      polygon in the set.
    """

    def __init__(self, sources, material, index, vcounts, xmlnode=None):
        """A Polylist should not be created manually. Instead, call the
        :meth:`collada.geometry.Geometry.createPolylist` method after
        creating a geometry instance.
        """

        if len(sources) == 0:
            raise DaeIncompleteError('A polylist set needs at least one input for vertex positions')
        if 'VERTEX' not in sources:
            raise DaeIncompleteError('Polylist requires vertex input')

        # find max offset - flatten and find max in one pass
        max_offset = max([max([input[0] for input in input_type_array])
                          for input_type_array in sources.values() if len(input_type_array) > 0])

        self.material = material
        self.index = index
        self.indices = self.index
        self.nindices = max_offset + 1
        self.vcounts = vcounts
        self.sources = sources
        self.index.shape = (-1, self.nindices)
        self.npolygons = len(self.vcounts)
        self.nvertices = numpy.sum(self.vcounts) if len(self.index) > 0 else 0
        self.polyends = numpy.cumsum(self.vcounts)
        self.polystarts = self.polyends - self.vcounts
        self.polyindex = numpy.dstack((self.polystarts, self.polyends))[0]

        if len(self.index) > 0:
            self._vertex = sources['VERTEX'][0][4].data
            self._vertex_index = self.index[:, sources['VERTEX'][0][0]]
            self.maxvertexindex = numpy.max(self._vertex_index)
            checkSource(sources['VERTEX'][0][4], ('X', 'Y', 'Z'), self.maxvertexindex)
        else:
            self._vertex = None
            self._vertex_index = None
            self.maxvertexindex = -1

        if 'NORMAL' in sources and len(sources['NORMAL']) > 0 and len(self.index) > 0:
            self._normal = sources['NORMAL'][0][4].data
            self._normal_index = self.index[:, sources['NORMAL'][0][0]]
            self.maxnormalindex = numpy.max(self._normal_index)
            checkSource(sources['NORMAL'][0][4], ('X', 'Y', 'Z'), self.maxnormalindex)
        else:
            self._normal = None
            self._normal_index = None
            self.maxnormalindex = -1

        if 'TEXCOORD' in sources and len(sources['TEXCOORD']) > 0 \
                and len(self.index) > 0:
            self._texcoordset = tuple([texinput[4].data
                                       for texinput in sources['TEXCOORD']])
            self._texcoord_indexset = tuple([self.index[:, sources['TEXCOORD'][i][0]]
                                             for i in range(len(sources['TEXCOORD']))])
            self.maxtexcoordsetindex = [numpy.max(each)
                                        for each in self._texcoord_indexset]
            for i, texinput in enumerate(sources['TEXCOORD']):
                checkSource(texinput[4], ('S', 'T'), self.maxtexcoordsetindex[i])
        else:
            self._texcoordset = tuple()
            self._texcoord_indexset = tuple()
            self.maxtexcoordsetindex = -1

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the line set."""
        else:
            txtindices = ' '.join(map(str, self.indices.flatten().tolist()))

            self.xmlnode = E.polylist(count=str(self.npolygons))
            if self.material is not None:
                self.xmlnode.set('material', self.material)

            all_inputs = []
            for semantic_list in self.sources.values():
                all_inputs.extend(semantic_list)
            for offset, semantic, sourceid, set, src in all_inputs:
                inpnode = E.input(offset=str(offset), semantic=semantic,
                                  source=sourceid)
                if set is not None:
                    inpnode.set('set', str(set))
                self.xmlnode.append(inpnode)

            vcountnode = E.vcount(' '.join(map(str, self.vcounts)))
            self.xmlnode.append(vcountnode)
            self.xmlnode.append(E.p(txtindices))

    def __len__(self):
        return self.npolygons

    def __getitem__(self, i):
        polyrange = self.polyindex[i]
        vertindex = self._vertex_index[polyrange[0]:polyrange[1]]
        v = self._vertex[vertindex]

        normalindex = None
        if self.normal is None:
            n = None
        else:
            normalindex = self._normal_index[polyrange[0]:polyrange[1]]
            n = self._normal[normalindex]

        uvindices = []
        uv = []
        for j, uvindex in enumerate(self._texcoord_indexset):
            uvindices.append(uvindex[polyrange[0]:polyrange[1]])
            uv.append(self._texcoordset[j][uvindex[polyrange[0]:polyrange[1]]])

        return Polygon(vertindex, v, normalindex, n, uvindices, uv, self.material)

    _triangleset = None

    def triangleset(self):
        """This performs a simple triangulation of the polylist using the fanning method.

        :rtype: :class:`collada.triangleset.TriangleSet`
        """

        if self._triangleset is None:
            indexselector = numpy.zeros(self.nvertices) == 0
            indexselector[self.polyindex[:, 1] - 1] = False
            indexselector[self.polyindex[:, 1] - 2] = False
            indexselector = numpy.arange(self.nvertices)[indexselector]

            firstpolyindex = numpy.arange(self.nvertices)
            firstpolyindex = firstpolyindex - numpy.repeat(self.polyends - self.vcounts, self.vcounts)
            firstpolyindex = firstpolyindex[indexselector]

            if len(self.index) > 0:
                triindex = numpy.dstack((self.index[indexselector - firstpolyindex],
                                         self.index[indexselector + 1],
                                         self.index[indexselector + 2]))
                triindex = numpy.swapaxes(triindex, 1, 2).flatten()
            else:
                triindex = numpy.array([], dtype=self.index.dtype)

            triset = triangleset.TriangleSet(self.sources, self.material, triindex, self.xmlnode)

            self._triangleset = triset
        return self._triangleset

    @staticmethod
    def load(collada, localscope, node):
        indexnode = node.find(collada.tag('p'))
        vcountnode = node.find(collada.tag('vcount'))
        input_tag = collada.tag('input')
        if indexnode is None:
            raise DaeIncompleteError('Missing index in polylist')
        if vcountnode is None:
            raise DaeIncompleteError('Missing vcount in polylist')

        try:
            if vcountnode.text is None or vcountnode.text.isspace():
                vcounts = numpy.array([], dtype=numpy.int32)
            else:
                try:
                    vcounts = numpy.fromstring(vcountnode.text, dtype=numpy.int32, sep=' ')
                except ValueError:
                    raise DaeMalformedError("Failed to parse polylist vcount integers")
            vcounts[numpy.isnan(vcounts)] = 0
        except ValueError:
            raise DaeMalformedError('Corrupted vcounts in polylist')

        all_inputs = primitive.Primitive._getInputs(collada, localscope, node.findall(input_tag))

        try:
            if indexnode.text is None or indexnode.text.isspace():
                index = numpy.array([], dtype=numpy.int32)
            else:
                try:
                    index = numpy.fromstring(indexnode.text, dtype=numpy.int32, sep=' ')
                except ValueError:
                    raise DaeMalformedError("Failed to parse poylist index integers")
            index[numpy.isnan(index)] = 0
        except BaseException:
            raise DaeMalformedError('Corrupted index in polylist')

        polylist = Polylist(all_inputs, node.get('material'), index, vcounts, node)
        return polylist

    def bind(self, matrix, materialnodebysymbol):
        """Create a bound polylist from this polylist, transform and material mapping"""
        return BoundPolylist(self, matrix, materialnodebysymbol)

    def __str__(self):
        return '<Polylist length=%d>' % len(self)

    def __repr__(self):
        return str(self)


class BoundPolylist(primitive.BoundPrimitive):
    """A polylist bound to a transform matrix and materials mapping.

    * If ``P`` is an instance of :class:`collada.polylist.BoundPolylist`, then ``len(P)``
      returns the number of polygons in the set. ``P[i]`` returns the i\\ :sup:`th`
      polygon in the set.
    """

    def __init__(self, pl, matrix, materialnodebysymbol):
        """Create a bound polylist from a polylist, transform and material mapping.
        This gets created when a polylist is instantiated in a scene. Do not create this manually."""
        M = numpy.asmatrix(matrix).transpose()
        self._vertex = None if pl._vertex is None else numpy.asarray(pl._vertex * M[:3, :3]) + matrix[:3, 3]
        self._normal = None if pl._normal is None else numpy.asarray(pl._normal * M[:3, :3])
        self._texcoordset = pl._texcoordset
        matnode = materialnodebysymbol.get(pl.material)
        if matnode:
            self.material = matnode.target
            self.inputmap = dict([(sem, (input_sem, set)) for sem, input_sem, set in matnode.inputs])
        else:
            self.inputmap = self.material = None
        self.index = pl.index
        self.nvertices = pl.nvertices
        self._vertex_index = pl._vertex_index
        self._normal_index = pl._normal_index
        self._texcoord_indexset = pl._texcoord_indexset
        self.polyindex = pl.polyindex
        self.npolygons = pl.npolygons
        self.matrix = matrix
        self.materialnodebysymbol = materialnodebysymbol
        self.original = pl

    def __len__(self):
        return self.npolygons

    def __getitem__(self, i):
        polyrange = self.polyindex[i]
        vertindex = self._vertex_index[polyrange[0]:polyrange[1]]
        v = self._vertex[vertindex]

        normalindex = None
        if self.normal is None:
            n = None
        else:
            normalindex = self._normal_index[polyrange[0]:polyrange[1]]
            n = self._normal[normalindex]

        uvindices = []
        uv = []
        for j, uvindex in enumerate(self._texcoord_indexset):
            uvindices.append(uvindex[polyrange[0]:polyrange[1]])
            uv.append(self._texcoordset[j][uvindex[polyrange[0]:polyrange[1]]])

        return Polygon(vertindex, v, normalindex, n, uvindices, uv, self.material)

    _triangleset = None

    def triangleset(self):
        """This performs a simple triangulation of the polylist using the fanning method.

        :rtype: :class:`collada.triangleset.BoundTriangleSet`
        """
        if self._triangleset is None:
            triset = self.original.triangleset()
            boundtriset = triset.bind(self.matrix, self.materialnodebysymbol)
            self._triangleset = boundtriset
        return self._triangleset

    def polygons(self):
        """Iterate through all the polygons contained in the set.

        :rtype: generator of :class:`collada.polylist.Polygon`
        """
        for i in range(self.npolygons):
            yield self[i]

    def shapes(self):
        """Iterate through all the polygons contained in the set.

        :rtype: generator of :class:`collada.polylist.Polygon`
        """
        return self.polygons()

    def __str__(self):
        return '<BoundPolylist length=%d>' % len(self)

    def __repr__(self):
        return str(self)
