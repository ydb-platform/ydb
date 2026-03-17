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

"""Module containing classes and functions for the <triangles> primitive."""

import numpy

from collada import primitive
from collada.common import E
from collada.common import DaeIncompleteError, DaeMalformedError
from collada.util import toUnitVec, checkSource, normalize_v3, dot_v3


class Triangle(object):
    """Single triangle representation."""

    def __init__(self, indices, vertices, normal_indices, normals,
                 texcoord_indices, texcoords, material):
        """A triangle should not be created manually."""

        self.vertices = vertices
        """A (3, 3) float array for points in the triangle"""
        self.normals = normals
        """A (3, 3) float array with the normals for points in the triangle.
        If the triangle didn't have normals, they will be computed."""
        self.texcoords = texcoords
        """A tuple with (3, 2) float arrays with the texture coordinates
          for the points in the triangle"""
        self.material = material
        """If coming from an unbound :class:`collada.triangleset.TriangleSet`, contains a
          string with the material symbol. If coming from a bound
          :class:`collada.triangleset.BoundTriangleSet`, contains the actual
          :class:`collada.material.Effect` the triangle is bound to."""
        self.indices = indices
        """A (3,) int array with vertex indexes of the 3 vertices in
           the vertex array"""
        self.normal_indices = normal_indices
        """A (3,) int array with normal indexes of the 3 vertices in
           the normal array"""
        self.texcoord_indices = texcoord_indices
        """A (3,2) int array with texture coordinate indexes of the 3
           vertices in the texcoord array."""

        if self.normals is None:
            # generate normals
            vec1 = numpy.subtract(vertices[0], vertices[1])
            vec2 = numpy.subtract(vertices[2], vertices[0])
            vec3 = toUnitVec(numpy.cross(toUnitVec(vec2), toUnitVec(vec1)))
            self.normals = numpy.array([vec3, vec3, vec3])

    def __repr__(self):
        return '<Triangle (%s, %s, %s, "%s")>' % (str(self.vertices[0]),
                                                  str(self.vertices[1]), str(self.vertices[2]),
                                                  str(self.material))

    def __str__(self):
        return repr(self)


class TriangleSet(primitive.Primitive):
    """Class containing the data COLLADA puts in a <triangles> tag, a collection of
    triangles.

    * The TriangleSet object is read-only. To modify a TriangleSet, create a new
      instance using :meth:`collada.geometry.Geometry.createTriangleSet`.
    * If ``T`` is an instance of :class:`collada.triangleset.TriangleSet`, then ``len(T)``
      returns the number of triangles in the set. ``T[i]`` returns the i\\ :sup:`th`
      triangle in the set.
    """

    def __init__(self, sources, material, index, xmlnode=None):
        """A TriangleSet should not be created manually. Instead, call the
        :meth:`collada.geometry.Geometry.createTriangleSet` method after
        creating a geometry instance.
        """

        if len(sources) == 0:
            raise DaeIncompleteError('A triangle set needs at least one input for vertex positions')
        if 'VERTEX' not in sources:
            raise DaeIncompleteError('Triangle set requires vertex input')

        max_offset = max([max([input[0] for input in input_type_array])
                          for input_type_array in sources.values()
                          if len(input_type_array) > 0])

        self.material = material
        self.index = index
        self.indices = self.index
        self.nindices = max_offset + 1
        self.index.shape = (-1, 3, self.nindices)
        self.ntriangles = len(self.index)
        self.sources = sources

        if len(self.index) > 0:
            self._vertex = sources['VERTEX'][0][4].data
            self._vertex_index = self.index[:, :, sources['VERTEX'][0][0]]
            self.maxvertexindex = numpy.max(self._vertex_index)
            checkSource(sources['VERTEX'][0][4], ('X', 'Y', 'Z'), self.maxvertexindex)
        else:
            self._vertex = None
            self._vertex_index = None
            self.maxvertexindex = -1

        if 'NORMAL' in sources and len(sources['NORMAL']) > 0 and len(self.index) > 0:
            self._normal = sources['NORMAL'][0][4].data
            self._normal_index = self.index[:, :, sources['NORMAL'][0][0]]
            self.maxnormalindex = numpy.max(self._normal_index)
            checkSource(sources['NORMAL'][0][4], ('X', 'Y', 'Z'), self.maxnormalindex)
        else:
            self._normal = None
            self._normal_index = None
            self.maxnormalindex = -1

        if 'TEXCOORD' in sources and len(sources['TEXCOORD']) > 0 and len(self.index) > 0:
            self._texcoordset = tuple([texinput[4].data for texinput in sources['TEXCOORD']])
            self._texcoord_indexset = tuple([self.index[:, :, sources['TEXCOORD'][i][0]]
                                             for i in range(len(sources['TEXCOORD']))])
            self.maxtexcoordsetindex = [numpy.max(tex_index) for tex_index in self._texcoord_indexset]
            for i, texinput in enumerate(sources['TEXCOORD']):
                checkSource(texinput[4], ('S', 'T'), self.maxtexcoordsetindex[i])
        else:
            self._texcoordset = tuple()
            self._texcoord_indexset = tuple()
            self.maxtexcoordsetindex = -1

        if 'TEXTANGENT' in sources and len(sources['TEXTANGENT']) > 0 and len(self.index) > 0:
            self._textangentset = tuple([texinput[4].data for texinput in sources['TEXTANGENT']])
            self._textangent_indexset = tuple([self.index[:, :, sources['TEXTANGENT'][i][0]]
                                               for i in range(len(sources['TEXTANGENT']))])
            self.maxtextangentsetindex = [numpy.max(tex_index) for tex_index in self._textangent_indexset]
            for i, texinput in enumerate(sources['TEXTANGENT']):
                checkSource(texinput[4], ('X', 'Y', 'Z'), self.maxtextangentsetindex[i])
        else:
            self._textangentset = tuple()
            self._textangent_indexset = tuple()
            self.maxtextangentsetindex = -1

        if 'TEXBINORMAL' in sources and len(sources['TEXBINORMAL']) > 0 and len(self.index) > 0:
            self._texbinormalset = tuple([texinput[4].data for texinput in sources['TEXBINORMAL']])
            self._texbinormal_indexset = tuple([self.index[:, :, sources['TEXBINORMAL'][i][0]]
                                                for i in range(len(sources['TEXBINORMAL']))])
            self.maxtexbinormalsetindex = [numpy.max(tex_index) for tex_index in self._texbinormal_indexset]
            for i, texinput in enumerate(sources['TEXBINORMAL']):
                checkSource(texinput[4], ('X', 'Y', 'Z'), self.maxtexbinormalsetindex[i])
        else:
            self._texbinormalset = tuple()
            self._texbinormal_indexset = tuple()
            self.maxtexbinormalsetindex = -1

        if xmlnode is not None:
            self.xmlnode = xmlnode
        else:
            self._recreateXmlNode()

    def __len__(self):
        return len(self.index)

    def _recreateXmlNode(self):
        self.index.shape = (-1)
        len(self.index)
        txtindices = ' '.join(map(str, self.index.tolist()))
        self.index.shape = (-1, 3, self.nindices)

        self.xmlnode = E.triangles(count=str(self.ntriangles))
        if self.material is not None:
            self.xmlnode.set('material', self.material)

        all_inputs = []
        for semantic_list in self.sources.values():
            all_inputs.extend(semantic_list)
        for offset, semantic, sourceid, set, src in all_inputs:
            inpnode = E.input(offset=str(offset), semantic=semantic, source=sourceid)
            if set is not None:
                inpnode.set('set', str(set))
            self.xmlnode.append(inpnode)

        self.xmlnode.append(E.p(txtindices))

    def __getitem__(self, i):
        v = self._vertex[self._vertex_index[i]]
        n = self._normal[self._normal_index[i]] if self._normal is not None else None
        uvindices = []
        uv = []
        for j, uvindex in enumerate(self._texcoord_indexset):
            uvindices.append(uvindex[i])
            uv.append(self._texcoordset[j][uvindex[i]])
        return Triangle(self._vertex_index[i], v, self._normal_index[i] if self._normal_index is not None else 0, n, uvindices, uv, self.material)

    @staticmethod
    def load(collada, localscope, node):
        indexnodes = node.findall(collada.tag('p'))
        input_tag = collada.tag('input')
        if not indexnodes:
            raise DaeIncompleteError('Missing index in triangle set')

        source_array = primitive.Primitive._getInputs(collada, localscope, node.findall(input_tag))

        def parse_p(indexnode):
            if indexnode.text is None or indexnode.text.isspace():
                index = numpy.array([], dtype=numpy.int32)
            else:
                try:
                    index = numpy.fromstring(indexnode.text, dtype=numpy.int32, sep=' ')
                except ValueError:
                    raise DaeMalformedError("Failed to parse triangleset index ints")
            index[numpy.isnan(index)] = 0
            return index

        indexlist = []
        tag_bare = node.tag.split('}')[-1]

        extendfunc = _indexExtendFunctions[tag_bare]

        max_offset = max(input[0] for input_type_array in source_array.values()
                         for input in input_type_array)

        try:
            for indexnode in indexnodes:
                index = parse_p(indexnode)
                if extendfunc is None:
                    break

                extendfunc(indexlist, index.reshape((-1, max_offset + 1)))
            else:
                index = numpy.concatenate(indexlist)
        except BaseException:
            raise DaeMalformedError('Corrupted index in triangleset')

        triset = TriangleSet(source_array, node.get('material'), index, node)
        triset.xmlnode = node
        return triset

    def bind(self, matrix, materialnodebysymbol):
        """Create a bound triangle set from this triangle set, transform and material mapping"""
        return BoundTriangleSet(self, matrix, materialnodebysymbol)

    def generateNormals(self):
        """If :attr:`normals` is `None` or you wish for normals to be
        recomputed, call this method to recompute them."""
        norms = numpy.zeros(self._vertex.shape, dtype=self._vertex.dtype)
        tris = self._vertex[self._vertex_index]
        n = numpy.cross(tris[::, 1] - tris[::, 0], tris[::, 2] - tris[::, 0])
        normalize_v3(n)
        norms[self._vertex_index[:, 0]] += n
        norms[self._vertex_index[:, 1]] += n
        norms[self._vertex_index[:, 2]] += n
        normalize_v3(norms)

        self._normal = norms
        self._normal_index = self._vertex_index

    def generateTexTangentsAndBinormals(self):
        """If there are no texture tangents, this method will compute them.
        Texture coordinates must exist and it uses the first texture coordinate set."""

        # The following is taken from:
        # http://www.terathon.com/code/tangent.html
        # It's pretty much a direct translation, using numpy arrays

        tris = self._vertex[self._vertex_index]
        uvs = self._texcoordset[0][self._texcoord_indexset[0]]

        x1 = tris[:, 1, 0] - tris[:, 0, 0]
        x2 = tris[:, 2, 0] - tris[:, 1, 0]
        y1 = tris[:, 1, 1] - tris[:, 0, 1]
        y2 = tris[:, 2, 1] - tris[:, 1, 1]
        z1 = tris[:, 1, 2] - tris[:, 0, 2]
        z2 = tris[:, 2, 2] - tris[:, 1, 2]

        s1 = uvs[:, 1, 0] - uvs[:, 0, 0]
        s2 = uvs[:, 2, 0] - uvs[:, 1, 0]
        t1 = uvs[:, 1, 1] - uvs[:, 0, 1]
        t2 = uvs[:, 2, 1] - uvs[:, 1, 1]

        r = 1.0 / (s1 * t2 - s2 * t1)

        sdirx = (t2 * x1 - t1 * x2) * r
        sdiry = (t2 * y1 - t1 * y2) * r
        sdirz = (t2 * z1 - t1 * z2) * r
        sdir = numpy.vstack((sdirx, sdiry, sdirz)).T

        tans1 = numpy.zeros(self._vertex.shape, dtype=self._vertex.dtype)
        tans1[self._vertex_index[:, 0]] += sdir
        tans1[self._vertex_index[:, 1]] += sdir
        tans1[self._vertex_index[:, 2]] += sdir

        norm = self._normal[self._normal_index]
        norm.shape = (-1, 3)
        tan1 = tans1[self._vertex_index]
        tan1.shape = (-1, 3)

        tangent = normalize_v3(tan1 - norm * dot_v3(norm, tan1)[:, numpy.newaxis])

        self._textangentset = (tangent,)
        self._textangent_indexset = (numpy.arange(len(self._vertex_index) * 3, dtype=self._vertex_index.dtype),)
        self._textangent_indexset[0].shape = (len(self._vertex_index), 3)

        tdirx = (s1 * x2 - s2 * x1) * r
        tdiry = (s1 * y2 - s2 * y1) * r
        tdirz = (s1 * z2 - s2 * z1) * r
        tdir = numpy.vstack((tdirx, tdiry, tdirz)).T

        tans2 = numpy.zeros(self._vertex.shape, dtype=self._vertex.dtype)
        tans2[self._vertex_index[:, 0]] += tdir
        tans2[self._vertex_index[:, 1]] += tdir
        tans2[self._vertex_index[:, 2]] += tdir

        tan2 = tans2[self._vertex_index]
        tan2.shape = (-1, 3)

        tanw = dot_v3(numpy.cross(norm, tan1), tan2)
        tanw = numpy.sign(tanw)

        binorm = numpy.cross(norm, tangent).flatten()
        binorm.shape = (-1, 3)
        binorm = binorm * tanw[:, numpy.newaxis]

        self._texbinormalset = (binorm,)
        self._texbinormal_indexset = (numpy.arange(len(self._vertex_index) * 3,
                                                   dtype=self._vertex_index.dtype),)
        self._texbinormal_indexset[0].shape = (len(self._vertex_index), 3)

    def __str__(self):
        return '<TriangleSet length=%d>' % len(self)

    def __repr__(self):
        return str(self)


class BoundTriangleSet(primitive.BoundPrimitive):
    """A triangle set bound to a transform matrix and materials mapping.

    * If ``T`` is an instance of :class:`collada.triangleset.BoundTriangleSet`, then ``len(T)``
      returns the number of triangles in the set. ``T[i]`` returns the i\\ :sup:`th`
      triangle in the set.
    """

    def __init__(self, ts, matrix, materialnodebysymbol):
        """Create a bound triangle set from a triangle set, transform and material mapping.
        This gets created when a triangle set is instantiated in a scene. Do not create this manually."""
        M = numpy.asmatrix(matrix).transpose()
        self._vertex = None if ts.vertex is None else numpy.asarray(ts._vertex * M[:3, :3]) + matrix[:3, 3]
        self._normal = None if ts._normal is None else numpy.asarray(ts._normal * M[:3, :3])
        self._texcoordset = ts._texcoordset
        self._textangentset = ts._textangentset
        self._texbinormalset = ts._texbinormalset
        matnode = materialnodebysymbol.get(ts.material)
        if matnode:
            self.material = matnode.target
            self.inputmap = dict([(sem, (input_sem, set)) for sem, input_sem, set in matnode.inputs])
        else:
            self.inputmap = self.material = None
        self.index = ts.index
        self._vertex_index = ts._vertex_index
        self._normal_index = ts._normal_index
        self._texcoord_indexset = ts._texcoord_indexset
        self._textangent_indexset = ts._textangent_indexset
        self._texbinormal_indexset = ts._texbinormal_indexset
        self.ntriangles = ts.ntriangles
        self.original = ts

    def __len__(self):
        return len(self.index)

    def __getitem__(self, i):
        vindex = self._vertex_index[i]
        v = self._vertex[vindex]

        if self._normal is None:
            n = None
            nindex = None
        else:
            nindex = self._normal_index[i]
            n = self._normal[nindex]

        uvindices = []
        uv = []
        for j, uvindex in enumerate(self._texcoord_indexset):
            uvindices.append(uvindex[i])
            uv.append(self._texcoordset[j][uvindex[i]])

        return Triangle(vindex, v, nindex, n, uvindices, uv, self.material)

    def triangles(self):
        """Iterate through all the triangles contained in the set.

        :rtype: generator of :class:`collada.triangleset.Triangle`
        """
        for i in range(self.ntriangles):
            yield self[i]

    def shapes(self):
        """Iterate through all the triangles contained in the set.

        :rtype: generator of :class:`collada.triangleset.Triangle`
        """
        return self.triangles()

    def generateNormals(self):
        """If :attr:`normals` is `None` or you wish for normals to be
        recomputed, call this method to recompute them."""
        norms = numpy.zeros(self._vertex.shape, dtype=self._vertex.dtype)
        tris = self._vertex[self._vertex_index]
        n = numpy.cross(tris[::, 1] - tris[::, 0], tris[::, 2] - tris[::, 0])
        normalize_v3(n)
        norms[self._vertex_index[:, 0]] += n
        norms[self._vertex_index[:, 1]] += n
        norms[self._vertex_index[:, 2]] += n
        normalize_v3(norms)

        self._normal = norms
        self._normal_index = self._vertex_index

    def __str__(self):
        return '<BoundTriangleSet length=%d>' % len(self)

    def __repr__(self):
        return str(self)


def _extendFromStrip(indexlist, index):
    """Convert triangle strip indices to triangle indices

    :param list indexlist:
      list to append 1-dimensional index arrays to
    :param numpy.ndarray index:
      (#vertices, #inputs) shaped index array
    """
    cw_ = numpy.array([index[0:-2:2], index[1:-1:2], index[2::2]])
    ccw = numpy.array([index[2:-1:2], index[1:-2:2], index[3::2]])
    indexlist.append(cw_.swapaxes(0, 1).ravel())
    indexlist.append(ccw.swapaxes(0, 1).ravel())


def _extendFromFan(indexlist, index):
    """Convert triangle fan indices to triangle indices
    """
    c = numpy.concatenate((
        numpy.repeat(index[:1], len(index) - 2, 0),
        index[1:-1],
        index[2:]), 1)
    indexlist.append(c.reshape(-1))


_indexExtendFunctions = {
    'tristrips': _extendFromStrip,
    'trifans': _extendFromFan,
    'triangles': None,
}
