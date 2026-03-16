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

"""Module containing classes and functions for the <lines> primitive."""

import numpy

from collada import primitive
from collada.util import checkSource
from collada.common import E
from collada.common import DaeIncompleteError, DaeMalformedError


class Line(object):
    """Single line representation. Represents the line between two points
    ``(x0,y0,z0)`` and ``(x1,y1,z1)``. A Line is read-only."""

    def __init__(self, indices, vertices, normals, texcoords, material):
        """A Line should not be created manually."""

        self.vertices = vertices
        """A (2, 3) numpy float array containing the endpoints of the line"""
        self.normals = normals
        """A (2, 3) numpy float array with the normals for the endpoints of the line. Can be None."""
        self.texcoords = texcoords
        """A tuple where entries are numpy float arrays of size (2, 2) containing
        the texture coordinates for the endpoints of the line for each texture
        coordinate set. Can be length 0 if there are no texture coordinates."""
        self.material = material
        """If coming from an unbound :class:`collada.lineset.LineSet`, contains a
        string with the material symbol. If coming from a bound
        :class:`collada.lineset.BoundLineSet`, contains the actual
        :class:`collada.material.Effect` the line is bound to."""
        self.indices = indices

        # Note: we can't generate normals for lines if there are none

    def __repr__(self):
        return '<Line (%s, %s, "%s")>' % (str(self.vertices[0]), str(self.vertices[1]), str(self.material))

    def __str__(self):
        return repr(self)


class LineSet(primitive.Primitive):
    """Class containing the data COLLADA puts in a <lines> tag, a collection of
    lines. The LineSet object is read-only. To modify a LineSet, create a new
    instance using :meth:`collada.geometry.Geometry.createLineSet`.

    * If ``L`` is an instance of :class:`collada.lineset.LineSet`, then ``len(L)``
      returns the number of lines in the set. ``L[i]`` returns the i\\ :sup:`th`
      line in the set."""

    def __init__(self, sources, material, index, xmlnode=None):
        """A LineSet should not be created manually. Instead, call the
        :meth:`collada.geometry.Geometry.createLineSet` method after
        creating a geometry instance.
        """

        if len(sources) == 0:
            raise DaeIncompleteError('A line set needs at least one input for vertex positions')
        if not sources.get('VERTEX'):
            raise DaeIncompleteError('Line set requires vertex input')

        # find max offset
        max_offset = max([max([input[0] for input in input_type_array])
                          for input_type_array in sources.values() if len(input_type_array) > 0])

        self.sources = sources
        self.material = material
        self.index = index
        self.indices = self.index
        self.nindices = max_offset + 1
        self.index.shape = (-1, 2, self.nindices)
        self.nlines = len(self.index)

        if len(self.index) > 0:
            self._vertex = sources['VERTEX'][0][4].data
            self._vertex_index = self.index[:, :, sources['VERTEX'][0][0]]
            self.maxvertexindex = numpy.max(self._vertex_index)
            checkSource(sources['VERTEX'][0][4], ('X', 'Y', 'Z'),
                        self.maxvertexindex)
        else:
            self._vertex = None
            self._vertex_index = None
            self.maxvertexindex = -1

        if 'NORMAL' in sources and len(sources['NORMAL']) > 0 \
                and len(self.index) > 0:
            self._normal = sources['NORMAL'][0][4].data
            self._normal_index = self.index[:, :, sources['NORMAL'][0][0]]
            self.maxnormalindex = numpy.max(self._normal_index)
            checkSource(sources['NORMAL'][0][4], ('X', 'Y', 'Z'),
                        self.maxnormalindex)
        else:
            self._normal = None
            self._normal_index = None
            self.maxnormalindex = -1

        if 'TEXCOORD' in sources and len(sources['TEXCOORD']) > 0 \
                and len(self.index) > 0:
            self._texcoordset = tuple([texinput[4].data
                                       for texinput in sources['TEXCOORD']])
            self._texcoord_indexset = tuple([self.index[:, :, sources['TEXCOORD'][i][0]]
                                             for i in range(len(sources['TEXCOORD']))])
            self.maxtexcoordsetindex = [numpy.max(tex_index)
                                        for tex_index in self._texcoord_indexset]
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
            self.index.shape = (-1)
            txtindices = ' '.join(map(str, self.index.tolist()))
            self.index.shape = (-1, 2, self.nindices)

            self.xmlnode = E.lines(count=str(self.nlines))
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

            self.xmlnode.append(E.p(txtindices))

    def __len__(self):
        """The number of lines in this line set."""
        return len(self.index)

    def __getitem__(self, i):
        v = self._vertex[self._vertex_index[i]]
        if self._normal is None:
            n = None
        else:
            n = self._normal[self._normal_index[i]]
        uv = []
        for j, uvindex in enumerate(self._texcoord_indexset):
            uv.append(self._texcoordset[j][uvindex[i]])
        return Line(self._vertex_index[i], v, n, uv, self.material)

    @staticmethod
    def load(collada, localscope, node):
        indexnode = node.find(collada.tag('p'))
        input_tag = collada.tag('input')
        if indexnode is None:
            raise DaeIncompleteError('Missing index in line set')

        source_array = primitive.Primitive._getInputs(collada, localscope, node.findall(input_tag))

        try:
            if indexnode.text is None or indexnode.text.isspace():
                index = numpy.array([], dtype=numpy.int32)
            else:
                try:
                    index = numpy.fromstring(indexnode.text, dtype=numpy.int32, sep=' ')
                except ValueError:
                    raise DaeMalformedError("Failed to parse lineset index integers")
            index[numpy.isnan(index)] = 0
        except BaseException:
            raise DaeMalformedError('Corrupted index in line set')

        lineset = LineSet(source_array, node.get('material'), index, node)
        lineset.xmlnode = node
        return lineset

    def bind(self, matrix, materialnodebysymbol):
        """Create a bound line set from this line set, transform and material mapping"""
        return BoundLineSet(self, matrix, materialnodebysymbol)

    def __str__(self):
        return '<LineSet length=%d>' % len(self)

    def __repr__(self):
        return str(self)


class BoundLineSet(primitive.BoundPrimitive):
    """A line set bound to a transform matrix and materials mapping.

    * If ``bs`` is an instance of :class:`collada.lineset.BoundLineSet`, ``len(bs)``
      returns the number of lines in the set and ``bs[i]`` returns the i\\ :superscript:`th`
      line in the set.

    """

    def __init__(self, ls, matrix, materialnodebysymbol):
        """Create a bound line set from a line set, transform and material mapping. This gets created when a
        line set is instantiated in a scene. Do not create this manually."""
        M = numpy.asmatrix(matrix).transpose()
        self._vertex = None
        if ls._vertex is not None:
            self._vertex = numpy.asarray(ls._vertex * M[:3, :3]) + matrix[:3, 3]
        self._normal = None
        if ls._normal is not None:
            self._normal = numpy.asarray(ls._normal * M[:3, :3])
        self._texcoordset = ls._texcoordset
        matnode = materialnodebysymbol.get(ls.material)
        if matnode:
            self.material = matnode.target
            self.inputmap = dict([(sem, (input_sem, set))
                                  for sem, input_sem, set in matnode.inputs])
        else:
            self.inputmap = self.material = None
        self.index = ls.index
        self._vertex_index = ls._vertex_index
        self._normal_index = ls._normal_index
        self._texcoord_indexset = ls._texcoord_indexset
        self.nlines = ls.nlines
        self.original = ls

    def __len__(self):
        return len(self.index)

    def __getitem__(self, i):
        v = self._vertex[self._vertex_index[i]]
        if self._normal is None:
            n = None
        else:
            n = self._normal[self._normal_index[i]]
        uv = []
        for j, uvindex in enumerate(self._texcoord_indexset):
            uv.append(self._texcoordset[j][uvindex[i]])
        return Line(self._vertex_index[i], v, n, uv, self.material)

    def lines(self):
        """Iterate through all the lines contained in the set.

        :rtype: generator of :class:`collada.lineset.Line`
        """
        for i in range(self.nlines):
            yield self[i]

    def shapes(self):
        """Iterate through all the lines contained in the set.

        :rtype: generator of :class:`collada.lineset.Line`
        """
        return self.lines()

    def __str__(self):
        return '<BoundLineSet length=%d>' % len(self)

    def __repr__(self):
        return str(self)
