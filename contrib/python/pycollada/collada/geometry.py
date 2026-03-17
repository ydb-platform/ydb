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

"""Contains objects for representing a geometry."""


from collada import source
from collada import triangleset
from collada import lineset
from collada import polylist
from collada import polygons
from collada import primitive
from collada.common import DaeObject, E, tag
from collada.common import DaeIncompleteError, DaeUnsupportedError


class Geometry(DaeObject):
    """A class containing the data coming from a COLLADA <geometry> tag"""

    def __init__(self, collada, id, name, sourcebyid, primitives=None,
                 xmlnode=None, double_sided=False):
        """Create a geometry instance

          :param collada.Collada collada:
            The collada object this geometry belongs to
          :param str id:
            A unique string identifier for the geometry
          :param str name:
            A text string naming the geometry
          :param sourcebyid:
            A list of :class:`collada.source.Source` objects or
            a dictionary mapping source ids to the actual objects
          :param list primitives:
            List of primitive objects contained within the geometry.
            Do not set this argument manually. Instead, create a
            :class:`collada.geometry.Geometry` first and then append
            to :attr:`primitives` with the `create*` functions.
          :param xmlnode:
            When loaded, the xmlnode it comes from.
          :param bool double_sided:
            Whether or not the geometry should be rendered double sided

        """
        self.collada = collada
        """The :class:`collada.Collada` object this geometry belongs to"""

        self.id = id
        """The unique string identifier for the geometry"""

        self.name = name
        """The text string naming the geometry"""

        self.double_sided = double_sided
        """A boolean indicating whether or not the geometry should be rendered double sided"""

        self.sourceById = sourcebyid
        """A dictionary containing :class:`collada.source.Source` objects indexed by their id."""

        if isinstance(sourcebyid, list):
            self.sourceById = {}
            for src in sourcebyid:
                self.sourceById[src.id] = src

        self.primitives = []
        """List of primitives (base type :class:`collada.primitive.Primitive`) inside this geometry."""
        if primitives is not None:
            self.primitives = primitives

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the geometry."""
        else:
            sourcenodes = []
            verticesnode = None
            for srcid, src in self.sourceById.items():
                sourcenodes.append(src.xmlnode)
                if verticesnode is None:
                    # pick first source to be in the useless <vertices> tag
                    verticesnode = E.vertices(E.input(semantic='POSITION', source="#%s" % srcid),
                                              id=srcid + '-vertices')
            meshnode = E.mesh(*sourcenodes)
            meshnode.append(verticesnode)
            self.xmlnode = E.geometry(meshnode)
            if len(self.id) > 0:
                self.xmlnode.set("id", self.id)
            if len(self.name) > 0:
                self.xmlnode.set("name", self.name)

    def createLineSet(self, indices, inputlist, materialid=None):
        """Create a set of lines for use in this geometry instance.

        :param numpy.array indices:
          unshaped numpy array that contains the indices for
          the inputs referenced in inputlist
        :param collada.source.InputList inputlist:
          The inputs for this primitive
        :param str materialid:
          A string containing a symbol that will get used to bind this lineset
          to a material when instantiating into a scene

        :rtype: :class:`collada.lineset.LineSet`
        """
        inputdict = primitive.Primitive._getInputsFromList(self.collada, self.sourceById, inputlist.getList())
        return lineset.LineSet(inputdict, materialid, indices)

    def createTriangleSet(self, indices, inputlist, materialid=None):
        """Create a set of triangles for use in this geometry instance.

        :param numpy.array indices:
          unshaped numpy array that contains the indices for
          the inputs referenced in inputlist
        :param collada.source.InputList inputlist:
          The inputs for this primitive
        :param str materialid:
          A string containing a symbol that will get used to bind this triangleset
          to a material when instantiating into a scene

        :rtype: :class:`collada.triangleset.TriangleSet`
        """
        inputdict = primitive.Primitive._getInputsFromList(self.collada, self.sourceById, inputlist.getList())
        return triangleset.TriangleSet(inputdict, materialid, indices)

    def createPolylist(self, indices, vcounts, inputlist, materialid=None):
        """Create a polylist for use with this geometry instance.

        :param numpy.array indices:
          unshaped numpy array that contains the indices for
          the inputs referenced in inputlist
        :param numpy.array vcounts:
          unshaped numpy array that contains the vertex count
          for each polygon in this polylist
        :param collada.source.InputList inputlist:
          The inputs for this primitive
        :param str materialid:
          A string containing a symbol that will get used to bind this polylist
          to a material when instantiating into a scene

        :rtype: :class:`collada.polylist.Polylist`
        """
        inputdict = primitive.Primitive._getInputsFromList(self.collada, self.sourceById, inputlist.getList())
        return polylist.Polylist(inputdict, materialid, indices, vcounts)

    def createPolygons(self, indices, inputlist, materialid=None):
        """Create a polygons for use with this geometry instance.

        :param numpy.array indices:
          list of unshaped numpy arrays that each contain the indices for
          a single polygon
        :param collada.source.InputList inputlist:
          The inputs for this primitive
        :param str materialid:
          A string containing a symbol that will get used to bind this polygons
          to a material when instantiating into a scene

        :rtype: :class:`collada.polygons.Polygons`
        """
        inputdict = primitive.Primitive._getInputsFromList(self.collada, self.sourceById, inputlist.getList())
        return polygons.Polygons(inputdict, materialid, indices)

    @staticmethod
    def load(collada, localscope, node):
        id = node.get("id") or ""
        name = node.get("name") or ""
        tag_mesh = collada.tag('mesh')
        tag_source = collada.tag('source')
        tag_vertices = collada.tag('vertices')
        tag_input = collada.tag('input')
        tag_extra = collada.tag('extra')
        tag_double_sided = collada.tag('double_sided')
        tag_polylist = collada.tag('polylist')
        tag_triangles = collada.tag('triangles')
        tag_tristrips = collada.tag('tristrips')
        tag_trifans = collada.tag('trifans')
        tag_lines = collada.tag('lines')
        tag_polygons = collada.tag('polygons')

        meshnode = node.find(tag_mesh)
        if meshnode is None:
            raise DaeUnsupportedError('Unknown geometry node')

        # Single pass over meshnode: collect sources, vertices, and primitive nodes
        sourcebyid = {}
        verticesnode = None
        primitive_nodes = []
        tri_tags = {tag_triangles, tag_tristrips, tag_trifans}

        for subnode in meshnode:
            tag = subnode.tag
            if tag == tag_source:
                ch = source.Source.load(collada, {}, subnode)
                sourcebyid[ch.id] = ch
            elif tag == tag_vertices:
                verticesnode = subnode
            elif tag == tag_polylist:
                primitive_nodes.append(('polylist', subnode))
            elif tag in tri_tags:
                primitive_nodes.append(('triangles', subnode))
            elif tag == tag_lines:
                primitive_nodes.append(('lines', subnode))
            elif tag == tag_polygons:
                primitive_nodes.append(('polygons', subnode))
            elif tag != tag_extra:
                raise DaeUnsupportedError('Unknown geometry tag %s' % tag)

        # Process vertices node (needs sourcebyid populated)
        if verticesnode is not None:
            inputnodes = {}
            for inputnode in verticesnode.iterfind(tag_input):
                semantic = inputnode.get('semantic')
                inputsource = inputnode.get('source')
                if not semantic or not inputsource or not inputsource.startswith('#'):
                    raise DaeIncompleteError('Bad input definition inside vertices')
                inputnodes[semantic] = sourcebyid.get(inputsource[1:])
            vertices_id = verticesnode.get('id')
            if not vertices_id or len(inputnodes) == 0 or 'POSITION' not in inputnodes:
                raise DaeIncompleteError('Bad vertices definition in mesh')
            sourcebyid[vertices_id] = inputnodes

        # Check for double_sided in extra nodes (on parent node, not meshnode)
        double_sided = False
        for extra in node.iterfind(tag_extra):
            double_sided_node = extra.find(f".//{tag_double_sided}")
            if double_sided_node is not None and double_sided_node.text is not None:
                try:
                    if int(double_sided_node.text) == 1:
                        double_sided = True
                        break
                except ValueError:
                    pass

        # Load primitives (needs complete sourcebyid with vertices)
        _primitives = []
        for ptype, pnode in primitive_nodes:
            if ptype == 'polylist':
                _primitives.append(polylist.Polylist.load(collada, sourcebyid, pnode))
            elif ptype == 'triangles':
                _primitives.append(triangleset.TriangleSet.load(collada, sourcebyid, pnode))
            elif ptype == 'lines':
                _primitives.append(lineset.LineSet.load(collada, sourcebyid, pnode))
            else:  # polygons
                _primitives.append(polygons.Polygons.load(collada, sourcebyid, pnode))

        return Geometry(collada, id, name, sourcebyid, _primitives, xmlnode=node, double_sided=double_sided)

    def save(self):
        """Saves the geometry back to :attr:`xmlnode`"""
        meshnode = self.xmlnode.find(tag('mesh'))
        for src in self.sourceById.values():
            if isinstance(src, source.Source):
                src.save()
                if src.xmlnode not in meshnode:
                    meshnode.insert(0, src.xmlnode)

        deletenodes = []
        for oldsrcnode in meshnode.findall(tag('source')):
            if oldsrcnode not in [src.xmlnode
                                  for src in self.sourceById.values()
                                  if isinstance(src, source.Source)]:
                deletenodes.append(oldsrcnode)
        for d in deletenodes:
            meshnode.remove(d)

        # Look through primitives to find a vertex source
        vnode = self.xmlnode.find(tag('mesh')).find(tag('vertices'))

        # delete any inputs in vertices tag that no longer exist and find the vertex input
        delete_inputs = []
        for input_node in vnode.findall(tag('input')):
            if input_node.get('semantic') == 'POSITION':
                input_vnode = input_node
            else:
                srcid = input_node.get('source')[1:]
                if srcid not in self.sourceById:
                    delete_inputs.append(input_node)

        for node in delete_inputs:
            vnode.remove(node)

        vert_sources = []
        for prim in self.primitives:
            for src in prim.sources['VERTEX']:
                vert_sources.append(src[2][1:])

        vert_src = vnode.get('id')
        vert_ref = input_vnode.get('source')[1:]

        if not (vert_src in vert_sources or vert_ref in vert_sources) and len(vert_sources) > 0:
            if vert_ref in self.sourceById and vert_ref in vert_sources:
                new_source = vert_ref
            else:
                new_source = vert_sources[0]
            self.sourceById[new_source + '-vertices'] = self.sourceById[new_source]
            input_vnode.set('source', '#' + new_source)
            vnode.set('id', new_source + '-vertices')

        # any source references in primitives that are pointing to the
        # same source that the vertices tag is pointing to to instead
        # point to the vertices id
        vert_src = vnode.get('id')
        vert_ref = input_vnode.get('source')[1:]
        for prim in self.primitives:
            for node in prim.xmlnode.findall(tag('input')):
                src = node.get('source')[1:]
                if src == vert_ref:
                    node.set('source', '#%s' % vert_src)

        self.xmlnode.set('id', self.id)
        self.xmlnode.set('name', self.name)

        for prim in self.primitives:
            if isinstance(prim, triangleset.TriangleSet) and prim.xmlnode.tag != tag('triangles'):
                prim._recreateXmlNode()
            if prim.xmlnode not in meshnode:
                meshnode.append(prim.xmlnode)

        deletenodes = []
        primnodes = [prim.xmlnode for prim in self.primitives]
        for child in meshnode:
            if child.tag != tag('vertices') and child.tag != tag('source') and child not in primnodes:
                deletenodes.append(child)
        for d in deletenodes:
            meshnode.remove(d)

    def bind(self, matrix, materialnodebysymbol):
        """Binds this geometry to a transform matrix and material mapping.
        The geometry's points get transformed by the given matrix and its
        inputs get mapped to the given materials.

        :param numpy.array matrix:
          A 4x4 numpy float matrix
        :param dict materialnodebysymbol:
          A dictionary with the material symbols inside the primitive
          assigned to :class:`collada.scene.MaterialNode` defined in the
          scene

        :rtype: :class:`collada.geometry.BoundGeometry`

        """
        return BoundGeometry(self, matrix, materialnodebysymbol)

    def __str__(self):
        return '<Geometry id=%s, %d primitives>' % (self.id, len(self.primitives))

    def __repr__(self):
        return str(self)


class BoundGeometry(object):
    """A geometry bound to a transform matrix and material mapping.
        This gets created when a geometry is instantiated in a scene.
        Do not create this manually."""

    def __init__(self, geom, matrix, materialnodebysymbol):
        self.matrix = matrix
        """The matrix bound to"""
        self.materialnodebysymbol = materialnodebysymbol
        """Dictionary with the material symbols inside the primitive
          assigned to :class:`collada.scene.MaterialNode` defined in the
          scene"""
        self._primitives = geom.primitives
        self.original = geom
        """The original :class:`collada.geometry.Geometry` object this
        is bound to"""

    def __len__(self):
        """Returns the number of primitives in the bound geometry"""
        return len(self._primitives)

    def primitives(self):
        """Returns an iterator that iterates through the primitives in
        the bound geometry. Each value returned will be of base type
        :class:`collada.primitive.BoundPrimitive`"""
        for p in self._primitives:
            boundp = p.bind(self.matrix, self.materialnodebysymbol)
            yield boundp

    def __str__(self):
        return '<BoundGeometry id=%s, %d primitives>' % (self.original.id, len(self))

    def __repr__(self):
        return str(self)
