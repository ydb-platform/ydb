"""
Module to DRY-up code which is repeated across API modules.

Definitions of types
--------------------
H3Index:
    An unsigned 64-bit integer representing a valid H3 cell or
    unidirectional edge.
    Depending on the API, an H3Index may be represented as an
    unsigned integer type, or as a hexadecimal string.

H3 cell:
    A pentagon or hexagon that can be represented by an H3Index.

H3Cell:
    H3Index representation of an H3 cell.

H3Edge:
    H3Index representation of an H3 unidirectional edge.


Definitions of collections
--------------------------
Collection types vary between APIs. We'll use the following terms:

unordered collection:
    Inputs and outputs are interpreted as *unordered* collections.
    Examples: `set`, `numpy.ndarray`.

ordered collection:
    Inputs and outputs are interpreted as *ordered* collections.
    Examples: `list`, `numpy.ndarray`.

Notes
--------------------
todo: how do we lint these functions and docstrings? it seems to currently
be skipped due to it being inside the `_api_functions` function.
"""

from .. import _cy


class _API_FUNCTIONS(object):
    def __init__(
        self,
        _in_scalar,
        _out_scalar,
        _in_collection,
        _out_unordered,
        _out_ordered,
    ):
        self._in_scalar = _in_scalar
        self._out_scalar = _out_scalar
        self._in_collection = _in_collection
        self._out_unordered = _out_unordered
        self._out_ordered = _out_ordered

    @staticmethod
    def versions():
        """
        Version numbers for the Python (wrapper) and C (wrapped) libraries.

        Versions are output as strings of the form ``'X.Y.Z'``.
        C and Python should match on ``X`` (major) and ``Y`` (minor),
        but may differ on ``Z`` (patch).

        Returns
        -------
        dict like ``{'c': 'X.Y.Z', 'python': 'A.B.C'}``
        """
        from .._version import __version__

        v = {
            'c': _cy.c_version(),
            'python': __version__,
        }

        return v

    @staticmethod
    def string_to_h3(h):
        """
        Converts a hexadecimal string to an H3 64-bit integer index.

        Parameters
        ----------
        h : str
            Hexadecimal string like ``'89754e64993ffff'``

        Returns
        -------
        int
            Unsigned 64-bit integer
        """
        return _cy.hex2int(h)

    @staticmethod
    def h3_to_string(x):
        """
        Converts an H3 64-bit integer index to a hexadecimal string.

        Parameters
        ----------
        x : int
            Unsigned 64-bit integer

        Returns
        -------
        str
            Hexadecimal string like ``'89754e64993ffff'``
        """
        return _cy.int2hex(x)

    @staticmethod
    def num_hexagons(resolution):
        """
        Return the total number of *cells* (hexagons and pentagons)
        for the given resolution.

        Returns
        -------
        int
        """
        return _cy.num_hexagons(resolution)

    @staticmethod
    def hex_area(resolution, unit='km^2'):
        """
        Return the average area of an H3 *hexagon*
        for the given resolution.

        This average *excludes* pentagons.

        Returns
        -------
        float
        """
        # todo: `mean_hex_area` in 4.0
        return _cy.mean_hex_area(resolution, unit)

    @staticmethod
    def edge_length(resolution, unit='km'):
        """
        Return the average *hexagon* edge length
        for the given resolution.

        This average *excludes* pentagons.

        Returns
        -------
        float
        """
        # todo: `mean_edge_length` in 4.0
        return _cy.mean_edge_length(resolution, unit)

    def h3_is_valid(self, h):
        """
        Validates an H3 cell (hexagon or pentagon).

        Returns
        -------
        bool
        """
        try:
            h = self._in_scalar(h)
            return _cy.is_cell(h)
        except (ValueError, TypeError):
            return False

    def h3_unidirectional_edge_is_valid(self, edge):
        """
        Validates an H3 unidirectional edge.

        Returns
        -------
        bool
        """
        try:
            e = self._in_scalar(edge)
            return _cy.is_edge(e)
        except (ValueError, TypeError):
            return False

    def geo_to_h3(self, lat, lng, resolution):
        """
        Return the cell containing the (lat, lng) point
        for a given resolution.

        Returns
        -------
        H3Cell

        """
        return self._out_scalar(_cy.geo_to_h3(lat, lng, resolution))

    def h3_to_geo(self, h):
        """
        Return the center point of an H3 cell as a lat/lng pair.

        Parameters
        ----------
        h : H3Cell

        Returns
        -------
        lat : float
            Latitude
        lng : float
            Longitude
        """
        return _cy.h3_to_geo(self._in_scalar(h))

    def h3_get_resolution(self, h):
        """
        Return the resolution of an H3 cell.

        Parameters
        ----------
        h : H3Cell

        Returns
        -------
        int
        """
        # todo: could also work for edges
        return _cy.resolution(self._in_scalar(h))

    def h3_to_parent(self, h, res=None):
        """
        Get the parent of a cell.

        Parameters
        ----------
        h : H3Cell
        res : int or None, optional
            The resolution for the parent
            If ``None``, then ``res = resolution(h) - 1``

        Returns
        -------
        H3Cell
        """
        h = self._in_scalar(h)
        p = _cy.parent(h, res)
        p = self._out_scalar(p)

        return p

    def h3_distance(self, h1, h2):
        """
        Compute the H3 distance between two cells.

        The H3 distance is defined as the length of the shortest
        path between the cells in the graph formed by connecting
        adjacent cells.

        This function will return an H3ValueError if the
        cells are too far apart to compute the distance.

        Parameters
        ----------
        h1 : H3Cell
        h2 : H3Cell

        Returns
        -------
        int
        """
        h1 = self._in_scalar(h1)
        h2 = self._in_scalar(h2)

        d = _cy.distance(h1, h2)

        return d

    def h3_to_geo_boundary(self, h, geo_json=False):
        """
        Return tuple of lat/lng pairs describing the cell boundary.

        Parameters
        ----------
        h : H3Cell
        geo_json : bool, optional
            If ``True``, return output in GeoJson format:
            lng/lat pairs (opposite order), and
            have the last pair be the same as the first.
            If ``False`` (default), return lat/lng pairs, with the last
            pair distinct from the first.

        Returns
        -------
        tuple of (float, float) tuples
        """
        return _cy.cell_boundary(self._in_scalar(h), geo_json)

    def k_ring(self, h, k=1):
        """
        Return unordered set of cells with H3 distance ``<= k`` from ``h``.
        That is, the "filled-in" disk.

        Parameters
        ----------
        h : H3Cell
        k : int
            Size of disk.

        Returns
        -------
        unordered collection of H3Cell
        """
        mv = _cy.disk(self._in_scalar(h), k)

        return self._out_unordered(mv)

    def hex_range(self, h, k=1):
        """
        Alias for `k_ring`.
        "Filled-in" disk.

        Notes
        -----
        This name differs from the C API.
        """
        mv = _cy.disk(self._in_scalar(h), k)

        return self._out_unordered(mv)

    def hex_ring(self, h, k=1):
        """
        Return unordered set of cells with H3 distance ``== k`` from ``h``.
        That is, the "hollow" ring.

        Parameters
        ----------
        h : H3Cell
        k : int
            Size of ring.

        Returns
        -------
        unordered collection of H3Cell
        """
        mv = _cy.ring(self._in_scalar(h), k)

        return self._out_unordered(mv)

    def hex_range_distances(self, h, K):
        """
        Ordered list of the "hollow" rings around ``h``,
        up to and including distance ``K``.

        Parameters
        ----------
        h : H3Cell
        K : int
            Largest distance considered.

        Returns
        -------
        ordered collection of (unordered collection of H3Cell)
        """
        h = self._in_scalar(h)

        out = [
            self._out_unordered(_cy.ring(h, k))
            for k in range(K + 1)
        ]

        return out

    def hex_ranges(self, hexes, K):
        """
        Returns the dictionary ``{h: hex_range_distances(h, K) for h in hexes}``

        Returns
        -------
        Dict[H3Cell, List[ UnorderedCollection[H3Cell] ]]
        """
        # todo: can we drop this function? the user can implement if needed.
        # TODO: should we call `out_scalar` on the dict keys?
        out = {h: self.hex_range_distances(h, K) for h in hexes}

        return out

    def k_ring_distances(self, h, K):
        """Alias for `hex_range_distances`."""
        return self.hex_range_distances(h, K)

    def h3_to_children(self, h, res=None):
        """
        Children of a hexagon.

        Parameters
        ----------
        h : H3Cell
        res : int or None, optional
            The resolution for the children.
            If ``None``, then ``res = resolution(h) + 1``

        Returns
        -------
        unordered collection of H3Cell
        """
        mv = _cy.children(self._in_scalar(h), res)

        return self._out_unordered(mv)

    # todo: nogil for expensive C operation?
    def compact(self, hexes):
        """
        Compact a collection of H3 cells by combining
        smaller cells into larger cells, if all child cells
        are present.

        Parameters
        ----------
        hexes : iterable of H3Cell

        Returns
        -------
        unordered collection of H3Cell
        """
        # todo: does compact work on mixed-resolution collections?
        hu = self._in_collection(hexes)
        hc = _cy.compact(hu)

        return self._out_unordered(hc)

    def uncompact(self, hexes, res):
        """
        Reverse the `compact` operation.

        Return a collection of H3 cells, all of resolution ``res``.

        Parameters
        ----------
        hexes : iterable of H3Cell
        res : int
            Resolution of desired output cells.

        Returns
        -------
        unordered collection of H3Cell

        Raises
        ------
        todo: add test to make sure an error is returned when input
        contains hex smaller than output res.
        https://github.com/uber/h3/blob/master/src/h3lib/lib/h3Index.c#L425
        """
        hc = self._in_collection(hexes)
        hu = _cy.uncompact(hc, res)

        return self._out_unordered(hu)

    def h3_set_to_multi_polygon(self, hexes, geo_json=False):
        """
        Get GeoJSON-like MultiPolygon describing the outline of the area
        covered by a set of H3 cells.

        Parameters
        ----------
        hexes : unordered collection of H3Cell
        geo_json : bool, optional
            If `True`, output geo sequences will be lng/lat pairs, with the
            last the same as the first.
            If `False`, output geo sequences will be lat/lng pairs, with the
            last distinct from the first.
            Defaults to `False`

        Returns
        -------
        list
            List of "polygons".
            Each polygon is a list of "geo sequences" like
            ``[outer, hole1, hole2, ...]``. The holes may not be present.
            Each geo sequence is a list of lat/lng or lng/lat pairs.
        """
        # todo: this function output does not match with `polyfill`.
        # This function returns a list of polygons, while `polyfill` returns
        # a GeoJSON-like dictionary object.
        hexes = self._in_collection(hexes)
        return _cy.h3_set_to_multi_polygon(hexes, geo_json=geo_json)

    def polyfill_polygon(self, outer, res, holes=None, lnglat_order=False):
        mv = _cy.polyfill_polygon(outer, res, holes=holes, lnglat_order=lnglat_order)

        return self._out_unordered(mv)

    def polyfill_geojson(self, geojson, res):
        mv = _cy.polyfill_geojson(geojson, res)

        return self._out_unordered(mv)

    def polyfill(self, geojson, res, geo_json_conformant=False):
        """
        Get set of hexagons whose *centers* are contained within
        a GeoJSON-style polygon.

        Parameters
        ----------
        geojson : dict
            GeoJSON-style input dictionary describing a polygon (optionally
            including holes).

            Dictionary should be formatted like:

            .. code-block:: text

                {
                    'type': 'Polygon',
                    'coordinates': [outer, hole1, hole2, ...],
                }

            `outer`, `hole1`, etc., are lists of geo coordinate tuples.
            The holes are optional.

        res : int
            Desired output resolution for cells.
        geo_json_conformant : bool, optional
            When ``True``, ``outer``, ``hole1``, etc. must be sequences of
            lng/lat pairs, with the last the same as the first.
            When ``False``, they must be sequences of lat/lng pairs,
            with the last not needing to match the first.

        Returns
        -------
        unordered collection of H3Cell
        """
        mv = _cy.polyfill(geojson, res, geo_json_conformant=geo_json_conformant)

        return self._out_unordered(mv)

    def h3_is_pentagon(self, h):
        """
        Identify if an H3 cell is a pentagon.

        Parameters
        ----------
        h : H3Index

        Returns
        -------
        bool
            ``True`` if input is a valid H3 cell which is a pentagon.

        Notes
        -----
        A pentagon should *also* pass ``h3_is_cell()``.
        Will return ``False`` for valid H3Edge.
        """
        return _cy.is_pentagon(self._in_scalar(h))

    def h3_get_base_cell(self, h):
        """
        Return the base cell *number* (``0`` to ``121``) of the given cell.

        The base cell *number* and the H3Index are two different representations
        of the same cell: the parent cell of resolution ``0``.

        The base cell *number* is encoded within the corresponding
        H3Index.

        todo: could work with edges

        Parameters
        ----------
        h : H3Cell

        Returns
        -------
        int
        """
        return _cy.get_base_cell(self._in_scalar(h))

    def h3_indexes_are_neighbors(self, h1, h2):
        """
        Returns ``True`` if ``h1`` and ``h2`` are neighboring cells.

        Parameters
        ----------
        h1 : H3Cell
        h2 : H3Cell

        Returns
        -------
        bool
        """
        h1 = self._in_scalar(h1)
        h2 = self._in_scalar(h2)

        return _cy.are_neighbors(h1, h2)

    def get_h3_unidirectional_edge(self, origin, destination):
        """
        Create an H3 Index denoting a unidirectional edge.

        The edge is constructed from neighboring cells ``origin`` and
        ``destination``.

        Parameters
        ----------
        origin : H3Cell
        destination : H3Cell

        Raises
        ------
        ValueError
            When cells are not adjacent.

        Returns
        -------
        H3Edge
        """
        o = self._in_scalar(origin)
        d = self._in_scalar(destination)
        e = _cy.edge(o, d)
        e = self._out_scalar(e)

        return e

    def get_origin_h3_index_from_unidirectional_edge(self, e):
        """
        Origin cell from an H3 directed edge.

        Parameters
        ----------
        e : H3Edge

        Returns
        -------
        H3Cell
        """
        e = self._in_scalar(e)
        o = _cy.edge_origin(e)
        o = self._out_scalar(o)

        return o

    def get_destination_h3_index_from_unidirectional_edge(self, e):
        """
        Destination cell from an H3 directed edge.

        Parameters
        ----------
        e : H3Edge

        Returns
        -------
        H3Cell
        """
        e = self._in_scalar(e)
        d = _cy.edge_destination(e)
        d = self._out_scalar(d)

        return d

    def get_h3_indexes_from_unidirectional_edge(self, e):
        """
        Return (origin, destination) tuple from H3 directed edge

        Parameters
        ----------
        e : H3Edge

        Returns
        -------
        H3Cell
            Origin cell of edge
        H3Cell
            Destination cell of edge
        """
        e = self._in_scalar(e)
        o, d = _cy.edge_cells(e)
        o, d = self._out_scalar(o), self._out_scalar(d)

        return o, d

    def get_h3_unidirectional_edges_from_hexagon(self, origin):
        """
        Return all directed edges starting from ``origin`` cell.

        Parameters
        ----------
        origin : H3Cell

        Returns
        -------
        unordered collection of H3Edge
        """
        mv = _cy.edges_from_cell(self._in_scalar(origin))

        return self._out_unordered(mv)

    def get_h3_unidirectional_edge_boundary(self, edge, geo_json=False):
        return _cy.edge_boundary(self._in_scalar(edge), geo_json=geo_json)

    def h3_line(self, start, end):
        """
        Returns the ordered collection of cells denoting a
        minimum-length non-unique path between cells.

        Parameters
        ----------
        start : H3Cell
        end : H3Cell

        Returns
        -------
        ordered collection of H3Cell
            Starting with ``start``, and ending with ``end``.
        """
        mv = _cy.line(self._in_scalar(start), self._in_scalar(end))

        return self._out_ordered(mv)

    def h3_is_res_class_III(self, h):
        """
        Determine if cell has orientation "Class II" or "Class III".

        The orientation of pentagons/hexagons on the icosahedron can be one
        of two types: "Class II" or "Class III".

        All cells within a resolution have the same type, and the type
        alternates between resolutions.

        "Class II" cells have resolutions:  0,2,4,6,8,10,12,14
        "Class III" cells have resolutions: 1,3,5,7,9,11,13,15

        Parameters
        ----------
        h : H3Cell

        Returns
        -------
        bool
            ``True`` if ``h`` is "Class III".
            ``False`` if ``h`` is "Class II".

        References
        ----------
        1. https://uber.github.io/h3/#/documentation/core-library/coordinate-systems
        """
        return _cy.is_res_class_iii(self._in_scalar(h))

    def h3_is_res_class_iii(self, h):
        """Alias for `h3_is_res_class_III`."""
        return self.h3_is_res_class_III(h)

    def get_pentagon_indexes(self, resolution):
        """
        Return all pentagons at a given resolution.

        Parameters
        ----------
        resolution : int

        Returns
        -------
        unordered collection of H3Cell
        """
        mv = _cy.get_pentagon_indexes(resolution)

        return self._out_unordered(mv)

    def get_res0_indexes(self):
        """
        Return all cells at resolution 0.

        Parameters
        ----------
        None

        Returns
        -------
        unordered collection of H3Cell
        """
        mv = _cy.get_res0_indexes()

        return self._out_unordered(mv)

    def h3_to_center_child(self, h, res=None):
        """
        Get the center child of a cell at some finer resolution.

        Parameters
        ----------
        h : H3Cell
        res : int or None, optional
            The resolution for the child cell
            If ``None``, then ``res = resolution(h) + 1``

        Returns
        -------
        H3Cell
        """
        h = self._in_scalar(h)
        p = _cy.center_child(h, res)
        p = self._out_scalar(p)

        return p

    def h3_get_faces(self, h):
        """
        Return icosahedron faces intersecting a given H3 cell.

        There are twenty possible faces, ranging from 0--19.

        Note: Every interface returns a Python ``set`` of ``int``.

        Parameters
        ----------
        h : H3Cell

        Returns
        -------
        Python ``set`` of ``int``
        """
        h = self._in_scalar(h)
        faces = _cy.get_faces(h)

        return faces

    def experimental_h3_to_local_ij(self, origin, h):
        """
        Return local (i,j) coordinates of cell ``h`` in relation to ``origin`` cell


        Parameters
        ----------
        origin : H3Cell
            Origin/central cell for defining i,j coordinates.
        h: H3Cell
            Destination cell whose i,j coordinates we'd like, based off
            of the origin cell.


        Returns
        -------
        Tuple (i, j) of integer local coordinates of cell ``h``


        Notes
        -----

        The ``origin`` cell does not define (0, 0) for the IJ coordinate space.
        (0, 0) refers to the center of the base cell containing origin at the
        resolution of `origin`.
        Subtracting the IJ coordinates of ``origin`` from every cell would get
        you the property of (0, 0) being the ``origin``.

        This is done so we don't need to keep recomputing the coordinates of
        ``origin`` if not needed.
        """
        origin = self._in_scalar(origin)
        h = self._in_scalar(h)

        i, j = _cy.experimental_h3_to_local_ij(origin, h)

        return i, j

    def experimental_local_ij_to_h3(self, origin, i, j):
        """
        Return cell at local (i,j) position relative to the ``origin`` cell.

        Parameters
        ----------
        origin : H3Cell
            Origin/central cell for defining i,j coordinates.
        i, j: int
            Integer coordinates with respect to ``origin`` cell.


        Returns
        -------
        H3Cell at local (i,j) position relative to the ``origin`` cell


        Notes
        -----

        The ``origin`` cell does not define (0, 0) for the IJ coordinate space.
        (0, 0) refers to the center of the base cell containing origin at the
        resolution of ``origin``.
        Subtracting the IJ coordinates of ``origin`` from every cell would get
        you the property of (0, 0) being the ``origin``.

        This is done so we don't need to keep recomputing the coordinates of
        ``origin`` if not needed.
        """
        origin = self._in_scalar(origin)

        h = _cy.experimental_local_ij_to_h3(origin, i, j)
        h = self._out_scalar(h)

        return h

    def cell_area(self, h, unit='km^2'):
        """
        Compute the spherical surface area of a specific H3 cell.

        Parameters
        ----------
        h : H3Cell
        unit: str
            Unit for area result (``'km^2'``, 'm^2', or 'rads^2')


        Returns
        -------
        The area of the H3 cell in the given units


        Notes
        -----
        This function breaks the cell into spherical triangles, and computes
        their spherical area.
        The function uses the spherical distance calculation given by
        `point_dist`.
        """
        h = self._in_scalar(h)

        return _cy.cell_area(h, unit=unit)

    def exact_edge_length(self, e, unit='km'):
        """
        Compute the spherical length of a specific H3 edge.

        Parameters
        ----------
        h : H3Cell
        unit: str
            Unit for length result ('km', 'm', or 'rads')


        Returns
        -------
        The length of the edge in the given units


        Notes
        -----
        This function uses the spherical distance calculation given by
        `point_dist`.
        """
        e = self._in_scalar(e)

        return _cy.edge_length(e, unit=unit)

    @staticmethod
    def point_dist(point1, point2, unit='km'):
        """
        Compute the spherical distance between two (lat, lng) points.

        todo: do we handle lat/lng points consistently in the api? what
        about (lat1, lng1, lat2, lng2) as the input? How will this work
        for vectorized versions?

        Parameters
        ----------
        point1 : tuple
            (lat, lng) tuple in degrees
        point2 : tuple
            (lat, lng) tuple in degrees
        unit: str
            Unit for distance result ('km', 'm', or 'rads')


        Returns
        -------
        Spherical (or "haversine") distance between the points
        """
        lat1, lng1 = point1
        lat2, lng2 = point2

        return _cy.point_dist(
            lat1, lng1,
            lat2, lng2,
            unit=unit
        )
