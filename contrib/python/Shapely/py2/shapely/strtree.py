from shapely.geos import lgeos
import ctypes

class STRtree:
    """
    STRtree is an R-tree spatial index that is created using the
    Sort-Tile-Recursive algorithm.
    
    STRtree takes a sequence of geometry objects as initialization
    parameter. Once created, it is immutable. The query method can
    be used to make spatial queries over the objects on the index.

    >>> from shapely.geometry import Polygon
    >>> polys = [ Polygon(((0, 0), (1, 0), (1, 1))), Polygon(((0, 1), (0, 0), (1, 0))), Polygon(((100, 100), (101, 100), (101, 101))) ]
    >>> s = STRtree(polys)
    >>> query_geom = Polygon(((-1, -1), (2, 0), (2, 2), (-1, 2)))
    >>> result = s.query(query_geom)
    >>> polys[0] in result
    True
    >>> polys[1] in result
    True
    >>> polys[2] in result
    False
    >>> # Test empty tree
    >>> s = STRtree([])
    >>> s.query(query_geom)
    []
    >>> # Test tree with one object
    >>> s = STRtree([polys[0]])
    >>> result = s.query(query_geom)
    >>> polys[0] in result
    True
    """

    def __init__(self, geoms):
        # filter empty geometries out of the input
        geoms = [geom for geom in geoms if not geom.is_empty]
        self._n_geoms = len(geoms)

        self._init_tree_handle(geoms)

        # Keep references to geoms.
        self._geoms = list(geoms)

    def _init_tree_handle(self, geoms):
        # GEOS STRtree capacity has to be > 1
        self._tree_handle = lgeos.GEOSSTRtree_create(max(2, len(geoms)))
        for geom in geoms:
            lgeos.GEOSSTRtree_insert(self._tree_handle, geom._geom, ctypes.py_object(geom))

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_tree_handle"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init_tree_handle(self._geoms)

    def __del__(self):
        if self._tree_handle is not None:
            try:
                lgeos.GEOSSTRtree_destroy(self._tree_handle)
            except AttributeError:
                pass  # lgeos might be empty on shutdown.

            self._tree_handle = None

    def query(self, geom):
        """Returns a list of objects on the index whose extents
        intersect the given geometry's extents.
        """
        if self._n_geoms == 0:
            return []

        result = []

        def callback(item, userdata):
            geom = ctypes.cast(item, ctypes.py_object).value
            result.append(geom)

        lgeos.GEOSSTRtree_query(self._tree_handle, geom._geom, lgeos.GEOSQueryCallback(callback), None)

        return result

    def nearest(self, geom):
        if self._n_geoms == 0:
            return None
        
        envelope = geom.envelope

        def callback(item1, item2, distance, userdata):
            try:
                geom1 = ctypes.cast(item1, ctypes.py_object).value
                geom2 = ctypes.cast(item2, ctypes.py_object).value
                dist = ctypes.cast(distance, ctypes.POINTER(ctypes.c_double))
                lgeos.GEOSDistance(geom1._geom, geom2._geom, dist)
                return 1
            except:
                return 0 
        
        item = lgeos.GEOSSTRtree_nearest_generic(self._tree_handle, ctypes.py_object(geom), envelope._geom, \
            lgeos.GEOSDistanceCallback(callback), None)
        result = ctypes.cast(item, ctypes.py_object).value
        
        return result

if __name__ == "__main__":
    import doctest
    doctest.testmod()
