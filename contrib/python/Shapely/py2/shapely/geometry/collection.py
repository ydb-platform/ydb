"""Multi-part collections of geometries
"""

from ctypes import c_void_p

from shapely.geos import lgeos
from shapely.geometry.base import BaseGeometry
from shapely.geometry.base import BaseMultipartGeometry
from shapely.geometry.base import HeterogeneousGeometrySequence
from shapely.geometry.base import geos_geom_from_py


class GeometryCollection(BaseMultipartGeometry):

    """A heterogenous collection of geometries

    Attributes
    ----------
    geoms : sequence
        A sequence of Shapely geometry instances
    """

    def __init__(self, geoms=None):
        """
        Parameters
        ----------
        geoms : list
            A list of shapely geometry instances, which may be heterogenous.
        
        Example
        -------
        Create a GeometryCollection with a Point and a LineString
        
          >>> p = Point(51, -1)
          >>> l = LineString([(52, -1), (49, 2)])
          >>> gc = GeometryCollection([p, l])
        """
        BaseMultipartGeometry.__init__(self)
        if not geoms:
            pass
        else:
            self._geom, self._ndim = geos_geometrycollection_from_py(geoms)

    @property
    def __geo_interface__(self):
        geometries = []
        for geom in self.geoms:
            geometries.append(geom.__geo_interface__)
        return dict(type='GeometryCollection', geometries=geometries)

    @property
    def geoms(self):
        if self.is_empty:
            return []
        return HeterogeneousGeometrySequence(self)

def geos_geometrycollection_from_py(ob):
    """Creates a GEOS GeometryCollection from a list of geometries"""
    L = len(ob)
    N = 2
    subs = (c_void_p * L)()
    for l in range(L):
        assert(isinstance(ob[l], BaseGeometry))
        if ob[l].has_z:
            N = 3
        geom, n = geos_geom_from_py(ob[l])
        subs[l] = geom
    
    return (lgeos.GEOSGeom_createCollection(7, subs, L), N)

# Test runner
def _test():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    _test()

