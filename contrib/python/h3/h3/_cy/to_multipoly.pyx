cimport h3lib
from h3lib cimport H3int
from .util cimport check_cell, coord2deg


# todo: it's driving me crazy that these three functions are all essentially the same linked list walker...
# grumble: no way to do iterators in with cdef functions!
cdef walk_polys(const h3lib.LinkedGeoPolygon* L):
    out = []
    while L:
        out += [walk_loops(L.data)]
        L = L.next

    return out


cdef walk_loops(const h3lib.LinkedGeoLoop* L):
    out = []
    while L:
        out += [walk_coords(L.data)]
        L = L.next

    return out


cdef walk_coords(const h3lib.LinkedGeoCoord* L):
    out = []
    while L:
        out += [coord2deg(L.data)]
        L = L.next

    return out

# todo: tuples instead of lists?
def _to_multi_polygon(const H3int[:] hexes):
    cdef:
        h3lib.LinkedGeoPolygon polygon

    for h in hexes:
        check_cell(h)

    h3lib.h3SetToLinkedGeo(&hexes[0], len(hexes), &polygon)

    out = walk_polys(&polygon)

    # we're still responsible for cleaning up the passed in `polygon`,
    # but not a problem here, since it is stack allocated
    h3lib.destroyLinkedPolygon(&polygon)

    return out


def _geojson_loop(loop):
    """ Swap lat/lng order and close loop.
    """
    loop = [e[::-1] for e in loop]
    loop += [loop[0]]

    return loop


def h3_set_to_multi_polygon(const H3int[:] hexes, geo_json=False):
    # todo: gotta be a more elegant way to handle these...
    if len(hexes) == 0:
        return []

    multipoly = _to_multi_polygon(hexes)

    if geo_json:
        multipoly = [
            [_geojson_loop(loop) for loop in poly]
            for poly in multipoly
        ]

    return multipoly
