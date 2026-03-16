cimport h3lib
from .h3lib cimport bool, H3int

from .util cimport (
    check_cell,
    check_edge,
    check_res,
    create_ptr,
    create_mv,
)

from .util import H3ValueError

cpdef bool are_neighbors(H3int h1, H3int h2):
    check_cell(h1)
    check_cell(h2)

    return h3lib.h3IndexesAreNeighbors(h1, h2) == 1


cpdef H3int edge(H3int origin, H3int destination) except 1:
    check_cell(origin)
    check_cell(destination)

    if h3lib.h3IndexesAreNeighbors(origin, destination) != 1:
        s = 'Cells are not neighbors: {} and {}'
        s = s.format(hex(origin), hex(destination))
        raise H3ValueError(s)

    return h3lib.getH3UnidirectionalEdge(origin, destination)


cpdef bool is_edge(H3int e):
    return h3lib.h3UnidirectionalEdgeIsValid(e) == 1

cpdef H3int edge_origin(H3int e) except 1:
    # without the check, with an invalid input, the function will just return 0
    check_edge(e)

    return h3lib.getOriginH3IndexFromUnidirectionalEdge(e)

cpdef H3int edge_destination(H3int e) except 1:
    check_edge(e)

    return h3lib.getDestinationH3IndexFromUnidirectionalEdge(e)

cpdef (H3int, H3int) edge_cells(H3int e) except *:
    check_edge(e)

    return edge_origin(e), edge_destination(e)

cpdef H3int[:] edges_from_cell(H3int origin):
    """ Returns the 6 (or 5 for pentagons) directed edges
    for the given origin cell
    """
    check_cell(origin)

    ptr = create_ptr(6)
    h3lib.getH3UnidirectionalEdgesFromHexagon(origin, ptr)
    mv = create_mv(ptr, 6)

    return mv


cpdef double mean_edge_length(int resolution, unit='km') except -1:
    check_res(resolution)

    length = h3lib.edgeLengthKm(resolution)

    # todo: multiple units
    convert = {
        'km': 1.0,
        'm': 1000.0
    }

    try:
        length *= convert[unit]
    except:
        raise H3ValueError('Unknown unit: {}'.format(unit))

    return length


cpdef double edge_length(H3int e, unit='km') except -1:
    check_edge(e)

    # todo: maybe kick this logic up to the python level
    # it might be a little cleaner, because we can do the "switch statement"
    # with a dict, but would require exposing more C functions

    if unit == 'rads':
        length = h3lib.exactEdgeLengthRads(e)
    elif unit == 'km':
        length = h3lib.exactEdgeLengthKm(e)
    elif unit == 'm':
        length = h3lib.exactEdgeLengthM(e)
    else:
        raise H3ValueError('Unknown unit: {}'.format(unit))

    return length
