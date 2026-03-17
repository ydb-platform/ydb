# flake8: noqa

"""
This module should serve as the interface between the C/Cython code and
the Python code. That is, it is an internal API.
This module should import all the Cython functions we
intend to expose to be used in pure Python code, and each of the H3-py
APIs should *only* reference functions and symbols listed here.

These functions should handle input validation, guard against the
possibility of segfaults, raise appropriate errors, and handle memory
management. The API wrapping code around this should focus on the cosmetic
function interface and input conversion (string to int, for instance).
"""

from .cells import (
    is_cell,
    is_pentagon,
    get_base_cell,
    resolution,
    parent,
    distance,
    disk,
    ring,
    children,
    compact,
    uncompact,
    num_hexagons,
    mean_hex_area,
    cell_area,
    line,
    is_res_class_iii,
    get_pentagon_indexes,
    get_res0_indexes,
    center_child,
    get_faces,
    experimental_h3_to_local_ij,
    experimental_local_ij_to_h3,
)

from .edges import (
    are_neighbors,
    edge,
    is_edge,
    edge_origin,
    edge_destination,
    edge_cells,
    edges_from_cell,
    mean_edge_length,
    edge_length,
)

from .geo import (
    geo_to_h3,
    h3_to_geo,
    polyfill_polygon,
    polyfill_geojson,
    polyfill,
    cell_boundary,
    edge_boundary,
    point_dist,
)

from .to_multipoly import (
    h3_set_to_multi_polygon
)

from .util import (
    c_version,
    hex2int,
    int2hex,
    from_iter,
    H3ValueError,
    H3CellError,
    H3ResolutionError,
    H3EdgeError,
    H3DistanceError,
)
