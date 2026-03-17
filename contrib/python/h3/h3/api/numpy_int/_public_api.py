"""Binding of public API names to module scope

Prior to binding the API to module scope explicitly, we dynamically modified the
`globals` object when `h3` was imported, which caused problems with static tooling not
being able to understand the H3 API.

This file exists to avoid dynamically modifying `globals` and support static tooling.
"""
from ._binding import _binding

cell_area = _binding.cell_area
compact = _binding.compact
edge_length = _binding.edge_length
exact_edge_length = _binding.exact_edge_length
experimental_h3_to_local_ij = _binding.experimental_h3_to_local_ij
experimental_local_ij_to_h3 = _binding.experimental_local_ij_to_h3
geo_to_h3 = _binding.geo_to_h3
get_destination_h3_index_from_unidirectional_edge = (
    _binding.get_destination_h3_index_from_unidirectional_edge
)
get_h3_indexes_from_unidirectional_edge = (
    _binding.get_h3_indexes_from_unidirectional_edge
)
get_h3_unidirectional_edge = _binding.get_h3_unidirectional_edge
get_h3_unidirectional_edge_boundary = _binding.get_h3_unidirectional_edge_boundary
get_h3_unidirectional_edges_from_hexagon = (
    _binding.get_h3_unidirectional_edges_from_hexagon
)
get_origin_h3_index_from_unidirectional_edge = (
    _binding.get_origin_h3_index_from_unidirectional_edge
)
get_pentagon_indexes = _binding.get_pentagon_indexes
get_res0_indexes = _binding.get_res0_indexes
h3_distance = _binding.h3_distance
h3_get_base_cell = _binding.h3_get_base_cell
h3_get_faces = _binding.h3_get_faces
h3_get_resolution = _binding.h3_get_resolution
h3_indexes_are_neighbors = _binding.h3_indexes_are_neighbors
h3_is_pentagon = _binding.h3_is_pentagon
h3_is_res_class_III = _binding.h3_is_res_class_III
h3_is_res_class_iii = _binding.h3_is_res_class_iii
h3_is_valid = _binding.h3_is_valid
h3_line = _binding.h3_line
h3_set_to_multi_polygon = _binding.h3_set_to_multi_polygon
h3_to_center_child = _binding.h3_to_center_child
h3_to_children = _binding.h3_to_children
h3_to_geo = _binding.h3_to_geo
h3_to_geo_boundary = _binding.h3_to_geo_boundary
h3_to_parent = _binding.h3_to_parent
h3_to_string = _binding.h3_to_string
h3_unidirectional_edge_is_valid = _binding.h3_unidirectional_edge_is_valid
hex_area = _binding.hex_area
hex_range = _binding.hex_range
hex_range_distances = _binding.hex_range_distances
hex_ranges = _binding.hex_ranges
hex_ring = _binding.hex_ring
k_ring = _binding.k_ring
k_ring_distances = _binding.k_ring_distances
num_hexagons = _binding.num_hexagons
point_dist = _binding.point_dist
polyfill = _binding.polyfill
polyfill_geojson = _binding.polyfill_geojson
polyfill_polygon = _binding.polyfill_polygon
string_to_h3 = _binding.string_to_h3
uncompact = _binding.uncompact
versions = _binding.versions
