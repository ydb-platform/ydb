# flake8: noqa

from ..api.basic_str import (
    # todo: implement is_valid_index

    compact as
        compact_cells,

    edge_length as
        get_hexagon_edge_length_avg,

    geo_to_h3 as
        point_to_cell,

    get_destination_h3_index_from_unidirectional_edge as
        get_directed_edge_destination,

    get_h3_indexes_from_unidirectional_edge as
        directed_edge_to_cells,

    get_h3_unidirectional_edge as
        cells_to_directed_edge,

    get_h3_unidirectional_edge_boundary as
        directed_edge_to_boundary,

    get_h3_unidirectional_edges_from_hexagon as
        origin_to_directed_edges,

    get_origin_h3_index_from_unidirectional_edge as
        get_directed_edge_origin,

    get_pentagon_indexes as
        get_pentagons,

    get_res0_indexes as
        get_res0_cells,

    h3_distance as
        grid_distance,

    h3_get_base_cell as
        get_base_cell_number,

    h3_get_faces as
        get_icosahedron_faces,

    h3_get_resolution as
        get_resolution,

    h3_indexes_are_neighbors as
        are_neighbor_cells,

    h3_is_pentagon as
        is_pentagon,

    h3_is_res_class_III as
        is_res_class_III,

    h3_is_valid as
        is_valid_cell,

    h3_line as
        grid_path_cells,

    h3_set_to_multi_polygon as
        cells_to_multipolygon,

    h3_to_center_child as
        cell_to_center_child,

    h3_to_children as
        cell_to_children,

    h3_to_geo as
        cell_to_point,

    h3_to_geo_boundary as
        cell_to_boundary,

    h3_to_parent as
        cell_to_parent,

    h3_to_string as
        int_to_str,

    h3_unidirectional_edge_is_valid as
        is_valid_directed_edge,

    hex_area as
        get_hexagon_area_avg,

    # hex_range as _,
    # hex_range_distances as _, # not sure we want this one; easy for user to implement
    # hex_ranges as _,

    hex_ring as
        grid_ring,

    k_ring as
        grid_disk,

    k_ring_distances as
        grid_disk_distances,

    num_hexagons as
        get_num_cells,

    polyfill as
        polygon_to_cells,

    polyfill_geojson as
        geojson_to_cells, # still need to figure out the final polyfill interface

    #polyfill_polygon as _,

    string_to_h3 as
        str_to_int,

    uncompact as
        uncompact_cells,

    versions as
        versions,
)
