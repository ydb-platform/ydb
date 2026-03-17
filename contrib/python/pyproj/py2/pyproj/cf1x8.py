"""
This module contains mappings necessary to convert from
a CRS to a CF-1.8 compliant projection.

http://cfconventions.org/cf-conventions/cf-conventions.html#appendix-grid-mappings


It is not complete and does not support all projections.

CF PARAMS (NOT SURE OF THE MAPPING):
------------------------------------
geoid_name (OGC WKT VERT_DATUM - ex GEOID12B)
geopotential_datum_name (OGC WKT VERT_DATUM - ex NAVD88)

"""

GRID_MAPPING_NAME_MAP = {
    "albers_conical_equal_area": "aea",
    "azimuthal_equidistant": "aeqd",
    "geostationary": "geos",
    "lambert_azimuthal_equal_area": "laea",
    "lambert_conformal_conic": "lcc",
    "lambert_cylindrical_equal_area": "cea",
    "mercator": "merc",
    "oblique_mercator": "omerc",
    "orthographic": "ortho",
    "polar_stereographic": "stere",
    "sinusoidal": "sinu",
    "stereographic": "stere",
    "transverse_mercator": "tmerc",
    "vertical_perspective": "nsper",
    "rotated_latitude_longitude": "ob_tran",
    "latitude_longitude": "latlon",
}


INVERSE_GRID_MAPPING_NAME_MAP = {
    value: key for key, value in GRID_MAPPING_NAME_MAP.items()
}

PROJ_PARAM_MAP = {
    "azimuth_of_central_line": "alpha",
    "earth_radius": "R",
    "fase_easting": "x_0",
    "fase_northing": "y_0",
    "latitude_of_projection_origin": "lat_0",
    "north_pole_grid_longitude": "lon_0",
    "straight_vertical_longitude_from_pole": "lon_0",
    "longitude_of_central_meridian": "lon_0",
    "longitude_of_projection_origin": "lon_0",
    "horizontal_datum_name": "datum",
    "reference_ellipsoid_name": "ellps",
    "towgs84": "towgs84",
    "prime_meridian_name": "pm",
    "scale_factor_at_central_meridian": "k_0",
    "scale_factor_at_projection_origin": "k_0",
    "unit": "units",
    "perspective_point_height": "h",
    "grid_north_pole_longitude": "o_lon_p",
    "grid_north_pole_latitude": "o_lat_p",
    "semi_major_axis": "a",
    "semi_minor_axis": "b",
    "inverse_flattening": "rf",
    "sweep_angle_axis": "sweep",
}


INVERSE_PROJ_PARAM_MAP = {value: key for key, value in PROJ_PARAM_MAP.items()}
INVERSE_PROJ_PARAM_MAP.update(lonc="longitude_of_projection_origin")

LON_0_MAP = {
    "DEFAULT": "longitude_of_projection_origin",
    "rotated_latitude_longitude": "north_pole_grid_longitude",
    "polar_stereographic": "straight_vertical_longitude_from_pole",
    "transverse_mercator": "longitude_of_central_meridian",
    "lambert_cylindrical_equal_area": "longitude_of_central_meridian",
    "lambert_conformal_conic": "longitude_of_central_meridian",
    "albers_conical_equal_area": "longitude_of_central_meridian",
}

K_0_MAP = {
    "DEFAULT": "scale_factor_at_projection_origin",
    "transverse_mercator": "scale_factor_at_central_meridian",
}


METHOD_NAME_TO_CF_MAP = {"Transverse Mercator": "transverse_mercator"}


PARAM_TO_CF_MAP = {
    "Latitude of natural origin": "latitude_of_projection_origin",
    "Longitude of natural origin": "longitude_of_central_meridian",
    "Latitude of false origin": "latitude_of_projection_origin",
    "Longitude of false origin": "longitude_of_central_meridian",
    "Scale factor at natural origin": "scale_factor_at_central_meridian",
    "Easting at projection centre": "false_easting",
    "Northing at projection centre": "false_northing",
    "Easting at false origin": "fase_easting",
    "Northing at false origin": "fase_northing",
    "False easting": "false_easting",
    "False northing": "false_northing",
}
