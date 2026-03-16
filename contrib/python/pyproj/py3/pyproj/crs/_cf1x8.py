"""
This module contains mappings necessary to convert from
a CRS to a CF-1.8 compliant projection.

http://cfconventions.org/cf-conventions/cf-conventions.html#appendix-grid-mappings

"""

import warnings

from pyproj._crs import Datum, Ellipsoid, PrimeMeridian
from pyproj.crs.coordinate_operation import (
    AlbersEqualAreaConversion,
    AzimuthalEquidistantConversion,
    GeostationarySatelliteConversion,
    HotineObliqueMercatorBConversion,
    LambertAzimuthalEqualAreaConversion,
    LambertConformalConic1SPConversion,
    LambertConformalConic2SPConversion,
    LambertCylindricalEqualAreaConversion,
    LambertCylindricalEqualAreaScaleConversion,
    MercatorAConversion,
    MercatorBConversion,
    OrthographicConversion,
    PolarStereographicAConversion,
    PolarStereographicBConversion,
    PoleRotationNetCDFCFConversion,
    SinusoidalConversion,
    StereographicConversion,
    TransverseMercatorConversion,
    VerticalPerspectiveConversion,
)
from pyproj.crs.datum import CustomDatum, CustomEllipsoid, CustomPrimeMeridian
from pyproj.exceptions import CRSError


def _horizontal_datum_from_params(cf_params):
    datum_name = cf_params.get("horizontal_datum_name")
    if datum_name and datum_name not in ("undefined", "unknown"):
        try:
            return Datum.from_name(datum_name)
        except CRSError:
            pass
    # step 1: build ellipsoid
    ellipsoid = None
    ellipsoid_name = cf_params.get("reference_ellipsoid_name")
    try:
        ellipsoid = CustomEllipsoid(
            name=ellipsoid_name or "undefined",
            semi_major_axis=cf_params.get("semi_major_axis"),
            semi_minor_axis=cf_params.get("semi_minor_axis"),
            inverse_flattening=cf_params.get("inverse_flattening"),
            radius=cf_params.get("earth_radius"),
        )
    except CRSError:
        if ellipsoid_name and ellipsoid_name not in ("undefined", "unknown"):
            ellipsoid = Ellipsoid.from_name(ellipsoid_name)

    # step 2: build prime meridian
    prime_meridian = None
    prime_meridian_name = cf_params.get("prime_meridian_name")
    try:
        prime_meridian = CustomPrimeMeridian(
            name=prime_meridian_name or "undefined",
            longitude=cf_params["longitude_of_prime_meridian"],
        )
    except KeyError:
        if prime_meridian_name and prime_meridian_name not in ("undefined", "unknown"):
            prime_meridian = PrimeMeridian.from_name(prime_meridian_name)

    # step 3: build datum
    if ellipsoid or prime_meridian:
        return CustomDatum(
            name=datum_name or "undefined",
            ellipsoid=ellipsoid or "WGS 84",
            prime_meridian=prime_meridian or "Greenwich",
        )
    return None


def _try_list_if_string(input_str):
    """
    Attempt to convert string to list if it is a string
    """
    if not isinstance(input_str, str):
        return input_str
    val_split = input_str.split(",")
    if len(val_split) > 1:
        return [float(sval.strip()) for sval in val_split]
    return input_str


def _get_standard_parallels(standard_parallel):
    standard_parallel = _try_list_if_string(standard_parallel)
    try:
        first_parallel = float(standard_parallel)
        second_parallel = None
    except TypeError:
        first_parallel, second_parallel = standard_parallel
    return first_parallel, second_parallel


def _albers_conical_equal_area(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_albers_equal_area
    """
    first_parallel, second_parallel = _get_standard_parallels(
        cf_params["standard_parallel"]
    )
    return AlbersEqualAreaConversion(
        latitude_first_parallel=first_parallel,
        latitude_second_parallel=second_parallel or 0.0,
        latitude_false_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_false_origin=cf_params.get("longitude_of_central_meridian", 0.0),
        easting_false_origin=cf_params.get("false_easting", 0.0),
        northing_false_origin=cf_params.get("false_northing", 0.0),
    )


def _azimuthal_equidistant(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#azimuthal-equidistant
    """
    return AzimuthalEquidistantConversion(
        latitude_natural_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_projection_origin", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _geostationary(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_geostationary_projection
    """
    try:
        sweep_angle_axis = cf_params["sweep_angle_axis"]
    except KeyError:
        sweep_angle_axis = {"x": "y", "y": "x"}[cf_params["fixed_angle_axis"].lower()]
    return GeostationarySatelliteConversion(
        sweep_angle_axis=sweep_angle_axis,
        satellite_height=cf_params["perspective_point_height"],
        latitude_natural_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_projection_origin", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _lambert_azimuthal_equal_area(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#lambert-azimuthal-equal-area
    """
    return LambertAzimuthalEqualAreaConversion(
        latitude_natural_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_projection_origin", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _lambert_conformal_conic(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_lambert_conformal
    """
    first_parallel, second_parallel = _get_standard_parallels(
        cf_params["standard_parallel"]
    )
    if second_parallel is not None:
        return LambertConformalConic2SPConversion(
            latitude_first_parallel=first_parallel,
            latitude_second_parallel=second_parallel,
            latitude_false_origin=cf_params.get("latitude_of_projection_origin", 0.0),
            longitude_false_origin=cf_params.get("longitude_of_central_meridian", 0.0),
            easting_false_origin=cf_params.get("false_easting", 0.0),
            northing_false_origin=cf_params.get("false_northing", 0.0),
        )
    return LambertConformalConic1SPConversion(
        latitude_natural_origin=first_parallel,
        longitude_natural_origin=cf_params.get("longitude_of_central_meridian", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _lambert_cylindrical_equal_area(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_lambert_cylindrical_equal_area
    """
    if "scale_factor_at_projection_origin" in cf_params:
        return LambertCylindricalEqualAreaScaleConversion(
            scale_factor_natural_origin=cf_params["scale_factor_at_projection_origin"],
            longitude_natural_origin=cf_params.get(
                "longitude_of_central_meridian", 0.0
            ),
            false_easting=cf_params.get("false_easting", 0.0),
            false_northing=cf_params.get("false_northing", 0.0),
        )
    return LambertCylindricalEqualAreaConversion(
        latitude_first_parallel=cf_params.get("standard_parallel", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_central_meridian", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _mercator(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_mercator
    """
    if "scale_factor_at_projection_origin" in cf_params:
        return MercatorAConversion(
            latitude_natural_origin=cf_params.get("standard_parallel", 0.0),
            longitude_natural_origin=cf_params.get(
                "longitude_of_projection_origin", 0.0
            ),
            false_easting=cf_params.get("false_easting", 0.0),
            false_northing=cf_params.get("false_northing", 0.0),
            scale_factor_natural_origin=cf_params["scale_factor_at_projection_origin"],
        )
    return MercatorBConversion(
        latitude_first_parallel=cf_params.get("standard_parallel", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_projection_origin", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _oblique_mercator(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_oblique_mercator
    """
    return HotineObliqueMercatorBConversion(
        latitude_projection_centre=cf_params["latitude_of_projection_origin"],
        longitude_projection_centre=cf_params["longitude_of_projection_origin"],
        azimuth_projection_centre=cf_params["azimuth_of_central_line"],
        angle_from_rectified_to_skew_grid=0.0,
        scale_factor_projection_centre=cf_params.get(
            "scale_factor_at_projection_origin", 1.0
        ),
        easting_projection_centre=cf_params.get("false_easting", 0.0),
        northing_projection_centre=cf_params.get("false_northing", 0.0),
    )


def _orthographic(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_orthographic
    """
    return OrthographicConversion(
        latitude_natural_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_projection_origin", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _polar_stereographic(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#polar-stereographic
    """
    if "standard_parallel" in cf_params:
        return PolarStereographicBConversion(
            latitude_standard_parallel=cf_params["standard_parallel"],
            longitude_origin=cf_params["straight_vertical_longitude_from_pole"],
            false_easting=cf_params.get("false_easting", 0.0),
            false_northing=cf_params.get("false_northing", 0.0),
        )
    return PolarStereographicAConversion(
        latitude_natural_origin=cf_params["latitude_of_projection_origin"],
        longitude_natural_origin=cf_params["straight_vertical_longitude_from_pole"],
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
        scale_factor_natural_origin=cf_params.get(
            "scale_factor_at_projection_origin", 1.0
        ),
    )


def _sinusoidal(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_sinusoidal
    """
    return SinusoidalConversion(
        longitude_natural_origin=cf_params.get("longitude_of_projection_origin", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _stereographic(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_stereographic
    """
    return StereographicConversion(
        latitude_natural_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_projection_origin", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
        scale_factor_natural_origin=cf_params.get(
            "scale_factor_at_projection_origin", 1.0
        ),
    )


def _transverse_mercator(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_transverse_mercator
    """
    return TransverseMercatorConversion(
        latitude_natural_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_natural_origin=cf_params.get("longitude_of_central_meridian", 0.0),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
        scale_factor_natural_origin=cf_params.get(
            "scale_factor_at_central_meridian", 1.0
        ),
    )


def _vertical_perspective(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#vertical-perspective
    """
    return VerticalPerspectiveConversion(
        viewpoint_height=cf_params["perspective_point_height"],
        latitude_topocentric_origin=cf_params.get("latitude_of_projection_origin", 0.0),
        longitude_topocentric_origin=cf_params.get(
            "longitude_of_projection_origin", 0.0
        ),
        false_easting=cf_params.get("false_easting", 0.0),
        false_northing=cf_params.get("false_northing", 0.0),
    )


def _rotated_latitude_longitude(cf_params):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_rotated_pole
    """
    return PoleRotationNetCDFCFConversion(
        grid_north_pole_latitude=cf_params["grid_north_pole_latitude"],
        grid_north_pole_longitude=cf_params["grid_north_pole_longitude"],
        north_pole_grid_longitude=cf_params.get("north_pole_grid_longitude", 0.0),
    )


_GRID_MAPPING_NAME_MAP = {
    "albers_conical_equal_area": _albers_conical_equal_area,
    "azimuthal_equidistant": _azimuthal_equidistant,
    "geostationary": _geostationary,
    "lambert_azimuthal_equal_area": _lambert_azimuthal_equal_area,
    "lambert_conformal_conic": _lambert_conformal_conic,
    "lambert_cylindrical_equal_area": _lambert_cylindrical_equal_area,
    "mercator": _mercator,
    "oblique_mercator": _oblique_mercator,
    "orthographic": _orthographic,
    "polar_stereographic": _polar_stereographic,
    "sinusoidal": _sinusoidal,
    "stereographic": _stereographic,
    "transverse_mercator": _transverse_mercator,
    "vertical_perspective": _vertical_perspective,
}

_GEOGRAPHIC_GRID_MAPPING_NAME_MAP = {
    "rotated_latitude_longitude": _rotated_latitude_longitude
}


def _to_dict(operation):
    param_dict = {}
    for param in operation.params:
        param_dict[param.name.lower().replace(" ", "_")] = param.value
    return param_dict


def _albers_conical_equal_area__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_albers_equal_area

    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "albers_conical_equal_area",
        "standard_parallel": (
            params["latitude_of_1st_standard_parallel"],
            params["latitude_of_2nd_standard_parallel"],
        ),
        "latitude_of_projection_origin": params["latitude_of_false_origin"],
        "longitude_of_central_meridian": params["longitude_of_false_origin"],
        "false_easting": params["easting_at_false_origin"],
        "false_northing": params["northing_at_false_origin"],
    }


def _azimuthal_equidistant__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#azimuthal-equidistant
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "azimuthal_equidistant",
        "latitude_of_projection_origin": params["latitude_of_natural_origin"],
        "longitude_of_projection_origin": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _geostationary__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_geostationary_projection
    """
    params = _to_dict(conversion)
    sweep_angle_axis = "y"
    if conversion.method_name.lower().replace(" ", "_").endswith("(sweep_x)"):
        sweep_angle_axis = "x"
    return {
        "grid_mapping_name": "geostationary",
        "sweep_angle_axis": sweep_angle_axis,
        "perspective_point_height": params["satellite_height"],
        # geostationary satellites orbit around equator
        # so latitude_of_natural_origin is often left off and assumed to be 0.0
        "latitude_of_projection_origin": params.get("latitude_of_natural_origin", 0.0),
        "longitude_of_projection_origin": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _lambert_azimuthal_equal_area__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#lambert-azimuthal-equal-area
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "lambert_azimuthal_equal_area",
        "latitude_of_projection_origin": params["latitude_of_natural_origin"],
        "longitude_of_projection_origin": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _lambert_conformal_conic__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_lambert_conformal
    """
    params = _to_dict(conversion)
    if conversion.method_name.lower().endswith("(2sp)"):
        return {
            "grid_mapping_name": "lambert_conformal_conic",
            "standard_parallel": (
                params["latitude_of_1st_standard_parallel"],
                params["latitude_of_2nd_standard_parallel"],
            ),
            "latitude_of_projection_origin": params["latitude_of_false_origin"],
            "longitude_of_central_meridian": params["longitude_of_false_origin"],
            "false_easting": params["easting_at_false_origin"],
            "false_northing": params["northing_at_false_origin"],
        }
    return {
        "grid_mapping_name": "lambert_conformal_conic",
        "standard_parallel": params["latitude_of_natural_origin"],
        "longitude_of_central_meridian": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _lambert_cylindrical_equal_area__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_lambert_cylindrical_equal_area
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "lambert_cylindrical_equal_area",
        "standard_parallel": params["latitude_of_1st_standard_parallel"],
        "longitude_of_central_meridian": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _mercator__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_mercator
    """
    params = _to_dict(conversion)
    if conversion.method_name.lower().replace(" ", "_").endswith("(variant_a)"):
        return {
            "grid_mapping_name": "mercator",
            "standard_parallel": params["latitude_of_natural_origin"],
            "longitude_of_projection_origin": params["longitude_of_natural_origin"],
            "false_easting": params["false_easting"],
            "false_northing": params["false_northing"],
            "scale_factor_at_projection_origin": params[
                "scale_factor_at_natural_origin"
            ],
        }
    return {
        "grid_mapping_name": "mercator",
        "standard_parallel": params["latitude_of_1st_standard_parallel"],
        "longitude_of_projection_origin": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _oblique_mercator__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_oblique_mercator
    """
    params = _to_dict(conversion)
    if params["angle_from_rectified_to_skew_grid"] != 0:
        warnings.warn(
            "angle from rectified to skew grid parameter lost in conversion to CF"
        )
    try:
        azimuth_of_central_line = params["azimuth_of_initial_line"]
    except KeyError:
        azimuth_of_central_line = params["azimuth_at_projection_centre"]
    try:
        scale_factor_at_projection_origin = params["scale_factor_on_initial_line"]
    except KeyError:
        scale_factor_at_projection_origin = params["scale_factor_at_projection_centre"]
    return {
        "grid_mapping_name": "oblique_mercator",
        "latitude_of_projection_origin": params["latitude_of_projection_centre"],
        "longitude_of_projection_origin": params["longitude_of_projection_centre"],
        "azimuth_of_central_line": azimuth_of_central_line,
        "scale_factor_at_projection_origin": scale_factor_at_projection_origin,
        "false_easting": params["easting_at_projection_centre"],
        "false_northing": params["northing_at_projection_centre"],
    }


def _orthographic__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_orthographic
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "orthographic",
        "latitude_of_projection_origin": params["latitude_of_natural_origin"],
        "longitude_of_projection_origin": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _polar_stereographic__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#polar-stereographic
    """
    params = _to_dict(conversion)
    if conversion.method_name.lower().endswith("(variant b)"):
        return {
            "grid_mapping_name": "polar_stereographic",
            "standard_parallel": params["latitude_of_standard_parallel"],
            "straight_vertical_longitude_from_pole": params["longitude_of_origin"],
            "false_easting": params["false_easting"],
            "false_northing": params["false_northing"],
        }
    return {
        "grid_mapping_name": "polar_stereographic",
        "latitude_of_projection_origin": params["latitude_of_natural_origin"],
        "straight_vertical_longitude_from_pole": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
        "scale_factor_at_projection_origin": params["scale_factor_at_natural_origin"],
    }


def _sinusoidal__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_sinusoidal
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "sinusoidal",
        "longitude_of_projection_origin": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _stereographic__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_stereographic
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "stereographic",
        "latitude_of_projection_origin": params["latitude_of_natural_origin"],
        "longitude_of_projection_origin": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
        "scale_factor_at_projection_origin": params["scale_factor_at_natural_origin"],
    }


def _transverse_mercator__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_transverse_mercator
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "transverse_mercator",
        "latitude_of_projection_origin": params["latitude_of_natural_origin"],
        "longitude_of_central_meridian": params["longitude_of_natural_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
        "scale_factor_at_central_meridian": params["scale_factor_at_natural_origin"],
    }


def _vertical_perspective__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#vertical-perspective
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "vertical_perspective",
        "perspective_point_height": params["viewpoint_height"],
        "latitude_of_projection_origin": params["latitude_of_topocentric_origin"],
        "longitude_of_projection_origin": params["longitude_of_topocentric_origin"],
        "false_easting": params["false_easting"],
        "false_northing": params["false_northing"],
    }


def _rotated_latitude_longitude__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_rotated_pole
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "rotated_latitude_longitude",
        "grid_north_pole_latitude": params["o_lat_p"],
        # https://github.com/pyproj4/pyproj/issues/927
        "grid_north_pole_longitude": params["lon_0"] - 180,
        "north_pole_grid_longitude": params["o_lon_p"],
    }


def _pole_rotation_netcdf__to_cf(conversion):
    """
    http://cfconventions.org/cf-conventions/cf-conventions.html#_rotated_pole

    https://github.com/OSGeo/PROJ/pull/2835
    """
    params = _to_dict(conversion)
    return {
        "grid_mapping_name": "rotated_latitude_longitude",
        "grid_north_pole_latitude": params[
            "grid_north_pole_latitude_(netcdf_cf_convention)"
        ],
        "grid_north_pole_longitude": params[
            "grid_north_pole_longitude_(netcdf_cf_convention)"
        ],
        "north_pole_grid_longitude": params[
            "north_pole_grid_longitude_(netcdf_cf_convention)"
        ],
    }


_INVERSE_GRID_MAPPING_NAME_MAP = {
    "albers_equal_area": _albers_conical_equal_area__to_cf,
    "modified_azimuthal_equidistant": _azimuthal_equidistant__to_cf,
    "azimuthal_equidistant": _azimuthal_equidistant__to_cf,
    "geostationary_satellite_(sweep_x)": _geostationary__to_cf,
    "geostationary_satellite_(sweep_y)": _geostationary__to_cf,
    "lambert_azimuthal_equal_area": _lambert_azimuthal_equal_area__to_cf,
    "lambert_conic_conformal_(2sp)": _lambert_conformal_conic__to_cf,
    "lambert_conic_conformal_(1sp)": _lambert_conformal_conic__to_cf,
    "lambert_cylindrical_equal_area": _lambert_cylindrical_equal_area__to_cf,
    "mercator_(variant_a)": _mercator__to_cf,
    "mercator_(variant_b)": _mercator__to_cf,
    "hotine_oblique_mercator_(variant_b)": _oblique_mercator__to_cf,
    "orthographic": _orthographic__to_cf,
    "polar_stereographic_(variant_a)": _polar_stereographic__to_cf,
    "polar_stereographic_(variant_b)": _polar_stereographic__to_cf,
    "sinusoidal": _sinusoidal__to_cf,
    "stereographic": _stereographic__to_cf,
    "transverse_mercator": _transverse_mercator__to_cf,
    "vertical_perspective": _vertical_perspective__to_cf,
}

_INVERSE_GEOGRAPHIC_GRID_MAPPING_NAME_MAP = {
    "proj ob_tran o_proj=longlat": _rotated_latitude_longitude__to_cf,
    "proj ob_tran o_proj=lonlat": _rotated_latitude_longitude__to_cf,
    "proj ob_tran o_proj=latlon": _rotated_latitude_longitude__to_cf,
    "proj ob_tran o_proj=latlong": _rotated_latitude_longitude__to_cf,
    "pole rotation (netcdf cf convention)": _pole_rotation_netcdf__to_cf,
}
