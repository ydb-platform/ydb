"""Tests of fiona.crs."""

import pytest

from .conftest import requires_gdal33

from fiona import crs
from fiona.env import Env
from fiona.errors import CRSError, FionaDeprecationWarning


def test_proj_keys():
    assert len(crs.all_proj_keys) == 87
    assert 'init' in crs.all_proj_keys
    assert 'proj' in crs.all_proj_keys
    assert 'no_mayo' in crs.all_proj_keys


def test_to_string():
    # Make a string from a mapping with a few bogus items
    val = {
        'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84',
        'no_defs': True, 'foo': True, 'axis': False, 'belgium': [1, 2]}
    assert crs.CRS.from_user_input(val).to_string() == "EPSG:4326"


def test_to_string_utm():
    # Make a string from a mapping with a few bogus items
    val = {
        'proj': 'utm', 'ellps': 'WGS84', 'zone': 13,
        'no_defs': True, 'foo': True, 'axis': False, 'belgium': [1, 2]}
    assert crs.CRS.from_user_input(val).to_string() == "EPSG:32613"


def test_to_string_epsg():
    val = {'init': 'epsg:4326', 'no_defs': True}
    assert crs.CRS.from_user_input(val).to_string() == "EPSG:4326"


def test_from_epsg():
    val = crs.CRS.from_epsg(4326)
    assert val['init'] == "epsg:4326"


def test_from_epsg_neg():
    with pytest.raises(CRSError):
        crs.CRS.from_epsg(-1)


@pytest.mark.parametrize("invalid_input", [
    "a random string that is invalid",
    ("a", "tuple"),
    "-48567=409 =2095"
])
def test_invalid_crs(invalid_input):
    with pytest.raises(CRSError):
        crs.CRS.from_user_input(invalid_input)


def test_custom_crs():
    class CustomCRS:
        def to_wkt(self):
            return (
                'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",'
                '6378137,298.257223563,AUTHORITY["EPSG","7030"]],'
                'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,'
                'AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,'
                'AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
            )

    assert crs.CRS.from_user_input(CustomCRS()).to_wkt().startswith('GEOGCS["WGS 84"')


def test_crs__version():
    target_crs = (
        'PROJCS["IaRCS_04_Sioux_City-Iowa_Falls_NAD_1983_2011_LCC_US_Feet",'
        'GEOGCS["GCS_NAD_1983_2011",DATUM["D_NAD_1983_2011",'
        'SPHEROID["GRS_1980",6378137.0,298.257222101]],'
        'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],'
        'PROJECTION["Lambert_Conformal_Conic"],'
        'PARAMETER["False_Easting",14500000.0],'
        'PARAMETER["False_Northing",8600000.0],'
        'PARAMETER["Central_Meridian",-94.83333333333333],'
        'PARAMETER["Standard_Parallel_1",42.53333333333333],'
        'PARAMETER["Standard_Parallel_2",42.53333333333333],'
        'PARAMETER["Scale_Factor",1.000045],'
        'PARAMETER["Latitude_Of_Origin",42.53333333333333],'
        'UNIT["Foot_US",0.3048006096012192]]'
    )
    assert (
        crs.CRS.from_user_input(target_crs)
        .to_wkt(version="WKT2_2018")
        .startswith(
            'PROJCRS["IaRCS_04_Sioux_City-Iowa_Falls_NAD_1983_2011_LCC_US_Feet"'
        )
    )


@requires_gdal33
def test_crs__esri_only_wkt():
    """https://github.com/Toblerity/Fiona/issues/977"""
    target_crs = (
        'PROJCS["IaRCS_04_Sioux_City-Iowa_Falls_NAD_1983_2011_LCC_US_Feet",'
        'GEOGCS["GCS_NAD_1983_2011",DATUM["D_NAD_1983_2011",'
        'SPHEROID["GRS_1980",6378137.0,298.257222101]],'
        'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],'
        'PROJECTION["Lambert_Conformal_Conic"],'
        'PARAMETER["False_Easting",14500000.0],'
        'PARAMETER["False_Northing",8600000.0],'
        'PARAMETER["Central_Meridian",-94.83333333333333],'
        'PARAMETER["Standard_Parallel_1",42.53333333333333],'
        'PARAMETER["Standard_Parallel_2",42.53333333333333],'
        'PARAMETER["Scale_Factor",1.000045],'
        'PARAMETER["Latitude_Of_Origin",42.53333333333333],'
        'UNIT["Foot_US",0.3048006096012192]]'
    )
    assert (
        crs.CRS.from_user_input(target_crs)
        .to_wkt()
        .startswith(
            (
                'PROJCS["IaRCS_04_Sioux_City-Iowa_Falls_NAD_1983_2011_LCC_US_Feet"',
                'PROJCRS["IaRCS_04_Sioux_City-Iowa_Falls_NAD_1983_2011_LCC_US_Feet"',  # GDAL 3.3+
            )
        )
    )


def test_to_wkt__env_version():
    with Env(OSR_WKT_FORMAT="WKT2_2018"):
        assert crs.CRS.from_string("EPSG:4326").to_wkt().startswith('GEOGCRS["WGS 84",')


def test_to_wkt__invalid_version():
    with pytest.raises(CRSError):
        crs.CRS.from_string("EPSG:4326").to_wkt(version="invalid")


@pytest.mark.parametrize(
    "func, arg",
    [
        (crs.from_epsg, 4326),
        (crs.from_string, "EPSG:4326"),
        (crs.to_string, "EPSG:4326"),
    ],
)
def test_from_func_deprecations(func, arg):
    with pytest.warns(FionaDeprecationWarning):
        _ = func(arg)


def test_xx():
    """Create a CRS from WKT with a vertical datum."""
    wkt = """
COMPD_CS["NAD83(CSRS) / UTM zone 10N + CGVD28 height",
PROJCS["NAD83(CSRS) / UTM zone 10N",
GEOGCS["NAD83(CSRS)",
DATUM["NAD83_Canadian_Spatial_Reference_System",
SPHEROID["GRS 1980",6378137,298.257222101,
AUTHORITY["EPSG","7019"]],
AUTHORITY["EPSG","6140"]],
PRIMEM["Greenwich",0,
AUTHORITY["EPSG","8901"]],
UNIT["degree",0.0174532925199433,
AUTHORITY["EPSG","9122"]],
AUTHORITY["EPSG","4617"]],
PROJECTION["Transverse_Mercator"],
PARAMETER["latitude_of_origin",0],
PARAMETER["central_meridian",-123],
PARAMETER["scale_factor",0.9996],
PARAMETER["false_easting",500000],
PARAMETER["false_northing",0],
UNIT["metre",1,
AUTHORITY["EPSG","9001"]],
AXIS["Easting",EAST],
AXIS["Northing",NORTH],
AUTHORITY["EPSG","3157"]],
VERT_CS["CGVD28 height",
VERT_DATUM["Canadian Geodetic Vertical Datum of 1928",2005,
AUTHORITY["EPSG","5114"]],
UNIT["metre",1,
AUTHORITY["EPSG","9001"]],
AXIS["Gravity-related height",UP],
AUTHORITY["EPSG","5713"]]]
"""
    val = crs.CRS.from_wkt(wkt)
    assert val.wkt.startswith("COMPD_CS")
