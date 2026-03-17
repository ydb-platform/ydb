import os
import tempfile

import pytest

import fiona
from fiona.drvsupport import driver_mode_mingdal
from fiona.env import GDALVersion
from fiona.errors import SchemaError, UnsupportedGeometryTypeError, DriverSupportError
from fiona.model import Feature
from fiona.schema import NAMED_FIELD_TYPES, normalize_field_type

from .conftest import get_temp_filename
from .conftest import requires_only_gdal1, requires_gdal2


def test_schema_ordering_items(tmpdir):
    name = str(tmpdir.join("test_scheme.shp"))
    items = [("title", "str:80"), ("date", "date")]
    with fiona.open(
        name,
        "w",
        driver="ESRI Shapefile",
        schema={"geometry": "LineString", "properties": items},
    ) as c:
        assert list(c.schema["properties"].items()) == items
    with fiona.open(name) as c:
        assert list(c.schema["properties"].items()) == items


def test_shapefile_schema(tmpdir):
    name = str(tmpdir.join("test_schema.shp"))
    items = sorted(
        {
            "AWATER10": "float",
            "CLASSFP10": "str",
            "ZipCodeType": "str",
            "EstimatedPopulation": "float",
            "LocationType": "str",
            "ALAND10": "float",
            "TotalWages": "float",
            "FUNCSTAT10": "str",
            "Long": "float",
            "City": "str",
            "TaxReturnsFiled": "float",
            "State": "str",
            "Location": "str",
            "GSrchCnt": "float",
            "INTPTLAT10": "str",
            "Lat": "float",
            "MTFCC10": "str",
            "Decommisioned": "str",
            "GEOID10": "str",
            "INTPTLON10": "str",
        }.items()
    )
    with fiona.open(
        name,
        "w",
        driver="ESRI Shapefile",
        schema={"geometry": "Polygon", "properties": items},
    ) as c:
        assert list(c.schema["properties"].items()) == items
        c.write(
            Feature.from_dict(
                **{
                    "geometry": {
                        "coordinates": [
                            [
                                (-117.882442, 33.783633),
                                (-117.882284, 33.783817),
                                (-117.863348, 33.760016),
                                (-117.863478, 33.760016),
                                (-117.863869, 33.760017),
                                (-117.864, 33.760017999999995),
                                (-117.864239, 33.760019),
                                (-117.876608, 33.755769),
                                (-117.882886, 33.783114),
                                (-117.882688, 33.783345),
                                (-117.882639, 33.783401999999995),
                                (-117.88259, 33.78346),
                                (-117.882442, 33.783633),
                            ]
                        ],
                        "type": "Polygon",
                    },
                    "id": "1",
                    "properties": {
                        "ALAND10": 8819240.0,
                        "AWATER10": 309767.0,
                        "CLASSFP10": "B5",
                        "City": "SANTA ANA",
                        "Decommisioned": False,
                        "EstimatedPopulation": 27773.0,
                        "FUNCSTAT10": "S",
                        "GEOID10": "92706",
                        "GSrchCnt": 0.0,
                        "INTPTLAT10": "+33.7653010",
                        "INTPTLON10": "-117.8819759",
                        "Lat": 33.759999999999998,
                        "Location": "NA-US-CA-SANTA ANA",
                        "LocationType": "PRIMARY",
                        "Long": -117.88,
                        "MTFCC10": "G6350",
                        "State": "CA",
                        "TaxReturnsFiled": 14635.0,
                        "TotalWages": 521280485.0,
                        "ZipCodeType": "STANDARD",
                    },
                    "type": "Feature",
                }
            )
        )
        assert len(c) == 1
    with fiona.open(name) as c:
        assert list(c.schema["properties"].items()) == sorted(
            [
                ("AWATER10", "float:24.15"),
                ("CLASSFP10", "str:80"),
                ("ZipCodeTyp", "str:80"),
                ("EstimatedP", "float:24.15"),
                ("LocationTy", "str:80"),
                ("ALAND10", "float:24.15"),
                ("INTPTLAT10", "str:80"),
                ("FUNCSTAT10", "str:80"),
                ("Long", "float:24.15"),
                ("City", "str:80"),
                ("TaxReturns", "float:24.15"),
                ("State", "str:80"),
                ("Location", "str:80"),
                ("GSrchCnt", "float:24.15"),
                ("TotalWages", "float:24.15"),
                ("Lat", "float:24.15"),
                ("MTFCC10", "str:80"),
                ("INTPTLON10", "str:80"),
                ("GEOID10", "str:80"),
                ("Decommisio", "str:80"),
            ]
        )
        f = next(iter(c))
        assert f.properties["EstimatedP"] == 27773.0


def test_field_truncation_issue177(tmpdir):
    name = str(tmpdir.join("output.shp"))

    kwargs = {
        "driver": "ESRI Shapefile",
        "crs": "EPSG:4326",
        "schema": {"geometry": "Point", "properties": [("a_fieldname", "float")]},
    }

    with fiona.open(name, "w", **kwargs) as dst:
        rec = {}
        rec["geometry"] = {"type": "Point", "coordinates": (0, 0)}
        rec["properties"] = {"a_fieldname": 3.0}
        dst.write(Feature.from_dict(**rec))

    with fiona.open(name) as src:
        first = next(iter(src))
        assert first.geometry.type == "Point"
        assert first.geometry.coordinates == (0, 0)
        assert first.properties["a_fieldnam"] == 3.0


def test_unsupported_geometry_type():
    tmpdir = tempfile.mkdtemp()
    tmpfile = os.path.join(tmpdir, "test-test-geom.shp")

    profile = {
        "driver": "ESRI Shapefile",
        "schema": {"geometry": "BOGUS", "properties": {}},
    }

    with pytest.raises(UnsupportedGeometryTypeError):
        fiona.open(tmpfile, "w", **profile)


@pytest.mark.parametrize("x", list(range(1, 10)))
def test_normalize_int32(x):
    assert normalize_field_type(f"int:{x}") == "int32"


@requires_gdal2
@pytest.mark.parametrize("x", list(range(10, 20)))
def test_normalize_int64(x):
    assert normalize_field_type(f"int:{x}") == "int64"


@pytest.mark.parametrize("x", list(range(0, 20)))
def test_normalize_str(x):
    assert normalize_field_type(f"str:{x}") == "str"


def test_normalize_bool():
    assert normalize_field_type("bool") == "bool"


def test_normalize_float():
    assert normalize_field_type("float:25.8") == "float"


def test_normalize_():
    assert normalize_field_type("float:25.8") == "float"


def generate_field_types():
    """
    Produce a unique set of field types in a consistent order.

    This ensures that tests are able to run in parallel.
    """
    types = set(NAMED_FIELD_TYPES.keys())
    return list(sorted(types))


@pytest.mark.parametrize("x", generate_field_types())
def test_normalize_std(x):
    assert normalize_field_type(x) == x


def test_normalize_error():
    with pytest.raises(SchemaError):
        assert normalize_field_type("thingy")


@requires_only_gdal1
@pytest.mark.parametrize("field_type", ["time", "datetime"])
def test_check_schema_driver_support_shp(tmpdir, field_type):

    with pytest.raises(DriverSupportError):
        name = str(tmpdir.join("test_scheme.shp"))
        items = [("field1", field_type)]
        with fiona.open(
            name,
            "w",
            driver="ESRI Shapefile",
            schema={"geometry": "LineString", "properties": items},
        ) as c:
            pass


@requires_only_gdal1
def test_check_schema_driver_support_gpkg(tmpdir):
    with pytest.raises(DriverSupportError):
        name = str(tmpdir.join("test_scheme.gpkg"))
        items = [("field1", "time")]
        with fiona.open(
            name,
            "w",
            driver="GPKG",
            schema={"geometry": "LineString", "properties": items},
        ) as c:
            pass


@pytest.mark.parametrize("driver", ["GPKG", "GeoJSON"])
def test_geometry_only_schema_write(tmpdir, driver):
    schema = {
        "geometry": "Polygon",
        # No properties defined here.
    }

    record = Feature.from_dict(
        **{
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]
                ],
            }
        }
    )

    path = str(tmpdir.join(get_temp_filename(driver)))

    with fiona.open(path, mode="w", driver=driver, schema=schema) as c:
        c.write(record)

    with fiona.open(path, mode="r", driver=driver) as c:
        data = [f for f in c]
        assert len(data) == 1
        assert len(data[0].properties) == 0
        assert data[0].geometry.type == record.geometry["type"]


@pytest.mark.parametrize("driver", ["GPKG", "GeoJSON"])
def test_geometry_only_schema_update(tmpdir, driver):

    # Guard unsupported drivers
    if driver in driver_mode_mingdal["a"] and GDALVersion.runtime() < GDALVersion(
        *driver_mode_mingdal["a"][driver][:2]
    ):
        return

    schema = {
        "geometry": "Polygon",
        # No properties defined here.
    }

    record1 = Feature.from_dict(
        **{
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]
                ],
            }
        }
    )
    record2 = Feature.from_dict(
        **{
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 0.0)]
                ],
            }
        }
    )

    path = str(tmpdir.join(get_temp_filename(driver)))

    # Create file
    with fiona.open(path, mode="w", driver=driver, schema=schema) as c:
        c.write(record1)

    # Append record
    with fiona.open(path, mode="a", driver=driver) as c:
        c.write(record2)

    with fiona.open(path, mode="r", driver=driver) as c:
        data = [f for f in c]
        assert len(data) == 2

        for f in data:
            assert len(f.properties) == 0

        assert data[0].geometry.type == record1.geometry["type"]
        assert data[1].geometry.type == record2.geometry["type"]


@pytest.mark.parametrize("driver", ["GPKG", "GeoJSON"])
def test_property_only_schema_write(tmpdir, driver):

    schema = {
        # No geometry defined here.
        "properties": {"prop1": "str"}
    }

    record1 = Feature.from_dict(**{"properties": {"prop1": "one"}})

    path = str(tmpdir.join(get_temp_filename(driver)))

    with fiona.open(path, mode="w", driver=driver, schema=schema) as c:
        c.write(record1)

    with fiona.open(path, mode="r", driver=driver) as c:
        data = [f for f in c]
        assert len(data) == 1
        assert len(data[0].properties) == 1
        assert "prop1" in data[0].properties and data[0].properties["prop1"] == "one"
        for f in data:
            assert f.geometry is None


@pytest.mark.parametrize("driver", ["GPKG", "GeoJSON"])
def test_property_only_schema_update(tmpdir, driver):

    # Guard unsupported drivers
    if driver in driver_mode_mingdal["a"] and GDALVersion.runtime() < GDALVersion(
        *driver_mode_mingdal["a"][driver][:2]
    ):
        return

    schema = {
        # No geometry defined here.
        "properties": {"prop1": "str"}
    }

    record1 = Feature.from_dict(**{"properties": {"prop1": "one"}})
    record2 = Feature.from_dict(**{"properties": {"prop1": "two"}})

    path = str(tmpdir.join(get_temp_filename(driver)))

    # Create file
    with fiona.open(path, mode="w", driver=driver, schema=schema) as c:
        c.write(record1)

    # Append record
    with fiona.open(path, mode="a", driver=driver) as c:
        c.write(record2)

    with fiona.open(path, mode="r", driver=driver) as c:
        data = [f for f in c]
        assert len(data) == 2
        for f in data:
            assert len(f.properties) == 1
            assert f.geometry is None
        assert "prop1" in data[0].properties and data[0].properties["prop1"] == "one"
        assert "prop1" in data[1].properties and data[1].properties["prop1"] == "two"


def test_schema_default_fields_wrong_type(tmpdir):
    """Test for SchemaError if a default field is specified with a different type"""

    name = str(tmpdir.join("test.gpx"))
    schema = {
        "properties": {"ele": "str", "time": "datetime"},
        "geometry": "Point",
    }

    with pytest.raises(SchemaError):
        with fiona.open(name, "w", driver="GPX", schema=schema) as c:
            pass


def test_schema_string_list(tmp_path):
    output_file = tmp_path / "fio_test.geojson"
    schema = {
    "properties": {
        "time_range": "str",
    },
    "geometry": "Point",
}
    with fiona.open(
        output_file, "w", driver="GeoJSON", schema=schema, crs="EPSG:4326"
    ) as fds:
        fds.writerecords(
            [
                {
                    "id": 1,
                    "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                    "properties": {
                        "time_range": '["2020-01-01", "2020-01-02"]',
                    },
                },
            ]
        )

    with fiona.open(output_file) as fds:
        assert fds.schema["properties"] == {"time_range": "List[str]"}
        layers = list(fds)
        assert layers[0]["properties"] == {
            "time_range": ["2020-01-01", "2020-01-02"]
        }
