"""Testing collections and workspaces."""
# coding=utf-8

import datetime
import json
import logging
import os
import random
import re
import sys

import pytest

import fiona
from fiona.collection import Collection
from fiona.drvsupport import supported_drivers
from fiona.env import getenv
from fiona.errors import (
    AttributeFilterError,
    FionaValueError,
    DriverError,
    FionaDeprecationWarning,
)
from fiona.model import Feature, Geometry

from .conftest import WGS84PATTERN


class TestSupportedDrivers:
    def test_shapefile(self):
        assert "ESRI Shapefile" in supported_drivers
        assert set(supported_drivers["ESRI Shapefile"]) == set("raw")

    def test_map(self):
        assert "MapInfo File" in supported_drivers
        assert set(supported_drivers["MapInfo File"]) == set("raw")


class TestCollectionArgs:
    def test_path(self):
        with pytest.raises(TypeError):
            Collection(0)

    def test_mode(self):
        with pytest.raises(TypeError):
            Collection("foo", mode=0)

    def test_driver(self):
        with pytest.raises(TypeError):
            Collection("foo", mode="w", driver=1)

    def test_schema(self):
        with pytest.raises(TypeError):
            Collection("foo", mode="w", driver="ESRI Shapefile", schema=1)

    def test_crs(self):
        with pytest.raises(TypeError):
            Collection("foo", mode="w", driver="ESRI Shapefile", schema=0, crs=1)

    def test_encoding(self):
        with pytest.raises(TypeError):
            Collection("foo", mode="r", encoding=1)

    def test_layer(self):
        with pytest.raises(TypeError):
            Collection("foo", mode="r", layer=0.5)

    def test_vsi(self):
        with pytest.raises(TypeError):
            Collection("foo", mode="r", vsi="git")

    def test_archive(self):
        with pytest.raises(TypeError):
            Collection("foo", mode="r", archive=1)

    def test_write_numeric_layer(self):
        with pytest.raises(ValueError):
            Collection("foo", mode="w", layer=1)

    def test_write_geojson_layer(self):
        with pytest.raises(ValueError):
            Collection("foo", mode="w", driver="GeoJSON", layer="foo")

    def test_append_geojson(self):
        with pytest.raises(ValueError):
            Collection("foo", mode="w", driver="ARCGEN")


class TestOpenException:
    def test_no_archive(self):
        with pytest.warns(FionaDeprecationWarning), pytest.raises(DriverError):
            fiona.open("/", mode="r", vfs="zip:///foo.zip")


class TestReading:
    @pytest.fixture(autouse=True)
    def shapefile(self, path_coutwildrnp_shp):
        self.c = fiona.open(path_coutwildrnp_shp, "r")
        yield
        self.c.close()

    def test_open_repr(self, path_coutwildrnp_shp):
        assert repr(self.c) == (
            f"<open Collection '{path_coutwildrnp_shp}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_closed_repr(self, path_coutwildrnp_shp):
        self.c.close()
        assert repr(self.c) == (
            f"<closed Collection '{path_coutwildrnp_shp}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_path(self, path_coutwildrnp_shp):
        assert self.c.path == path_coutwildrnp_shp

    def test_name(self):
        assert self.c.name == "coutwildrnp"

    def test_mode(self):
        assert self.c.mode == "r"

    def test_encoding(self):
        assert self.c.encoding is None

    def test_iter(self):
        assert iter(self.c)

    def test_closed_no_iter(self):
        self.c.close()
        with pytest.raises(ValueError):
            iter(self.c)

    def test_len(self):
        assert len(self.c) == 67

    def test_closed_len(self):
        # Len is lazy, it's never computed in this case. TODO?
        self.c.close()
        assert len(self.c) == 0

    def test_len_closed_len(self):
        # Lazy len is computed in this case and sticks.
        len(self.c)
        self.c.close()
        assert len(self.c) == 67

    def test_driver(self):
        assert self.c.driver == "ESRI Shapefile"

    def test_closed_driver(self):
        self.c.close()
        assert self.c.driver is None

    def test_driver_closed_driver(self):
        self.c.driver
        self.c.close()
        assert self.c.driver == "ESRI Shapefile"

    def test_schema(self):
        s = self.c.schema["properties"]
        assert s["PERIMETER"] == "float:24.15"
        assert s["NAME"] == "str:80"
        assert s["URL"] == "str:101"
        assert s["STATE_FIPS"] == "str:80"
        assert s["WILDRNP020"] == "int:10"

    def test_closed_schema(self):
        # Schema is lazy too, never computed in this case. TODO?
        self.c.close()
        assert self.c.schema is None

    def test_schema_closed_schema(self):
        self.c.schema
        self.c.close()
        assert sorted(self.c.schema.keys()) == ["geometry", "properties"]

    def test_crs(self):
        crs = self.c.crs
        assert crs["init"] == "epsg:4326"

    def test_crs_wkt(self):
        crs = self.c.crs_wkt
        assert re.match(WGS84PATTERN, crs)

    def test_closed_crs(self):
        # Crs is lazy too, never computed in this case. TODO?
        self.c.close()
        assert self.c.crs is None

    def test_crs_closed_crs(self):
        self.c.crs
        self.c.close()
        assert sorted(self.c.crs.keys()) == ["init"]

    def test_meta(self):
        assert sorted(self.c.meta.keys()) == ["crs", "crs_wkt", "driver", "schema"]

    def test_profile(self):
        assert sorted(self.c.profile.keys()) == ["crs", "crs_wkt", "driver", "schema"]

    def test_bounds(self):
        assert self.c.bounds[0] == pytest.approx(-113.564247)
        assert self.c.bounds[1] == pytest.approx(37.068981)
        assert self.c.bounds[2] == pytest.approx(-104.970871)
        assert self.c.bounds[3] == pytest.approx(41.996277)

    def test_context(self, path_coutwildrnp_shp):
        with fiona.open(path_coutwildrnp_shp, "r") as c:
            assert c.name == "coutwildrnp"
            assert len(c) == 67
            assert c.crs
        assert c.closed

    def test_iter_one(self):
        itr = iter(self.c)
        f = next(itr)
        assert f.id == "0"
        assert f.properties["STATE"] == "UT"

    def test_iter_list(self):
        f = list(self.c)[0]
        assert f.id == "0"
        assert f.properties["STATE"] == "UT"

    def test_re_iter_list(self):
        f = list(self.c)[0]  # Run through iterator
        f = list(self.c)[0]  # Run through a new, reset iterator
        assert f.id == "0"
        assert f.properties["STATE"] == "UT"

    def test_getitem_one(self):
        f = self.c[0]
        assert f.id == "0"
        assert f.properties["STATE"] == "UT"

    def test_getitem_iter_combo(self):
        i = iter(self.c)
        f = next(i)
        f = next(i)
        assert f.id == "1"
        f = self.c[0]
        assert f.id == "0"
        f = next(i)
        assert f.id == "2"

    def test_no_write(self):
        with pytest.raises(OSError):
            self.c.write({})

    def test_iter_items_list(self):
        i, f = list(self.c.items())[0]
        assert i == 0
        assert f.id == "0"
        assert f.properties["STATE"] == "UT"

    def test_iter_keys_list(self):
        i = list(self.c.keys())[0]
        assert i == 0

    def test_in_keys(self):
        assert 0 in self.c.keys()
        assert 0 in self.c


class TestReadingPathTest:
    def test_open_path(self, path_coutwildrnp_shp):
        pathlib = pytest.importorskip("pathlib")
        with fiona.open(pathlib.Path(path_coutwildrnp_shp)) as collection:
            assert collection.name == "coutwildrnp"


@pytest.mark.usefixtures("unittest_path_coutwildrnp_shp")
class TestIgnoreFieldsAndGeometry:
    def test_without_ignore(self):
        with fiona.open(self.path_coutwildrnp_shp, "r") as collection:
            assert "AREA" in collection.schema["properties"].keys()
            assert "STATE" in collection.schema["properties"].keys()
            assert "NAME" in collection.schema["properties"].keys()
            assert "geometry" in collection.schema.keys()

            feature = next(iter(collection))
            assert feature["properties"]["AREA"] is not None
            assert feature["properties"]["STATE"] is not None
            assert feature["properties"]["NAME"] is not None
            assert feature["geometry"] is not None

    def test_ignore_fields(self):
        with fiona.open(
            self.path_coutwildrnp_shp, "r", ignore_fields=["AREA", "STATE"]
        ) as collection:
            assert "AREA" not in collection.schema["properties"].keys()
            assert "STATE" not in collection.schema["properties"].keys()
            assert "NAME" in collection.schema["properties"].keys()
            assert "geometry" in collection.schema.keys()

            feature = next(iter(collection))
            assert "AREA" not in feature["properties"].keys()
            assert "STATE" not in feature["properties"].keys()
            assert feature["properties"]["NAME"] is not None
            assert feature["geometry"] is not None

    def test_ignore_invalid_field_missing(self):
        with fiona.open(
            self.path_coutwildrnp_shp, "r", ignore_fields=["DOES_NOT_EXIST"]
        ):
            pass

    def test_ignore_invalid_field_not_string(self):
        with pytest.raises(TypeError):
            with fiona.open(self.path_coutwildrnp_shp, "r", ignore_fields=[42]):
                pass

    def test_include_fields(self):
        with fiona.open(
            self.path_coutwildrnp_shp, "r", include_fields=["AREA", "STATE"]
        ) as collection:
            assert sorted(collection.schema["properties"]) == ["AREA", "STATE"]
            assert "geometry" in collection.schema.keys()

            feature = next(iter(collection))
            assert sorted(feature["properties"]) == ["AREA", "STATE"]
            assert feature["properties"]["AREA"] is not None
            assert feature["properties"]["STATE"] is not None
            assert feature["geometry"] is not None

    def test_include_fields__geom_only(self):
        with fiona.open(
            self.path_coutwildrnp_shp, "r", include_fields=()
        ) as collection:
            assert sorted(collection.schema["properties"]) == []
            assert "geometry" in collection.schema.keys()

            feature = next(iter(collection))
            assert sorted(feature["properties"]) == []
            assert feature["geometry"] is not None

    def test_include_fields__ignore_fields_error(self):
        with pytest.raises(ValueError):
            with fiona.open(
                self.path_coutwildrnp_shp,
                "r",
                include_fields=["AREA"],
                ignore_fields=["STATE"],
            ):
                pass

    def test_ignore_geometry(self):
        with fiona.open(
            self.path_coutwildrnp_shp, "r", ignore_geometry=True
        ) as collection:
            assert "AREA" in collection.schema["properties"].keys()
            assert "STATE" in collection.schema["properties"].keys()
            assert "NAME" in collection.schema["properties"].keys()
            assert "geometry" not in collection.schema.keys()

            feature = next(iter(collection))
            assert feature.properties["AREA"] is not None
            assert feature.properties["STATE"] is not None
            assert feature.properties["NAME"] is not None
            assert feature.geometry is None


class TestFilterReading:
    @pytest.fixture(autouse=True)
    def shapefile(self, path_coutwildrnp_shp):
        self.c = fiona.open(path_coutwildrnp_shp, "r")
        yield
        self.c.close()

    def test_filter_1(self):
        results = list(self.c.filter(bbox=(-120.0, 30.0, -100.0, 50.0)))
        assert len(results) == 67
        f = results[0]
        assert f.id == "0"
        assert f.properties["STATE"] == "UT"

    def test_filter_reset(self):
        results = list(self.c.filter(bbox=(-112.0, 38.0, -106.0, 40.0)))
        assert len(results) == 26
        results = list(self.c.filter())
        assert len(results) == 67

    def test_filter_mask(self):
        mask = Geometry.from_dict(
            **{
                "type": "Polygon",
                "coordinates": (
                    ((-112, 38), (-112, 40), (-106, 40), (-106, 38), (-112, 38)),
                ),
            }
        )
        results = list(self.c.filter(mask=mask))
        assert len(results) == 26

    def test_filter_where(self):
        results = list(self.c.filter(where="NAME LIKE 'Mount%'"))
        assert len(results) == 9
        assert all([x.properties["NAME"].startswith("Mount") for x in results])
        results = list(self.c.filter(where="NAME LIKE '%foo%'"))
        assert len(results) == 0
        results = list(self.c.filter())
        assert len(results) == 67

    def test_filter_where_error(self):
        for w in ["bad stuff", "NAME=3", "NNAME LIKE 'Mount%'"]:
            with pytest.raises(AttributeFilterError):
                self.c.filter(where=w)

    def test_filter_bbox_where(self):
        # combined filter criteria
        results = set(
            self.c.keys(bbox=(-120.0, 40.0, -100.0, 50.0), where="NAME LIKE 'Mount%'")
        )
        assert results == {0, 2, 5, 13}
        results = set(self.c.keys())
        assert len(results) == 67


class TestUnsupportedDriver:
    def test_immediate_fail_driver(self, tmpdir):
        schema = {
            "geometry": "Point",
            "properties": {"label": "str", "verit\xe9": "int"},
        }
        with pytest.raises(DriverError):
            fiona.open(str(tmpdir.join("foo")), "w", "Bogus", schema=schema)


@pytest.mark.iconv
class TestGenericWritingTest:
    @pytest.fixture(autouse=True)
    def no_iter_shp(self, tmpdir):
        schema = {
            "geometry": "Point",
            "properties": [("label", "str"), ("verit\xe9", "int")],
        }
        self.c = fiona.open(
            str(tmpdir.join("test-no-iter.shp")),
            "w",
            driver="ESRI Shapefile",
            schema=schema,
            encoding="Windows-1252",
        )
        yield
        self.c.close()

    def test_encoding(self):
        assert self.c.encoding == "Windows-1252"

    def test_no_iter(self):
        with pytest.raises(OSError):
            iter(self.c)

    def test_no_filter(self):
        with pytest.raises(OSError):
            self.c.filter()


class TestPropertiesNumberFormatting:
    @pytest.fixture(autouse=True)
    def shapefile(self, tmpdir):
        self.filename = str(tmpdir.join("properties_number_formatting_test"))

    _records_with_float_property1 = [
        {
            "geometry": {"type": "Point", "coordinates": (0.0, 0.1)},
            "properties": {"property1": 12.22},
        },
        {
            "geometry": {"type": "Point", "coordinates": (0.0, 0.2)},
            "properties": {"property1": 12.88},
        },
    ]

    _records_with_float_property1_as_string = [
        {
            "geometry": {"type": "Point", "coordinates": (0.0, 0.1)},
            "properties": {"property1": "12.22"},
        },
        {
            "geometry": {"type": "Point", "coordinates": (0.0, 0.2)},
            "properties": {"property1": "12.88"},
        },
    ]

    _records_with_invalid_number_property1 = [
        {
            "geometry": {"type": "Point", "coordinates": (0.0, 0.3)},
            "properties": {"property1": "invalid number"},
        }
    ]

    def _write_collection(self, records, schema, driver):
        with fiona.open(
            self.filename,
            "w",
            driver=driver,
            schema=schema,
            crs="epsg:4326",
            encoding="utf-8",
        ) as c:
            c.writerecords([Feature.from_dict(**rec) for rec in records])

    def test_shape_driver_truncates_float_property_to_requested_int_format(self):
        driver = "ESRI Shapefile"
        self._write_collection(
            self._records_with_float_property1,
            {"geometry": "Point", "properties": [("property1", "int")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            assert 12 == rf1.properties["property1"]
            assert 12 == rf2.properties["property1"]

    def test_shape_driver_rounds_float_property_to_requested_digits_number(self):
        driver = "ESRI Shapefile"
        self._write_collection(
            self._records_with_float_property1,
            {"geometry": "Point", "properties": [("property1", "float:15.1")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            assert 12.2 == rf1.properties["property1"]
            assert 12.9 == rf2.properties["property1"]

    def test_string_is_converted_to_number_and_truncated_to_requested_int_by_shape_driver(
        self,
    ):
        driver = "ESRI Shapefile"
        self._write_collection(
            self._records_with_float_property1_as_string,
            {"geometry": "Point", "properties": [("property1", "int")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            assert 12 == rf1.properties["property1"]
            assert 12 == rf2.properties["property1"]

    def test_string_is_converted_to_number_and_rounded_to_requested_digits_number_by_shape_driver(
        self,
    ):
        driver = "ESRI Shapefile"
        self._write_collection(
            self._records_with_float_property1_as_string,
            {"geometry": "Point", "properties": [("property1", "float:15.1")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            assert 12.2 == rf1.properties["property1"]
            assert 12.9 == rf2.properties["property1"]

    def test_invalid_number_is_converted_to_0_and_written_by_shape_driver(self):
        driver = "ESRI Shapefile"
        self._write_collection(
            self._records_with_invalid_number_property1,
            # {'geometry': 'Point', 'properties': [('property1', 'int')]},
            {"geometry": "Point", "properties": [("property1", "float:15.1")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 1 == len(c)

            rf1 = c[0]

            assert 0 == rf1.properties["property1"]

    def test_geojson_driver_truncates_float_property_to_requested_int_format(self):
        driver = "GeoJSON"
        self._write_collection(
            self._records_with_float_property1,
            {"geometry": "Point", "properties": [("property1", "int")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            assert 12 == rf1.properties["property1"]
            assert 12 == rf2.properties["property1"]

    def test_geojson_driver_does_not_round_float_property_to_requested_digits_number(
        self,
    ):
        driver = "GeoJSON"
        self._write_collection(
            self._records_with_float_property1,
            {"geometry": "Point", "properties": [("property1", "float:15.1")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            # ****************************************
            # FLOAT FORMATTING IS NOT RESPECTED...
            assert 12.22 == rf1.properties["property1"]
            assert 12.88 == rf2.properties["property1"]

    def test_string_is_converted_to_number_and_truncated_to_requested_int_by_geojson_driver(
        self,
    ):
        driver = "GeoJSON"
        self._write_collection(
            self._records_with_float_property1_as_string,
            {"geometry": "Point", "properties": [("property1", "int")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            assert 12 == rf1.properties["property1"]
            assert 12 == rf2.properties["property1"]

    def test_string_is_converted_to_number_but_not_rounded_to_requested_digits_number_by_geojson_driver(
        self,
    ):
        driver = "GeoJSON"
        self._write_collection(
            self._records_with_float_property1_as_string,
            {"geometry": "Point", "properties": [("property1", "float:15.1")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 2 == len(c)

            rf1, rf2 = list(c)

            # ****************************************
            # FLOAT FORMATTING IS NOT RESPECTED...
            assert 12.22 == rf1.properties["property1"]
            assert 12.88 == rf2.properties["property1"]

    def test_invalid_number_is_converted_to_0_and_written_by_geojson_driver(self):
        driver = "GeoJSON"
        self._write_collection(
            self._records_with_invalid_number_property1,
            {"geometry": "Point", "properties": [("property1", "float:15.1")]},
            driver,
        )

        with fiona.open(self.filename, driver=driver, encoding="utf-8") as c:
            assert 1 == len(c)

            rf1 = c[0]

            assert 0 == rf1.properties["property1"]


class TestPointWriting:
    @pytest.fixture(autouse=True)
    def shapefile(self, tmpdir):
        self.filename = str(tmpdir.join("point_writing_test.shp"))
        self.sink = fiona.open(
            self.filename,
            "w",
            driver="ESRI Shapefile",
            schema={
                "geometry": "Point",
                "properties": [("title", "str"), ("date", "date")],
            },
            crs="epsg:4326",
            encoding="utf-8",
        )
        yield
        self.sink.close()

    def test_cpg(self, tmpdir):
        """Requires GDAL 1.9"""
        self.sink.close()
        encoding = tmpdir.join("point_writing_test.cpg").read()
        assert encoding == "UTF-8"

    def test_write_one(self):
        assert len(self.sink) == 0
        assert self.sink.bounds == (0.0, 0.0, 0.0, 0.0)
        f = Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": (0.0, 0.1)},
                "properties": {"title": "point one", "date": "2012-01-29"},
            }
        )
        self.sink.writerecords([f])
        assert len(self.sink) == 1
        assert self.sink.bounds == (0.0, 0.1, 0.0, 0.1)
        self.sink.close()

    def test_write_two(self):
        assert len(self.sink) == 0
        assert self.sink.bounds == (0.0, 0.0, 0.0, 0.0)
        f1 = Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": (0.0, 0.1)},
                "properties": {"title": "point one", "date": "2012-01-29"},
            }
        )
        f2 = Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": (0.0, -0.1)},
                "properties": {"title": "point two", "date": "2012-01-29"},
            }
        )
        self.sink.writerecords([f1, f2])
        assert len(self.sink) == 2
        assert self.sink.bounds == (0.0, -0.1, 0.0, 0.1)

    def test_write_one_null_geom(self):
        assert len(self.sink) == 0
        assert self.sink.bounds == (0.0, 0.0, 0.0, 0.0)
        f = Feature.from_dict(
            **{
                "geometry": None,
                "properties": {"title": "point one", "date": "2012-01-29"},
            }
        )
        self.sink.writerecords([f])
        assert len(self.sink) == 1
        assert self.sink.bounds == (0.0, 0.0, 0.0, 0.0)

    def test_validate_record(self):
        fvalid = {
            "geometry": {"type": "Point", "coordinates": (0.0, 0.1)},
            "properties": {"title": "point one", "date": "2012-01-29"},
        }
        finvalid = {
            "geometry": {"type": "Point", "coordinates": (0.0, -0.1)},
            "properties": {"not-a-title": "point two", "date": "2012-01-29"},
        }
        assert self.sink.validate_record(fvalid)
        assert not self.sink.validate_record(finvalid)


class TestLineWriting:
    @pytest.fixture(autouse=True)
    def shapefile(self, tmpdir):
        self.sink = fiona.open(
            str(tmpdir.join("line_writing_test.shp")),
            "w",
            driver="ESRI Shapefile",
            schema={
                "geometry": "LineString",
                "properties": [("title", "str"), ("date", "date")],
            },
            crs={"init": "epsg:4326", "no_defs": True},
        )
        yield
        self.sink.close()

    def test_write_one(self):
        assert len(self.sink) == 0
        assert self.sink.bounds == (0.0, 0.0, 0.0, 0.0)
        f = Feature.from_dict(
            **{
                "geometry": {
                    "type": "LineString",
                    "coordinates": [(0.0, 0.1), (0.0, 0.2)],
                },
                "properties": {"title": "line one", "date": "2012-01-29"},
            }
        )
        self.sink.writerecords([f])
        assert len(self.sink) == 1
        assert self.sink.bounds == (0.0, 0.1, 0.0, 0.2)

    def test_write_two(self):
        assert len(self.sink) == 0
        assert self.sink.bounds == (0.0, 0.0, 0.0, 0.0)
        f1 = Feature.from_dict(
            **{
                "geometry": {
                    "type": "LineString",
                    "coordinates": [(0.0, 0.1), (0.0, 0.2)],
                },
                "properties": {"title": "line one", "date": "2012-01-29"},
            }
        )
        f2 = Feature.from_dict(
            **{
                "geometry": {
                    "type": "MultiLineString",
                    "coordinates": [
                        [(0.0, 0.0), (0.0, -0.1)],
                        [(0.0, -0.1), (0.0, -0.2)],
                    ],
                },
                "properties": {"title": "line two", "date": "2012-01-29"},
            }
        )
        self.sink.writerecords([f1, f2])
        assert len(self.sink) == 2
        assert self.sink.bounds == (0.0, -0.2, 0.0, 0.2)


class TestPointAppend:
    @pytest.fixture(autouse=True)
    def shapefile(self, tmpdir, path_coutwildrnp_shp):
        with fiona.open(path_coutwildrnp_shp, "r") as input:
            output_schema = input.schema
            output_schema["geometry"] = "3D Point"
            with fiona.open(
                str(tmpdir.join("test_append_point.shp")),
                "w",
                crs=None,
                driver="ESRI Shapefile",
                schema=output_schema,
            ) as output:
                for f in input:
                    fnew = Feature(
                        id=f.id,
                        properties=f.properties,
                        geometry=Geometry(
                            type="Point", coordinates=f.geometry.coordinates[0][0]
                        ),
                    )
                    output.write(fnew)

    def test_append_point(self, tmpdir):
        with fiona.open(str(tmpdir.join("test_append_point.shp")), "a") as c:
            assert c.schema["geometry"] == "3D Point"
            c.write(
                Feature.from_dict(
                    **{
                        "geometry": {"type": "Point", "coordinates": (0.0, 45.0)},
                        "properties": {
                            "PERIMETER": 1.0,
                            "FEATURE2": None,
                            "NAME": "Foo",
                            "FEATURE1": None,
                            "URL": "http://example.com",
                            "AGBUR": "BAR",
                            "AREA": 0.0,
                            "STATE_FIPS": 1,
                            "WILDRNP020": 1,
                            "STATE": "XL",
                        },
                    }
                )
            )
            assert len(c) == 68


class TestLineAppend:
    @pytest.fixture(autouse=True)
    def shapefile(self, tmpdir):
        with fiona.open(
            str(tmpdir.join("test_append_line.shp")),
            "w",
            driver="ESRI Shapefile",
            schema={
                "geometry": "MultiLineString",
                "properties": {"title": "str", "date": "date"},
            },
            crs={"init": "epsg:4326", "no_defs": True},
        ) as output:
            f = Feature.from_dict(
                **{
                    "geometry": {
                        "type": "MultiLineString",
                        "coordinates": [[(0.0, 0.1), (0.0, 0.2)]],
                    },
                    "properties": {"title": "line one", "date": "2012-01-29"},
                }
            )
            output.writerecords([f])

    def test_append_line(self, tmpdir):
        with fiona.open(str(tmpdir.join("test_append_line.shp")), "a") as c:
            assert c.schema["geometry"] == "LineString"
            f1 = Feature.from_dict(
                **{
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [(0.0, 0.1), (0.0, 0.2)],
                    },
                    "properties": {"title": "line one", "date": "2012-01-29"},
                }
            )
            f2 = Feature.from_dict(
                **{
                    "geometry": {
                        "type": "MultiLineString",
                        "coordinates": [
                            [(0.0, 0.0), (0.0, -0.1)],
                            [(0.0, -0.1), (0.0, -0.2)],
                        ],
                    },
                    "properties": {"title": "line two", "date": "2012-01-29"},
                }
            )
            c.writerecords([f1, f2])
            assert len(c) == 3
            assert c.bounds == (0.0, -0.2, 0.0, 0.2)


def test_shapefile_field_width(tmpdir):
    name = str(tmpdir.join("textfield.shp"))
    with fiona.open(
        name,
        "w",
        schema={"geometry": "Point", "properties": {"text": "str:254"}},
        driver="ESRI Shapefile",
    ) as c:
        c.write(
            Feature.from_dict(
                **{
                    "geometry": {"type": "Point", "coordinates": (0.0, 45.0)},
                    "properties": {"text": "a" * 254},
                }
            )
        )
    c = fiona.open(name, "r")
    assert c.schema["properties"]["text"] == "str:254"
    f = next(iter(c))
    assert f.properties["text"] == "a" * 254
    c.close()


class TestCollection:
    def test_invalid_mode(self, tmpdir):
        with pytest.raises(ValueError):
            fiona.open(str(tmpdir.join("bogus.shp")), "r+")

    def test_w_args(self, tmpdir):
        with pytest.raises(FionaValueError):
            fiona.open(str(tmpdir.join("test-no-iter.shp")), "w")
        with pytest.raises(FionaValueError):
            fiona.open(str(tmpdir.join("test-no-iter.shp")), "w", "Driver")

    def test_no_path(self):
        with pytest.raises(Exception):
            fiona.open("no-path.shp", "a")

    def test_no_read_conn_str(self):
        with pytest.raises(DriverError):
            fiona.open("PG:dbname=databasename", "r")

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="test only for *nix based system"
    )
    def test_no_read_directory(self):
        with pytest.raises(DriverError):
            fiona.open("/dev/null", "r")


def test_date(tmpdir):
    name = str(tmpdir.join("date_test.shp"))
    sink = fiona.open(
        name,
        "w",
        driver="ESRI Shapefile",
        schema={"geometry": "Point", "properties": [("id", "int"), ("date", "date")]},
        crs={"init": "epsg:4326", "no_defs": True},
    )

    recs = [
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": (7.0, 50.0)},
                "properties": {"id": 1, "date": "2013-02-25"},
            }
        ),
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": (7.0, 50.2)},
                "properties": {"id": 1, "date": datetime.date(2014, 2, 3)},
            }
        ),
    ]
    sink.writerecords(recs)
    sink.close()
    assert len(sink) == 2

    with fiona.open(name, "r") as c:
        assert len(c) == 2

        rf1, rf2 = list(c)
        assert rf1.properties["date"] == "2013-02-25"
        assert rf2.properties["date"] == "2014-02-03"


def test_open_kwargs(tmpdir, path_coutwildrnp_shp):
    dstfile = str(tmpdir.join("test.json"))
    with fiona.open(path_coutwildrnp_shp) as src:
        kwds = src.profile
        kwds["driver"] = "GeoJSON"
        kwds["coordinate_precision"] = 2
        with fiona.open(dstfile, "w", **kwds) as dst:
            dst.writerecords(ftr for ftr in src)

    with open(dstfile) as f:
        assert '"coordinates": [ [ [ -111.74, 42.0 ], [ -111.66, 42.0 ]' in f.read(2000)


@pytest.mark.network
def test_collection_http():
    ds = fiona.Collection(
        "https://raw.githubusercontent.com/Toblerity/Fiona/main/tests/data/coutwildrnp.shp",
        vsi="https",
    )
    assert (
        ds.path
        == "/vsicurl/https://raw.githubusercontent.com/Toblerity/Fiona/main/tests/data/coutwildrnp.shp"
    )
    assert len(ds) == 67


@pytest.mark.network
def test_collection_zip_http():
    ds = fiona.Collection(
        "https://raw.githubusercontent.com/Toblerity/Fiona/main/tests/data/coutwildrnp.zip",
        vsi="zip+https",
    )
    assert ds.path == "/vsizip/vsicurl/https://raw.githubusercontent.com/Toblerity/Fiona/main/tests/data/coutwildrnp.zip"
    assert len(ds) == 67


def test_encoding_option_warning(tmpdir, caplog):
    """There is no ENCODING creation option log warning for GeoJSON"""
    with caplog.at_level(logging.WARNING):
        fiona.Collection(
            str(tmpdir.join("test.geojson")),
            "w",
            driver="GeoJSON",
            crs="EPSG:4326",
            schema={"geometry": "Point", "properties": {"foo": "int"}},
            encoding="bogus",
        )
        assert not caplog.text


def test_closed_session_next(gdalenv, path_coutwildrnp_shp):
    """Confirm fix for  issue #687"""
    src = fiona.open(path_coutwildrnp_shp)
    itr = iter(src)
    list(itr)
    src.close()
    with pytest.raises(FionaValueError):
        next(itr)


def test_collection_no_env(path_coutwildrnp_shp):
    """We have no GDAL env left over from open"""
    collection = fiona.open(path_coutwildrnp_shp)
    assert collection
    with pytest.raises(Exception):
        getenv()


def test_collection_env(path_coutwildrnp_shp):
    """We have a GDAL env within collection context"""
    with fiona.open(path_coutwildrnp_shp):
        assert "FIONA_ENV" in getenv()


@pytest.mark.parametrize(
    "driver,filename",
    [("ESRI Shapefile", "test.shp"), ("GeoJSON", "test.json"), ("GPKG", "test.gpkg")],
)
def test_mask_polygon_triangle(tmpdir, driver, filename):
    """Test if mask works for non trivial geometries"""
    schema = {
        "geometry": "Polygon",
        "properties": {"position_i": "int", "position_j": "int"},
    }
    records = [
        Feature.from_dict(
            **{
                "geometry": {
                    "type": "Polygon",
                    "coordinates": (
                        (
                            (float(i), float(j)),
                            (float(i + 1), float(j)),
                            (float(i + 1), float(j + 1)),
                            (float(i), float(j + 1)),
                            (float(i), float(j)),
                        ),
                    ),
                },
                "properties": {"position_i": i, "position_j": j},
            }
        )
        for i in range(10)
        for j in range(10)
    ]
    random.shuffle(records)

    path = str(tmpdir.join(filename))

    with fiona.open(
        path,
        "w",
        driver=driver,
        schema=schema,
    ) as c:
        c.writerecords(records)

    with fiona.open(path) as c:
        items = list(
            c.items(
                mask=Geometry.from_dict(
                    **{
                        "type": "Polygon",
                        "coordinates": (
                            ((2.0, 2.0), (4.0, 4.0), (4.0, 6.0), (2.0, 2.0)),
                        ),
                    }
                )
            )
        )
        assert len(items) == 15


def test_collection__empty_column_name(tmpdir):
    """Based on pull #955"""
    tmpfile = str(tmpdir.join("test_empty.geojson"))
    with pytest.warns(UserWarning, match="Empty field name at index 0"):
        with fiona.open(
            tmpfile,
            "w",
            driver="GeoJSON",
            schema={"geometry": "Point", "properties": {"": "str", "name": "str"}},
        ) as tmp:
            tmp.writerecords(
                [
                    {
                        "geometry": {"type": "Point", "coordinates": [8, 49]},
                        "properties": {"": "", "name": "test"},
                    }
                ]
            )

    with fiona.open(tmpfile) as tmp:
        with pytest.warns(UserWarning, match="Empty field name at index 0"):
            assert tmp.schema == {
                "geometry": "Point",
                "properties": {"": "str", "name": "str"},
            }
        with pytest.warns(UserWarning, match="Empty field name at index 0"):
            next(tmp)


@pytest.mark.parametrize(
    "extension, driver",
    [
        ("shp", "ESRI Shapefile"),
        ("geojson", "GeoJSON"),
        ("json", "GeoJSON"),
        ("gpkg", "GPKG"),
        ("SHP", "ESRI Shapefile"),
    ],
)
def test_driver_detection(tmpdir, extension, driver):
    with fiona.open(
        str(tmpdir.join(f"test.{extension}")),
        "w",
        schema={
            "geometry": "MultiLineString",
            "properties": {"title": "str", "date": "date"},
        },
        crs="EPSG:4326",
    ) as output:
        assert output.driver == driver


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows test runners don't have full unicode support",
)
def test_collection_name(tmp_path):
    """A Collection name is plumbed all the way through."""
    filename = os.fspath(tmp_path.joinpath("test.geojson"))

    with fiona.Collection(
        filename,
        "w",
        driver="GeoJSON",
        crs="EPSG:4326",
        schema={"geometry": "Point", "properties": {"foo": "int"}},
        layer="Darwin Núñez",
        write_name=True,
    ) as colxn:
        assert colxn.name == "Darwin Núñez"

    with open(filename) as f:
        geojson = json.load(f)

    assert geojson["name"] == "Darwin Núñez"
