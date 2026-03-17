"""Tests of behavior specific to GeoJSON"""

import json

import pytest

import fiona
from fiona.collection import supported_drivers
from fiona.errors import FionaValueError, DriverError, SchemaError, CRSError
from fiona.model import Feature


def test_json_read(path_coutwildrnp_json):
    with fiona.open(path_coutwildrnp_json, "r") as c:
        assert len(c) == 67


def test_json(tmpdir):
    """Write a simple GeoJSON file"""
    path = str(tmpdir.join("foo.json"))
    with fiona.open(
        path,
        "w",
        driver="GeoJSON",
        schema={"geometry": "Unknown", "properties": [("title", "str")]},
    ) as c:
        c.writerecords(
            [
                Feature.from_dict(
                    **{
                        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                        "properties": {"title": "One"},
                    }
                )
            ]
        )
        c.writerecords(
            [
                Feature.from_dict(
                    **{
                        "geometry": {"type": "MultiPoint", "coordinates": [[0.0, 0.0]]},
                        "properties": {"title": "Two"},
                    }
                )
            ]
        )
    with fiona.open(path) as c:
        assert c.schema["geometry"] == "Unknown"
        assert len(c) == 2


def test_json_overwrite(tmpdir):
    """Overwrite an existing GeoJSON file"""
    path = str(tmpdir.join("foo.json"))

    driver = "GeoJSON"
    schema1 = {"geometry": "Unknown", "properties": [("title", "str")]}
    schema2 = {"geometry": "Unknown", "properties": [("other", "str")]}

    features1 = [
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                "properties": {"title": "One"},
            }
        ),
        Feature.from_dict(
            **{
                "geometry": {"type": "MultiPoint", "coordinates": [[0.0, 0.0]]},
                "properties": {"title": "Two"},
            }
        ),
    ]
    features2 = [
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                "properties": {"other": "Three"},
            }
        ),
    ]

    # write some data to a file
    with fiona.open(path, "w", driver=driver, schema=schema1) as c:
        c.writerecords(features1)

    # test the data was written correctly
    with fiona.open(path, "r") as c:
        assert len(c) == 2
        feature = next(iter(c))
        assert feature.properties["title"] == "One"

    # attempt to overwrite the existing file with some new data
    with fiona.open(path, "w", driver=driver, schema=schema2) as c:
        c.writerecords(features2)

    # test the second file was written correctly
    with fiona.open(path, "r") as c:
        assert len(c) == 1
        feature = next(iter(c))
        assert feature.properties["other"] == "Three"


def test_json_overwrite_invalid(tmpdir):
    """Overwrite an existing file that isn't a valid GeoJSON"""

    # write some invalid data to a file
    path = str(tmpdir.join("foo.json"))
    with open(path, "w") as f:
        f.write("This isn't a valid GeoJSON file!!!")

    schema1 = {"geometry": "Unknown", "properties": [("title", "str")]}
    features1 = [
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                "properties": {"title": "One"},
            }
        ),
        Feature.from_dict(
            **{
                "geometry": {"type": "MultiPoint", "coordinates": [[0.0, 0.0]]},
                "properties": {"title": "Two"},
            }
        ),
    ]

    # attempt to overwrite it with a valid file
    with fiona.open(path, "w", driver="GeoJSON", schema=schema1) as dst:
        dst.writerecords(features1)

    # test the data was written correctly
    with fiona.open(path, "r") as src:
        assert len(src) == 2


def test_write_json_invalid_directory(tmpdir):
    """Attempt to create a file in a directory that doesn't exist"""
    path = str(tmpdir.join("does-not-exist", "foo.json"))
    schema = {"geometry": "Unknown", "properties": [("title", "str")]}
    with pytest.raises(DriverError):
        fiona.open(path, "w", driver="GeoJSON", schema=schema)


def test_empty_array_property(tmp_path):
    """Confirm fix for bug reported in gh-1227."""
    tmp_path.joinpath("test.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [12, 24]},
                        "properties": {"array_prop": ["some_value"]},
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [12, 24]},
                        "properties": {"array_prop": []},
                    },
                ],
            }
        )
    )
    list(fiona.open(tmp_path.joinpath("test.geojson")))
