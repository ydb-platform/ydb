"""Tests of schema sub-types."""

import os

import fiona
from fiona.model import Feature


def test_read_bool_subtype(tmp_path):
    test_data = """{"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"bool": true, "not_bool": 1, "float": 42.5}, "geometry": null}]}"""
    path = tmp_path.joinpath("test_read_bool_subtype.geojson")

    with open(os.fspath(path), "w") as f:
        f.write(test_data)

    with fiona.open(path, "r") as src:
        feature = next(iter(src))

    assert type(feature["properties"]["bool"]) is bool
    assert isinstance(feature["properties"]["not_bool"], int)
    assert type(feature["properties"]["float"]) is float


def test_write_bool_subtype(tmp_path):
    path = tmp_path.joinpath("test_write_bool_subtype.geojson")

    schema = {
        "geometry": "Point",
        "properties": {
            "bool": "bool",
            "not_bool": "int",
            "float": "float",
        },
    }

    feature = Feature.from_dict(
        **{
            "geometry": None,
            "properties": {
                "bool": True,
                "not_bool": 1,
                "float": 42.5,
            },
        }
    )

    with fiona.open(path, "w", driver="GeoJSON", schema=schema) as dst:
        dst.write(feature)

    with open(os.fspath(path)) as f:
        data = f.read()

    assert """"bool": true""" in data
    assert """"not_bool": 1""" in data


def test_write_int16_subtype(tmp_path):
    path = tmp_path.joinpath("test_write_bool_subtype.gpkg")

    schema = {
        "geometry": "Point",
        "properties": {
            "a": "int",
            "b": "int16",
        },
    }

    feature = Feature.from_dict(
        **{
            "geometry": None,
            "properties": {
                "a": 1,
                "b": 2,
            },
        }
    )

    with fiona.open(path, "w", driver="GPKG", schema=schema) as colxn:
        colxn.write(feature)

    with fiona.open(path) as colxn:
        assert colxn.schema["properties"]["a"] == "int"
        assert colxn.schema["properties"]["b"] == "int16"
