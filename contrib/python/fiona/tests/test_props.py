import json
import os.path
import tempfile

import pytest

import fiona
from fiona import prop_type, prop_width
from fiona.model import Feature

from .conftest import gdal_version


def test_width_str():
    assert prop_width("str:254") == 254
    assert prop_width("str") == 80


def test_width_other():
    assert prop_width("int") == None
    assert prop_width("float") == None
    assert prop_width("date") == None


def test_types():
    assert prop_type("str:254") == str
    assert prop_type("str") == str
    assert isinstance(0, prop_type("int"))
    assert isinstance(0.0, prop_type("float"))
    assert prop_type("date") == str


@pytest.mark.xfail(not gdal_version.at_least("3.5"), reason="Requires at least GDAL 3.5.0")
def test_read_json_object_properties():
    """JSON object properties are properly serialized"""
    data = """
{
  "type": "FeatureCollection",
  "features": [
    {
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              87.33588,
              43.53139
            ],
            [
              87.33588,
              45.66894
            ],
            [
              90.27542,
              45.66894
            ],
            [
              90.27542,
              43.53139
            ],
            [
              87.33588,
              43.53139
            ]
          ]
        ]
      },
      "type": "Feature",
      "properties": {
        "upperLeftCoordinate": {
          "latitude": 45.66894,
          "longitude": 87.91166
        },
        "tricky": "{gotcha"
      }
    }
  ]
}
"""
    tmpdir = tempfile.mkdtemp()
    filename = os.path.join(tmpdir, "test.json")

    with open(filename, "w") as f:
        f.write(data)

    with fiona.open(filename) as src:
        ftr = next(iter(src))
        props = ftr["properties"]
        assert props["upperLeftCoordinate"]["latitude"] == 45.66894
        assert props["upperLeftCoordinate"]["longitude"] == 87.91166
        assert props["tricky"] == "{gotcha"


@pytest.mark.xfail(not gdal_version.at_least("3.5"), reason="Requires at least GDAL 3.5.0")
def test_write_json_object_properties():
    """Python object properties are properly serialized"""
    data = """
{
  "type": "FeatureCollection",
  "features": [
    {
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              87.33588,
              43.53139
            ],
            [
              87.33588,
              45.66894
            ],
            [
              90.27542,
              45.66894
            ],
            [
              90.27542,
              43.53139
            ],
            [
              87.33588,
              43.53139
            ]
          ]
        ]
      },
      "type": "Feature",
      "properties": {
        "upperLeftCoordinate": {
          "latitude": 45.66894,
          "longitude": 87.91166
        },
        "tricky": "{gotcha"
      }
    }
  ]
}
"""
    data = Feature.from_dict(**json.loads(data)["features"][0])
    tmpdir = tempfile.mkdtemp()
    filename = os.path.join(tmpdir, "test.json")
    with fiona.open(
        filename,
        "w",
        driver="GeoJSON",
        schema={
            "geometry": "Polygon",
            "properties": {"upperLeftCoordinate": "str", "tricky": "str"},
        },
    ) as dst:
        dst.write(data)

    with fiona.open(filename) as src:
        ftr = next(iter(src))
        props = ftr["properties"]
        assert props["upperLeftCoordinate"]["latitude"] == 45.66894
        assert props["upperLeftCoordinate"]["longitude"] == 87.91166
        assert props["tricky"] == "{gotcha"


def test_json_prop_decode_non_geojson_driver():
    feature = Feature.from_dict(
        **{
            "type": "Feature",
            "properties": {
                "ulc": {"latitude": 45.66894, "longitude": 87.91166},
                "tricky": "{gotcha",
            },
            "geometry": {"type": "Point", "coordinates": [10, 15]},
        }
    )

    meta = {
        "crs": "EPSG:4326",
        "driver": "ESRI Shapefile",
        "schema": {
            "geometry": "Point",
            "properties": {"ulc": "str:255", "tricky": "str:255"},
        },
    }

    tmpdir = tempfile.mkdtemp()
    filename = os.path.join(tmpdir, "test.json")
    with fiona.open(filename, "w", **meta) as dst:
        dst.write(feature)

    with fiona.open(filename) as src:
        actual = next(iter(src))

    assert isinstance(actual["properties"]["ulc"], str)
    a = json.loads(actual["properties"]["ulc"])
    e = json.loads(actual["properties"]["ulc"])
    assert e == a
    assert actual["properties"]["tricky"].startswith("{")
