"""
Tests related to the validation of feature geometry types against the schema.
"""

import fiona
import pytest

from fiona.errors import GeometryTypeValidationError, UnsupportedGeometryTypeError
from fiona.model import Feature


@pytest.fixture
def filename_shp(tmpdir):
    return str(tmpdir.join("example.shp"))


@pytest.fixture
def filename_json(tmpdir):
    return str(tmpdir.join("example.json"))


properties = {"name": "str"}
PROPERTIES = {"name": "example"}
POINT = {"type": "Point", "coordinates": (1.0, 2.0)}
LINESTRING = {"type": "LineString", "coordinates": [(1.0, 2.0), (3.0, 4.0)]}
POLYGON = {"type": "Polygon", "coordinates": [[(0.0, 0.0), (1.0, 1.0), (0.0, 0.1)]]}
MULTILINESTRING = {
    "type": "MultiLineString",
    "coordinates": [[(0.0, 0.0), (1.0, 1.0)], [(1.0, 2.0), (3.0, 4.0)]],
}
GEOMETRYCOLLECTION = {
    "type": "GeometryCollection",
    "geometries": [POINT, LINESTRING, POLYGON],
}
INVALID = {"type": "InvalidType", "coordinates": (42.0, 43.0)}
POINT_3D = {"type": "Point", "coordinates": (1.0, 2.0, 3.0)}


def write_point(collection):
    feature = Feature.from_dict(**{"geometry": POINT, "properties": PROPERTIES})
    collection.write(feature)


def write_linestring(collection):
    feature = Feature.from_dict(**{"geometry": LINESTRING, "properties": PROPERTIES})
    collection.write(feature)


def write_polygon(collection):
    feature = Feature.from_dict(**{"geometry": POLYGON, "properties": PROPERTIES})
    collection.write(feature)


def write_invalid(collection):
    feature = Feature.from_dict(**{"geometry": INVALID, "properties": PROPERTIES})
    collection.write(feature)


def write_multilinestring(collection):
    feature = Feature.from_dict(
        **{"geometry": MULTILINESTRING, "properties": PROPERTIES}
    )
    collection.write(feature)


def write_point_3d(collection):
    feature = Feature.from_dict(**{"geometry": POINT_3D, "properties": PROPERTIES})
    collection.write(feature)


def write_geometrycollection(collection):
    feature = Feature.from_dict(
        **{"geometry": GEOMETRYCOLLECTION, "properties": PROPERTIES}
    )
    collection.write(feature)


def write_null(collection):
    feature = Feature.from_dict(**{"geometry": None, "properties": PROPERTIES})
    collection.write(feature)


def test_point(filename_shp):
    schema = {"geometry": "Point", "properties": properties}
    with fiona.open(
        filename_shp, "w", driver="ESRI Shapefile", schema=schema
    ) as collection:
        write_point(collection)
        write_point_3d(collection)
        write_null(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_linestring(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_invalid(collection)


def test_multi_type(filename_json):
    schema = {"geometry": ("Point", "LineString"), "properties": properties}
    with fiona.open(filename_json, "w", driver="GeoJSON", schema=schema) as collection:
        write_point(collection)
        write_linestring(collection)
        write_null(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_polygon(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_invalid(collection)


def test_unknown(filename_json):
    """Reading and writing layers with "Unknown" (i.e. any) geometry type"""
    # write a layer with a mixture of geometry types
    schema = {"geometry": "Unknown", "properties": properties}
    with fiona.open(filename_json, "w", driver="GeoJSON", schema=schema) as collection:
        write_point(collection)
        write_linestring(collection)
        write_polygon(collection)
        write_geometrycollection(collection)
        write_null(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_invalid(collection)

    # copy the features to a new layer, reusing the layers metadata
    with fiona.open(filename_json, "r", driver="GeoJSON") as src:
        filename_dst = filename_json.replace(".json", "_v2.json")
        assert src.schema["geometry"] == "Unknown"
        with fiona.open(filename_dst, "w", **src.meta) as dst:
            dst.writerecords(src)


def test_any(filename_json):
    schema = {"geometry": "Any", "properties": properties}
    with fiona.open(filename_json, "w", driver="GeoJSON", schema=schema) as collection:
        write_point(collection)
        write_linestring(collection)
        write_polygon(collection)
        write_geometrycollection(collection)
        write_null(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_invalid(collection)


def test_broken(filename_json):
    schema = {"geometry": "NOT_VALID", "properties": properties}
    with pytest.raises(UnsupportedGeometryTypeError):
        with fiona.open(filename_json, "w", driver="GeoJSON", schema=schema):
            pass


def test_broken_list(filename_json):
    schema = {
        "geometry": ("Point", "LineString", "NOT_VALID"),
        "properties": properties,
    }
    with pytest.raises(UnsupportedGeometryTypeError):
        collection = fiona.open(filename_json, "w", driver="GeoJSON", schema=schema)


def test_invalid_schema(filename_shp):
    """Features match schema but geometries not supported by driver"""
    schema = {"geometry": ("Point", "LineString"), "properties": properties}
    with fiona.open(
        filename_shp, "w", driver="ESRI Shapefile", schema=schema
    ) as collection:
        write_linestring(collection)

        with pytest.raises(RuntimeError):
            # ESRI Shapefile can only store a single geometry type
            write_point(collection)


def test_esri_multi_geom(filename_shp):
    """ESRI Shapefile doesn't differentiate between LineString/MultiLineString"""
    schema = {"geometry": "LineString", "properties": properties}
    with fiona.open(
        filename_shp, "w", driver="ESRI Shapefile", schema=schema
    ) as collection:
        write_linestring(collection)
        write_multilinestring(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_point(collection)


def test_3d_schema_ignored(filename_json):
    schema = {"geometry": "3D Point", "properties": properties}
    with fiona.open(filename_json, "w", driver="GeoJSON", schema=schema) as collection:
        write_point(collection)
        write_point_3d(collection)


def test_geometrycollection_schema(filename_json):
    schema = {"geometry": "GeometryCollection", "properties": properties}
    with fiona.open(filename_json, "w", driver="GeoJSON", schema=schema) as collection:
        write_geometrycollection(collection)


def test_none_schema(filename_json):
    schema = {"geometry": None, "properties": properties}
    with fiona.open(filename_json, "w", driver="GeoJSON", schema=schema) as collection:
        write_null(collection)

        with pytest.raises(GeometryTypeValidationError):
            write_point(collection)
        with pytest.raises(GeometryTypeValidationError):
            write_linestring(collection)
