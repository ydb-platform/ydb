"""Tests of the ogrext.Session class"""
import pytest

import fiona
from fiona.errors import GDALVersionError, UnsupportedOperation
from .conftest import gdal_version


def test_get(path_coutwildrnp_shp):
    with fiona.open(path_coutwildrnp_shp) as col:
        feat3 = col.get(2)
        assert feat3['properties']['NAME'] == 'Mount Zirkel Wilderness'


@pytest.mark.parametrize("layer, namespace, tags", [
    (None, None, {"test_tag1": "test_value1", "test_tag2": "test_value2"}),
    (None, "test", {"test_tag1": "test_value1", "test_tag2": "test_value2"}),
    (None, None, {}),
    (None, "test", {}),  
    ("layer", None, {"test_tag1": "test_value1", "test_tag2": "test_value2"}),
    ("layer", "test", {"test_tag1": "test_value1", "test_tag2": "test_value2"}),
    ("layer", None, {}),
    ("layer", "test", {}),  
])
@pytest.mark.skipif(gdal_version.major < 2, reason="Broken on GDAL 1.x")
def test_update_tags(layer, namespace, tags, tmpdir):
    test_geopackage = str(tmpdir.join("test.gpkg"))
    schema = {'properties': {'CDATA1': 'str:254'}, 'geometry': 'Polygon'}
    with fiona.Env(), fiona.open(
            test_geopackage, "w", driver="GPKG", schema=schema, layer=layer) as gpkg:
        assert gpkg.tags() == {}
        gpkg.update_tags(tags, ns=namespace)

    with fiona.Env(), fiona.open(test_geopackage, layer=layer) as gpkg:
        assert gpkg.tags(ns=namespace) == tags
        if namespace is not None:
            assert gpkg.tags() == {}
        with pytest.raises(UnsupportedOperation):
            gpkg.update_tags({}, ns=namespace)


@pytest.mark.parametrize("layer, namespace", [
    (None, None),
    (None, "test"),
    ("test", None),
    ("test", "test"),
])
@pytest.mark.skipif(gdal_version.major < 2, reason="Broken on GDAL 1.x")
def test_update_tag_item(layer, namespace, tmpdir):
    test_geopackage = str(tmpdir.join("test.gpkg"))
    schema = {'properties': {'CDATA1': 'str:254'}, 'geometry': 'Polygon'}
    with fiona.Env(), fiona.open(
            test_geopackage, "w", driver="GPKG", schema=schema, layer=layer) as gpkg:
        assert gpkg.get_tag_item("test_tag1", ns=namespace) is None
        gpkg.update_tag_item("test_tag1", "test_value1", ns=namespace)

    with fiona.Env(), fiona.open(test_geopackage, layer=layer) as gpkg:
        if namespace is not None:
            assert gpkg.get_tag_item("test_tag1") is None
        assert gpkg.get_tag_item("test_tag1", ns=namespace) == "test_value1"
        with pytest.raises(UnsupportedOperation):
            gpkg.update_tag_item("test_tag1", "test_value1", ns=namespace)


@pytest.mark.skipif(gdal_version.major >= 2, reason="Only raises on GDAL 1.x")
def test_gdal_version_error(tmpdir):
    test_geopackage = str(tmpdir.join("test.gpkg"))
    schema = {'properties': {'CDATA1': 'str:254'}, 'geometry': 'Polygon'}
    with fiona.Env(), fiona.open(
            test_geopackage, "w", driver="GPKG", schema=schema, layer="layer") as gpkg:
        with pytest.raises(GDALVersionError):
            gpkg.update_tags({"test_tag1": "test_value1"}, ns="test")
        with pytest.raises(GDALVersionError):
            gpkg.update_tag_item("test_tag1", "test_value1", ns="test")
        with pytest.raises(GDALVersionError):
            gpkg.tags()
        with pytest.raises(GDALVersionError):
            gpkg.get_tag_item("test_tag1")
