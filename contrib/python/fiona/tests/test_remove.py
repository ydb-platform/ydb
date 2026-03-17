import logging
import sys
import os
import itertools
from .conftest import requires_gpkg

import pytest

import fiona
from fiona.errors import DatasetDeleteError
from fiona.model import Feature


def create_sample_data(filename, driver, **extra_meta):
    meta = {"driver": driver, "schema": {"geometry": "Point", "properties": {}}}
    meta.update(extra_meta)
    with fiona.open(filename, "w", **meta) as dst:
        dst.write(
            Feature.from_dict(
                **{
                    "geometry": {
                        "type": "Point",
                        "coordinates": (0, 0),
                    },
                    "properties": {},
                }
            )
        )
    assert os.path.exists(filename)


drivers = ["ESRI Shapefile", "GeoJSON"]
kinds = ["path", "collection"]
specify_drivers = [True, False]
test_data = itertools.product(drivers, kinds, specify_drivers)


@pytest.mark.parametrize("driver, kind, specify_driver", test_data)
def test_remove(tmpdir, kind, driver, specify_driver):
    """Test various dataset removal operations"""
    extension = {"ESRI Shapefile": "shp", "GeoJSON": "json"}[driver]
    filename = f"delete_me.{extension}"
    output_filename = str(tmpdir.join(filename))

    create_sample_data(output_filename, driver=driver)
    if kind == "collection":
        to_delete = fiona.open(output_filename, "r")
    else:
        to_delete = output_filename

    assert os.path.exists(output_filename)
    if specify_driver:
        fiona.remove(to_delete, driver=driver)
    else:
        fiona.remove(to_delete)
    assert not os.path.exists(output_filename)


def test_remove_nonexistent(tmpdir):
    """Attempting to remove a file that does not exist results in an OSError"""
    filename = str(tmpdir.join("does_not_exist.shp"))
    assert not os.path.exists(filename)
    with pytest.raises(OSError):
        fiona.remove(filename)


@requires_gpkg
def test_remove_layer(tmpdir):
    filename = str(tmpdir.join("a_filename.gpkg"))
    create_sample_data(filename, "GPKG", layer="layer1")
    create_sample_data(filename, "GPKG", layer="layer2")
    create_sample_data(filename, "GPKG", layer="layer3")
    create_sample_data(filename, "GPKG", layer="layer4")
    assert fiona.listlayers(filename) == ["layer1", "layer2", "layer3", "layer4"]

    # remove by index
    fiona.remove(filename, layer=2)
    assert fiona.listlayers(filename) == ["layer1", "layer2", "layer4"]

    # remove by name
    fiona.remove(filename, layer="layer2")
    assert fiona.listlayers(filename) == ["layer1", "layer4"]

    # remove by negative index
    fiona.remove(filename, layer=-1)
    assert fiona.listlayers(filename) == ["layer1"]

    # invalid layer name
    with pytest.raises(ValueError):
        fiona.remove(filename, layer="invalid_layer_name")

    # invalid layer index
    with pytest.raises(DatasetDeleteError):
        fiona.remove(filename, layer=999)


def test_remove_layer_shapefile(tmpdir):
    """Removal of layer in shapefile actually deletes the datasource"""
    filename = str(tmpdir.join("a_filename.shp"))
    create_sample_data(filename, "ESRI Shapefile")
    fiona.remove(filename, layer=0)
    assert not os.path.exists(filename)


def test_remove_layer_geojson(tmpdir):
    """Removal of layers is not supported by GeoJSON driver

    The reason for failure is slightly different between GDAL 2.2+ and < 2.2.
    With < 2.2 the datasource will fail to open in write mode (OSError), while
    with 2.2+ the datasource will open but the removal operation will fail (not
    supported).
    """
    filename = str(tmpdir.join("a_filename.geojson"))
    create_sample_data(filename, "GeoJSON")
    with pytest.raises((RuntimeError, OSError)):
        fiona.remove(filename, layer=0)
    assert os.path.exists(filename)
