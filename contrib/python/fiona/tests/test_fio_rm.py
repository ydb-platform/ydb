import os

import pytest
from click.testing import CliRunner

import fiona
from fiona.model import Feature
from fiona.fio.main import main_group


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


@pytest.mark.parametrize("driver", drivers)
def test_remove(tmpdir, driver):
    extension = {"ESRI Shapefile": "shp", "GeoJSON": "json"}[driver]
    filename = f"delete_me.{extension}"
    filename = str(tmpdir.join(filename))
    create_sample_data(filename, driver)

    result = CliRunner().invoke(main_group, ["rm", filename, "--yes"])
    assert result.exit_code == 0
    assert not os.path.exists(filename)


has_gpkg = "GPKG" in fiona.supported_drivers.keys()


@pytest.mark.skipif(not has_gpkg, reason="Requires GPKG driver")
def test_remove_layer(tmpdir):
    filename = str(tmpdir.join("a_filename.gpkg"))
    create_sample_data(filename, "GPKG", layer="layer1")
    create_sample_data(filename, "GPKG", layer="layer2")
    assert fiona.listlayers(filename) == ["layer1", "layer2"]

    result = CliRunner().invoke(
        main_group, ["rm", filename, "--layer", "layer2", "--yes"]
    )
    assert result.exit_code == 0
    assert os.path.exists(filename)
    assert fiona.listlayers(filename) == ["layer1"]
