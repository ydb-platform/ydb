"""Test listing a datasource's layers."""

from pathlib import Path
import os

import pytest

import fiona
import fiona.ogrext
from fiona.errors import DriverError, FionaDeprecationWarning, FionaValueError
from fiona.io import ZipMemoryFile


def test_single_file_private(path_coutwildrnp_shp):
    with fiona.Env():
        assert fiona.ogrext._listlayers(
            path_coutwildrnp_shp) == ['coutwildrnp']


def test_single_file(path_coutwildrnp_shp):
    assert fiona.listlayers(path_coutwildrnp_shp) == ['coutwildrnp']


def test_directory(data_dir):
    assert sorted(fiona.listlayers(data_dir)) == ['coutwildrnp', 'gre', 'test_tin']


def test_directory_trailing_slash(data_dir):
    assert sorted(fiona.listlayers(data_dir)) == ['coutwildrnp', 'gre', 'test_tin']


def test_zip_path(path_coutwildrnp_zip):
    assert fiona.listlayers(
        f'zip://{path_coutwildrnp_zip}') == ['coutwildrnp']


def test_zip_path_arch(path_coutwildrnp_zip):
    vfs = f'zip://{path_coutwildrnp_zip}'
    with pytest.warns(FionaDeprecationWarning):
        assert fiona.listlayers('/coutwildrnp.shp', vfs=vfs) == ['coutwildrnp']


def test_list_not_existing(data_dir):
    """Test underlying Cython function correctly raises"""
    path = os.path.join(data_dir, "does_not_exist.geojson")
    with pytest.raises(DriverError):
        fiona.ogrext._listlayers(path)


def test_invalid_path():
    with pytest.raises(TypeError):
        fiona.listlayers(1)


def test_invalid_vfs():
    with pytest.raises(TypeError):
        fiona.listlayers("/", vfs=1)


def test_invalid_path_ioerror():
    with pytest.raises(DriverError):
        fiona.listlayers("foobar")


def test_path_object(path_coutwildrnp_shp):
    path_obj = Path(path_coutwildrnp_shp)
    assert fiona.listlayers(path_obj) == ['coutwildrnp']


def test_listing_file(path_coutwildrnp_json):
    """list layers from an open file object"""
    with open(path_coutwildrnp_json, "rb") as f:
        assert len(fiona.listlayers(f)) == 1


def test_listing_pathobj(path_coutwildrnp_json):
    """list layers from a Path object"""
    pathlib = pytest.importorskip("pathlib")
    assert len(fiona.listlayers(pathlib.Path(path_coutwildrnp_json))) == 1


def test_listdir_path(path_coutwildrnp_zip):
    """List directories in a path"""
    assert sorted(fiona.listdir(f"zip://{path_coutwildrnp_zip}")) == [
        "coutwildrnp.dbf",
        "coutwildrnp.prj",
        "coutwildrnp.shp",
        "coutwildrnp.shx",
    ]


def test_listdir_path_not_existing(data_dir):
    """Test listing of a non existent directory"""
    path = os.path.join(data_dir, "does_not_exist.zip")
    with pytest.raises(FionaValueError):
        fiona.listdir(path)


def test_listdir_invalid_path():
    """List directories with invalid path"""
    with pytest.raises(TypeError):
        assert fiona.listdir(1)


def test_listdir_file(path_coutwildrnp_zip):
    """Test list directories of a file"""
    with pytest.raises(FionaValueError):
        fiona.listdir(f"zip://{path_coutwildrnp_zip}/coutwildrnp.shp")


def test_listdir_zipmemoryfile(bytes_coutwildrnp_zip):
    """Test list directories of a zipped memory file."""
    with ZipMemoryFile(bytes_coutwildrnp_zip) as memfile:
        print(memfile.name)
        assert sorted(fiona.listdir(memfile.name)) == [
            "coutwildrnp.dbf",
            "coutwildrnp.prj",
            "coutwildrnp.shp",
            "coutwildrnp.shx",
        ]
