"""Tests of GDAL and PROJ data finding"""

import os.path

from click.testing import CliRunner
import pytest

import fiona
from fiona._env import GDALDataFinder, PROJDataFinder
from fiona.fio.main import main_group


@pytest.mark.wheel
def test_gdal_data_wheel():
    """Get GDAL data path from a wheel"""
    assert GDALDataFinder().search() == os.path.join(os.path.dirname(fiona.__file__), 'gdal_data')


@pytest.mark.wheel
def test_proj_data_wheel():
    """Get GDAL data path from a wheel"""
    assert PROJDataFinder().search() == os.path.join(os.path.dirname(fiona.__file__), 'proj_data')


@pytest.mark.wheel
def test_env_gdal_data_wheel():
    runner = CliRunner()
    result = runner.invoke(main_group, ['env', '--gdal-data'])
    assert result.exit_code == 0
    assert result.output.strip() == os.path.join(os.path.dirname(fiona.__file__), 'gdal_data')


@pytest.mark.wheel
def test_env_proj_data_wheel():
    runner = CliRunner()
    result = runner.invoke(main_group, ['env', '--proj-data'])
    assert result.exit_code == 0
    assert result.output.strip() == os.path.join(os.path.dirname(fiona.__file__), 'proj_data')


def test_env_gdal_data_environ(monkeypatch):
    monkeypatch.setenv('GDAL_DATA', '/foo/bar')
    runner = CliRunner()
    result = runner.invoke(main_group, ['env', '--gdal-data'])
    assert result.exit_code == 0
    assert result.output.strip() == '/foo/bar'


@pytest.mark.parametrize("data_directory_env", ["PROJ_LIB", "PROJ_DATA"])
def test_env_proj_data_environ(data_directory_env, monkeypatch):
    monkeypatch.delenv('PROJ_DATA', raising=False)
    monkeypatch.delenv('PROJ_LIB', raising=False)
    monkeypatch.setenv(data_directory_env, '/foo/bar')
    runner = CliRunner()
    result = runner.invoke(main_group, ['env', '--proj-data'])
    assert result.exit_code == 0
    assert result.output.strip() == '/foo/bar'
