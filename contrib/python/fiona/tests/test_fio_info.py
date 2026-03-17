"""Tests for ``$ fio info``."""


import json
import re
import sys

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

from click.testing import CliRunner
import pytest

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

from fiona.fio.main import main_group


def test_info_json(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(main_group, ['info', path_coutwildrnp_shp])
    assert result.exit_code == 0
    assert '"count": 67' in result.output
    assert '"crs": "EPSG:4326"' in result.output
    assert '"driver": "ESRI Shapefile"' in result.output
    assert '"name": "coutwildrnp"' in result.output


def test_info_count(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['info', '--count', path_coutwildrnp_shp])
    assert result.exit_code == 0
    assert result.output == "67\n"


def test_info_bounds(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['info', '--bounds', path_coutwildrnp_shp])
    assert result.exit_code == 0
    assert len(re.findall(r'\d*\.\d*', result.output)) == 4


def test_all_registered():
    """Make sure all the subcommands are actually registered to the main CLI
    group."""
    for ep in entry_points(group="fiona.fio_commands"):
        assert ep.name in main_group.commands


def _filter_info_warning(lines):
    """$ fio info can issue a RuntimeWarning, but click adds stderr to stdout
    so we have to filter it out before decoding JSON lines."""
    lines = list(filter(lambda x: 'RuntimeWarning' not in x, lines))
    return lines


def test_info_no_count(path_gpx):
    """Make sure we can still get a `$ fio info` report on datasources that do
    not support feature counting, AKA `len(collection)`.
    """
    runner = CliRunner()
    result = runner.invoke(main_group, ['info', path_gpx])
    assert result.exit_code == 0
    lines = _filter_info_warning(result.output.splitlines())
    assert len(lines) == 1, "First line is warning & second is JSON.  No more."
    assert json.loads(lines[0])['count'] is None


def test_info_layer(path_gpx):
    for layer in ('routes', '1'):
        runner = CliRunner()
        result = runner.invoke(main_group, [
            'info',
            path_gpx,
            '--layer', layer])
        assert result.exit_code == 0
        lines = _filter_info_warning(result.output.splitlines())
        assert len(lines) == 1, "1st line is warning & 2nd is JSON - no more."
        assert json.loads(lines[0])['name'] == 'routes'


def test_info_vfs(path_coutwildrnp_zip, path_coutwildrnp_shp):
    runner = CliRunner()
    zip_result = runner.invoke(main_group, [
        'info', f'zip://{path_coutwildrnp_zip}'])
    shp_result = runner.invoke(main_group, [
        'info', path_coutwildrnp_shp])
    assert zip_result.exit_code == shp_result.exit_code == 0
    assert zip_result.output == shp_result.output
