"""Tests for `$ fio cat`."""


from click.testing import CliRunner

from fiona.fio.main import main_group


def test_one(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(main_group, ['cat', path_coutwildrnp_shp])
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 67


def test_two(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(main_group, ['cat', path_coutwildrnp_shp, path_coutwildrnp_shp])
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 134


def test_bbox_no(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, '--bbox', '0,10,80,20'],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == ""


def test_bbox_yes(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, '--bbox', '-109,37,-107,39'],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 19


def test_bbox_yes_two_files(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, path_coutwildrnp_shp, '--bbox', '-109,37,-107,39'],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 38


def test_bbox_json_yes(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, '--bbox', '[-109,37,-107,39]'],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 19


def test_bbox_where(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, '--bbox', '-120,40,-100,50',
         '--where', "NAME LIKE 'Mount%'"],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 4


def test_where_no(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, '--where', "STATE LIKE '%foo%'"],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == ""


def test_where_yes(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, '--where', "NAME LIKE 'Mount%'"],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 9


def test_where_yes_two_files(path_coutwildrnp_shp):
    runner = CliRunner()
    result = runner.invoke(
        main_group,
        ['cat', path_coutwildrnp_shp, path_coutwildrnp_shp,
         '--where', "NAME LIKE 'Mount%'"],
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 18


def test_where_fail(data_dir):
    runner = CliRunner()
    result = runner.invoke(main_group, ['cat', '--where', "NAME=3",
                           data_dir])
    assert result.exit_code != 0


def test_multi_layer(data_dir):
    layerdef = "1:coutwildrnp,1:coutwildrnp"
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['cat', '--layer', layerdef, data_dir])
    assert result.output.count('"Feature"') == 134


def test_multi_layer_fail(data_dir):
    runner = CliRunner()
    result = runner.invoke(main_group, ['cat', '--layer', '200000:coutlildrnp',
                           data_dir])
    assert result.exit_code != 0


def test_vfs(path_coutwildrnp_zip):
    runner = CliRunner()
    result = runner.invoke(main_group, [
        'cat', f'zip://{path_coutwildrnp_zip}'])
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 67


def test_dst_crs_epsg3857(path_coutwildrnp_shp):
    """Confirm fix of issue #952"""
    runner = CliRunner()
    result = runner.invoke(
        main_group, ["cat", "--dst-crs", "EPSG:3857", path_coutwildrnp_shp]
    )
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 67
