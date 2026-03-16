"""Tests for `$ fio load`."""

from functools import partial
import json
import os
import shutil

import pytest

import fiona
from fiona.fio.main import main_group
from fiona.model import ObjectEncoder


def test_err(runner):
    result = runner.invoke(
        main_group, ['load'], '', catch_exceptions=False)
    assert result.exit_code == 2


def test_exception(tmpdir, runner):
    tmpfile = str(tmpdir.mkdir('tests').join('test_exception.shp'))
    result = runner.invoke(main_group, [
        'load', '-f', 'Shapefile', tmpfile
    ], '42', catch_exceptions=False)
    assert result.exit_code == 1


def test_collection(tmpdir, feature_collection, runner):
    tmpfile = str(tmpdir.mkdir('tests').join('test_collection.shp'))
    result = runner.invoke(
        main_group, ['load', '-f', 'Shapefile', tmpfile], feature_collection)
    assert result.exit_code == 0
    assert len(fiona.open(tmpfile)) == 2


def test_seq_rs(feature_seq_pp_rs, tmpdir, runner):
    tmpfile = str(tmpdir.mkdir('tests').join('test_seq_rs.shp'))
    result = runner.invoke(
        main_group, ['load', '-f', 'Shapefile', tmpfile], feature_seq_pp_rs)
    assert result.exit_code == 0
    assert len(fiona.open(tmpfile)) == 2


def test_seq_no_rs(tmpdir, runner, feature_seq):
    tmpfile = str(tmpdir.mkdir('tests').join('test_seq_no_rs.shp'))
    result = runner.invoke(main_group, [
        'load', '-f', 'Shapefile', tmpfile], feature_seq)
    assert result.exit_code == 0
    assert len(fiona.open(tmpfile)) == 2


def test_dst_crs_default_to_src_crs(tmpdir, runner, feature_seq):
    """When --dst-crs is not given default to --src-crs."""
    tmpfile = str(tmpdir.mkdir('tests').join('test_src_vs_dst_crs.shp'))
    result = runner.invoke(main_group, [
        'load',
        '--src-crs',
        'EPSG:32617',
        '-f', 'Shapefile',
        tmpfile
    ], feature_seq)
    assert result.exit_code == 0
    with fiona.open(tmpfile) as src:
        assert src.crs == {'init': 'epsg:32617'}
        assert len(src) == len(feature_seq.splitlines())


def test_different_crs(tmpdir, runner, feature_seq):
    tmpfile = str(tmpdir.mkdir('tests').join('test_different_crs.shp'))
    result = runner.invoke(
        main_group, [
            'load', '--src-crs', 'EPSG:32617', '--dst-crs', 'EPSG:32610',
            '-f', 'Shapefile', tmpfile
        ], feature_seq)
    assert result.exit_code == 0
    with fiona.open(tmpfile) as src:
        assert src.crs == {'init': 'epsg:32610'}
        assert len(src) == len(feature_seq.splitlines())


def test_dst_crs_no_src(tmpdir, runner, feature_seq):
    tmpfile = str(tmpdir.mkdir('tests').join('test_dst_crs_no_src.shp'))
    result = runner.invoke(main_group, [
        'load',
        '--dst-crs',
        'EPSG:32610',
        '-f', 'Shapefile',
        tmpfile
    ], feature_seq)
    assert result.exit_code == 0
    with fiona.open(tmpfile) as src:
        assert src.crs == {'init': 'epsg:32610'}
        assert len(src) == len(feature_seq.splitlines())


def test_fio_load_layer(tmpdir, runner):
    outdir = str(tmpdir.mkdir('tests').mkdir('test_fio_load_layer'))
    try:
        feature = {
            'type': 'Feature',
            'properties': {'key': 'value'},
            'geometry': {
                'type': 'Point',
                'coordinates': (5.0, 39.0)
            }
        }
        sequence = os.linesep.join(map(partial(json.dumps, cls=ObjectEncoder), [feature, feature]))
        result = runner.invoke(main_group, [
            'load',
            outdir,
            '--driver', 'ESRI Shapefile',
            '--src-crs', 'EPSG:4236',
            '--layer', 'test_layer'],
            input=sequence)
        assert result.exit_code == 0

        with fiona.open(outdir) as src:
            assert len(src) == 2
            assert src.name == 'test_layer'
            assert src.schema['geometry'] == 'Point'

    finally:
        shutil.rmtree(outdir)

@pytest.mark.iconv
def test_creation_options(tmpdir, runner, feature_seq):
    tmpfile = str(tmpdir.mkdir("tests").join("test.shp"))
    result = runner.invoke(
        main_group,
        ["load", "-f", "Shapefile", "--co", "ENCODING=LATIN1", tmpfile],
        feature_seq,
    )
    assert result.exit_code == 0
    assert tmpdir.join("tests/test.cpg").read() == "LATIN1"


@pytest.mark.parametrize("extension, driver", [
    ("shp", "ESRI Shapefile"),
    ("geojson", "GeoJSON"),
    ("json", "GeoJSON"),
    ("gpkg", "GPKG"),
    ("SHP", "ESRI Shapefile"),
])
def test_load__auto_detect_format(tmpdir, runner, feature_seq, extension, driver):
    tmpfile = str(tmpdir.mkdir('tests').join(f'test_src_vs_dst_crs.{extension}'))
    result = runner.invoke(main_group, [
        'load',
        '--src-crs',
        'EPSG:32617',
        tmpfile
    ], feature_seq)
    assert result.exit_code == 0
    with fiona.open(tmpfile.lower()) as src:
        assert src.crs == {'init': 'epsg:32617'}
        assert len(src) == len(feature_seq.splitlines())
        assert src.driver == driver


def test_fio_load_layer_append(tmpdir, runner):
    """Checking append mode."""
    outdir = str(tmpdir.mkdir("tests").mkdir("test_fio_load_layer"))
    try:
        feature = {
            "type": "Feature",
            "properties": {"key": "value"},
            "geometry": {"type": "Point", "coordinates": (5.0, 39.0)},
        }
        sequence = os.linesep.join(
            map(partial(json.dumps, cls=ObjectEncoder), [feature, feature])
        )

        # Write mode to create layer.
        result = runner.invoke(
            main_group,
            [
                "load",
                outdir,
                "--driver",
                "ESRI Shapefile",
                "--src-crs",
                "EPSG:4236",
                "--layer",
                "test_layer",
            ],
            input=sequence,
        )
        assert result.exit_code == 0

        # Here's the append.
        result = runner.invoke(
            main_group,
            [
                "load",
                outdir,
                "--driver=ESRI Shapefile",
                "--src-crs=EPSG:4236",
                "--layer=test_layer",
                "--append",
            ],
            input=sequence,
        )
        assert result.exit_code == 0

        with fiona.open(outdir) as src:
            assert len(src) == 4
            assert src.name == "test_layer"
            assert src.schema["geometry"] == "Point"

    finally:
        shutil.rmtree(outdir)
