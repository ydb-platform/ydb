"""Tests for `$ fio collect`."""


import json
import sys

from click.testing import CliRunner
import pytest

# from fiona.fio import collect
from fiona.fio.main import main_group


def test_collect_rs(feature_seq_pp_rs):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['collect', '--src-crs', 'EPSG:3857'],
        feature_seq_pp_rs,
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 2


def test_collect_no_rs(feature_seq):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['collect', '--src-crs', 'EPSG:3857'],
        feature_seq,
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 2


def test_collect_ld(feature_seq):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['collect', '--with-ld-context', '--add-ld-context-item', 'foo=bar'],
        feature_seq,
        catch_exceptions=False)
    assert result.exit_code == 0
    assert '"@context": {' in result.output
    assert '"foo": "bar"' in result.output


def test_collect_rec_buffered(feature_seq):
    runner = CliRunner()
    result = runner.invoke(main_group, ['collect', '--record-buffered'], feature_seq)
    assert result.exit_code == 0
    assert '"FeatureCollection"' in result.output


def test_collect_noparse(feature_seq):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['collect', '--no-parse'],
        feature_seq,
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 2
    assert len(json.loads(result.output)['features']) == 2


def test_collect_noparse_records(feature_seq):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['collect', '--no-parse', '--record-buffered'],
        feature_seq,
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 2
    assert len(json.loads(result.output)['features']) == 2


def test_collect_src_crs(feature_seq):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['collect', '--no-parse', '--src-crs', 'epsg:4326'],
        feature_seq,
        catch_exceptions=False)
    assert result.exit_code == 2


def test_collect_noparse_rs(feature_seq_pp_rs):
    runner = CliRunner()
    result = runner.invoke(
        main_group, ['collect', '--no-parse'],
        feature_seq_pp_rs,
        catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.count('"Feature"') == 2
    assert len(json.loads(result.output)['features']) == 2
