"""Tests for `$ fio bounds`."""


import re

from fiona.fio import bounds
from fiona.fio.main import main_group


def test_fail(runner):
    result = runner.invoke(main_group, ['bounds', ], '5')
    assert result.exit_code == 1


def test_seq(feature_seq, runner):
    result = runner.invoke(main_group, ['bounds', ], feature_seq)
    assert result.exit_code == 0
    assert result.output.count('[') == result.output.count(']') == 2
    assert len(re.findall(r'\d*\.\d*', result.output)) == 8


def test_seq_rs(feature_seq_pp_rs, runner):
    result = runner.invoke(main_group, ['bounds', ], feature_seq_pp_rs)
    assert result.exit_code == 0
    assert result.output.count('[') == result.output.count(']') == 2
    assert len(re.findall(r'\d*\.\d*', result.output)) == 8


def test_precision(feature_seq, runner):
    result = runner.invoke(main_group, ['bounds', '--precision', 1], feature_seq)
    assert result.exit_code == 0
    assert result.output.count('[') == result.output.count(']') == 2
    assert len(re.findall(r'\d*\.\d{1}\D', result.output)) == 8


def test_explode(feature_collection, runner):
    result = runner.invoke(main_group, ['bounds', '--explode'], feature_collection)
    assert result.exit_code == 0
    assert result.output.count('[') == result.output.count(']') == 2
    assert len(re.findall(r'\d*\.\d*', result.output)) == 8


def test_explode_pp(feature_collection_pp, runner):
    result = runner.invoke(main_group, ['bounds', '--explode'], feature_collection_pp)
    assert result.exit_code == 0
    assert result.output.count('[') == result.output.count(']') == 2
    assert len(re.findall(r'\d*\.\d*', result.output)) == 8


def test_with_id(feature_seq, runner):
    result = runner.invoke(main_group, ['bounds', '--with-id'], feature_seq)
    assert result.exit_code == 0
    assert result.output.count('id') == result.output.count('bbox') == 2


def test_explode_with_id(feature_collection, runner):
    result = runner.invoke(
        main_group, ['bounds', '--explode', '--with-id'], feature_collection)
    assert result.exit_code == 0
    assert result.output.count('id') == result.output.count('bbox') == 2


def test_with_obj(feature_seq, runner):
    result = runner.invoke(main_group, ['bounds', '--with-obj'], feature_seq)
    assert result.exit_code == 0
    assert result.output.count('geometry') == result.output.count('bbox') == 2


def test_bounds_explode_with_obj(feature_collection, runner):
    result = runner.invoke(
        main_group, ['bounds', '--explode', '--with-obj'], feature_collection)
    assert result.exit_code == 0
    assert result.output.count('geometry') == result.output.count('bbox') == 2


def test_explode_output_rs(feature_collection, runner):
    result = runner.invoke(main_group, ['bounds', '--explode', '--rs'], feature_collection)
    assert result.exit_code == 0
    assert result.output.count('\x1e') == 2
    assert result.output.count('[') == result.output.count(']') == 2
    assert len(re.findall(r'\d*\.\d*', result.output)) == 8
