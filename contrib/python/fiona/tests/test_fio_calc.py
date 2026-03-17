"""Tests for `$ fio calc`."""


import json

from click.testing import CliRunner

from fiona.fio.main import main_group


def test_fail():
    runner = CliRunner()
    result = runner.invoke(main_group, ['calc', "TEST", "f.properties.test > 5"],
                           '{"type": "no_properties"}')
    assert result.exit_code == 1


def _load(output):
    features = []
    for x in output.splitlines():
        try:
            features.append(json.loads(x))
        except:
            # Click combines stdout and stderr and shapely dumps logs to
            # stderr that are not JSON
            # https://github.com/pallets/click/issues/371
            pass
    return features


def test_calc_seq(feature_seq, runner):
    result = runner.invoke(main_group, ['calc', 
        "TEST",
        "f.properties.AREA / f.properties.PERIMETER"],
        feature_seq)
    assert result.exit_code == 0

    feats = _load(result.output)
    assert len(feats) == 2
    for feat in feats:
        assert feat['properties']['TEST'] == \
            feat['properties']['AREA'] / feat['properties']['PERIMETER']


def test_bool_seq(feature_seq, runner):
    result = runner.invoke(main_group, ['calc', "TEST", "f.properties.AREA > 0.015"],
                           feature_seq)
    assert result.exit_code == 0
    feats = _load(result.output)
    assert len(feats) == 2
    assert feats[0]['properties']['TEST']
    assert not feats[1]['properties']['TEST']


def test_existing_property(feature_seq, runner):
    result = runner.invoke(
        main_group, ["calc", "AREA", "f.properties.AREA * 2"], feature_seq
    )
    assert result.exit_code == 2

    result = runner.invoke(main_group, ['calc', "--overwrite", "AREA", "f.properties.AREA * 2"],
                           feature_seq)
    assert result.exit_code == 0
    feats = _load(result.output)
    assert len(feats) == 2
    for feat in feats:
        assert 'AREA' in feat['properties']
