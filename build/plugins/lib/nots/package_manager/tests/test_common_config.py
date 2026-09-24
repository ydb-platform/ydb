import json

import pytest

from build.plugins.lib.nots.package_manager.common_config import load_common_config, is_version_range
from build.plugins.lib.nots.package_manager.package_json import PackageJson


def fixture(tmp_path, content="catalogs:\n  project/common:\n    colors: 1.4.0\n"):
    module = tmp_path / "project/pkg"
    module.mkdir(parents=True)
    config = module.parent / "common.yaml"
    config.write_text(content)
    pj_path = module / "package.json"
    pj_path.write_text(
        json.dumps(
            {
                "nots": {"commonConfigPath": "../common.yaml"},
                "dependencies": {"colors": "catalog:project/common"},
            }
        )
    )
    return PackageJson.load(str(pj_path)), config


def test_load(tmp_path):
    pj, _ = fixture(tmp_path)
    filename, catalogs = load_common_config(pj, str(tmp_path), True)
    assert filename == "project/common.yaml"
    assert catalogs == {"project/common": {"colors": "1.4.0"}}
    assert pj.data["dependencies"]["colors"] == "catalog:project/common"


@pytest.mark.parametrize(
    "content",
    [
        "[]",
        "catalogs: []",
        "catalogs: null",
        "catalogs: {wrong: {colors: 1.4.0}}",
        "catalogs: {project/common: {colors: 'file:../colors'}}",
        "catalogs: {project/common: {other: 1.4.0}}",
        "catalogs: {project/common: {colors: 12}}",
        "catalogs: [",
    ],
)
def test_invalid(tmp_path, content):
    pj, _ = fixture(tmp_path, content)
    with pytest.raises(ValueError):
        load_common_config(pj, str(tmp_path), True)


def test_unknown_key(tmp_path, caplog):
    pj, _ = fixture(tmp_path, "overrides: {}\ncatalogs: {project/common: {colors: 1.4.0}}")
    assert load_common_config(pj, str(tmp_path), True)[1]
    assert "ignoring unknown" in caplog.text


def test_legacy_ignores_missing_config(tmp_path, caplog):
    pj, config = fixture(tmp_path)
    config.unlink()
    assert load_common_config(pj, str(tmp_path), False) == (None, {})
    assert "requires injected peers" in caplog.text
    with pytest.raises(ValueError, match="cannot read"):
        load_common_config(pj, str(tmp_path), True)


def test_own_config_required(tmp_path):
    pj, _ = fixture(tmp_path)
    del pj.data["nots"]
    with pytest.raises(ValueError, match="unresolved"):
        load_common_config(pj, str(tmp_path), True)


def test_path_escape(tmp_path):
    pj, _ = fixture(tmp_path)
    pj.data["nots"]["commonConfigPath"] = "../../../outside.yaml"
    with pytest.raises(ValueError, match="escapes Arcadia"):
        load_common_config(pj, str(tmp_path), True)


@pytest.mark.parametrize(
    "value", ["1.4.0", "^9.0.0", "~1.2", ">=1.2.3 <2", "1.2 - 2.0", "1.x", "*", "1.0.0-beta.1", "1 || 2"]
)
def test_ranges(value):
    assert is_version_range(value)


@pytest.mark.parametrize(
    "value", ["", "latest", "file:../a", "workspace:*", "catalog:other", "01.2.3", "1.2.3.4", "^no", 12, None]
)
def test_invalid_ranges(value):
    assert not is_version_range(value)


def test_duplicate_yaml_keys(tmp_path):
    pj, _ = fixture(tmp_path, "catalogs: {project/common: {colors: 1.4.0}, project/common: {colors: 2.0.0}}")
    with pytest.raises(ValueError, match="Duplicate YAML key"):
        load_common_config(pj, str(tmp_path), True)
