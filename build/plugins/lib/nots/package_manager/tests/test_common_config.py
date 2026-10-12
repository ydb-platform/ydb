import json
from pathlib import Path

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
    filename, catalogs = load_common_config(pj, str(tmp_path))
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
        load_common_config(pj, str(tmp_path))


def test_unknown_key(tmp_path, caplog):
    pj, _ = fixture(tmp_path, "overrides: {}\ncatalogs: {project/common: {colors: 1.4.0}}")
    assert load_common_config(pj, str(tmp_path))[1]
    assert "ignoring unknown" in caplog.text


def test_own_config_required(tmp_path):
    pj, _ = fixture(tmp_path)
    del pj.data["nots"]
    with pytest.raises(ValueError, match="unresolved"):
        load_common_config(pj, str(tmp_path))


def test_path_escape(tmp_path):
    pj, _ = fixture(tmp_path)
    pj.data["nots"]["commonConfigPath"] = "../../../outside.yaml"
    with pytest.raises(ValueError, match="escapes Arcadia"):
        load_common_config(pj, str(tmp_path))


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
        load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize(
    "config_path", ["common.yaml", "./common.yaml", "../common.yaml", "../../common.yaml", "../pkg/../common.yaml"]
)
def test_current_and_parent_directories(tmp_path, config_path):
    pj, _ = fixture(tmp_path)
    pj.data["dependencies"] = {}
    pj.data["nots"]["commonConfigPath"] = config_path
    config = Path(pj.path).parent / config_path
    config.write_text("catalogs: {}")
    filename, catalogs = load_common_config(pj, str(tmp_path))
    assert filename == str(config.resolve().relative_to(tmp_path))
    assert catalogs == {}


@pytest.mark.parametrize("config_path", [".configs/common.yaml", "../.configs/common.yaml", "../sibling/common.yaml"])
def test_child_and_sibling_directories(tmp_path, config_path):
    pj, _ = fixture(tmp_path)
    pj.data["nots"]["commonConfigPath"] = config_path
    with pytest.raises(ValueError, match="module directory or a parent directory"):
        load_common_config(pj, str(tmp_path))


def test_absolute_config_path(tmp_path):
    pj, config = fixture(tmp_path)
    pj.data["nots"]["commonConfigPath"] = str(config)
    with pytest.raises(ValueError, match="relative path"):
        load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize("destination", ["project/pkg/.configs", "project/sibling", "outside"])
def test_symlink_config_directory(tmp_path, destination):
    pj, _ = fixture(tmp_path)
    target = tmp_path / destination / "target.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("catalogs: {}")
    link = tmp_path / "project/pkg/link.yaml"
    link.symlink_to(target)
    pj.data["nots"]["commonConfigPath"] = "link.yaml"
    with pytest.raises(ValueError, match="must not contain symlinks"):
        load_common_config(pj, str(tmp_path))


def test_symlink_to_parent_config(tmp_path):
    pj, config = fixture(tmp_path)
    (tmp_path / "project/pkg/link.yaml").symlink_to(config)
    pj.data["nots"]["commonConfigPath"] = "link.yaml"
    with pytest.raises(ValueError, match="must not contain symlinks"):
        load_common_config(pj, str(tmp_path))


def test_symlink_escape_arcadia(tmp_path):
    source_root = tmp_path / "arcadia"
    source_root.mkdir()
    pj, _ = fixture(source_root)
    outside = tmp_path / "outside.yaml"
    outside.write_text("catalogs: {}")
    (source_root / "project/pkg/link.yaml").symlink_to(outside)
    pj.data["nots"]["commonConfigPath"] = "link.yaml"
    with pytest.raises(ValueError, match="must not contain symlinks"):
        load_common_config(pj, str(source_root))


@pytest.mark.parametrize("broken", [False, True])
def test_symlink_in_config_directory(tmp_path, broken):
    pj, _ = fixture(tmp_path)
    real = tmp_path / "real-project"
    (tmp_path / "project").rename(real)
    (tmp_path / "project").symlink_to(tmp_path / "missing" if broken else real, target_is_directory=True)
    with pytest.raises(ValueError, match="must not contain symlinks"):
        load_common_config(pj, str(tmp_path))


def test_broken_config_symlink(tmp_path):
    pj, _ = fixture(tmp_path)
    (tmp_path / "project/pkg/link.yaml").symlink_to(tmp_path / "missing.yaml")
    pj.data["nots"]["commonConfigPath"] = "link.yaml"
    with pytest.raises(ValueError, match="must not contain symlinks"):
        load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize("config_path", [".configs/link.yaml", "../sibling/link.yaml"])
def test_directory_rule_precedes_symlink_check(tmp_path, monkeypatch, config_path):
    pj, config = fixture(tmp_path)
    link = Path(pj.path).parent / config_path
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(config)
    pj.data["nots"]["commonConfigPath"] = config_path

    def unexpected_symlink_check(_):
        pytest.fail("Invalid config directory must be rejected before filesystem inspection")

    monkeypatch.setattr("os.path.islink", unexpected_symlink_check)
    with pytest.raises(ValueError, match="module directory or a parent directory"):
        load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize("config_path", ["../..", "../../.", "../../project/.."])
def test_config_path_must_not_be_arcadia_root(tmp_path, monkeypatch, config_path):
    pj, _ = fixture(tmp_path)
    pj.data["nots"]["commonConfigPath"] = config_path

    def unexpected_read(*args, **kwargs):
        pytest.fail("Arcadia root must be rejected before reading the config")

    monkeypatch.setattr("builtins.open", unexpected_read)
    with pytest.raises(ValueError, match="module directory or a parent directory"):
        load_common_config(pj, str(tmp_path))
