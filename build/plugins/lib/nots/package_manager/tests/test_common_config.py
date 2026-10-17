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
    pj, _ = fixture(tmp_path, "unknown: {}\nstoreDir: /ignored\ncatalogs: {project/common: {colors: 1.4.0}}")
    assert load_common_config(pj, str(tmp_path))[1]
    warnings = [record for record in caplog.records if record.name.endswith("common_config")]
    assert len(warnings) == 1
    assert warnings[0].message == f"{pj.path}: ignoring common config fields: unknown, storeDir"


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


def test_shared_pnpm_settings(tmp_path):
    pj, _ = fixture(
        tmp_path,
        """catalogs: {project/common: {colors: 1.4.0}}
overrides: {colors: 'file:./replacement'}
packageExtensions: {colors: {dependencies: {extra: 'file:./extra'}, peerDependenciesMeta: {react: {optional: true}}}}
patchedDependencies: {colors: patches/colors.patch}
peerDependencyRules: {ignoreMissing: [react], allowAny: [foo], allowedVersions: {bar: '2'}}
""",
    )
    from build.plugins.lib.nots.package_manager.common_config import rebase_pnpm_settings

    filename, _, settings = load_common_config(pj, str(tmp_path), include_settings=True)
    assert filename == "project/common.yaml"
    rebased = rebase_pnpm_settings(settings, "/build/project", "/build/project/pkg")
    assert rebased["overrides"] == {"colors": "file:../replacement"}
    assert rebased["packageExtensions"]["colors"]["dependencies"] == {"extra": "file:../extra"}
    assert rebased["patchedDependencies"] == {"colors": "../patches/colors.patch"}
    assert rebased["peerDependencyRules"] == settings["peerDependencyRules"]
    assert settings["overrides"]["colors"] == "file:./replacement"


@pytest.mark.parametrize(
    "settings",
    [
        "overrides: []",
        "overrides: {foo: 12}",
        "patchedDependencies: {foo: null}",
        "patchedDependencies: {foo: ../../escape.patch}",
        "packageExtensions: {foo: {dependencies: {bar: 12}}}",
        "peerDependencyRules: {ignoreMissing: foo}",
        "peerDependencyRules: {allowedVersions: {bar: 12}}",
    ],
)
def test_invalid_pnpm_settings(tmp_path, settings):
    pj, _ = fixture(tmp_path, "catalogs: {project/common: {colors: 1.4.0}}\n" + settings)
    with pytest.raises(ValueError):
        load_common_config(pj, str(tmp_path))


def test_common_inputs_ignore_file_override_peer_configs(tmp_path):
    from build.plugins.lib.nots.package_manager.common_config import common_config_inputs

    pj, _ = fixture(tmp_path, "catalogs: {project/common: {colors: 1.4.0}}\noverrides: {colors: 'file:./replacement'}")
    peer = tmp_path / "project/replacement"
    peer.mkdir()
    (peer / "package.json").write_text(json.dumps({"nots": {"commonConfigPath": "../peer.yaml"}}))
    (peer.parent / "peer.yaml").write_text("patchedDependencies: {other: patches/other.patch}")
    assert common_config_inputs(pj, str(tmp_path)) == ["project/common.yaml"]


@pytest.mark.parametrize(
    "common,local,expected", [(None, None, None), (True, None, True), (False, True, True), (True, False, False)]
)
def test_dedupe_peers_opt_in(tmp_path, common, local, expected):
    from build.plugins.lib.nots.package_manager.common_config import (
        join_pnpm_settings,
        rebase_pnpm_settings,
        select_pnpm_settings,
    )

    pj, _ = fixture(
        tmp_path,
        "catalogs: {project/common: {colors: 1.4.0}}\n"
        + ("" if common is None else f"dedupePeers: {str(common).lower()}\n"),
    )
    _, _, settings = load_common_config(pj, str(tmp_path), include_settings=True)
    settings = rebase_pnpm_settings(settings, str(tmp_path), str(tmp_path / "project/pkg"))
    target = {} if local is None else {"dedupePeers": local}
    assert join_pnpm_settings(settings, select_pnpm_settings(target)).get("dedupePeers") is expected


@pytest.mark.parametrize("value", ["'true'", "1", "{}", "[]", "null"])
def test_dedupe_peers_requires_boolean(tmp_path, value):
    pj, _ = fixture(tmp_path, f"catalogs: {{project/common: {{colors: 1.4.0}}}}\ndedupePeers: {value}\n")
    with pytest.raises(ValueError, match="dedupePeers must be a boolean"):
        load_common_config(pj, str(tmp_path), include_settings=True)


@pytest.mark.parametrize(
    "common,local,expected", [(None, None, None), (True, None, True), (False, True, True), (True, False, False)]
)
def test_force_legacy_deploy_merge(tmp_path, common, local, expected):
    from build.plugins.lib.nots.package_manager.common_config import (
        join_pnpm_settings,
        rebase_pnpm_settings,
        select_pnpm_settings,
    )

    pj, _ = fixture(
        tmp_path,
        "catalogs: {project/common: {colors: 1.4.0}}\n"
        + ("" if common is None else f"forceLegacyDeploy: {str(common).lower()}\n"),
    )
    _, _, settings = load_common_config(pj, str(tmp_path), include_settings=True)
    settings = rebase_pnpm_settings(settings, str(tmp_path), str(tmp_path / "project/pkg"))
    target = {} if local is None else {"forceLegacyDeploy": local}
    assert join_pnpm_settings(settings, select_pnpm_settings(target)).get("forceLegacyDeploy") is expected


@pytest.mark.parametrize("value", ["'true'", "1", "{}", "[]", "null"])
def test_force_legacy_deploy_requires_boolean(tmp_path, value):
    pj, _ = fixture(tmp_path, f"catalogs: {{project/common: {{colors: 1.4.0}}}}\nforceLegacyDeploy: {value}\n")
    with pytest.raises(ValueError, match="forceLegacyDeploy must be a boolean"):
        load_common_config(pj, str(tmp_path), include_settings=True)


@pytest.mark.parametrize("value", ["true", 1, {}, [], None])
def test_local_force_legacy_deploy_requires_boolean(value):
    from build.plugins.lib.nots.package_manager.common_config import select_pnpm_settings

    with pytest.raises(ValueError, match="forceLegacyDeploy must be a boolean"):
        select_pnpm_settings({"forceLegacyDeploy": value})


@pytest.mark.parametrize(
    "common,local,expected", [(None, None, None), (2000, None, 2000), (2000, 500, 500), (2000, 0, 0)]
)
def test_peers_suffix_max_length_merge(tmp_path, common, local, expected):
    from build.plugins.lib.nots.package_manager.common_config import (
        join_pnpm_settings,
        rebase_pnpm_settings,
        select_pnpm_settings,
    )

    pj, _ = fixture(
        tmp_path,
        "catalogs: {project/common: {colors: 1.4.0}}\n"
        + ("" if common is None else f"peersSuffixMaxLength: {common}\n"),
    )
    _, _, settings = load_common_config(pj, str(tmp_path), include_settings=True)
    settings = rebase_pnpm_settings(settings, str(tmp_path), str(tmp_path / "project/pkg"))
    target = {} if local is None else {"peersSuffixMaxLength": local}
    assert join_pnpm_settings(settings, select_pnpm_settings(target)).get("peersSuffixMaxLength") == expected


@pytest.mark.parametrize("value", [True, -1, 1.5, "1000", None, {}, []])
def test_invalid_peers_suffix_max_length(tmp_path, value):
    from build.plugins.lib.nots.package_manager.common_config import select_pnpm_settings

    with pytest.raises(ValueError, match="peersSuffixMaxLength must be a non-negative integer"):
        select_pnpm_settings({"peersSuffixMaxLength": value})
    pj, _ = fixture(tmp_path, "catalogs: {project/common: {colors: 1.4.0}}\npeersSuffixMaxLength: " + json.dumps(value))
    with pytest.raises(ValueError, match="peersSuffixMaxLength must be a non-negative integer"):
        load_common_config(pj, str(tmp_path), include_settings=True)


@pytest.mark.parametrize(
    "common,local,expected", [(None, None, None), (True, None, True), (False, True, True), (True, False, False)]
)
def test_enable_global_virtual_store_merge(tmp_path, common, local, expected):
    from build.plugins.lib.nots.package_manager.common_config import (
        join_pnpm_settings,
        rebase_pnpm_settings,
        select_pnpm_settings,
    )

    pj, _ = fixture(
        tmp_path,
        "catalogs: {project/common: {colors: 1.4.0}}\n"
        + ("" if common is None else f"enableGlobalVirtualStore: {str(common).lower()}\n"),
    )
    _, _, settings = load_common_config(pj, str(tmp_path), include_settings=True)
    settings = rebase_pnpm_settings(settings, str(tmp_path), str(tmp_path / "project/pkg"))
    target = {} if local is None else {"enableGlobalVirtualStore": local}
    assert join_pnpm_settings(settings, select_pnpm_settings(target)).get("enableGlobalVirtualStore") is expected


@pytest.mark.parametrize("value", ["'true'", "1", "{}", "[]", "null"])
def test_enable_global_virtual_store_requires_boolean(tmp_path, value):
    pj, _ = fixture(tmp_path, f"catalogs: {{project/common: {{colors: 1.4.0}}}}\nenableGlobalVirtualStore: {value}\n")
    with pytest.raises(ValueError, match="enableGlobalVirtualStore must be a boolean"):
        load_common_config(pj, str(tmp_path), include_settings=True)


@pytest.mark.parametrize("value", ["true", 1, {}, [], None])
def test_local_enable_global_virtual_store_requires_boolean(value):
    from build.plugins.lib.nots.package_manager.common_config import select_pnpm_settings

    with pytest.raises(ValueError, match="enableGlobalVirtualStore must be a boolean"):
        select_pnpm_settings({"enableGlobalVirtualStore": value})


@pytest.mark.parametrize(
    "settings,message",
    [
        ({"overrides": {"foo": 12}}, "invalid overrides value for foo"),
        ({"overrides": []}, "overrides must be a mapping"),
        ({"packageExtensions": {"foo": {"dependencies": {"bar": 12}}}}, "invalid extension dependency specifier"),
    ],
)
def test_invalid_local_peer_settings(tmp_path, settings, message):
    from types import SimpleNamespace
    from build.plugins.lib.nots.package_manager.package_manager import PackageManager

    pj, _ = fixture(tmp_path)
    pj.data["pnpm"] = settings
    pm = SimpleNamespace(
        sources_path=str(tmp_path / "project/pkg"),
        sources_root=str(tmp_path),
        module_path="project/pkg",
        load_package_json_from_dir=lambda _: pj,
    )
    with pytest.raises(ValueError, match=message):
        PackageManager.get_local_peers_from_package_json(pm)


def test_local_peer_settings_ignore_unknown_fields(tmp_path):
    from build.plugins.lib.nots.package_manager.common_config import select_pnpm_settings

    assert select_pnpm_settings({"onlyBuiltDependencies": 12, "futureOption": []}) == {}


def test_scalar_types_come_from_shared_policy(tmp_path, monkeypatch):
    from build.plugins.lib.nots.package_manager import common_config

    schema = dict(common_config.PNPM_SETTING_TYPES)
    schema.update(futureBoolean="boolean", futureInteger="non-negative-integer", futureList="string-list")
    monkeypatch.setattr(common_config, "PNPM_SETTING_TYPES", schema)
    monkeypatch.setattr(common_config, "PNPM_SETTINGS", tuple(key for key in schema if key != "catalogs"))
    pj, _ = fixture(
        tmp_path,
        "catalogs: {project/common: {colors: 1.4.0}}\n" "futureBoolean: true\nfutureInteger: 12\nfutureList: [common]",
    )
    settings = common_config.load_common_config(pj, str(tmp_path), include_settings=True)[2]
    assert settings == {"futureBoolean": True, "futureInteger": 12, "futureList": ["common"]}
    assert common_config.rebase_pnpm_settings(settings, "/source", "/target") == settings
    assert common_config.join_pnpm_settings(settings, {"futureBoolean": False, "futureList": ["local"]}) == {
        "futureBoolean": False,
        "futureInteger": 12,
        "futureList": ["common", "local"],
    }
    for key, value in (("futureBoolean", "true"), ("futureInteger", -1), ("futureList", True)):
        with pytest.raises(ValueError, match=key):
            common_config.select_pnpm_settings({key: value})


@pytest.mark.parametrize("relative", ["./replacement", "./unused/../replacement", "replacement"])
def test_common_file_overrides_are_rebased_for_injected_dependencies(tmp_path, relative):
    from build.plugins.lib.nots.package_manager.common_config import (
        rebase_pnpm_settings,
        workspace_settings_paths,
    )

    pj, config = fixture(
        tmp_path, "catalogs: {project/common: {colors: 1.4.0}}\noverrides: {colors: 'file:" + relative + "'}"
    )
    _, _, settings = load_common_config(pj, str(tmp_path), include_settings=True)
    peers = rebase_pnpm_settings(settings, str(config.parent), str(Path(pj.path).parent))
    assert workspace_settings_paths(peers) == ["../replacement"]
    generated = rebase_pnpm_settings(settings, "/build/project", "/build/project/pkg")
    assert generated["overrides"] == {"colors": "file:../replacement"}
    assert settings["overrides"] == {"colors": "file:" + relative}


def test_local_file_override_keeps_spelling():
    from build.plugins.lib.nots.package_manager.common_config import rebase_pnpm_settings

    settings = {"overrides": {"colors": "file:../pkg/../replacement"}}
    result = rebase_pnpm_settings(settings, "/build/project/pkg", "/build/project/pkg")
    assert result == settings
    assert result is not settings


def test_common_file_override_in_current_directory():
    from build.plugins.lib.nots.package_manager.common_config import rebase_pnpm_settings

    settings = {"overrides": {"colors": "file:../replacement"}}
    assert rebase_pnpm_settings(settings, "/build/pkg", "/build/pkg") == {
        "overrides": {"colors": "file:../replacement"}
    }


@pytest.mark.parametrize("local", [False, True])
def test_override_only_peers_are_available_without_dependencies(tmp_path, local):
    from build.plugins.lib.nots.package_manager.package_manager import PackageManager

    pj, _ = fixture(tmp_path, "overrides: {foo: 'file:./replacement'}")
    pj.data.pop("dependencies")
    if local:
        pj.data["pnpm"] = {"overrides": {"foo": "file:../local"}}
    pj.write()
    for peer_name in ("replacement", "local"):
        peer = tmp_path / "project" / peer_name
        peer.mkdir()
        (peer / "ya.make").write_text("TS_PACKAGE()\nEND()")
    pm = PackageManager(
        str(tmp_path / "build"),
        str(tmp_path / "build/project/pkg"),
        str(Path(pj.path).parent),
        sources_root=str(tmp_path),
    )
    assert pm.get_local_peers_from_package_json() == ["project/local" if local else "project/replacement"]


@pytest.mark.parametrize("common", [False, True])
@pytest.mark.parametrize("value", ["workspace:../replacement", "workspace:*", "workspace:^", "workspace:~", "$colors"])
def test_workspace_overrides_are_rejected(tmp_path, common, value):
    pj, _ = fixture(tmp_path, "overrides: {colors: '" + value + "'}" if common else "catalogs: {}")
    pj.data["dependencies"] = {"colors": "workspace:../replacement"}
    if not common:
        pj.data["pnpm"] = {"overrides": {"colors": value}}
    with pytest.raises(ValueError, match="overrides colors: workspace: протокол не поддерживается; перейдите на file:"):
        load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize("common", [False, True])
@pytest.mark.parametrize("section", ["dependencies", "devDependencies", "optionalDependencies"])
@pytest.mark.parametrize("value", ["file:../replacement", "1.0.0"])
def test_override_references_to_supported_dependencies_are_allowed(tmp_path, common, section, value):
    pj, _ = fixture(tmp_path, "overrides: {colors: '$colors'}" if common else "catalogs: {}")
    pj.data.pop("dependencies")
    pj.data[section] = {"colors": value}
    if not common:
        pj.data["pnpm"] = {"overrides": {"colors": "$colors"}}
    load_common_config(pj, str(tmp_path))
    assert pj.data[section]["colors"] == value


@pytest.mark.parametrize("section", ["dependencies", "devDependencies", "optionalDependencies"])
def test_workspace_dependencies_are_allowed_without_workspace_overrides(tmp_path, section):
    pj, _ = fixture(tmp_path, "catalogs: {}")
    pj.data.pop("dependencies")
    pj.data[section] = {"colors": "workspace:../replacement"}
    load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize("section", ["dependencies", "devDependencies", "optionalDependencies"])
def test_workspace_override_references_are_rejected_in_each_dependency_section(tmp_path, section):
    pj, _ = fixture(tmp_path, "catalogs: {}")
    pj.data.pop("dependencies")
    pj.data[section] = {"colors": "workspace:../replacement"}
    pj.data["pnpm"] = {"overrides": {"host>colors": "$colors"}}
    with pytest.raises(ValueError, match="overrides host>colors: workspace: протокол не поддерживается"):
        load_common_config(pj, str(tmp_path))


def test_local_override_replaces_common_workspace_override(tmp_path):
    pj, _ = fixture(tmp_path, "overrides: {colors: 'workspace:./replacement'}")
    pj.data["dependencies"] = {"colors": "workspace:../replacement"}
    pj.data["pnpm"] = {"overrides": {"colors": "file:../replacement"}}
    load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize("section", ["dependencies", "devDependencies", "optionalDependencies"])
def test_file_dependencies_add_tier0_peers(tmp_path, section):
    from build.plugins.lib.nots.package_manager.package_manager import PackageManager

    pj, _ = fixture(tmp_path, "catalogs: {}")
    pj.data.pop("dependencies")
    pj.data[section] = {
        "replacement": "file:../replacement",
        "archive": "file:../archive.tgz",
        "plain": "file:../plain",
    }
    pj.write()
    peer = tmp_path / "project/replacement"
    peer.mkdir()
    (peer / "ya.make").write_text("TS_PACKAGE()\nEND()")
    plain = tmp_path / "project/plain"
    plain.mkdir()
    (plain / "package.json").write_text('{"name":"plain"}')
    pm = PackageManager(
        str(tmp_path / "build"),
        str(tmp_path / "build/project/pkg"),
        str(Path(pj.path).parent),
        sources_root=str(tmp_path),
    )
    assert pm.get_local_peers_from_package_json() == ["project/replacement"]


def test_file_peer_dependencies_are_not_supported(tmp_path):
    pj, _ = fixture(tmp_path)
    pj.data["peerDependencies"] = {"peer": "file:../peer"}
    with pytest.raises(ValueError, match="peerDependencies peer: file: не поддерживается"):
        load_common_config(pj, str(tmp_path))


@pytest.mark.parametrize("section", ["dependencies", "devDependencies", "optionalDependencies", "overrides"])
def test_absolute_file_dependency_is_not_a_build_peer(tmp_path, section):
    from types import SimpleNamespace
    from build.plugins.lib.nots.package_manager.package_manager import PackageManager

    pj, _ = fixture(tmp_path)
    peer = tmp_path / "project/peer"
    peer.mkdir()
    (peer / "ya.make").write_text("TS_PACKAGE()\nEND()")
    (peer / "pnpm-workspace.yaml").write_text("packages: []\n")
    dependencies = {"peer": "file:" + str(peer)}
    if section == "overrides":
        pj.data["pnpm"] = {"overrides": dependencies}
    else:
        pj.data[section] = dependencies
    pm = SimpleNamespace(
        sources_path=str(tmp_path / "project/pkg"),
        sources_root=str(tmp_path),
        build_path=str(tmp_path / "build/project/pkg"),
        module_path="project/pkg",
        load_package_json_from_dir=lambda _: pj,
    )
    assert PackageManager.get_local_peers_from_package_json(pm) == []


@pytest.mark.parametrize("selector", ["foo@^1", "foo>bar@^1", "@scope/foo@^1>@scope/bar@^2"])
def test_file_override_selector_is_not_a_peer_name(tmp_path, selector):
    from types import SimpleNamespace
    from build.plugins.lib.nots.package_manager.package_manager import PackageManager

    pj, _ = fixture(tmp_path)
    pj.data["pnpm"] = {"overrides": {selector: "file:../replacement"}}
    peer = tmp_path / "project/replacement"
    peer.mkdir()
    (peer / "package.json").write_text(json.dumps({"name": "replacement"}))
    (peer / "ya.make").write_text("TS_PACKAGE()\nEND()")
    pm = SimpleNamespace(
        sources_path=str(tmp_path / "project/pkg"),
        sources_root=str(tmp_path),
        build_path=str(tmp_path / "build/project/pkg"),
        module_path="project/pkg",
        load_package_json_from_dir=lambda _: pj,
    )
    assert PackageManager.get_local_peers_from_package_json(pm) == ["project/replacement"]
    assert pj.data["pnpm"]["overrides"] == {selector: "file:../replacement"}
