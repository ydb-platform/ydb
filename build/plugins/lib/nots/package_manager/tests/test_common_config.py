import json
from pathlib import Path

import pytest

from build.plugins.lib.nots.package_manager.common_config import load_common_config
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


def test_local_peer_settings_ignore_unknown_fields(tmp_path):
    from build.plugins.lib.nots.package_manager.common_config import select_pnpm_settings

    assert select_pnpm_settings({"onlyBuiltDependencies": 12, "futureOption": []}) == {}


def test_scalar_types_come_from_shared_policy(tmp_path, monkeypatch):
    from build.plugins.lib.nots.package_manager import common_config

    schema = dict(common_config.pnpm_setting_types())
    schema.update(futureBoolean="boolean", futureInteger="non-negative-integer", futureList="string-list")
    monkeypatch.setattr(common_config, "pnpm_setting_types", lambda: schema)
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


def test_supported_architectures_and_deprecation_exceptions(tmp_path):
    from build.plugins.lib.nots.package_manager.common_config import join_pnpm_settings, select_pnpm_settings

    pj, _ = fixture(
        tmp_path,
        "catalogs: {project/common: {colors: 1.4.0}}\n"
        "supportedArchitectures: {os: [current, linux], cpu: [x64]}\n"
        "allowedDeprecatedVersions: {old: '1', common: '*'}",
    )
    common = load_common_config(pj, str(tmp_path), include_settings=True)[2]
    target = {
        "supportedArchitectures": {"os": ["linux", "darwin"], "cpu": ["arm64"], "libc": ["musl"]},
        "allowedDeprecatedVersions": {"old": "2"},
    }
    merged = join_pnpm_settings(common, select_pnpm_settings(target))
    assert merged == {
        "supportedArchitectures": {
            "os": ["current", "linux", "darwin"],
            "cpu": ["x64", "arm64"],
            "libc": ["musl"],
        },
        "allowedDeprecatedVersions": {"old": "2", "common": "*"},
    }
    merged["supportedArchitectures"]["os"].append("win32")
    assert common["supportedArchitectures"]["os"] == ["current", "linux"]
    assert target["supportedArchitectures"]["os"] == ["linux", "darwin"]
    assert select_pnpm_settings({"supportedArchitectures": {}}) == {"supportedArchitectures": {}}


def test_configuration_leaves_semantic_validation_to_cli_prepare(tmp_path):
    pj, _ = fixture(tmp_path, "catalogs: {wrong: {colors: latest}}\nforceLegacyDeploy: invalid")
    pj.data["peerDependencies"] = {"peer": "file:../peer"}
    pj.data["pnpm"] = {"overrides": {"colors": "workspace:*"}}
    _, catalogs, settings = load_common_config(pj, str(tmp_path), include_settings=True)
    assert catalogs == {"wrong": {"colors": "latest"}}
    assert settings == {"forceLegacyDeploy": "invalid"}


def test_tier0_policy_is_read_from_selected_sources_root(tmp_path, monkeypatch):
    from build.plugins.lib.nots.package_manager import common_config

    for name, registry in (("first", "https://first.test"), ("second", "https://second.test")):
        root = tmp_path / name
        policy = root / common_config.T0_SETTINGS_PATH
        policy.parent.mkdir(parents=True)
        policy.write_text("pnpm:\n  defaults:\n    registry: " + registry + "\n")
        monkeypatch.setattr(common_config, "_tier0_sources_root", str(root))
        assert common_config.load_tier0_settings()["pnpm"]["defaults"]["registry"] == registry
