import json

import pytest

from build.plugins.lib.nots.package_manager import PackageJson, PackageJsonWorkspaceError


def test_get_name_exist():
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "name": "package-name",
    }

    name = pj.get_name()

    assert name == "package-name"


def test_get_name_none():
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {}

    name = pj.get_name()

    assert name == "packages-foo"


def test_get_workspace_dep_spec_paths_ok():
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@yandex-int/bar": "workspace:../bar",
        },
        "devDependencies": {
            "@yandex-int/baz": "workspace:../baz",
        },
    }

    ws_dep_spec_paths = pj.get_workspace_dep_spec_paths()

    assert ws_dep_spec_paths == [
        ("@yandex-int/bar", "../bar"),
        ("@yandex-int/baz", "../baz"),
    ]


def test_get_workspace_dep_spec_paths_invalid_path():
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@yandex-int/bar": "workspace:*",
        },
    }

    with pytest.raises(PackageJsonWorkspaceError) as e:
        pj.get_workspace_dep_spec_paths()

    assert (
        str(e.value)
        == "Expected relative path specifier for workspace dependency, but got 'workspace:*' for @yandex-int/bar in /packages/foo/package.json"
    )


def test_get_workspace_dep_paths_ok():
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@yandex-int/bar": "workspace:../bar",
        },
        "devDependencies": {
            "@yandex-int/baz": "workspace:../baz",
        },
    }

    ws_dep_paths = pj.get_workspace_dep_paths()

    assert ws_dep_paths == [
        "/packages/bar",
        "/packages/baz",
    ]


def test_get_dep_specifier():
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "jestify": "0.0.1",
            "eslint": ">= 7.27.0",
        },
        "devDependencies": {
            "jest": "27.1.0",
            "eslinting": "0.0.2",
        },
    }

    jest_spec = pj.get_dep_specifier("jest")
    assert jest_spec == "27.1.0", "Got unexpected jest specifier: {}".format(jest_spec)

    eslint_spec = pj.get_dep_specifier("eslint")
    assert eslint_spec == ">= 7.27.0", "Got unexpected eslint specifier: {}".format(eslint_spec)


def test_get_workspace_dep_paths_with_custom_base_path():
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@yandex-int/bar": "workspace:../bar",
        },
        "devDependencies": {
            "@yandex-int/baz": "workspace:../baz",
        },
    }

    ws_dep_paths = pj.get_workspace_dep_paths(base_path="custom/dir")

    assert ws_dep_paths == [
        "custom/bar",
        "custom/baz",
    ]


@pytest.mark.parametrize("selector", ["foo", "foo@^1", "parent>foo@^1", "@scope/parent@^1>@scope/foo@^2"])
@pytest.mark.parametrize("marker", ["ya.make", "output.tar.uuid"])
def test_file_override_peer_paths(tmp_path, selector, marker):
    app = tmp_path / "app"
    peer = tmp_path / "peer"
    leaf = tmp_path / "leaf"
    for directory in (app, peer, leaf):
        directory.mkdir()
    (peer / marker).write_text("")
    (leaf / marker).write_text("")
    (app / "package.json").write_text(json.dumps({"name": "app", "pnpm": {"overrides": {selector: "file:../peer"}}}))
    (peer / "package.json").write_text(
        json.dumps({"name": "replacement", "dependencies": {"leaf-alias": "file:../leaf"}})
    )
    (leaf / "package.json").write_text(json.dumps({"name": "leaf"}))
    pj = PackageJson.load(str(app / "package.json"))

    assert pj.get_workspace_dep_paths() == [str(peer)]


@pytest.mark.parametrize("section", ["dependencies", "devDependencies", "optionalDependencies"])
def test_direct_file_peer_with_dollar_override_keeps_path(tmp_path, section):
    app = tmp_path / "app"
    peer = tmp_path / "peer"
    app.mkdir()
    peer.mkdir()
    (peer / "ya.make").write_text("")
    (peer / "package.json").write_text(json.dumps({"name": "replacement"}))
    (app / "package.json").write_text(
        json.dumps({"name": "app", section: {"alias": "file:../peer"}, "pnpm": {"overrides": {"alias": "$alias"}}})
    )
    pj = PackageJson.load(str(app / "package.json"))
    assert pj.get_workspace_dep_paths() == [str(peer)]


@pytest.mark.parametrize("selector", ["foo@^1", "parent>foo@^1"])
def test_file_override_without_manifest_keeps_path(tmp_path, selector):
    app = tmp_path / "app"
    peer = tmp_path / "peer"
    app.mkdir()
    peer.mkdir()
    (peer / "ya.make").write_text("")
    (app / "package.json").write_text(json.dumps({"name": "app", "pnpm": {"overrides": {selector: "file:../peer"}}}))
    pj = PackageJson.load(str(app / "package.json"))
    assert pj.get_workspace_dep_paths() == [str(peer)]


@pytest.mark.parametrize("indent", ["  ", "    ", "\t"])
def test_write_preserves_indent(tmp_path, indent):
    source = tmp_path / "package.json"
    data = {"name": "example", "engines": {"pnpm": "10"}}
    original = json.dumps(data, indent=indent) + "\n"
    source.write_text(original)
    package_json = PackageJson.load(str(source))
    package_json.data["engines"]["pnpm"] = "11.27.0"
    output = tmp_path / "generated.json"
    package_json.write(str(output))
    assert output.read_text() == original.replace('"10"', '"11.27.0"')
    assert source.read_text() == original


def test_read_redetects_indent(tmp_path):
    source = tmp_path / "package.json"
    package_json = PackageJson(str(source))
    for indent in ("\t", "    ", None):
        source.write_text(json.dumps({"name": "example"}, indent=indent))
        package_json.read()
        package_json.write()
        assert source.read_text() == json.dumps({"name": "example"}, indent=indent or "  ") + "\n"


@pytest.mark.parametrize(
    "flags, local, common, missing",
    [
        ({"native@1.0.0": True}, {"native@1.0.0": "npm:@fat/native@1.0.0"}, {}, []),
        ({"native@1.0.0": True}, {}, {"native@1.0.0": "npm:@fat/native@1.0.0"}, []),
        ({"native@1.0.0": False}, {}, {}, []),
        ({"native@1.0.0": True, "other@2.0.0": False}, {}, {}, ["native@1.0.0"]),
    ],
)
def test_prebuilder_requires_overrides_for_enabled_prebuilds(flags, local, common, missing):
    pj = PackageJson("/package.json")
    pj.data = {"@yatool/prebuilder": {"usePrebuild": flags}, "pnpm": {"overrides": local}}
    before = dict(common)
    valid, messages = pj.validate_prebuilds(common)
    assert valid == (not missing)
    assert messages == (
        ["These packages have addons but overrides are not set:"] + ["  - " + key for key in missing] if missing else []
    )
    assert common == before
