import json
import os

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


def test_get_workspace_deps_ok(monkeypatch):
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@yandex-int/bar": "workspace:../bar",
        },
        "devDependencies": {
            "@yandex-int/baz": "workspace:../baz",
        },
    }

    def load_mock(cls, path):
        p = PackageJson(path)
        p.data = {
            "name": "@yandex-int/{}".format(os.path.basename(os.path.dirname(path))),
        }
        return p

    monkeypatch.setattr(PackageJson, "load", classmethod(load_mock))

    ws_deps = pj.get_workspace_deps()

    assert len(ws_deps) == 2
    assert ws_deps[0].path == "/packages/bar/package.json"
    assert ws_deps[1].path == "/packages/baz/package.json"


def test_get_workspace_deps_with_wrong_name(monkeypatch):
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@yandex-int/bar": "workspace:../bar",
        },
    }

    def load_mock(cls, path):
        p = PackageJson(path)
        p.data = {
            "name": "@shouldbe/{}".format(os.path.basename(os.path.dirname(path))),
        }
        return p

    monkeypatch.setattr(PackageJson, "load", classmethod(load_mock))

    with pytest.raises(PackageJsonWorkspaceError) as e:
        pj.get_workspace_deps()

    assert (
        str(e.value)
        == "Workspace dependency name mismatch, found '@yandex-int/bar' instead of '@shouldbe/bar' in /packages/foo/package.json"
    )


def test_get_workspace_map_ok(monkeypatch):
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@yandex-int/bar": "workspace:../bar",
        },
    }

    def load_mock(cls, path):
        name = os.path.basename(os.path.dirname(path))
        p = PackageJson(path)
        p.data = {
            "name": "@yandex-int/{}".format(name),
            "dependencies": ({"@yandex-int/qux": "workspace:../qux"} if name == "bar" else {}),
        }
        return p

    monkeypatch.setattr(PackageJson, "load", classmethod(load_mock))

    ws_map = pj.get_workspace_map()

    assert len(ws_map) == 3
    assert ws_map["/packages/foo"][0].path == "/packages/foo/package.json"
    assert ws_map["/packages/foo"][1] == 0
    assert ws_map["/packages/bar"][0].path == "/packages/bar/package.json"
    assert ws_map["/packages/bar"][1] == 1
    assert ws_map["/packages/qux"][0].path == "/packages/qux/package.json"
    assert ws_map["/packages/qux"][1] == 2


def test_get_bin_path_string():
    # arrange
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "name": "next",
        "bin": "./dist/bin/next",
    }

    # act + assert
    assert pj.get_bin_path() == "./dist/bin/next"


def test_get_bin_path_same_name():
    # arrange
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "name": "next",
        "bin": {"next": "./dist/bin/next"},
    }

    # act + assert
    assert pj.get_bin_path() == "./dist/bin/next"


def test_get_bin_path_custom_bin_name():
    # arrange
    pj = PackageJson("/packages/foo/package.json")
    pj.data = {
        "name": "typescript",
        "bin": {"tsc": "./bin/tsc"},
    }

    # act + assert
    assert pj.get_bin_path("tsc") == "./bin/tsc"


@pytest.mark.parametrize("selector", ["foo", "foo@^1", "parent>foo@^1", "@scope/parent@^1>@scope/foo@^2"])
@pytest.mark.parametrize("marker", ["ya.make", "output.tar.uuid"])
def test_file_override_workspace_map_uses_manifest_names(tmp_path, selector, marker):
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
    assert [dep.get_name() for dep in pj.get_workspace_deps()] == ["replacement"]
    assert pj.get_dep_paths_by_names() == {"app": str(app), "replacement": str(peer), "leaf": str(leaf)}
    assert pj.get_workspace_map()[str(leaf)][1] == 2


@pytest.mark.parametrize("section", ["dependencies", "devDependencies", "optionalDependencies"])
def test_direct_file_peer_with_dollar_override_uses_manifest_name(tmp_path, section):
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
    assert pj.get_dep_paths_by_names() == {"app": str(app), "replacement": str(peer)}


@pytest.mark.parametrize("selector", ["foo@^1", "parent>foo@^1"])
def test_file_override_without_manifest_uses_directory_name(tmp_path, selector):
    app = tmp_path / "app"
    peer = tmp_path / "peer"
    app.mkdir()
    peer.mkdir()
    (peer / "ya.make").write_text("")
    (app / "package.json").write_text(json.dumps({"name": "app", "pnpm": {"overrides": {selector: "file:../peer"}}}))
    pj = PackageJson.load(str(app / "package.json"))
    assert pj.get_dep_paths_by_names() == {"app": str(app), "peer": str(peer)}


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
