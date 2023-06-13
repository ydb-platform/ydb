import os
import pytest

from build.plugins.lib.nots.package_manager.base.package_json import PackageJson, PackageJsonWorkspaceError


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


def test_get_workspace_deps_ok():
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

    PackageJson.load = classmethod(load_mock)

    ws_deps = pj.get_workspace_deps()

    assert len(ws_deps) == 2
    assert ws_deps[0].path == "/packages/bar/package.json"
    assert ws_deps[1].path == "/packages/baz/package.json"


def test_get_workspace_deps_with_wrong_name():
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

    PackageJson.load = classmethod(load_mock)

    with pytest.raises(PackageJsonWorkspaceError) as e:
        pj.get_workspace_deps()

    assert (
        str(e.value)
        == "Workspace dependency name mismatch, found '@yandex-int/bar' instead of '@shouldbe/bar' in /packages/foo/package.json"
    )


def test_get_workspace_map_ok():
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

    PackageJson.load = classmethod(load_mock)

    ws_map = pj.get_workspace_map()

    assert len(ws_map) == 3
    assert ws_map["/packages/foo"][0].path == "/packages/foo/package.json"
    assert ws_map["/packages/foo"][1] == 0
    assert ws_map["/packages/bar"][0].path == "/packages/bar/package.json"
    assert ws_map["/packages/bar"][1] == 1
    assert ws_map["/packages/qux"][0].path == "/packages/qux/package.json"
    assert ws_map["/packages/qux"][1] == 2
