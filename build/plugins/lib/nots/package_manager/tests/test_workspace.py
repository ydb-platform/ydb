from build.plugins.lib.nots.package_manager import PackageJson
from build.plugins.lib.nots.package_manager.pnpm_workspace import PnpmWorkspace


def test_workspace_get_paths():
    ws = PnpmWorkspace(path="/packages/foo/pnpm-workspace.yaml")
    ws.packages = set([".", "../bar", "../../another/baz"])

    assert sorted(ws.get_paths()) == [
        "/another/baz",
        "/packages/bar",
        "/packages/foo",
    ]


def test_workspace_get_paths_with_custom_base_path_without_self():
    ws = PnpmWorkspace(path="/packages/foo/pnpm-workspace.yaml")
    ws.packages = set([".", "../bar", "../../another/baz"])

    assert sorted(ws.get_paths(base_path="some/custom/dir", ignore_self=True)) == [
        "some/another/baz",
        "some/custom/bar",
    ]


def test_workspace_set_from_package_json():
    ws = PnpmWorkspace(path="/packages/foo/pnpm-workspace.yaml")
    pj = PackageJson(path="/packages/foo/package.json")
    pj.data = {
        "dependencies": {
            "@a/bar": "workspace:../bar",
        },
        "devDependencies": {
            "@a/baz": "workspace:../../another/baz",
        },
        "peerDependencies": {
            "@a/qux": "workspace:../../another/qux",
        },
        "optionalDependencies": {
            "@a/quux": "workspace:../../another/quux",
        },
    }

    ws.set_from_package_json(pj)

    assert sorted(ws.get_paths()) == [
        "/another/baz",
        "/another/quux",
        "/another/qux",
        "/packages/bar",
        "/packages/foo",
    ]


def test_workspace_set_from_package_json_writes_pnpm_11_settings(tmp_path):
    workspace_path = tmp_path / "pnpm-workspace.yaml"
    package_json = PackageJson(path=str(tmp_path / "package.json"))
    package_json.data = {
        "pnpm": {
            "overrides": {"foo": "1.0.0"},
            "packageExtensions": {"bar": {"peerDependencies": {"baz": "2.0.0"}}},
            "patchedDependencies": {"qux@3.0.0": "patches/qux.patch"},
            "peerDependencyRules": {"ignoreMissing": ["react"]},
            "onlyBuiltDependencies": ["esbuild", "sharp"],
            "neverBuiltDependencies": ["core-js"],
            "ignoredBuiltDependencies": ["sharp"],
            "allowNonAppliedPatches": True,
            "managePackageManagerVersions": False,
            "packageManagerStrict": False,
            "packageManagerStrictVersion": True,
        }
    }
    workspace = PnpmWorkspace(path=str(workspace_path))

    workspace.set_from_package_json(package_json)
    workspace.write()

    written_workspace = PnpmWorkspace.load(str(workspace_path))
    assert written_workspace.packages == {"."}
    assert written_workspace.settings == {
        "overrides": {"foo": "1.0.0"},
        "packageExtensions": {"bar": {"peerDependencies": {"baz": "2.0.0"}}},
        "patchedDependencies": {"qux@3.0.0": "patches/qux.patch"},
        "peerDependencyRules": {"ignoreMissing": ["react"]},
        "allowBuilds": {"esbuild": True, "sharp": False, "core-js": False},
        "allowUnusedPatches": True,
        "pmOnFail": "error",
    }


def test_workspace_drops_settings_without_automatic_pnpm_11_migration(tmp_path):
    package_json = PackageJson(path=str(tmp_path / "package.json"))
    package_json.data = {
        "pnpm": {
            "onlyBuiltDependenciesFile": "allowed-builds.json",
            "ignoreDepScripts": True,
            "ignorePatchFailures": True,
            "useNodeVersion": "22.0.0",
            "executionEnv": {"nodeVersion": "22.0.0"},
            "auditConfig": {"ignoreCves": ["CVE-2025-0001"]},
        }
    }
    workspace = PnpmWorkspace(path=str(tmp_path / "pnpm-workspace.yaml"))

    workspace.set_from_package_json(package_json)

    assert workspace.settings == {}


def test_workspace_read_write_preserves_settings(tmp_path):
    workspace_path = tmp_path / "pnpm-workspace.yaml"
    workspace_path.write_text("packages:\n  - .\noverrides:\n  foo: 1.0.0\n")

    workspace = PnpmWorkspace.load(str(workspace_path))
    workspace.packages.add("../bar")
    workspace.write()

    written_workspace = PnpmWorkspace.load(str(workspace_path))
    assert written_workspace.packages == {".", "../bar"}
    assert written_workspace.settings == {"overrides": {"foo": "1.0.0"}}


def test_workspace_merge():
    ws1 = PnpmWorkspace(path="/packages/foo/pnpm-workspace.yaml")
    ws1.packages = set([".", "../bar", "../../another/baz"])
    ws2 = PnpmWorkspace(path="/another/baz/pnpm-workspace.yaml")
    ws2.packages = set([".", "../qux"])

    ws1.merge(ws2)

    assert sorted(ws1.get_paths()) == [
        "/another/baz",
        "/another/qux",
        "/packages/bar",
        "/packages/foo",
    ]
