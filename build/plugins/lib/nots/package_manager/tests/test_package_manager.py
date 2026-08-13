import importlib
import os

package_manager_module = importlib.import_module("build.plugins.lib.nots.package_manager.package_manager")


def _package_manager(monkeypatch):
    package_manager = object.__new__(package_manager_module.PackageManager)
    commands = []
    package_manager._exec_command = lambda args, cwd: commands.append((args, cwd))
    monkeypatch.setattr(package_manager_module, "sync_mutex_file", lambda path: lambda function: function)
    return package_manager, commands


def test_pnpm_install_copies_external_node_modules_across_filesystems(monkeypatch, tmp_path):
    package_manager, commands = _package_manager(monkeypatch)
    cwd = str(tmp_path / "build" / "project" / "module")
    node_modules_path = str(tmp_path / "ram" / "project" / "module" / "node_modules")
    virtual_store_dir = os.path.join(node_modules_path, ".pnpm")
    os.makedirs(tmp_path / "build" / "store")
    os.makedirs(os.path.dirname(node_modules_path))
    monkeypatch.setattr(package_manager_module, "_same_filesystem", lambda source, destination: False)

    package_manager._run_pnpm_install(
        str(tmp_path / "build" / "store"),
        cwd,
        False,
        virtual_store_dir,
        True,
        node_modules_path,
    )

    command, actual_cwd = commands[0]
    assert actual_cwd == cwd
    assert command[command.index("--modules-dir") + 1] == os.path.relpath(node_modules_path, cwd)
    assert command[command.index("--virtual-store-dir") + 1] == virtual_store_dir
    assert command[command.index("--package-import-method") + 1] == "copy"


def test_pnpm_install_hardlinks_external_node_modules_on_same_filesystem(monkeypatch, tmp_path):
    package_manager, commands = _package_manager(monkeypatch)
    cwd = str(tmp_path / "build" / "project" / "module")
    store_dir = str(tmp_path / "ram" / "pnpm-ca-store")
    node_modules_path = str(tmp_path / "ram" / "project" / "module" / "node_modules")
    virtual_store_dir = os.path.join(node_modules_path, ".pnpm")
    os.makedirs(store_dir)
    os.makedirs(os.path.dirname(node_modules_path))

    package_manager._run_pnpm_install(
        store_dir,
        cwd,
        False,
        virtual_store_dir,
        True,
        node_modules_path,
    )

    command, actual_cwd = commands[0]
    assert actual_cwd == cwd
    assert command[command.index("--modules-dir") + 1] == os.path.relpath(node_modules_path, cwd)
    assert command[command.index("--package-import-method") + 1] == "hardlink"


def test_pnpm_install_preserves_legacy_node_modules_layout(monkeypatch, tmp_path):
    package_manager, commands = _package_manager(monkeypatch)
    cwd = str(tmp_path / "build" / "project" / "module")
    node_modules_path = os.path.join(cwd, "node_modules")
    virtual_store_dir = os.path.join(node_modules_path, ".pnpm")
    os.makedirs(tmp_path / "build" / "store")
    os.makedirs(cwd)

    package_manager._run_pnpm_install(
        str(tmp_path / "build" / "store"),
        cwd,
        False,
        virtual_store_dir,
        True,
        node_modules_path,
    )

    command, actual_cwd = commands[0]
    assert actual_cwd == cwd
    assert "--modules-dir" not in command
    assert command[command.index("--virtual-store-dir") + 1] == virtual_store_dir
    assert command[command.index("--package-import-method") + 1] == "hardlink"


def test_prepare_deps_publishes_package_json(tmp_path):
    package_manager = object.__new__(package_manager_module.PackageManager)
    package_manager.module_path = "project/module"
    package_manager.sources_path = str(tmp_path / "project" / "module")
    package_manager.inject_peers = False

    inputs, outputs, _ = package_manager.calc_prepare_deps_inouts_and_resources(
        store_path="__tarballs__",
        has_deps=False,
        local_cli=True,
    )

    assert "$S/project/module/pnpm-lock.yaml" not in inputs
    assert "$B/project/module/package.json" in outputs


def test_build_package_json_preserves_source_bytes(tmp_path):
    source_path = tmp_path / "source"
    build_path = tmp_path / "build"
    source_path.mkdir()
    package_json_content = '{\n\t"name": "keep-original-formatting",\n\t"version": "1.0.0",\n\t"private": true\n}\n'
    (source_path / "package.json").write_text(package_json_content)

    package_manager = object.__new__(package_manager_module.PackageManager)
    package_manager.sources_path = str(source_path)
    package_manager.build_path = str(build_path)
    package_manager.module_path = "project/module"
    package_manager.load_package_json = lambda path: _load_package_json(path)

    package_json = package_manager._build_package_json()

    assert (build_path / "package.json").read_text() == package_json_content
    assert package_json.path == str(build_path / "package.json")
    assert package_json.data["name"] == "keep-original-formatting"


def test_build_package_json_adds_missing_pack_metadata(tmp_path):
    source_path = tmp_path / "source"
    build_path = tmp_path / "build"
    source_path.mkdir()
    (source_path / "package.json").write_text('{"private": true, "files": ["build/"]}\n')

    package_manager = object.__new__(package_manager_module.PackageManager)
    package_manager.sources_path = str(source_path)
    package_manager.build_path = str(build_path)
    package_manager.module_path = "project/module"
    package_manager.load_package_json = lambda path: _load_package_json(path)

    package_json = package_manager._build_package_json()

    assert package_json.path == str(build_path / "package.json")
    assert package_json.data == {
        "private": True,
        "files": ["build/"],
        "name": "@project/module",
        "version": "0.0.0",
    }


def test_build_workspace_without_lockfile(tmp_path):
    package_manager = object.__new__(package_manager_module.PackageManager)
    package_manager.sources_path = str(tmp_path / "source")
    package_manager.build_path = str(tmp_path / "build")
    package_manager.module_path = "project/module"
    package_manager.inject_peers = False

    os.makedirs(package_manager.sources_path)
    os.makedirs(package_manager.build_path)
    with open(os.path.join(package_manager.sources_path, "package.json"), "w") as f:
        f.write("{}")

    package_manager.build_workspace(tarballs_store="__tarballs__", local_cli=True)

    lockfile = package_manager.load_lockfile(os.path.join(package_manager.build_path, "pnpm-lock.yaml"))
    assert lockfile.data["lockfileVersion"] == "9.0"


def test_build_workspace_merges_transitive_workspace_lockfiles(tmp_path):
    source_path = tmp_path / "source" / "consumer"
    build_path = tmp_path / "build" / "consumer"
    reporter_path = tmp_path / "build" / "reporter"
    ci_reporter_path = tmp_path / "build" / "ci-reporter"
    source_path.mkdir(parents=True)
    reporter_path.mkdir(parents=True)
    ci_reporter_path.mkdir(parents=True)

    (source_path / "package.json").write_text(
        '{"dependencies":{"reporter":"workspace:../reporter"}}\n'
    )
    source_lockfile = package_manager_module.Lockfile(str(source_path / "pnpm-lock.yaml"))
    source_lockfile.data = {
        "lockfileVersion": "9.0",
        "importers": {
            ".": {
                "dependencies": {
                    "reporter": {"specifier": "workspace:../reporter", "version": "link:../reporter"}
                }
            }
        },
    }
    source_lockfile.write()

    reporter_workspace = package_manager_module.PnpmWorkspace(str(reporter_path / "pnpm-workspace.yaml"))
    reporter_workspace.packages = {".", "../ci-reporter"}
    reporter_workspace.write()

    reporter_lockfile = package_manager_module.Lockfile(str(reporter_path / "pnpm-lock.yaml"))
    reporter_lockfile.data = {
        "lockfileVersion": "9.0",
        "importers": {".": {"dependencies": {"ci-reporter": {"specifier": "workspace:../ci-reporter"}}}},
    }
    reporter_lockfile.write()

    ci_reporter_lockfile = package_manager_module.Lockfile(str(ci_reporter_path / "pnpm-lock.yaml"))
    ci_reporter_lockfile.data = {
        "lockfileVersion": "9.0",
        "importers": {
            ".": {"devDependencies": {"typescript": {"specifier": "5.9.3", "version": "5.9.3"}}}
        },
    }
    ci_reporter_lockfile.write()

    package_manager = object.__new__(package_manager_module.PackageManager)
    package_manager.sources_path = str(source_path)
    package_manager.build_path = str(build_path)
    package_manager.module_path = "consumer"
    package_manager.inject_peers = False

    package_manager.build_workspace(tarballs_store="__tarballs__", local_cli=True)

    lockfile = package_manager.load_lockfile(str(build_path / "pnpm-lock.yaml"))
    assert set(lockfile.get_importers()) == {".", "../reporter", "../ci-reporter"}


def test_rebase_file_tarball_resolutions(tmp_path):
    source_dir = tmp_path / "source" / "module"
    target_dir = tmp_path / "target" / "module"
    lockfile = package_manager_module.Lockfile(str(source_dir / "pnpm-lock.yaml"))
    lockfile.data = {
        "lockfileVersion": "9.0",
        "packages": {
            "pkg@1.0.0": {
                "resolution": {
                    "integrity": "sha512-YQ==",
                    "tarball": "file:__tarballs__/pkg/-/pkg-1.0.0.tgz",
                }
            }
        },
    }

    package_manager_module.PackageManager._rebase_file_tarball_resolutions(lockfile, str(target_dir))

    tarball = lockfile.data["packages"]["pkg@1.0.0"]["resolution"]["tarball"]
    assert tarball == "file:../../source/module/__tarballs__/pkg/-/pkg-1.0.0.tgz"


def _load_package_json(path):
    package_json = package_manager_module.PackageJson(path)
    package_json.read()
    return package_json
