import pytest
import json

from build.plugins.lib.nots.package_manager.lockfile import Lockfile, PnpmLockfileHelper
from build.plugins.lib.nots.package_manager import lockfile as lockfile_module


@pytest.mark.parametrize("version", ["9.0", 9.0])
def test_lockfile_read_v9(tmp_path, version):
    path = tmp_path / "pnpm-lock.yaml"
    data = {"lockfileVersion": version, "importers": {".": {}}}
    path.write_text(json.dumps(data))
    lf = Lockfile.load(str(path))
    assert lf.data == {"lockfileVersion": "9.0", "importers": {".": {}}}


@pytest.mark.parametrize("version", ["6.0", "8.0", "9.1", "10.0", 0, None])
def test_lockfile_rejects_unsupported_version(tmp_path, version):
    path = tmp_path / "pnpm-lock.yaml"
    path.write_text(json.dumps({"lockfileVersion": version}))
    with pytest.raises(ValueError) as exc:
        Lockfile.load(str(path))
    assert str(path) in str(exc.value)
    assert f"lockfileVersion: {version}" in str(exc.value)
    assert "Only lockfileVersion 9.0 is supported" in str(exc.value)
    assert "ya tool nots --clean update-lockfile" in str(exc.value)


@pytest.mark.parametrize("data", [[], {"packages": {}}, "some text"])
def test_lockfile_rejects_missing_version(tmp_path, data):
    path = tmp_path / "pnpm-lock.yaml"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="lockfileVersion: <no-version>"):
        Lockfile.load(str(path))


@pytest.mark.parametrize(
    "snapshot, package_id, name",
    [
        ("pkg@1.2.3", "pkg@1.2.3", "pkg"),
        ("@scope/pkg@1.2.3", "@scope/pkg@1.2.3", "@scope/pkg"),
        ("pkg@1.2.3(peer@4.5.6)", "pkg@1.2.3", "pkg"),
        ("@scope/pkg@1.2.3(@peer/a@2.0.0)(b@3.0.0(@peer/nested@4.0.0))", "@scope/pkg@1.2.3", "@scope/pkg"),
    ],
)
def test_v9_package_ids_keep_peer_contexts_separate(snapshot, package_id, name):
    assert PnpmLockfileHelper.package_id_from_snapshot_id(snapshot) == package_id
    assert PnpmLockfileHelper.package_name_from_package_id(package_id) == name


def test_get_importers_does_not_convert_legacy_root_dependencies():
    lf = Lockfile("/pnpm-lock.yaml")
    lf.data = {"lockfileVersion": "9.0", "dependencies": {"pkg": "1.0.0"}, "specifiers": {"pkg": "^1"}}
    assert lf.get_importers() == {}


def test_lockfile_get_packages_meta_ok():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "@babel/cli@7.6.2": {
                "resolution": {
                    "integrity": "sha512-JDZ+T/br9pPfT2lmAMJypJDTTTHM9ePD/ED10TRjRzJVdEVy+JB3iRlhzYmTt5YkNgHvxWGlUVnLtdv6ruiDrQ==",
                    "tarball": "@babel%2fcli/-/cli-7.6.2.tgz?rbtorrent=cb1849da3e4947e56a8f6bde6a1ec42703ddd187",
                },
            },
        },
    }

    packages = list(lf.get_packages_meta())
    pkg = packages[0]

    assert len(packages) == 1
    assert pkg.tarball_url == "@babel%2fcli/-/cli-7.6.2.tgz"
    assert pkg.tarball_path == "@babel/cli/-/cli-7.6.2.tgz"
    assert pkg.sky_id == "rbtorrent:cb1849da3e4947e56a8f6bde6a1ec42703ddd187"
    assert pkg.integrity == "JDZ+T/br9pPfT2lmAMJypJDTTTHM9ePD/ED10TRjRzJVdEVy+JB3iRlhzYmTt5YkNgHvxWGlUVnLtdv6ruiDrQ=="
    assert pkg.integrity_algorithm == "sha512"


def test_lockfile_get_packages_meta_scoped_tarball_with_literal_slash():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "@types/semver@7.7.1": {
                "resolution": {
                    "integrity": "sha512-FmgJfu+MOcQ370SD0ev7EI8TlCAfKYU+B4m5T3yXc1CiRN94g/SZPtsCkk506aUDtlMnFZvasDwHHUcZUEaYuA==",
                    "tarball": "https://npm.yandex-team.ru/@types/semver/-/semver-7.7.1.tgz",
                },
            },
        },
    }

    packages = list(lf.get_packages_meta())

    assert len(packages) == 1
    assert packages[0].tarball_path == "@types/semver/-/semver-7.7.1.tgz"


def test_lockfile_get_packages_meta_scoped_tarball_with_scope_in_filename():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "@yandex-lego/storybook-deploy@0.2.1": {
                "resolution": {
                    "integrity": "sha512-Z21GgVKGvxWmuBSo5d6OOMqYsYJaw5AMI63uPHJDmpxxrCmRy3VDWuA7Oi3IObaPofVVWZ/BO1op5m4qjRAEgw==",
                    "tarball": "https://npm.yandex-team.ru/@yandex-lego/storybook-deploy/-/@yandex-lego/storybook-deploy-0.2.1.tgz",
                },
            },
        },
    }

    packages = list(lf.get_packages_meta())

    assert len(packages) == 1
    assert packages[0].tarball_path == "@yandex-lego/storybook-deploy/-/storybook-deploy-0.2.1.tgz"
    assert packages[0].to_uri() == (
        "https://npm.yandex-team.ru/@yandex-lego/storybook-deploy/-/storybook-deploy-0.2.1.tgz"
        "#integrity=sha512-Z21GgVKGvxWmuBSo5d6OOMqYsYJaw5AMI63uPHJDmpxxrCmRy3VDWuA7Oi3IObaPofVVWZ/BO1op5m4qjRAEgw=="
    )


def test_lockfile_get_packages_skip_directory():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "@plus-int/auth@file:../../packages/platform/auth": {
                "resolution": {"directory": "../../packages/platform/auth", "type": "directory"}
            }
        },
    }

    assert len(list(lf.get_packages_meta())) == 0


def test_lockfile_get_packages_empty():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {}

    assert len(list(lf.get_packages_meta())) == 0


def test_package_meta_invalid_key():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "in/valid": {},
        },
    }

    with pytest.raises(TypeError) as e:
        list(lf.get_packages_meta())

    assert str(e.value) == "Invalid package meta for 'in/valid', missing 'resolution' key"


def test_package_meta_missing_resolution():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "valid@1.2.3": {},
        },
    }

    with pytest.raises(TypeError) as e:
        list(lf.get_packages_meta())

    assert str(e.value) == "Invalid package meta for 'valid@1.2.3', missing 'resolution' key"


def test_package_meta_missing_integrity_without_tarball():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "valid@1.2.3": {
                "resolution": {},
            },
        },
    }

    with pytest.raises(TypeError) as e:
        list(lf.get_packages_meta())

    assert str(e.value) == "Invalid package meta for 'valid@1.2.3', missing 'integrity' key"


@pytest.mark.parametrize(
    "key, path",
    [
        ("@emnapi/runtime@1.11.2", "@emnapi%2fruntime/-/runtime-1.11.2.tgz"),
        ("lodash@4.17.21", "lodash/-/lodash-4.17.21.tgz"),
        ("@scope.name/Foo_bar-baz@1.2.3", "@scope.name%2fFoo_bar-baz/-/Foo_bar-baz-1.2.3.tgz"),
        ("@scope~name/pkg!name@1.2.3", "@scope~name%2fpkg!name/-/pkg!name-1.2.3.tgz"),
        ("pkg@1.2.3-rc.1+build.2", "pkg/-/pkg-1.2.3-rc.1+build.2.tgz"),
        ("pkg@1.2.3(@scope/peer@4.5.6)", "pkg/-/pkg-1.2.3.tgz"),
    ],
)
def test_lockfile_infers_missing_tarball(key, path):
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {"packages": {key: {"resolution": {"integrity": "sha512-aGFzaA=="}}}}

    (pkg,) = lf.get_packages_meta()

    assert pkg.tarball_url == "https://npm.yandex-team.ru/" + path
    assert pkg.tarball_path == path.replace("%2f", "/")
    assert pkg.to_uri() == "https://npm.yandex-team.ru/" + path + "#integrity=sha512-aGFzaA=="
    assert pkg.sky_id == ""
    assert "tarball" not in lf.data["packages"][key]["resolution"]

    lf.update_tarball_resolutions(lambda meta: "file:__tarballs__/" + meta.tarball_path)

    assert lf.data["packages"][key]["resolution"]["tarball"] == "file:__tarballs__/" + path.replace("%2f", "/")


@pytest.mark.parametrize(
    "key",
    [
        "invalid",
        "pkg@file:../pkg",
        "pkg@https://example.org/pkg.tgz",
        "pkg@^1.2.3",
        ".@1.2.3",
        "..@1.2.3",
        "@scope/..@1.2.3",
    ],
)
def test_lockfile_does_not_infer_tarball_for_non_registry_keys(key):
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {"packages": {key: {"resolution": {"integrity": "sha512-aGFzaA=="}}}}

    with pytest.raises(TypeError, match="cannot infer tarball URL"):
        list(lf.get_packages_meta())


@pytest.mark.parametrize("name", ["foo{}bar", "@scope/foo{}bar", "@scope{}name/foo"])
@pytest.mark.parametrize("character", ["?", "#", "%", " ", "\\", "\t", "\n", "é"])
def test_lockfile_rejects_unsafe_package_names(name, character):
    key = name.format(character) + "@1.2.3"
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {"packages": {key: {"resolution": {"integrity": "sha512-aGFzaA=="}}}}

    with pytest.raises(TypeError, match="cannot infer tarball URL"):
        list(lf.get_packages_meta())
    with pytest.raises(TypeError, match="cannot infer tarball URL"):
        lf.update_tarball_resolutions(lambda meta: "file:__tarballs__/" + meta.tarball_path)
    assert "tarball" not in lf.data["packages"][key]["resolution"]


@pytest.mark.parametrize(
    "version",
    [
        "01.2.3",
        "1.02.3",
        "1.2.03",
        "1.2.3-..",
        "1.2.3-alpha..1",
        "1.2.3-.alpha",
        "1.2.3-alpha.",
        "1.2.3-01",
        "1.2.3-alpha.01",
        "1.2.3-",
        "1.2.3+",
        "1.2.3+build..2",
        "1.2.3+.build",
        "1.2.3+build.",
    ],
)
def test_lockfile_rejects_invalid_fixed_semver(version):
    key = "pkg@" + version
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {"packages": {key: {"resolution": {"integrity": "sha512-aGFzaA=="}}}}

    with pytest.raises(TypeError, match="cannot infer tarball URL"):
        list(lf.get_packages_meta())
    with pytest.raises(TypeError, match="cannot infer tarball URL"):
        lf.update_tarball_resolutions(lambda meta: "file:__tarballs__/" + meta.tarball_path)
    assert "tarball" not in lf.data["packages"][key]["resolution"]


@pytest.mark.parametrize(
    "version",
    ["0.0.0", "1.2.3-0", "1.2.3-alpha.0", "1.2.3-01a", "1.2.3-x-y-z.--", "1.2.3+build.001", "1.2.3-0.alpha+001.sha-2"],
)
def test_lockfile_infers_tarball_for_valid_fixed_semver(version):
    key = "pkg@" + version
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {"packages": {key: {"resolution": {"integrity": "sha512-aGFzaA=="}}}}

    (pkg,) = lf.get_packages_meta()

    assert pkg.tarball_url == "https://npm.yandex-team.ru/pkg/-/pkg-" + version + ".tgz"
    assert pkg.tarball_path == "pkg/-/pkg-" + version + ".tgz"


def test_package_meta_missing_rbtorrent():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "valid@1.2.3": {
                "resolution": {
                    "integrity": "sha512-JDZ+T/br9pPfT2lmAMJypJDTTTHM9ePD/ED10TRjRzJVdEVy+JB3iRlhzYmTt5YkNgHvxWGlUVnLtdv6ruiDrQ==",
                    "tarball": "valid-without-rbtorrent-1.2.3.tgz",
                },
            },
        },
    }

    packages = list(lf.get_packages_meta())
    pkg = packages[0]

    assert len(packages) == 1
    assert pkg.sky_id == ""


def test_lockfile_meta_file_tarball_prohibits_file_protocol():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "@babel/cli@7.6.2": {
                "resolution": {
                    "integrity": "sha512-JDZ+T/br9pPfT2lmAMJypJDTTTHM9ePD/ED10TRjRzJVdEVy+JB3iRlhzYmTt5YkNgHvxWGlUVnLtdv6ruiDrQ==",
                    "tarball": "file:/some/abs/path.tgz",
                },
            },
        },
    }

    with pytest.raises(TypeError) as e:
        list(lf.get_packages_meta())

    assert (
        str(e.value)
        == "Invalid package meta for '@babel/cli@7.6.2', parse error: tarball cannot point to a file, got 'file:/some/abs/path.tgz'"
    )


def test_lockfile_update_tarball_resolutions_ok():
    lf = Lockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "@babel/cli@7.6.2_@babel+core@7.6.2": {
                "resolution": {
                    "integrity": "sha512-JDZ+T/br9pPfT2lmAMJypJDTTTHM9ePD/ED10TRjRzJVdEVy+JB3iRlhzYmTt5YkNgHvxWGlUVnLtdv6ruiDrQ==",
                    "tarball": "@babel%2fcli/-/cli-7.6.2.tgz?rbtorrent=cb1849da3e4947e56a8f6bde6a1ec42703ddd187",
                },
            },
        },
    }

    lf.update_tarball_resolutions(lambda p: p.tarball_url)

    assert (
        lf.data["packages"]["@babel/cli@7.6.2_@babel+core@7.6.2"]["resolution"]["tarball"]
        == "@babel%2fcli/-/cli-7.6.2.tgz"
    )


@pytest.mark.parametrize("content", [None, "", " \n\t\n", "# no dependencies\n", "{}\n", "null\n"])
def test_lockfile_accepts_missing_or_empty_file(tmp_path, content):
    path = tmp_path / "pnpm-lock.yaml"
    if content is not None:
        path.write_text(content)

    lockfile = Lockfile.load(str(path))

    assert lockfile.data == {"lockfileVersion": "9.0"}
    assert lockfile.get_importers() == {}
    assert list(lockfile.get_packages_meta()) == []
    if content is None:
        assert not path.exists()
    else:
        assert path.read_text() == content


def test_lockfile_read_with_file_only_yaml_loader(tmp_path, monkeypatch):
    # The ymake YAML backend treats bytes/strings as filenames, unlike PyYAML.
    path = tmp_path / "pnpm-lock.yaml"
    path.write_text("lockfileVersion: '9.0'\nimporters:\n  .: {}\n")
    original_load = lockfile_module.yaml.load

    def load_file(stream, **kwargs):
        assert hasattr(stream, "read")
        assert stream.tell() == 0
        return original_load(stream, **kwargs)

    monkeypatch.setattr(lockfile_module.yaml, "load", load_file)

    assert Lockfile.load(str(path)).data == {"lockfileVersion": "9.0", "importers": {".": {}}}
