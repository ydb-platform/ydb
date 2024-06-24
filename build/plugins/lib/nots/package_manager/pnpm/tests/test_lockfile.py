import pytest
import io

from build.plugins.lib.nots.package_manager.pnpm.lockfile import PnpmLockfile


@pytest.fixture()
def patch_open_correct_version(monkeypatch):
    def mock_open(a, b):
        file_like = io.BytesIO(b'lockfileVersion: 5.4')
        return io.BufferedReader(file_like)

    monkeypatch.setattr(io, "open", mock_open)


@pytest.fixture()
def patch_open_v6(monkeypatch):
    def mock_open(a, b):
        file_like = io.BytesIO(b'lockfileVersion: "6.0"')
        return io.BufferedReader(file_like)

    monkeypatch.setattr(io, "open", mock_open)


@pytest.fixture()
def patch_open_incorrect_version(monkeypatch):
    def mock_open(a, b):
        file_like = io.BytesIO(b'lockfileVersion: 0')
        return io.BufferedReader(file_like)

    monkeypatch.setattr(io, "open", mock_open)


@pytest.fixture()
def patch_open_no_version(monkeypatch):
    def mock_open(a, b):
        file_like = io.BytesIO(b'some text')
        return io.BufferedReader(file_like)

    monkeypatch.setattr(io, "open", mock_open)


def test_lockfile_read_yaml_ok(patch_open_correct_version):
    lf = PnpmLockfile(path="/pnpm-lock.yaml")

    lf.read()

    assert lf.data == {"lockfileVersion": 5.4}


def test_lockfile_read_v6(patch_open_v6):
    lf = PnpmLockfile(path="/pnpm-lock.yaml")

    lf.read()

    assert lf.data == {"lockfileVersion": '6.0'}


def test_lockfile_read_yaml_error_incorrect_lockfile_version(patch_open_incorrect_version):
    lf = PnpmLockfile(path="/pnpm-lock.yaml")

    with pytest.raises(Exception) as e:
        lf.read()

    assert str(e.value) == (
        'Error of project configuration: /pnpm-lock.yaml has lockfileVersion: 0. '
        + 'This version is not supported. Please, delete pnpm-lock.yaml and regenerate it using "ya tool nots --clean update-lockfile"'
    )


def test_lockfile_read_yaml_error_no_lockfile_version(patch_open_no_version):
    lf = PnpmLockfile(path="/pnpm-lock.yaml")

    with pytest.raises(Exception) as e:
        lf.read()

    assert str(e.value) == (
        'Error of project configuration: /pnpm-lock.yaml has lockfileVersion: <no-version>. '
        + 'This version is not supported. Please, delete pnpm-lock.yaml and regenerate it using "ya tool nots --clean update-lockfile"'
    )


def test_lockfile_get_packages_meta_ok():
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "/@babel/cli/7.6.2_@babel+core@7.6.2": {
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
    assert pkg.sky_id == "rbtorrent:cb1849da3e4947e56a8f6bde6a1ec42703ddd187"
    assert pkg.integrity == "JDZ+T/br9pPfT2lmAMJypJDTTTHM9ePD/ED10TRjRzJVdEVy+JB3iRlhzYmTt5YkNgHvxWGlUVnLtdv6ruiDrQ=="
    assert pkg.integrity_algorithm == "sha512"


def test_lockfile_get_packages_empty():
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {}

    assert len(list(lf.get_packages_meta())) == 0


def test_package_meta_invalid_key():
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "in/valid": {},
        },
    }

    with pytest.raises(TypeError) as e:
        list(lf.get_packages_meta())

    assert str(e.value) == "Invalid package meta for key in/valid, missing 'resolution' key"


def test_package_meta_missing_resolution():
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "/valid/1.2.3": {},
        },
    }

    with pytest.raises(TypeError) as e:
        list(lf.get_packages_meta())

    assert str(e.value) == "Invalid package meta for key /valid/1.2.3, missing 'resolution' key"


def test_package_meta_missing_tarball():
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "/valid/1.2.3": {
                "resolution": {},
            },
        },
    }

    with pytest.raises(TypeError) as e:
        list(lf.get_packages_meta())

    assert str(e.value) == "Invalid package meta for key /valid/1.2.3, missing 'tarball' key"


def test_package_meta_missing_rbtorrent():
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "/valid/1.2.3": {
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
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "/@babel/cli/7.6.2": {
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
        == "Invalid package meta for key /@babel/cli/7.6.2, parse error: tarball cannot point to a file, got file:/some/abs/path.tgz"
    )


def test_lockfile_update_tarball_resolutions_ok():
    lf = PnpmLockfile(path="/pnpm-lock.yaml")
    lf.data = {
        "packages": {
            "/@babel/cli/7.6.2_@babel+core@7.6.2": {
                "resolution": {
                    "integrity": "sha512-JDZ+T/br9pPfT2lmAMJypJDTTTHM9ePD/ED10TRjRzJVdEVy+JB3iRlhzYmTt5YkNgHvxWGlUVnLtdv6ruiDrQ==",
                    "tarball": "@babel%2fcli/-/cli-7.6.2.tgz?rbtorrent=cb1849da3e4947e56a8f6bde6a1ec42703ddd187",
                },
            },
        },
    }

    lf.update_tarball_resolutions(lambda p: p.tarball_url)

    assert (
        lf.data["packages"]["/@babel/cli/7.6.2_@babel+core@7.6.2"]["resolution"]["tarball"]
        == "@babel%2fcli/-/cli-7.6.2.tgz"
    )


def test_lockfile_merge():
    lf1 = PnpmLockfile(path="/foo/pnpm-lock.yaml")
    lf1.data = {
        "dependencies": {
            "a": "1.0.0",
        },
        "specifiers": {
            "a": "1.0.0",
        },
        "packages": {
            "/a/1.0.0": {},
        },
    }

    lf2 = PnpmLockfile(path="/bar/pnpm-lock.yaml")
    lf2.data = {
        "dependencies": {
            "b": "1.0.0",
        },
        "specifiers": {
            "b": "1.0.0",
        },
        "packages": {
            "/b/1.0.0": {},
        },
    }

    lf3 = PnpmLockfile(path="/another/baz/pnpm-lock.yaml")
    lf3.data = {
        "importers": {
            ".": {
                "dependencies": {
                    "@a/qux": "link:../qux",
                    "a": "1.0.0",
                },
                "specifiers": {
                    "@a/qux": "workspace:../qux",
                    "a": "1.0.0",
                },
            },
            "../qux": {
                "dependencies": {
                    "b": "1.0.1",
                },
                "specifiers": {
                    "b": "1.0.1",
                },
            },
        },
        "packages": {
            "/a/1.0.0": {},
            "/b/1.0.1": {},
        },
    }

    lf4 = PnpmLockfile(path="/another/quux/pnpm-lock.yaml")
    lf4.data = {
        "dependencies": {
            "@a/bar": "link:../../bar",
        },
        "specifiers": {
            "@a/bar": "workspace:../../bar",
        },
    }

    lf1.merge(lf2)
    lf1.merge(lf3)
    lf1.merge(lf4)

    assert lf1.data == {
        "importers": {
            ".": {
                "dependencies": {
                    "a": "1.0.0",
                },
                "specifiers": {
                    "a": "1.0.0",
                },
            },
            "../bar": {
                "dependencies": {
                    "b": "1.0.0",
                },
                "specifiers": {
                    "b": "1.0.0",
                },
            },
            "../another/baz": {
                "dependencies": {
                    "@a/qux": "link:../qux",
                    "a": "1.0.0",
                },
                "specifiers": {
                    "@a/qux": "workspace:../qux",
                    "a": "1.0.0",
                },
            },
            "../another/qux": {
                "dependencies": {
                    "b": "1.0.1",
                },
                "specifiers": {
                    "b": "1.0.1",
                },
            },
            "../another/quux": {
                "dependencies": {
                    "@a/bar": "link:../../bar",
                },
                "specifiers": {
                    "@a/bar": "workspace:../../bar",
                },
            },
        },
        "packages": {
            "/a/1.0.0": {},
            "/b/1.0.0": {},
            "/b/1.0.1": {},
        },
    }


def test_lockfile_merge_dont_overrides_packages():
    lf1 = PnpmLockfile(path="/foo/pnpm-lock.yaml")
    lf1.data = {
        "dependencies": {
            "a": "1.0.0",
        },
        "specifiers": {
            "a": "1.0.0",
        },
        "packages": {
            "/a/1.0.0": {},
        },
    }

    lf2 = PnpmLockfile(path="/bar/pnpm-lock.yaml")
    lf2.data = {
        "dependencies": {
            "a": "1.0.0",
            "b": "1.0.0",
        },
        "specifiers": {
            "a": "1.0.0",
            "b": "1.0.0",
        },
        "packages": {
            "/a/1.0.0": {
                "overriden": True,
            },
            "/b/1.0.0": {},
        },
    }

    lf1.merge(lf2)

    assert lf1.data == {
        "importers": {
            ".": {
                "dependencies": {
                    "a": "1.0.0",
                },
                "specifiers": {
                    "a": "1.0.0",
                },
            },
            "../bar": {
                "dependencies": {
                    "a": "1.0.0",
                    "b": "1.0.0",
                },
                "specifiers": {
                    "a": "1.0.0",
                    "b": "1.0.0",
                },
            },
        },
        "packages": {
            "/a/1.0.0": {},
            "/b/1.0.0": {},
        },
    }
