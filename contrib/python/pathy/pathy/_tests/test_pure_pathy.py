from pathlib import Path

import pytest

from pathy import PurePathy


def test_pure_pathy_repr() -> None:
    p = PurePathy("gs://bucket/fake_file.txt")
    assert repr(p) == "PurePathy('gs://bucket/fake_file.txt')"
    assert str(PurePathy("gs://bucket/fake_file.txt")) == "gs://bucket/fake_file.txt"
    assert bytes(PurePathy("gs://bucket/fake_file.txt")) == b"gs://bucket/fake_file.txt"


def test_pure_pathy_scheme_extraction() -> None:
    assert PurePathy("gs://var/tests/fake").scheme == "gs"
    assert PurePathy("s3://var/tests/fake").scheme == "s3"
    assert PurePathy("file://var/tests/fake").scheme == "file"


def test_pure_pathy_join_strs() -> None:
    assert PurePathy("foo", "some/path", "bar") == PurePathy("foo/some/path/bar")


def test_pure_pathy_parse_parts() -> None:
    # Needs two parts to extract scheme/bucket
    root = PurePathy("foo:")
    assert root.drive == ""
    assert root.root == ""
    assert root.parts == ("", "", "foo:")

    other = PurePathy("foo://bar/baz")
    assert other.drive == "foo"
    assert other.root == "bar"
    assert other.parts == ("foo", "bar", "baz")


def test_pure_pathy_join_paths() -> None:
    assert PurePathy(Path("foo"), Path("bar")) == PurePathy("foo/bar")


def test_pure_pathy_empty() -> None:
    assert PurePathy() == PurePathy(".")


# def test_pure_pathy_absolute_paths() -> None:
#     assert PurePathy("/etc", "/usr", "lib64") == PurePathy("/usr/lib64")


def test_pure_pathy_slashes_single_double_dots() -> None:
    assert PurePathy("foo//bar") == PurePathy("foo/bar")
    assert PurePathy("foo/./bar") == PurePathy("foo/bar")
    assert PurePathy("../bar") == PurePathy("../bar")


def test_pure_pathy_operators() -> None:
    assert PurePathy("/etc") / "init.d" / "apache2" == PurePathy("/etc/init.d/apache2")
    assert "/var" / PurePathy("tests") / "fake" == PurePathy("/var/tests/fake")


def test_pure_pathy_parts() -> None:
    assert PurePathy("gs://foo/bar").parts == ("gs", "foo", "bar")
    assert PurePathy("azure://foo/bar/baz").parts == ("azure", "foo", "bar", "baz")


def test_pure_pathy_prefix() -> None:
    assert PurePathy("gs://bar").prefix == ""
    assert PurePathy("gs://bar/baz/").prefix == "baz/"
    assert PurePathy("gs://bar/baz").prefix == "baz/"


def test_pure_pathy_drive() -> None:
    assert PurePathy("foo//bar").drive == ""
    assert PurePathy("foo/./bar").drive == ""
    assert PurePathy("foo/../bar").drive == ""
    assert PurePathy("../bar").drive == ""
    assert PurePathy("foo", "../bar").drive == ""
    assert PurePathy("/foo/bar").drive == ""


def test_pure_pathy_name() -> None:
    assert PurePathy("my/library/fake_file.txt").name == "fake_file.txt"


def test_pure_pathy_suffix() -> None:
    assert PurePathy("my/library/fake_file.txt").suffix == ".txt"
    assert PurePathy("my/library.tar.gz").suffix == ".gz"
    assert PurePathy("my/library").suffix == ""


def test_pure_pathy_suffixes() -> None:
    assert PurePathy("my/library.tar.gar").suffixes == [".tar", ".gar"]
    assert PurePathy("my/library.tar.gz").suffixes == [".tar", ".gz"]
    assert PurePathy("my/library").suffixes == []


def test_pure_pathy_stem() -> None:
    assert PurePathy("my/library.tar.gar").stem == "library.tar"
    assert PurePathy("my/library.tar").stem == "library"
    assert PurePathy("my/library").stem == "library"


def test_pure_pathy_absolute() -> None:
    assert PurePathy("gs://bucket/path").is_absolute()
    assert not PurePathy("a/b").is_absolute()


def test_pure_pathy_reserved() -> None:
    assert not PurePathy("/a/b").is_reserved()
    assert not PurePathy("a/b").is_reserved()


def test_pure_pathy_match() -> None:
    assert PurePathy("a/b.py").match("*.py")
    assert PurePathy("/a/b/c.py").match("b/*.py")
    assert not PurePathy("/a/b/c.py").match("a/*.py")
    assert PurePathy("/a.py").match("/*.py")
    assert not PurePathy("a/b.py").match("*.Py")


def test_pure_pathy_relative_to() -> None:
    gcs_path = PurePathy("/etc/passwd")
    assert gcs_path.relative_to("/") == PurePathy("etc/passwd")
    assert gcs_path.relative_to("/etc") == PurePathy("passwd")
    with pytest.raises(ValueError):
        gcs_path.relative_to("/usr")


def test_pure_pathy_with_name() -> None:
    gcs_path = PurePathy("/Downloads/pathlib.tar.gz")
    assert gcs_path.with_name("fake_file.txt") == PurePathy("/Downloads/fake_file.txt")
    gcs_path = PurePathy("/")
    with pytest.raises(ValueError):
        gcs_path.with_name("fake_file.txt")


def test_pure_pathy_with_suffix() -> None:
    gcs_path = PurePathy("/Downloads/pathlib.tar.gz")
    assert gcs_path.with_suffix(".bz2") == PurePathy("/Downloads/pathlib.tar.bz2")
    gcs_path = PurePathy("README")
    assert gcs_path.with_suffix(".txt") == PurePathy("README.txt")
    gcs_path = PurePathy("README.txt")
    assert gcs_path.with_suffix("") == PurePathy("README")
