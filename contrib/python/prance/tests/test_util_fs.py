"""Test suite for prance.util.fs ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


import os
import sys

import pytest

from prance.util import fs

from . import sandbox, platform

import yatest.common as yc

@pytest.fixture
def create_symlink():
    # Defined as a fixture so we can selectively use it for tests."
    testname = "tests/specs/symlink_test"
    import os.path

    if os.path.islink(testname):
        return testname

    if os.path.exists(testname):
        os.unlink(testname)

    target = "with_externals.yaml"
    os.symlink(target, testname)

    return testname


@pytest.mark.skip("There is no symlinks in Arcadia")
def test_canonical(create_symlink):
    res = fs.canonical_filename(create_symlink)
    expected = os.path.join(os.getcwd(), "tests/specs/with_externals.yaml")
    assert res == expected


def test_to_posix_rel():
    test = yc.test_source_path("specs/with_externals.yaml")
    assert fs.to_posix(os.path.normpath(test)) == test


@pytest.mark.skipif(platform("win32"), reason="Skip on win32")
def test_to_posix_abs_posix():
    test = "/etc/passwd"
    expected = test
    assert fs.to_posix(test) == expected


@pytest.mark.skipif(platform("!win32"), reason="Skip on !win32")
def test_to_posix_abs_win32():
    test = "c:\\windows\\notepad.exe"
    expected = "/c:/windows/notepad.exe"
    assert fs.to_posix(test) == expected


def test_from_posix_rel():
    test = yc.test_source_path("specs/with_externals.yaml")
    assert fs.from_posix(test) == os.path.normpath(test)


@pytest.mark.skipif(platform("win32"), reason="Skip on win32")
def test_from_posix_abs_posix():
    test = "/etc/passwd"
    expected = test
    assert fs.from_posix(test) == expected


@pytest.mark.skipif(platform("!win32"), reason="Skip on !win32")
def test_from_posix_abs_win32():
    test = "/c:/windows/notepad.exe"
    expected = "c:\\windows\\notepad.exe"
    assert fs.from_posix(test) == expected


def test_abspath_basics():
    testname = os.path.normpath(yc.test_source_path("specs/with_externals.yaml"))
    res = fs.abspath(testname)
    expected = fs.to_posix(os.path.join(os.getcwd(), testname))
    assert res == expected


def test_abspath_relative():
    testname = "error.json"
    relative = yc.test_source_path("specs/with_externals.yaml")
    res = fs.abspath(testname, relative)
    expected = fs.to_posix(os.path.join(yc.test_source_path(), "specs", testname))
    assert res == expected


def test_abspath_relative_dir():
    testname = "error.json"
    relative = os.path.join(yc.test_source_path(), "specs")
    res = fs.abspath(testname, relative)
    expected = fs.to_posix(os.path.join(yc.test_source_path(), "specs", testname))
    assert res == expected


def test_detect_encoding():
    # Quick detection should yield utf-8 for the petstore file.
    assert fs.detect_encoding(yc.test_source_path("specs/petstore.yaml")) == "utf-8"

    # Without defaulting to utf-8, it should also work.
    assert (
        fs.detect_encoding(yc.test_source_path("specs/petstore.yaml"), default_to_utf8=False)
        == "utf-8"
    )

    # Deep inspection should not change anything because the file size is
    # too small.
    assert (
        fs.detect_encoding(
            yc.test_source_path("specs/petstore.yaml"), default_to_utf8=False, read_all=True
        )
        == "utf-8"
    )

    # The UTF-8 file with BOM should be detected properly
    assert fs.detect_encoding(yc.test_source_path("specs/utf8bom.yaml")) == "utf-8-sig"


def test_issue_51_detect_encoding():
    # This should be UTF-8, but in issue 51 it was reported that it comes back
    # as iso-8859-2

    # Detection should be ok if the entire file is read
    assert (
        fs.detect_encoding(yc.test_source_path("specs/issue_51/openapi-part.yaml"), read_all=True)
        == "utf-8"
    )

    # After the heuristic change, it reads the whole file anyway.
    assert fs.detect_encoding(yc.test_source_path("specs/issue_51/openapi-part.yaml")) == "utf-8"

    # Specifically re-encoded as iso-8859-2 should fail - but not as
    # a call to the detect_encoding() function. Instead, we can only return
    # a badly detected encoding. Chardet sends iso-8859-1 here.
    assert (
        fs.detect_encoding(yc.test_source_path("specs/issue_51/openapi-part-iso-8859-2.yaml"))
        == "iso-8859-1"
    )


def test_load_nobom():
    contents = fs.read_file(yc.test_source_path("specs/petstore.yaml"))
    assert contents.index("Swagger Petstore") >= 0, "File reading failed!"


def test_load_utf8bom():
    contents = fs.read_file(yc.test_source_path("specs/utf8bom.yaml"))
    assert contents.index("söme välüe") >= 0, "UTF-8 BOM handling failed!"


def test_load_utf8bom_override():
    with pytest.raises(UnicodeDecodeError):
        fs.read_file(yc.test_source_path("specs/utf8bom.yaml"), "ascii")


def test_write_file(tmpdir):
    with sandbox.sandbox(tmpdir):
        test_text = "söme täxt"
        fs.write_file("test.out", test_text)

        # File must have been written
        files = [f for f in os.listdir(".") if os.path.isfile(f)]
        assert "test.out" in files

        # File contents must work
        contents = fs.read_file("test.out")
        assert test_text == contents


def test_write_file_bom(tmpdir):
    with sandbox.sandbox(tmpdir):
        test_text = "söme täxt"
        fs.write_file("test.out", test_text, "utf-8-sig")

        # File must have been written
        files = [f for f in os.listdir(".") if os.path.isfile(f)]
        assert "test.out" in files

        # Encoding must match the one we've given
        encoding = fs.detect_encoding("test.out")
        assert encoding == "utf-8-sig"

        # File contents must work
        contents = fs.read_file("test.out")
        assert test_text == contents


def test_valid_pathname():
    # A URL should not be valid
    from prance.util.fs import is_pathname_valid

    assert False == is_pathname_valid("\x00o.bar.org")

    # However, the current path should be.
    import os

    assert True == is_pathname_valid(os.getcwd())

    # Can't put non-strings into this function
    assert True == is_pathname_valid("foo")
    assert False == is_pathname_valid(123)

    # Can't accept too long components
    assert False == is_pathname_valid("a" * 256)
