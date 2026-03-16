"""Test suite for prance.util.url ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import sys

import pytest

from prance.util import url

from . import platform

import yatest.common as yc


def test_absurl_http():
    test = "http://foo.bar/asdf/#lala/quux"
    res = url.absurl(test)
    assert res.geturl() == test


def test_absurl_http_fragment():
    base = "http://foo.bar/asdf/#lala/quux"
    test = "#another"
    res = url.absurl(test, base)
    assert res.scheme == "http"
    assert res.netloc == "foo.bar"
    assert res.path == "/asdf/"
    assert res.fragment == "another"


@pytest.mark.skipif(platform("win32"), reason="Skip on win32")
def test_absurl_file_posix():
    base = "file:///etc/passwd"
    test = "group"
    expect = "file:///etc/group"
    res = url.absurl(test, base)
    assert res.geturl() == expect


@pytest.mark.skipif(platform("!win32"), reason="Skip on !win32")
def test_absurl_file_win32():
    base = "file:///c:/windows/notepad.exe"
    test = "regedit.exe"
    expect = "file:///c:/windows/regedit.exe"
    res = url.absurl(test, base)
    assert res.geturl() == expect


@pytest.mark.skipif(platform("win32"), reason="Skip on win32")
def test_absurl_absfile_posix():
    test = "file:///etc/passwd"
    res = url.absurl(test)
    assert res.geturl() == test


@pytest.mark.skipif(platform("!win32"), reason="Skip on !win32")
def test_absurl_absfile_win32():
    test = "file:///c:/windows/notepad.exe"
    res = url.absurl(test)
    assert res.geturl() == test


def test_absurl_fragment():
    base = "file:///etc/passwd"
    test = "#frag"
    with pytest.raises(url.ResolutionError):
        url.absurl(test)

    res = url.absurl(test, base)
    assert res.geturl() == "file:///etc/passwd#frag"


def test_absurl_relfile():
    base = "http://foo.bar"
    test = "relative.file"
    with pytest.raises(url.ResolutionError):
        url.absurl(test)
    with pytest.raises(url.ResolutionError):
        url.absurl(test, base)


@pytest.mark.skipif(platform("win32"), reason="Skip on win32")
def test_absurl_paths_posix():
    base = "/etc/passwd"
    test = "group"
    expect = "file:///etc/group"

    with pytest.raises(url.ResolutionError):
        url.absurl(test)

    res = url.absurl(test, base)
    assert res.geturl() == expect


@pytest.mark.skipif(platform("!win32"), reason="Skip on !win32")
def test_absurl_paths_win32():
    base = "c:\\windows\\notepad.exe"
    test = "regedit.exe"
    expect = "file:///c:/windows/regedit.exe"

    with pytest.raises(url.ResolutionError):
        url.absurl(test)

    res = url.absurl(test, base)
    assert res.geturl() == expect


def test_urlresource():
    parsed = url.absurl("http://foo.bar/asdf?some=query#myfrag")
    res = url.urlresource(parsed)
    assert res == "http://foo.bar/asdf"


def test_split_url_reference():
    base = url.absurl("http://foo.bar/")

    # Relative reference
    parsed, path = url.split_url_reference(base, "#foo/bar")
    assert parsed.netloc == "foo.bar"
    assert len(path) == 2
    assert path[0] == "foo"
    assert path[1] == "bar"

    # Leading slashes are stripped
    parsed, path = url.split_url_reference(base, "#///foo/bar")
    assert parsed.netloc == "foo.bar"
    assert len(path) == 2
    assert path[0] == "foo"
    assert path[1] == "bar"

    # Absolute reference
    parsed, path = url.split_url_reference(base, "http://somewhere/#foo/bar")
    assert parsed.netloc == "somewhere"
    assert len(path) == 2
    assert path[0] == "foo"
    assert path[1] == "bar"

    # Reference with escaped parts
    parsed, path = url.split_url_reference(
        base, "http://somewhere/#foo/bar~1baz~1quux~0foo"
    )
    assert parsed.netloc == "somewhere"
    assert len(path) == 2
    assert path[0] == "foo"
    assert path[1] == "bar/baz/quux~foo"


def test_fetch_url_file():
    from prance.util import fs

    content = url.fetch_url(url.absurl(fs.abspath(yc.test_source_path("specs/with_externals.yaml"))))
    assert content["swagger"] == "2.0"


def test_fetch_url_cached():
    from prance.util import fs

    cache = {}

    content1 = url.fetch_url(
        url.absurl(fs.abspath(yc.test_source_path("specs/with_externals.yaml"))), cache
    )
    assert content1["swagger"] == "2.0"

    content2 = url.fetch_url(
        url.absurl(fs.abspath(yc.test_source_path("specs/with_externals.yaml"))), cache
    )
    assert content2["swagger"] == "2.0"

    # Dicts are mutable, therefore we can't compare IDs. But individual
    # string fields should not be copied, because we shallow copy.
    assert id(content1["swagger"]) == id(content2["swagger"])


def test_fetch_url_text_cached():
    from prance.util import fs

    cache = {}

    content1, _ = url.fetch_url_text(
        url.absurl(fs.abspath(yc.test_source_path("specs/with_externals.yaml"))), cache
    )
    content2, _ = url.fetch_url_text(
        url.absurl(fs.abspath(yc.test_source_path("specs/with_externals.yaml"))), cache
    )

    # Strings are immutable, therefore IDs should be identical
    assert id(content1) == id(content2)


@pytest.mark.requires_network()
def test_fetch_url_http(httpserver):
    with open(yc.test_source_path("specs/petstore.yaml")) as f:
        httpserver.serve_content(content=f.read(), code=200)
    exturl = "{}/petstore.yaml#/definitions/Pet".format(httpserver.url)
    content = url.fetch_url(url.absurl(exturl))
    assert content["swagger"] == "2.0"


def test_fetch_url_python():
    exturl = "python://specs/petstore.yaml"
    content = url.fetch_url(url.absurl(exturl))
    assert content["swagger"] == "2.0"
