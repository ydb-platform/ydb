"""_path tests."""

import sys

from fiona._path import _parse_path, _vsi_path


def test_parse_zip_windows(monkeypatch):
    """Parse a zip+ Windows path."""
    monkeypatch.setattr(sys, "platform", "win32")
    path = _parse_path("zip://D:\\a\\Fiona\\Fiona\\tests\\data\\coutwildrnp.zip!coutwildrnp.shp")
    vsi_path = _vsi_path(path)
    assert vsi_path.startswith("/vsizip/D")
    assert vsi_path.endswith("coutwildrnp.zip/coutwildrnp.shp")


def test_parse_zip_windows(monkeypatch):
    """Parse a tar+ Windows path."""
    monkeypatch.setattr(sys, "platform", "win32")
    path = _parse_path("tar://D:\\a\\Fiona\\Fiona\\tests\\data\\coutwildrnp.tar!testing/coutwildrnp.shp")
    vsi_path = _vsi_path(path)
    assert vsi_path.startswith("/vsitar/D")
    assert vsi_path.endswith("coutwildrnp.tar/testing/coutwildrnp.shp")
