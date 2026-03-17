"""Tests of fiona.env"""

import os
import sys
from unittest import mock

import boto3
import pytest

import fiona
from fiona import _env
from fiona.env import getenv, hasenv, ensure_env, ensure_env_with_credentials
from fiona.errors import FionaDeprecationWarning
from fiona.session import AWSSession, GSSession


def test_nested_credentials(monkeypatch):
    """Check that rasterio.open() doesn't wipe out surrounding credentials"""

    @ensure_env_with_credentials
    def fake_opener(path):
        return fiona.env.getenv()

    with fiona.env.Env(
        session=AWSSession(aws_access_key_id="foo", aws_secret_access_key="bar")
    ):
        assert fiona.env.getenv()["AWS_ACCESS_KEY_ID"] == "foo"
        assert fiona.env.getenv()["AWS_SECRET_ACCESS_KEY"] == "bar"

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "lol")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "wut")
        gdalenv = fake_opener("s3://foo/bar")
        assert gdalenv["AWS_ACCESS_KEY_ID"] == "foo"
        assert gdalenv["AWS_SECRET_ACCESS_KEY"] == "bar"


def test_ensure_env_decorator(gdalenv):
    @ensure_env
    def f():
        return getenv()["FIONA_ENV"]

    assert f() is True


def test_ensure_env_decorator_sets_gdal_data(gdalenv, monkeypatch):
    """fiona.env.ensure_env finds GDAL from environment"""

    @ensure_env
    def f():
        return getenv()["GDAL_DATA"]

    monkeypatch.setenv("GDAL_DATA", "/lol/wut")
    assert f() == "/lol/wut"


@mock.patch("fiona._env.GDALDataFinder.find_file")
def test_ensure_env_decorator_sets_gdal_data_prefix(
    find_file, gdalenv, monkeypatch, tmpdir
):
    """fiona.env.ensure_env finds GDAL data under a prefix"""

    @ensure_env
    def f():
        return getenv()["GDAL_DATA"]

    find_file.return_value = None
    tmpdir.ensure("share/gdal/header.dxf")
    monkeypatch.delenv("GDAL_DATA", raising=False)
    monkeypatch.setattr(_env, "__file__", str(tmpdir.join("fake.py")))
    monkeypatch.setattr(sys, "prefix", str(tmpdir))

    assert f() == str(tmpdir.join("share").join("gdal"))


@mock.patch("fiona._env.GDALDataFinder.find_file")
def test_ensure_env_decorator_sets_gdal_data_wheel(
    find_file, gdalenv, monkeypatch, tmpdir
):
    """fiona.env.ensure_env finds GDAL data in a wheel"""

    @ensure_env
    def f():
        return getenv()["GDAL_DATA"]

    find_file.return_value = None
    tmpdir.ensure("gdal_data/header.dxf")
    monkeypatch.delenv("GDAL_DATA", raising=False)
    monkeypatch.setattr(
        _env, "__file__", str(tmpdir.join(os.path.basename(_env.__file__)))
    )

    assert f() == str(tmpdir.join("gdal_data"))


@mock.patch("fiona._env.GDALDataFinder.find_file")
def test_ensure_env_with_decorator_sets_gdal_data_wheel(
    find_file, gdalenv, monkeypatch, tmpdir
):
    """fiona.env.ensure_env finds GDAL data in a wheel"""

    @ensure_env_with_credentials
    def f(*args):
        return getenv()["GDAL_DATA"]

    find_file.return_value = None
    tmpdir.ensure("gdal_data/header.dxf")
    monkeypatch.delenv("GDAL_DATA", raising=False)
    monkeypatch.setattr(
        _env, "__file__", str(tmpdir.join(os.path.basename(_env.__file__)))
    )

    assert f("foo") == str(tmpdir.join("gdal_data"))


def test_ensure_env_crs(path_coutwildrnp_shp):
    """Decoration of .crs works"""
    assert fiona.open(path_coutwildrnp_shp).crs


def test_env_default_env(path_coutwildrnp_shp):
    with fiona.open(path_coutwildrnp_shp):
        assert hasenv()


def test_nested_gs_credentials(monkeypatch):
    """Check that rasterio.open() doesn't wipe out surrounding credentials"""

    @ensure_env_with_credentials
    def fake_opener(path):
        return fiona.env.getenv()

    with fiona.env.Env(session=GSSession(google_application_credentials="foo")):
        assert fiona.env.getenv()["GOOGLE_APPLICATION_CREDENTIALS"] == "foo"

        gdalenv = fake_opener("gs://foo/bar")
        assert gdalenv["GOOGLE_APPLICATION_CREDENTIALS"] == "foo"


def test_aws_session(gdalenv):
    """Create an Env with a boto3 session."""
    aws_session = boto3.Session(
        aws_access_key_id="id",
        aws_secret_access_key="key",
        aws_session_token="token",
        region_name="null-island-1",
    )
    with pytest.warns(FionaDeprecationWarning):
        with fiona.env.Env(session=aws_session) as s:
            assert (
                s.session._session.get_credentials().get_frozen_credentials().access_key
                == "id"
            )
            assert (
                s.session._session.get_credentials().get_frozen_credentials().secret_key
                == "key"
            )
            assert (
                s.session._session.get_credentials().get_frozen_credentials().token
                == "token"
            )
            assert s.session._session.region_name == "null-island-1"
