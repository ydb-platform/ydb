#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
from os import path
from unittest import mock
from unittest.mock import Mock

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import SHA256_DIGEST, ResultStatus

try:
    from snowflake.connector.util_text import random_string
except ImportError:
    from ..randomize import random_string

try:
    from snowflake.connector.errors import RequestExceedMaxRetryError
    from snowflake.connector.file_transfer_agent import (
        SnowflakeFileMeta,
        SnowflakeFileTransferAgent,
        StorageCredential,
    )
    from snowflake.connector.storage_client import METHODS
    from snowflake.connector.vendored.requests import Response
except ImportError:
    # Compatibility for olddriver tests
    from requests import Response

    SnowflakeFileMeta = dict
    RequestExceedMaxRetryError = None
    METHODS = {}
    megabytes = 1024 * 1024

try:  # pragma: no cover
    from snowflake.connector.gcs_storage_client import SnowflakeGCSRestClient
except ImportError:
    SnowflakeGCSRestClient = None

# We need these for our OldDriver tests. We run most up to date tests with the oldest supported driver version
try:
    from snowflake.connector.vendored import requests

    vendored_request = True
except ImportError:  # pragma: no cover
    import requests

    vendored_request = False

THIS_DIR = path.dirname(path.realpath(__file__))


@pytest.mark.parametrize("errno", [408, 429, 500, 503])
def test_upload_retry_errors(errno, tmpdir):
    """Tests whether retryable errors are handled correctly when upploading."""
    f_name = str(tmpdir.join("some_file.txt"))
    resp = requests.Response()
    resp.status_code = errno
    meta = SnowflakeFileMeta(
        name=f_name,
        src_file_name=f_name,
        stage_location_type="GCS",
        presigned_url="some_url",
        sha256_digest="asd",
    )
    if RequestExceedMaxRetryError is not None:
        mock_connection = mock.create_autospec(SnowflakeConnection)
        client = SnowflakeGCSRestClient(
            meta,
            StorageCredential({}, mock_connection, ""),
            {},
            mock_connection,
            "",
        )
    with open(f_name, "w") as f:
        f.write(random_string(15))
    if RequestExceedMaxRetryError is None:
        with mock.patch(
            "snowflake.connector.vendored.requests.put"
            if vendored_request
            else "requests.put",
            side_effect=requests.exceptions.HTTPError(response=resp),
        ):
            SnowflakeGCSUtil.upload_file(f_name, meta, None, 99, 64000)
            assert isinstance(meta.last_error, requests.exceptions.HTTPError)
            assert meta.result_status == ResultStatus.NEED_RETRY
    else:
        client.data_file = f_name

        if vendored_request:
            with mock.patch.dict(
                METHODS,
                {"PUT": lambda *a, **kw: resp},
            ):
                with pytest.raises(RequestExceedMaxRetryError):
                    # Retry quickly during unit tests
                    client.SLEEP_UNIT = 0.0
                    client.upload_chunk(0)
        else:
            # Old Driver test specific code
            with mock.patch("requests.put"):
                SnowflakeGCSUtil.upload_file(f_name, meta, None, 99, 64000)
                assert isinstance(meta.last_error, requests.exceptions.HTTPError)
                assert meta.result_status == ResultStatus.NEED_RETRY


def test_upload_uncaught_exception(tmpdir):
    """Tests whether non-retryable errors are handled correctly when uploading."""
    f_name = str(tmpdir.join("some_file.txt"))
    resp = requests.Response()
    resp.status_code = 501
    exc = requests.exceptions.HTTPError(response=resp)
    with open(f_name, "w") as f:
        f.write(random_string(15))
    agent = SnowflakeFileTransferAgent(
        mock.MagicMock(),
        f"put {f_name} @~",
        {
            "data": {
                "command": "UPLOAD",
                "src_locations": [f_name],
                "stageInfo": {
                    "locationType": "GCS",
                    "location": "",
                    "creds": {"AWS_SECRET_KEY": "", "AWS_KEY_ID": ""},
                    "region": "test",
                    "endPoint": None,
                },
                "localLocation": "/tmp",
            }
        },
    )
    with mock.patch(
        "snowflake.connector.gcs_storage_client.SnowflakeGCSRestClient.get_file_header",
    ), mock.patch(
        "snowflake.connector.gcs_storage_client.SnowflakeGCSRestClient._upload_chunk",
        side_effect=exc,
    ):
        agent.execute()
    assert agent._file_metadata[0].error_details is exc


@pytest.mark.parametrize("errno", [403, 408, 429, 500, 503])
def test_download_retry_errors(errno, tmp_path):
    """Tests whether retryable errors are handled correctly when downloading."""
    resp = requests.Response()
    resp.status_code = errno
    if errno == 403:
        pytest.skip("This behavior has changed in the move from SDKs")
    meta_info = {
        "name": "data1.txt.gz",
        "stage_location_type": "S3",
        "no_sleeping_time": True,
        "put_callback": None,
        "put_callback_output_stream": None,
        SHA256_DIGEST: "123456789abcdef",
        "dst_file_name": "data1.txt.gz",
        "src_file_name": path.join(THIS_DIR, "../data", "put_get_1.txt"),
        "overwrite": True,
    }
    meta = SnowflakeFileMeta(**meta_info)
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": ""}
    cnx = mock.MagicMock(autospec=SnowflakeConnection)
    rest_client = SnowflakeGCSRestClient(
        meta,
        StorageCredential(
            creds,
            cnx,
            "GET file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        cnx,
        "GET file:///tmp/file.txt @~",
    )
    from snowflake.connector.storage_client import METHODS

    rest_client.SLEEP_UNIT = 0
    with mock.patch.dict(METHODS, GET=mock.MagicMock(return_value=resp)):
        with pytest.raises(
            RequestExceedMaxRetryError,
            match="GET with url .* failed for exceeding maximum retries",
        ):
            rest_client.download_chunk(0)


@pytest.mark.parametrize("errno", (501, 403))
def test_download_uncaught_exception(tmp_path, errno):
    """Tests whether non-retryable errors are handled correctly when downloading."""
    resp = requests.Response()
    resp.status_code = errno
    meta_info = {
        "name": "data1.txt.gz",
        "stage_location_type": "S3",
        "no_sleeping_time": True,
        "put_callback": None,
        "put_callback_output_stream": None,
        SHA256_DIGEST: "123456789abcdef",
        "dst_file_name": "data1.txt.gz",
        "src_file_name": path.join(THIS_DIR, "../data", "put_get_1.txt"),
        "overwrite": True,
    }
    meta = SnowflakeFileMeta(**meta_info)
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": ""}
    cnx = mock.MagicMock(autospec=SnowflakeConnection)
    rest_client = SnowflakeGCSRestClient(
        meta,
        StorageCredential(
            creds,
            cnx,
            "GET file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        cnx,
        "GET file:///tmp/file.txt @~",
    )
    from snowflake.connector.storage_client import METHODS

    rest_client.SLEEP_UNIT = 0
    with mock.patch.dict(METHODS, GET=mock.MagicMock(return_value=resp)):
        with pytest.raises(
            requests.exceptions.HTTPError,
        ):
            rest_client.download_chunk(0)


def test_upload_put_timeout(tmp_path, caplog):
    """Tests whether timeout error is handled correctly when uploading."""
    caplog.set_level(logging.DEBUG, "snowflake.connector")
    f_name = str(tmp_path / "some_file.txt")
    resp = requests.Response()
    with open(f_name, "w") as f:
        f.write(random_string(15))
    agent = SnowflakeFileTransferAgent(
        mock.Mock(autospec=SnowflakeConnection, connection=None),
        f"put {f_name} @~",
        {
            "data": {
                "command": "UPLOAD",
                "src_locations": [f_name],
                "stageInfo": {
                    "locationType": "GCS",
                    "location": "",
                    "creds": {"AWS_SECRET_KEY": "", "AWS_KEY_ID": ""},
                    "region": "test",
                    "endPoint": None,
                },
                "localLocation": "/tmp",
            }
        },
    )
    mocked_put, mocked_head = mock.MagicMock(), mock.MagicMock()
    mocked_put.side_effect = requests.exceptions.Timeout(response=resp)
    resp = Response()
    resp.status_code = 404
    mocked_head.return_value = resp
    SnowflakeGCSRestClient.SLEEP_UNIT = 0
    from snowflake.connector.storage_client import METHODS

    with mock.patch.dict(METHODS, {"PUT": mocked_put, "HEAD": mocked_head}):
        agent.execute()
    assert (
        "snowflake.connector.storage_client",
        logging.WARNING,
        "PUT with url https://storage.googleapis.com//some_file.txt.gz failed for transient error: ",
    ) in caplog.record_tuples
    assert (
        "snowflake.connector.file_transfer_agent",
        logging.DEBUG,
        "Chunk 0 of file some_file.txt failed to transfer for unexpected exception PUT with url https://storage.googleapis.com//some_file.txt.gz failed for exceeding maximum retries.",
    ) in caplog.record_tuples


def test_download_timeout(tmp_path, caplog):
    """Tests whether timeout error is handled correctly when downloading."""
    timeout_exc = requests.exceptions.Timeout(response=requests.Response())
    meta_info = {
        "name": "data1.txt.gz",
        "stage_location_type": "S3",
        "no_sleeping_time": True,
        "put_callback": None,
        "put_callback_output_stream": None,
        SHA256_DIGEST: "123456789abcdef",
        "dst_file_name": "data1.txt.gz",
        "src_file_name": path.join(THIS_DIR, "../data", "put_get_1.txt"),
        "overwrite": True,
    }
    meta = SnowflakeFileMeta(**meta_info)
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": ""}
    cnx = mock.MagicMock(autospec=SnowflakeConnection)
    rest_client = SnowflakeGCSRestClient(
        meta,
        StorageCredential(
            creds,
            cnx,
            "GET file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        cnx,
        "GET file:///tmp/file.txt @~",
    )
    from snowflake.connector.storage_client import METHODS

    rest_client.SLEEP_UNIT = 0
    with mock.patch.dict(METHODS, GET=mock.MagicMock(side_effect=timeout_exc)):
        exc = Exception("stop execution")
        with mock.patch.object(rest_client.credentials, "update", side_effect=exc):
            with pytest.raises(RequestExceedMaxRetryError):
                rest_client.download_chunk(0)


def test_get_file_header_none_with_presigned_url(tmp_path):
    """Tests whether default file handle created by get_file_header is as expected."""
    meta = SnowflakeFileMeta(
        name=str(tmp_path / "some_file"),
        src_file_name=str(tmp_path / "some_file"),
        stage_location_type="GCS",
        presigned_url="www.example.com",
    )
    storage_credentials = Mock()
    storage_credentials.creds = {}
    stage_info = Mock()
    connection = Mock()
    client = SnowflakeGCSRestClient(
        meta, storage_credentials, stage_info, connection, ""
    )
    file_header = client.get_file_header(meta.name)
    assert file_header is None
