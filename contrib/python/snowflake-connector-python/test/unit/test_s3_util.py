#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
import re
from os import path
from unittest import mock
from unittest.mock import MagicMock

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import SHA256_DIGEST
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.file_transfer_agent import SnowflakeFileTransferAgent

from ..helpers import verify_log_tuple

try:
    from snowflake.connector.constants import megabyte
    from snowflake.connector.errors import RequestExceedMaxRetryError
    from snowflake.connector.file_transfer_agent import (
        SnowflakeFileMeta,
        StorageCredential,
    )
    from snowflake.connector.s3_storage_client import (
        ERRORNO_WSAECONNABORTED,
        EXPIRED_TOKEN,
        SnowflakeS3RestClient,
    )
    from snowflake.connector.vendored.requests import HTTPError, Response
except ImportError:
    # Compatibility for olddriver tests
    from requests import HTTPError, Response

    from snowflake.connector.s3_util import ERRORNO_WSAECONNABORTED  # NOQA

    SnowflakeFileMeta = dict
    SnowflakeS3RestClient = None
    RequestExceedMaxRetryError = None
    StorageCredential = None
    megabytes = 1024 * 1024
    DEFAULT_MAX_RETRY = 5

THIS_DIR = path.dirname(path.realpath(__file__))
MINIMAL_METADATA = SnowflakeFileMeta(
    name="file.txt",
    stage_location_type="S3",
    src_file_name="file.txt",
)


@pytest.mark.parametrize(
    "input, bucket_name, s3path",
    [
        ("sfc-dev1-regression/test_sub_dir/", "sfc-dev1-regression", "test_sub_dir/"),
        (
            "sfc-dev1-regression/stakeda/test_stg/test_sub_dir/",
            "sfc-dev1-regression",
            "stakeda/test_stg/test_sub_dir/",
        ),
        ("sfc-dev1-regression/", "sfc-dev1-regression", ""),
        ("sfc-dev1-regression//", "sfc-dev1-regression", "/"),
        ("sfc-dev1-regression///", "sfc-dev1-regression", "//"),
    ],
)
def test_extract_bucket_name_and_path(input, bucket_name, s3path):
    """Extracts bucket name and S3 path."""
    s3_loc = SnowflakeS3RestClient._extract_bucket_name_and_path(input)
    assert s3_loc.bucket_name == bucket_name
    assert s3_loc.path == s3path


def test_upload_file_with_s3_upload_failed_error(tmp_path):
    """Tests Upload file with S3UploadFailedError, which could indicate AWS token expires."""
    file1 = tmp_path / "file1"
    with file1.open("w") as f:
        f.write("test1")
    rest_client = SnowflakeFileTransferAgent(
        MagicMock(autospec=SnowflakeCursor),
        "PUT some_file.txt",
        {
            "data": {
                "command": "UPLOAD",
                "autoCompress": False,
                "src_locations": [file1],
                "sourceCompression": "none",
                "stageInfo": {
                    "creds": {
                        "AWS_SECRET_KEY": "secret key",
                        "AWS_KEY_ID": "secret id",
                        "AWS_TOKEN": "",
                    },
                    "location": "some_bucket",
                    "region": "no_region",
                    "locationType": "S3",
                    "path": "remote_loc",
                    "endPoint": "",
                },
            },
            "success": True,
        },
    )
    exc = Exception("Stop executing")

    def mock_transfer_accelerate_config(
        self: SnowflakeS3RestClient,
        use_accelerate_endpoint: bool | None = None,
    ) -> bool:
        self.endpoint = f"https://{self.s3location.bucket_name}.s3.awsamazon.com"
        return False

    with mock.patch(
        "snowflake.connector.s3_storage_client.SnowflakeS3RestClient._has_expired_token",
        return_value=True,
    ):
        with mock.patch(
            "snowflake.connector.s3_storage_client.SnowflakeS3RestClient.transfer_accelerate_config",
            mock_transfer_accelerate_config,
        ):
            with mock.patch(
                "snowflake.connector.file_transfer_agent.StorageCredential.update",
                side_effect=exc,
            ) as mock_update:
                rest_client.execute()
                assert mock_update.called
                assert rest_client._results[0].error_details is exc


def test_get_header_expiry_error():
    """Tests whether token expiry error is handled as expected when getting header."""
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
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": "", "AWS_TOKEN": ""}
    rest_client = SnowflakeS3RestClient(
        meta,
        StorageCredential(
            creds,
            MagicMock(autospec=SnowflakeConnection),
            "PUT file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        8 * megabyte,
    )
    resp = MagicMock(
        autospec=Response,
        status_code=400,
        text=f"<Error><Code>{EXPIRED_TOKEN}</Code></Error>",
    )
    from snowflake.connector.storage_client import METHODS

    with mock.patch.dict(METHODS, HEAD=MagicMock(return_value=resp)):
        exc = Exception("stop execution")
        with mock.patch.object(rest_client.credentials, "update", side_effect=exc):
            with pytest.raises(Exception) as caught_exc:
                rest_client.get_file_header("file.txt")
            assert caught_exc.value is exc


def test_get_header_unknown_error(caplog):
    """Tests whether unexpected errors are handled as expected when getting header."""
    caplog.set_level(logging.DEBUG, "snowflake.connector")
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
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": "", "AWS_TOKEN": ""}
    rest_client = SnowflakeS3RestClient(
        meta,
        StorageCredential(
            creds,
            MagicMock(autospec=SnowflakeConnection),
            "PUT file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        8 * megabyte,
    )
    resp = Response()
    # dont' use transient error codes
    resp.status_code = 555
    from snowflake.connector.storage_client import METHODS

    with mock.patch.dict(METHODS, HEAD=MagicMock(return_value=resp)):
        with pytest.raises(HTTPError, match="555 Server Error"):
            rest_client.get_file_header("file.txt")


def test_upload_expiry_error():
    """Tests whether token expiry error is handled as expected when uploading."""
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
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": "", "AWS_TOKEN": ""}
    rest_client = SnowflakeS3RestClient(
        meta,
        StorageCredential(
            creds,
            MagicMock(autospec=SnowflakeConnection),
            "PUT file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        8 * megabyte,
    )
    resp = MagicMock(
        autospec=Response,
        status_code=400,
        text=f"<Error><Code>{EXPIRED_TOKEN}</Code></Error>",
    )
    from snowflake.connector.storage_client import METHODS

    with mock.patch.dict(METHODS, PUT=MagicMock(return_value=resp)):
        exc = Exception("stop execution")
        with mock.patch.object(rest_client.credentials, "update", side_effect=exc):
            with mock.patch(
                "snowflake.connector.storage_client.SnowflakeStorageClient.preprocess"
            ):
                rest_client.prepare_upload()
            with pytest.raises(Exception) as caught_exc:
                rest_client.upload_chunk(0)
            assert caught_exc.value is exc


def test_upload_unknown_error():
    """Tests whether unknown errors are handled as expected when uploading."""
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
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": "", "AWS_TOKEN": ""}
    rest_client = SnowflakeS3RestClient(
        meta,
        StorageCredential(
            creds,
            MagicMock(autospec=SnowflakeConnection),
            "PUT file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        8 * megabyte,
    )
    resp = Response()
    resp.status_code = 555
    from snowflake.connector.storage_client import METHODS

    with mock.patch.dict(METHODS, PUT=MagicMock(return_value=resp)):
        exc = Exception("stop execution")
        with mock.patch.object(rest_client.credentials, "update", side_effect=exc):
            with mock.patch(
                "snowflake.connector.storage_client.SnowflakeStorageClient.preprocess"
            ):
                rest_client.prepare_upload()
            with pytest.raises(HTTPError, match="555 Server Error"):
                rest_client.upload_chunk(0)


def test_download_expiry_error():
    """Tests whether token expiry error is handled as expected when downloading."""
    meta_info = {
        "name": "data1.txt.gz",
        "stage_location_type": "S3",
        "no_sleeping_time": True,
        "put_callback": None,
        "put_callback_output_stream": None,
        SHA256_DIGEST: "123456789abcdef",
        "dst_file_name": "data1.txt.gz",
        "src_file_name": "path/to/put_get_1.txt",
        "overwrite": True,
    }
    meta = SnowflakeFileMeta(**meta_info)
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": "", "AWS_TOKEN": ""}
    rest_client = SnowflakeS3RestClient(
        meta,
        StorageCredential(
            creds,
            MagicMock(autospec=SnowflakeConnection),
            "GET file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        8 * megabyte,
    )
    resp = MagicMock(
        autospec=Response,
        status_code=400,
        text=f"<Error><Code>{EXPIRED_TOKEN}</Code></Error>",
    )
    from snowflake.connector.storage_client import METHODS

    with mock.patch.dict(METHODS, GET=MagicMock(return_value=resp)):
        exc = Exception("stop execution")
        with mock.patch.object(rest_client.credentials, "update", side_effect=exc):
            with pytest.raises(Exception) as caught_exc:
                rest_client.download_chunk(0)
            assert caught_exc.value is exc


def test_download_unknown_error(caplog):
    """Tests whether an unknown error is handled as expected when downloading."""
    caplog.set_level(logging.DEBUG, "snowflake.connector")
    agent = SnowflakeFileTransferAgent(
        MagicMock(),
        "get @~/f /tmp",
        {
            "data": {
                "command": "DOWNLOAD",
                "src_locations": ["/tmp/a"],
                "stageInfo": {
                    "locationType": "S3",
                    "location": "",
                    "creds": {"AWS_SECRET_KEY": "", "AWS_KEY_ID": "", "AWS_TOKEN": ""},
                    "region": "",
                    "endPoint": None,
                },
                "localLocation": "/tmp",
            }
        },
    )
    resp = Response()
    resp.status_code = 400
    resp.reason = "No, just chuck testing..."
    with mock.patch(
        "snowflake.connector.s3_storage_client.SnowflakeS3RestClient._send_request_with_authentication_and_retry",
        return_value=resp,
    ), mock.patch(
        "snowflake.connector.file_transfer_agent.SnowflakeFileTransferAgent._transfer_accelerate_config",
        side_effect=None,
    ):
        agent.execute()
    assert (
        str(agent._file_metadata[0].error_details)
        == "400 Client Error: No, just chuck testing... for url: None"
    )
    assert verify_log_tuple(
        "snowflake.connector.storage_client",
        logging.ERROR,
        re.compile("Failed to download a file: .*a"),
        caplog.record_tuples,
    )


def test_download_retry_exceeded_error():
    """Tests whether a retry exceeded error is handled as expected when downloading."""
    meta_info = {
        "name": "data1.txt.gz",
        "stage_location_type": "S3",
        "no_sleeping_time": True,
        "put_callback": None,
        "put_callback_output_stream": None,
        SHA256_DIGEST: "123456789abcdef",
        "dst_file_name": "data1.txt.gz",
        "src_file_name": "path/to/put_get_1.txt",
        "overwrite": True,
    }
    meta = SnowflakeFileMeta(**meta_info)
    creds = {"AWS_SECRET_KEY": "", "AWS_KEY_ID": "", "AWS_TOKEN": ""}
    rest_client = SnowflakeS3RestClient(
        meta,
        StorageCredential(
            creds,
            MagicMock(autospec=SnowflakeConnection),
            "GET file:/tmp/file.txt @~",
        ),
        {
            "locationType": "AWS",
            "location": "bucket/path",
            "creds": creds,
            "region": "test",
            "endPoint": None,
        },
        8 * megabyte,
    )
    rest_client.SLEEP_UNIT = 0
    resp = Response()
    resp.status_code = 500  # Use a transient error code
    from snowflake.connector.storage_client import METHODS

    with mock.patch.dict(METHODS, GET=MagicMock(return_value=resp)):
        with mock.patch.object(rest_client.credentials, "update"):
            with pytest.raises(
                RequestExceedMaxRetryError,
                match=r"GET with url .* failed for exceeding maximum retries",
            ):
                rest_client.download_chunk(0)
