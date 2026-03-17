#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import json
import os
from logging import getLogger
from typing import TYPE_CHECKING, Any, NamedTuple

from ._sql_util import is_put_statement
from .compat import quote
from .constants import (
    FILE_PROTOCOL,
    HTTP_HEADER_CONTENT_ENCODING,
    FileHeader,
    ResultStatus,
    kilobyte,
)
from .encryption_util import EncryptionMetadata
from .storage_client import SnowflakeStorageClient
from .vendored import requests

if TYPE_CHECKING:  # pragma: no cover
    from .connection import SnowflakeConnection
    from .file_transfer_agent import SnowflakeFileMeta, StorageCredential

logger = getLogger(__name__)

GCS_METADATA_PREFIX = "x-goog-meta-"
GCS_METADATA_SFC_DIGEST = GCS_METADATA_PREFIX + "sfc-digest"
GCS_METADATA_MATDESC_KEY = GCS_METADATA_PREFIX + "matdesc"
GCS_METADATA_ENCRYPTIONDATAPROP = GCS_METADATA_PREFIX + "encryptiondata"
GCS_FILE_HEADER_DIGEST = "gcs-file-header-digest"
GCS_FILE_HEADER_CONTENT_LENGTH = "gcs-file-header-content-length"
GCS_FILE_HEADER_ENCRYPTION_METADATA = "gcs-file-header-encryption-metadata"
CONTENT_CHUNK_SIZE = 10 * kilobyte
ACCESS_TOKEN = "GCS_ACCESS_TOKEN"


class GcsLocation(NamedTuple):
    bucket_name: str
    path: str


class SnowflakeGCSRestClient(SnowflakeStorageClient):
    def __init__(
        self,
        meta: SnowflakeFileMeta,
        credentials: StorageCredential,
        stage_info: dict[str, Any],
        cnx: SnowflakeConnection,
        command: str,
        use_s3_regional_url=False,
    ) -> None:
        """Creates a client object with given stage credentials.

        Args:
            stage_info: Access credentials and info of a stage.

        Returns:
            The client to communicate with GCS.
        """
        super().__init__(
            meta, stage_info, -1, credentials=credentials, chunked_transfer=False
        )
        self.stage_info = stage_info
        self._command = command
        self.meta = meta
        self._cursor = cnx.cursor()
        # presigned_url in meta is for downloading
        self.presigned_url: str = meta.presigned_url or stage_info.get("presignedUrl")
        self.security_token = credentials.creds.get("GCS_ACCESS_TOKEN")

        if self.security_token:
            logger.debug(f"len(GCS_ACCESS_TOKEN): {len(self.security_token)}")
        else:
            logger.debug("No access token received from GS, requesting presigned url")
            self._update_presigned_url()

    def _has_expired_token(self, response: requests.Response) -> bool:
        return self.security_token and response.status_code == 401

    def _has_expired_presigned_url(self, response: requests.Response) -> bool:
        # Presigned urls can be generated for any xml-api operation
        # offered by GCS. Hence the error codes expected are similar
        # to xml api.
        # https://cloud.google.com/storage/docs/xml-api/reference-status

        presigned_url_expired = (
            not self.security_token
        ) and response.status_code == 400
        if presigned_url_expired and self.last_err_is_presigned_url:
            logger.debug("Presigned url expiration error two times in a row.")
            response.raise_for_status()
        self.last_err_is_presigned_url = presigned_url_expired
        return presigned_url_expired

    def _upload_chunk(self, chunk_id: int, chunk: bytes) -> None:
        meta = self.meta

        content_encoding = ""
        if meta.dst_compression_type is not None:
            content_encoding = meta.dst_compression_type.name.lower()

        # We set the contentEncoding to blank for GZIP files. We don't
        # want GCS to think our gzip files are gzips because it makes
        # them download uncompressed, and none of the other providers do
        # that. There's essentially no way for us to prevent that
        # behavior. Bad Google.
        if content_encoding and content_encoding == "gzip":
            content_encoding = ""

        gcs_headers = {
            HTTP_HEADER_CONTENT_ENCODING: content_encoding,
            GCS_METADATA_SFC_DIGEST: meta.sha256_digest,
        }

        if self.encryption_metadata:
            gcs_headers.update(
                {
                    GCS_METADATA_ENCRYPTIONDATAPROP: json.dumps(
                        {
                            "EncryptionMode": "FullBlob",
                            "WrappedContentKey": {
                                "KeyId": "symmKey1",
                                "EncryptedKey": self.encryption_metadata.key,
                                "Algorithm": "AES_CBC_256",
                            },
                            "EncryptionAgent": {
                                "Protocol": "1.0",
                                "EncryptionAlgorithm": "AES_CBC_256",
                            },
                            "ContentEncryptionIV": self.encryption_metadata.iv,
                            "KeyWrappingMetadata": {"EncryptionLibrary": "Java 5.3.0"},
                        }
                    ),
                    GCS_METADATA_MATDESC_KEY: self.encryption_metadata.matdesc,
                }
            )

        def generate_url_and_rest_args() -> tuple[
            str, dict[str, dict[str | Any, str | None] | bytes]
        ]:
            if not self.presigned_url:
                upload_url = self.generate_file_url(
                    self.stage_info["location"], meta.dst_file_name.lstrip("/")
                )
                access_token = self.security_token
            else:
                upload_url = self.presigned_url
                access_token: str | None = None
            if access_token:
                gcs_headers.update({"Authorization": f"Bearer {access_token}"})
            rest_args = {"headers": gcs_headers, "data": chunk}
            return upload_url, rest_args

        response = self._send_request_with_retry(
            "PUT", generate_url_and_rest_args, chunk_id
        )
        response.raise_for_status()
        meta.gcs_file_header_digest = gcs_headers[GCS_METADATA_SFC_DIGEST]
        meta.gcs_file_header_content_length = meta.upload_size
        meta.gcs_file_header_encryption_metadata = json.loads(
            gcs_headers.get(GCS_METADATA_ENCRYPTIONDATAPROP, "null")
        )

    def download_chunk(self, chunk_id: int) -> None:
        meta = self.meta

        def generate_url_and_rest_args() -> tuple[
            str, dict[str, dict[str, str] | bool]
        ]:
            gcs_headers = {}
            if not self.presigned_url:
                download_url = self.generate_file_url(
                    self.stage_info["location"], meta.src_file_name.lstrip("/")
                )
                access_token = self.security_token
                gcs_headers["Authorization"] = f"Bearer {access_token}"
            else:
                download_url = self.presigned_url
            rest_args = {"headers": gcs_headers, "stream": True}
            return download_url, rest_args

        response = self._send_request_with_retry(
            "GET", generate_url_and_rest_args, chunk_id
        )
        response.raise_for_status()

        self.write_downloaded_chunk(chunk_id, response.content)

        encryption_metadata = None

        if response.headers.get(GCS_METADATA_ENCRYPTIONDATAPROP, None):
            encryptiondata = json.loads(
                response.headers[GCS_METADATA_ENCRYPTIONDATAPROP]
            )

            if encryptiondata:
                encryption_metadata = EncryptionMetadata(
                    key=encryptiondata["WrappedContentKey"]["EncryptedKey"],
                    iv=encryptiondata["ContentEncryptionIV"],
                    matdesc=response.headers[GCS_METADATA_MATDESC_KEY]
                    if GCS_METADATA_MATDESC_KEY in response.headers
                    else None,
                )

        meta.gcs_file_header_digest = response.headers.get(GCS_METADATA_SFC_DIGEST)
        meta.gcs_file_header_content_length = len(response.content)
        meta.gcs_file_header_encryption_metadata = encryption_metadata

    def finish_download(self) -> None:
        super().finish_download()
        # Sadly, we can only determine the src file size after we've
        # downloaded it, unlike the other cloud providers where the
        # metadata can be read beforehand.
        self.meta.src_file_size = os.path.getsize(self.intermediate_dst_path)

    def _update_presigned_url(self) -> None:
        """Updates the file metas with presigned urls if any.

        Currently only the file metas generated for PUT/GET on a GCP account need the presigned urls.
        """
        logger.debug("Updating presigned url")

        # Rewrite the command such that a new PUT call is made for each file
        # represented by the regex (if present) separately. This is the only
        # way to get the presigned url for that file.
        file_path_to_be_replaced = self._get_local_file_path_from_put_command()

        if not file_path_to_be_replaced:
            # This prevents GET statements to proceed
            return

        # At this point the connector has already figured out and
        # validated that the local file exists and has also decided
        # upon the destination file name and the compression type.
        # The only thing that's left to do is to get the presigned
        # url for the destination file. If the command originally
        # referred to a single file, then the presigned url got in
        # that case is simply ignore, since the file name is not what
        # we want.

        # GS only looks at the file name at the end of local file
        # path to figure out the remote object name. Hence the prefix
        # for local path is not necessary in the reconstructed command.
        file_path_to_replace_with = self.meta.dst_file_name
        command_with_single_file = self._command
        command_with_single_file = command_with_single_file.replace(
            file_path_to_be_replaced, file_path_to_replace_with
        )

        logger.debug("getting presigned url for %s", file_path_to_replace_with)
        ret = self._cursor._execute_helper(command_with_single_file)

        stage_info = ret.get("data", dict()).get("stageInfo", dict())
        self.meta.presigned_url = stage_info.get("presignedUrl")
        self.presigned_url = stage_info.get("presignedUrl")

    def _get_local_file_path_from_put_command(self) -> str | None:
        """Get the local file path from PUT command (Logic adopted from JDBC, written by Polita).

        Args:
            command: Command to be parsed and get the local file path out of.

        Returns:
            The local file path.
        """
        command = self._command
        if FILE_PROTOCOL not in self._command or not is_put_statement(command):
            return None

        file_path_begin_index = command.find(FILE_PROTOCOL)
        is_file_path_quoted = command[file_path_begin_index - 1] == "'"
        file_path_begin_index += len(FILE_PROTOCOL)

        file_path = ""

        if is_file_path_quoted:
            file_path_end_index = command.find("'", file_path_begin_index)

            if file_path_end_index > file_path_begin_index:
                file_path = command[file_path_begin_index:file_path_end_index]
        else:
            index_list = []
            for delimiter in [" ", "\n", ";"]:
                index = command.find(delimiter, file_path_begin_index)
                if index != -1:
                    index_list += [index]

            file_path_end_index = min(index_list) if index_list else -1

            if file_path_end_index > file_path_begin_index:
                file_path = command[file_path_begin_index:file_path_end_index]
            elif file_path_end_index == -1:
                file_path = command[file_path_begin_index:]

        return file_path

    def get_file_header(self, filename: str) -> FileHeader | None:
        """Gets the remote file's metadata.

        Args:
            filename: Not applicable to GCS.

        Returns:
            The file header, with expected properties populated or None, based on how the request goes with the
            storage provider.

        Notes:
            Sometimes this method is called to verify that the file has indeed been uploaded. In cases of presigned
            url, we have no way of verifying that, except with the http status code of 200 which we have already
            confirmed and set the meta.result_status = UPLOADED/DOWNLOADED.
        """
        meta = self.meta
        if (
            meta.result_status == ResultStatus.UPLOADED
            or meta.result_status == ResultStatus.DOWNLOADED
        ):
            return FileHeader(
                digest=meta.gcs_file_header_digest,
                content_length=meta.gcs_file_header_content_length,
                encryption_metadata=meta.gcs_file_header_encryption_metadata,
            )
        elif self.presigned_url:
            meta.result_status = ResultStatus.NOT_FOUND_FILE
        else:

            def generate_url_and_authenticated_headers():
                url = self.generate_file_url(
                    self.stage_info["location"], filename.lstrip("/")
                )
                gcs_headers = {"Authorization": f"Bearer {self.security_token}"}
                rest_args = {"headers": gcs_headers}
                return url, rest_args

            retry_id = "HEAD"
            self.retry_count[retry_id] = 0
            response = self._send_request_with_retry(
                "HEAD", generate_url_and_authenticated_headers, retry_id
            )
            if response.status_code == 404:
                meta.result_status = ResultStatus.NOT_FOUND_FILE
                return None
            elif response.status_code == 200:
                digest = response.headers.get(GCS_METADATA_SFC_DIGEST, None)
                content_length = int(response.headers.get("content-length", "0"))

                encryption_metadata = EncryptionMetadata("", "", "")
                if response.headers.get(GCS_METADATA_ENCRYPTIONDATAPROP, None):
                    encryption_data = json.loads(
                        response.headers[GCS_METADATA_ENCRYPTIONDATAPROP]
                    )

                    if encryption_data:
                        encryption_metadata = EncryptionMetadata(
                            key=encryption_data["WrappedContentKey"]["EncryptedKey"],
                            iv=encryption_data["ContentEncryptionIV"],
                            matdesc=response.headers[GCS_METADATA_MATDESC_KEY]
                            if GCS_METADATA_MATDESC_KEY in response.headers
                            else None,
                        )
                meta.result_status = ResultStatus.UPLOADED
                return FileHeader(
                    digest=digest,
                    content_length=content_length,
                    encryption_metadata=encryption_metadata,
                )
            response.raise_for_status()
            return None

    @staticmethod
    def extract_bucket_name_and_path(stage_location: str) -> GcsLocation:
        container_name = stage_location
        path = ""

        # split stage location as bucket name and path
        if "/" in stage_location:
            container_name = stage_location[0 : stage_location.index("/")]
            path = stage_location[stage_location.index("/") + 1 :]
            if path and not path.endswith("/"):
                path += "/"

        return GcsLocation(bucket_name=container_name, path=path)

    @staticmethod
    def generate_file_url(stage_location: str, filename: str) -> str:
        gcs_location = SnowflakeGCSRestClient.extract_bucket_name_and_path(
            stage_location
        )
        full_file_path = f"{gcs_location.path}{filename}"
        return f"https://storage.googleapis.com/{gcs_location.bucket_name}/{quote(full_file_path)}"
