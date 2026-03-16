#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from logging import getLogger
from random import choice
from string import hexdigits
from typing import TYPE_CHECKING, Any, NamedTuple

from .compat import quote
from .constants import FileHeader, ResultStatus
from .encryption_util import EncryptionMetadata
from .storage_client import SnowflakeStorageClient
from .vendored import requests

if TYPE_CHECKING:  # pragma: no cover
    from .file_transfer_agent import SnowflakeFileMeta, StorageCredential

logger = getLogger(__name__)


class AzureLocation(NamedTuple):
    container_name: str
    path: str


TOKEN_EXPIRATION_ERR_MESSAGE = (
    "Signature not valid in the specified time frame",
    "Server failed to authenticate the request.",
)
SFCDIGEST = "x-ms-meta-sfcdigest"
ENCRYPTION_DATA = "x-ms-meta-encryptiondata"
MATDESC = "x-ms-meta-matdesc"


class SnowflakeAzureRestClient(SnowflakeStorageClient):
    def __init__(
        self,
        meta: SnowflakeFileMeta,
        credentials: StorageCredential | None,
        chunk_size: int,
        stage_info: dict[str, Any],
        use_s3_regional_url: bool = False,
    ) -> None:
        super().__init__(meta, stage_info, chunk_size, credentials=credentials)
        end_point: str = stage_info["endPoint"]
        if end_point.startswith("blob."):
            end_point = end_point[len("blob.") :]
        self.endpoint = end_point
        self.storage_account: str = stage_info["storageAccount"]
        self.azure_location = self.extract_container_name_and_path(
            stage_info["location"]
        )
        self.block_ids: list[str] = []

    @staticmethod
    def extract_container_name_and_path(stage_location: str) -> AzureLocation:
        stage_location = os.path.expanduser(stage_location)
        container_name = stage_location
        path = ""

        # split stage location as bucket name and path
        if "/" in stage_location:
            container_name, _, path = stage_location.partition("/")
            if path and not path.endswith("/"):
                path += "/"

        return AzureLocation(container_name=container_name, path=path)

    def _has_expired_token(self, response: requests.Response) -> bool:
        return response.status_code == 403 and any(
            message in response.reason for message in TOKEN_EXPIRATION_ERR_MESSAGE
        )

    def _send_request_with_authentication_and_retry(
        self,
        verb: str,
        url: str,
        retry_id: int | str,
        headers: dict[str, Any] = None,
        data: bytes = None,
    ) -> requests.Response:
        if not headers:
            headers = {}

        def generate_authenticated_url_and_rest_args() -> tuple[bytes, dict[str, Any]]:
            curtime = datetime.utcnow()
            timestamp = curtime.strftime("YYYY-MM-DD")
            sas_token = self.credentials.creds["AZURE_SAS_TOKEN"]
            if sas_token and sas_token.startswith("?"):
                sas_token = sas_token[1:]
            if "?" in url:
                _url = url + "&" + sas_token
            else:
                _url = url + "?" + sas_token
            headers["Date"] = timestamp
            rest_args = {"headers": headers}
            if data:
                rest_args["data"] = data
            return _url, rest_args

        return self._send_request_with_retry(
            verb, generate_authenticated_url_and_rest_args, retry_id
        )

    def get_file_header(self, filename: str) -> FileHeader | None:
        """Gets Azure file properties."""
        container_name = quote(self.azure_location.container_name)
        path = quote(self.azure_location.path) + quote(filename)
        meta = self.meta
        # HTTP HEAD request
        url = f"https://{self.storage_account}.blob.{self.endpoint}/{container_name}/{path}"
        retry_id = "HEAD"
        self.retry_count[retry_id] = 0
        r = self._send_request_with_authentication_and_retry("HEAD", url, retry_id)
        if r.status_code == 200:
            meta.result_status = ResultStatus.UPLOADED
            enc_data_str = r.headers.get(ENCRYPTION_DATA)
            encryption_data = None if enc_data_str is None else json.loads(enc_data_str)
            encryption_metadata = (
                None
                if not encryption_data
                else EncryptionMetadata(
                    key=encryption_data["WrappedContentKey"]["EncryptedKey"],
                    iv=encryption_data["ContentEncryptionIV"],
                    matdesc=r.headers.get(MATDESC),
                )
            )
            return FileHeader(
                digest=r.headers.get("x-ms-meta-sfcdigest"),
                content_length=int(r.headers.get("Content-Length")),
                encryption_metadata=encryption_metadata,
            )
        elif r.status_code == 404:
            meta.result_status = ResultStatus.NOT_FOUND_FILE
            return FileHeader(
                digest=None, content_length=None, encryption_metadata=None
            )
        else:
            r.raise_for_status()

    def _prepare_file_metadata(self) -> dict[str, str | None]:
        azure_metadata = {
            SFCDIGEST: self.meta.sha256_digest,
        }
        encryption_metadata = self.encryption_metadata
        if encryption_metadata:
            azure_metadata.update(
                {
                    ENCRYPTION_DATA: json.dumps(
                        {
                            "EncryptionMode": "FullBlob",
                            "WrappedContentKey": {
                                "KeyId": "symmKey1",
                                "EncryptedKey": encryption_metadata.key,
                                "Algorithm": "AES_CBC_256",
                            },
                            "EncryptionAgent": {
                                "Protocol": "1.0",
                                "EncryptionAlgorithm": "AES_CBC_128",
                            },
                            "ContentEncryptionIV": encryption_metadata.iv,
                            "KeyWrappingMetadata": {"EncryptionLibrary": "Java 5.3.0"},
                        }
                    ),
                    MATDESC: encryption_metadata.matdesc,
                }
            )
        return azure_metadata

    def _initiate_multipart_upload(self) -> None:
        self.block_ids = [
            "".join(choice(hexdigits) for _ in range(20))
            for _ in range(self.num_of_chunks)
        ]

    def _upload_chunk(self, chunk_id: int, chunk: bytes) -> None:
        container_name = quote(self.azure_location.container_name)
        path = quote(self.azure_location.path + self.meta.dst_file_name.lstrip("/"))

        if self.num_of_chunks > 1:
            block_id = self.block_ids[chunk_id]
            url = (
                f"https://{self.storage_account}.blob.{self.endpoint}/{container_name}/{path}?comp=block"
                f"&blockid={block_id}"
            )
            headers = {"Content-Length": str(len(chunk))}
            r = self._send_request_with_authentication_and_retry(
                "PUT", url, chunk_id, headers=headers, data=chunk
            )
        else:
            # single request
            azure_metadata = self._prepare_file_metadata()
            url = f"https://{self.storage_account}.blob.{self.endpoint}/{container_name}/{path}"
            headers = {
                "x-ms-blob-type": "BlockBlob",
                "Content-Encoding": "utf-8",
            }
            headers.update(azure_metadata)
            r = self._send_request_with_authentication_and_retry(
                "PUT", url, chunk_id, headers=headers, data=chunk
            )
        r.raise_for_status()  # expect status code 201

    def _complete_multipart_upload(self) -> None:
        container_name = quote(self.azure_location.container_name)
        path = quote(self.azure_location.path + self.meta.dst_file_name.lstrip("/"))
        url = (
            f"https://{self.storage_account}.blob.{self.endpoint}/{container_name}/{path}?comp"
            f"=blocklist"
        )
        root = ET.Element("BlockList")
        for block_id in self.block_ids:
            part = ET.Element("Latest")
            part.text = block_id
            root.append(part)
        headers = {"x-ms-blob-content-encoding": "utf-8"}
        azure_metadata = self._prepare_file_metadata()
        headers.update(azure_metadata)
        retry_id = "COMPLETE"
        self.retry_count[retry_id] = 0
        r = self._send_request_with_authentication_and_retry(
            "PUT", url, "COMPLETE", headers=headers, data=ET.tostring(root)
        )
        r.raise_for_status()  # expects status code 201

    def download_chunk(self, chunk_id: int) -> None:
        container_name = quote(self.azure_location.container_name)
        path = quote(self.azure_location.path + self.meta.src_file_name.lstrip("/"))
        url = f"https://{self.storage_account}.blob.{self.endpoint}/{container_name}/{path}"
        if self.num_of_chunks > 1:
            chunk_size = self.chunk_size
            if chunk_id < self.num_of_chunks - 1:
                _range = f"{chunk_id * chunk_size}-{(chunk_id+1)*chunk_size-1}"
            else:
                _range = f"{chunk_id * chunk_size}-"
            headers = {"Range": f"bytes={_range}"}
            r = self._send_request_with_authentication_and_retry(
                "GET", url, chunk_id, headers=headers
            )  # expect 206
        else:
            # single request
            r = self._send_request_with_authentication_and_retry("GET", url, chunk_id)
        if r.status_code in (200, 206):
            self.write_downloaded_chunk(chunk_id, r.content)
        r.raise_for_status()
