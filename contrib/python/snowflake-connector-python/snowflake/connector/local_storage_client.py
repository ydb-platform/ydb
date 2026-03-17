#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import os
from logging import getLogger
from math import ceil
from typing import TYPE_CHECKING, Any

from .constants import ResultStatus
from .storage_client import SnowflakeStorageClient
from .vendored import requests

if TYPE_CHECKING:  # pragma: no cover
    from .file_transfer_agent import SnowflakeFileMeta

logger = getLogger(__name__)


class SnowflakeLocalStorageClient(SnowflakeStorageClient):
    def __init__(
        self,
        meta: SnowflakeFileMeta,
        stage_info: dict[str, Any],
        chunk_size: int,
        use_s3_regional_url: bool = False,
    ) -> None:
        super().__init__(meta, stage_info, chunk_size)
        self.data_file = meta.src_file_name
        self.full_dst_file_name: str = os.path.join(
            stage_info["location"], os.path.basename(meta.dst_file_name)
        )

    def get_file_header(self, filename: str) -> None:
        """
        Notes:
            Checks whether the file exits in specified directory, does not return FileHeader
        """
        # TODO return a FileHeader sometime
        target_dir = os.path.join(
            os.path.expanduser(self.stage_info["location"]),
            filename,
        )
        if os.path.isfile(target_dir):
            self.meta.result_status = ResultStatus.UPLOADED
        else:
            self.meta.result_status = ResultStatus.NOT_FOUND_FILE

    def download_chunk(self, chunk_id: int) -> None:
        with open(self.full_dst_file_name, "rb") as sfd:
            with open(
                os.path.join(
                    self.meta.local_location, os.path.basename(self.meta.dst_file_name)
                ),
                "wb",
            ) as tfd:
                tfd.seek(chunk_id * self.chunk_size)
                sfd.seek(chunk_id * self.chunk_size)
                tfd.write(sfd.read(self.chunk_size))

    def finish_download(self) -> None:
        self.meta.dst_file_size = os.stat(self.full_dst_file_name).st_size
        self.meta.result_status = ResultStatus.DOWNLOADED

    def _has_expired_token(self, response: requests.Response) -> bool:
        return False

    def prepare_upload(self) -> None:
        super().prepare_upload()
        if (
            self.meta.upload_size < self.meta.multipart_threshold
            or not self.chunked_transfer
        ):
            self.num_of_chunks = 1
        else:
            self.num_of_chunks = ceil(self.meta.upload_size / self.chunk_size)

    def _upload_chunk(self, chunk_id: int, chunk: bytes) -> None:
        with open(self.full_dst_file_name, "wb") as tfd:
            tfd.seek(chunk_id * self.chunk_size)
            tfd.write(chunk)

    def finish_upload(self) -> None:
        self.meta.result_status = ResultStatus.UPLOADED
        self.meta.dst_file_size = self.meta.upload_size
