#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import binascii
import glob
import mimetypes
import os
import sys
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from logging import getLogger
from time import time
from typing import IO, TYPE_CHECKING, Any, Callable, TypeVar

from .azure_storage_client import SnowflakeAzureRestClient
from .compat import GET_CWD, IS_WINDOWS
from .constants import (
    AZURE_FS,
    CMD_TYPE_DOWNLOAD,
    CMD_TYPE_UPLOAD,
    GCS_FS,
    LOCAL_FS,
    S3_FS,
    ResultStatus,
    megabyte,
)
from .converter_snowsql import SnowflakeConverterSnowSQL
from .errorcode import (
    ER_COMPRESSION_NOT_SUPPORTED,
    ER_FAILED_TO_DOWNLOAD_FROM_STAGE,
    ER_FAILED_TO_UPLOAD_TO_STAGE,
    ER_FILE_NOT_EXISTS,
    ER_INTERNAL_NOT_MATCH_ENCRYPT_MATERIAL,
    ER_INVALID_STAGE_FS,
    ER_INVALID_STAGE_LOCATION,
    ER_LOCAL_PATH_NOT_DIRECTORY,
)
from .errors import (
    DatabaseError,
    Error,
    InternalError,
    OperationalError,
    ProgrammingError,
)
from .file_compression_type import CompressionTypes, lookup_by_mime_sub_type
from .gcs_storage_client import SnowflakeGCSRestClient
from .local_storage_client import SnowflakeLocalStorageClient
from .s3_storage_client import SnowflakeS3RestClient
from .storage_client import SnowflakeFileEncryptionMaterial, SnowflakeStorageClient

if TYPE_CHECKING:  # pragma: no cover
    from .connection import SnowflakeConnection
    from .cursor import SnowflakeCursor
    from .file_compression_type import CompressionType

VALID_STORAGE = [LOCAL_FS, S3_FS, AZURE_FS, GCS_FS]

INJECT_WAIT_IN_PUT = 0

logger = getLogger(__name__)


def result_text_column_desc(name):
    return {
        "name": name,
        "type": "text",
        "length": 16777216,
        "precision": None,
        "scale": None,
        "nullable": False,
    }


def result_fixed_column_desc(name):
    return {
        "name": name,
        "type": "fixed",
        "length": 5,
        "precision": 0,
        "scale": 0,
        "nullable": False,
    }


# TODO: rewrite, we use this class to store information about file transfers
#  It'd make more sense to define a new object, like FileTransferMeta that then has
#  more FileMetas inside of it. This would help in some cases, where for example
#  consider the case where we run into an unrecoverable error for the whole transfer
#  job and we need to convey an error to the main thread and that error needs to be
#  raised by the main thread. Where should this go? Currently the answer could be
#  all of the current FileMetas. Hmmm...
@dataclass
class SnowflakeFileMeta:
    """Class to keep track of information necessary for file operations."""

    name: str
    src_file_name: str
    stage_location_type: str
    result_status: ResultStatus | None = None

    sfagent: SnowflakeFileTransferAgent | None = None
    put_callback: type[SnowflakeProgressPercentage] | None = None
    put_azure_callback: type[SnowflakeProgressPercentage] | None = None
    put_callback_output_stream: IO[str] | None = None
    get_callback: type[SnowflakeProgressPercentage] | None = None
    get_azure_callback: type[SnowflakeProgressPercentage] | None = None
    get_callback_output_stream: IO[str] | None = None
    show_progress_bar: bool = False
    multipart_threshold: int = 67108864  # Historical value
    presigned_url: str | None = None
    overwrite: bool = False
    sha256_digest: str | None = None
    upload_size: int | None = None
    real_src_file_name: str | None = None
    error_details: Exception | None = None
    last_error: Exception | None = None
    no_sleeping_time: bool = False
    gcs_file_header_digest: str | None = None
    gcs_file_header_content_length: int | None = None
    gcs_file_header_encryption_metadata: dict[str, Any] | None = None

    encryption_material: SnowflakeFileEncryptionMaterial | None = None
    # Specific to Uploads only
    src_file_size: int = 0
    src_compression_type: CompressionType | None = None
    dst_compression_type: CompressionType = None
    require_compress: bool = False
    dst_file_name: str | None = None
    dst_file_size: int = -1
    intermediate_stream: IO[bytes] | None = None
    src_stream: IO[bytes] | None = None
    # Specific to Downloads only
    local_location: str | None = None


def _update_progress(
    file_name: str,
    start_time: float,
    total_size: float,
    progress: float | int,
    output_stream: IO | None = sys.stdout,
    show_progress_bar: bool | None = True,
) -> bool:
    bar_length = 10  # Modify this to change the length of the progress bar
    total_size /= megabyte
    status = ""
    elapsed_time = time() - start_time
    throughput = (total_size / elapsed_time) if elapsed_time != 0.0 else 0.0
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = f"Done ({elapsed_time:.3f}s, {throughput:.2f}MB/s).\r\n"
    if not status and show_progress_bar:
        status = f"({elapsed_time:.3f}s, {throughput:.2f}MB/s)"
    if status:
        block = int(round(bar_length * progress))

        text = (
            f"\r{file_name}({total_size:.2f}MB): "
            f"[{'#' * block + '-' * (bar_length - block)}] "
            f"{progress * 100.0:.2f}% {status}"
        )
        output_stream.write(text)
        output_stream.flush()
    logger.debug(
        f"filename: {file_name}, start_time: {start_time}, total_size: {total_size}, "
        f"progress: {progress}, show_progress_bar: {show_progress_bar}"
    )
    return progress == 1.0


def percent(seen_so_far: int, size: float) -> float:
    return 1.0 if seen_so_far >= size or size <= 0 else float(seen_so_far / size)


class SnowflakeProgressPercentage:
    """Built-in Progress bar for PUT commands."""

    def __init__(
        self,
        filename: str,
        filesize: int | float,
        output_stream: IO | None = sys.stdout,
        show_progress_bar: bool | None = True,
    ):
        last_pound_char = filename.rfind("#")
        if last_pound_char < 0:
            last_pound_char = len(filename)
        self._filename = os.path.basename(filename[0:last_pound_char])
        self._output_stream = output_stream
        self._show_progress_bar = show_progress_bar
        self._size = float(filesize)
        self._seen_so_far = 0
        self._done = False
        self._start_time = time()
        self._lock = threading.Lock()

    def __call__(self, bytes_amount: int):
        raise NotImplementedError


class SnowflakeS3ProgressPercentage(SnowflakeProgressPercentage):
    def __init__(
        self,
        filename: str,
        filesize: int | float,
        output_stream: IO | None = sys.stdout,
        show_progress_bar: bool | None = True,
    ):
        super().__init__(
            filename,
            filesize,
            output_stream=output_stream,
            show_progress_bar=show_progress_bar,
        )

    def __call__(self, bytes_amount: int):
        with self._lock:
            if self._output_stream:
                self._seen_so_far += bytes_amount
                percentage = percent(self._seen_so_far, self._size)
                if not self._done:
                    self._done = _update_progress(
                        self._filename,
                        self._start_time,
                        self._size,
                        percentage,
                        output_stream=self._output_stream,
                        show_progress_bar=self._show_progress_bar,
                    )


class SnowflakeAzureProgressPercentage(SnowflakeProgressPercentage):
    def __init__(
        self,
        filename: str,
        filesize: int | float,
        output_stream: IO | None = sys.stdout,
        show_progress_bar: bool | None = True,
    ):
        super().__init__(
            filename,
            filesize,
            output_stream=output_stream,
            show_progress_bar=show_progress_bar,
        )

    def __call__(self, current: int):
        with self._lock:
            if self._output_stream:
                self._seen_so_far = current
                percentage = percent(self._seen_so_far, self._size)
                if not self._done:
                    self._done = _update_progress(
                        self._filename,
                        self._start_time,
                        self._size,
                        percentage,
                        output_stream=self._output_stream,
                        show_progress_bar=self._show_progress_bar,
                    )


class StorageCredential:
    def __init__(
        self,
        credentials: dict[str, Any],
        connection: SnowflakeConnection,
        command: str,
    ):
        self.creds = credentials
        self.timestamp = time()
        self.lock = threading.Lock()
        self.connection = connection
        self._command = command

    def update(self, cur_timestamp):
        with self.lock:
            if cur_timestamp < self.timestamp:
                return
            logger.debug("Renewing expired storage token.")
            ret = self.connection.cursor()._execute_helper(self._command)
            self.creds = ret["data"]["stageInfo"]["creds"]
            self.timestamp = time()


@dataclass
class TransferMetadata:
    num_files_started: int = 0
    num_files_completed: int = 0
    chunks_in_queue: int = 0


class SnowflakeFileTransferAgent:
    """Snowflake File Transfer Agent provides cloud provider independent implementation for putting/getting files."""

    def __init__(
        self,
        cursor: SnowflakeCursor,
        command: str,
        ret: dict[str, Any],
        put_callback: type[SnowflakeProgressPercentage] | None = None,
        put_azure_callback: type[SnowflakeProgressPercentage] | None = None,
        put_callback_output_stream: IO[str] = sys.stdout,
        get_callback: type[SnowflakeProgressPercentage] | None = None,
        get_azure_callback: type[SnowflakeProgressPercentage] | None = None,
        get_callback_output_stream: IO[str] = sys.stdout,
        show_progress_bar: bool = True,
        raise_put_get_error: bool = True,
        force_put_overwrite: bool = True,
        multipart_threshold: int | None = None,
        source_from_stream: IO[bytes] | None = None,
        use_s3_regional_url: bool = False,
    ):
        self._cursor = cursor
        self._command = command
        self._ret = ret
        self._put_callback = put_callback
        self._put_azure_callback = (
            put_azure_callback if put_azure_callback else put_callback
        )
        self._put_callback_output_stream = put_callback_output_stream
        self._get_callback = get_callback
        self._get_azure_callback = (
            get_azure_callback if get_azure_callback else get_callback
        )
        self._get_callback_output_stream = get_callback_output_stream
        # when we have not checked whether we should use accelerate, this boolean is None
        # _use_accelerate_endpoint in SnowflakeFileTransferAgent could be passed to each SnowflakeS3RestClient
        # so we could avoid check accelerate configuration for each S3 client created for each file meta.
        self._use_accelerate_endpoint: bool | None = None
        self._raise_put_get_error = raise_put_get_error
        self._show_progress_bar = show_progress_bar
        self._force_put_overwrite = force_put_overwrite
        self._source_from_stream = source_from_stream
        # The list of self-sufficient file metas that are sent to
        # remote storage clients to get operated on.
        self._file_metadata: list[SnowflakeFileMeta] = []
        self._results: list[SnowflakeFileMeta] = []
        self._multipart_threshold = multipart_threshold or 67108864  # Historical value
        self._use_s3_regional_url = use_s3_regional_url
        self._credentials: StorageCredential | None = None

    def execute(self) -> None:
        self._parse_command()
        self._init_file_metadata()

        if self._command_type == CMD_TYPE_UPLOAD:
            self._process_file_compression_type()

        for m in self._file_metadata:
            m.sfagent = self

        self._transfer_accelerate_config()

        if self._command_type == CMD_TYPE_DOWNLOAD:
            if not os.path.isdir(self._local_location):
                os.makedirs(self._local_location)

        if self._stage_location_type == LOCAL_FS:
            if not os.path.isdir(self._stage_info["location"]):
                os.makedirs(self._stage_info["location"])

        for m in self._file_metadata:
            m.overwrite = self._overwrite
            m.sfagent = self
            if self._stage_location_type != LOCAL_FS:
                m.put_callback = self._put_callback
                m.put_azure_callback = self._put_azure_callback
                m.put_callback_output_stream = self._put_callback_output_stream
                m.get_callback = self._get_callback
                m.get_azure_callback = self._get_azure_callback
                m.get_callback_output_stream = self._get_callback_output_stream
                m.show_progress_bar = self._show_progress_bar

                # multichunk threshold
                m.multipart_threshold = self._multipart_threshold

        logger.debug(f"parallel=[{self._parallel}]")
        if self._raise_put_get_error and not self._file_metadata:
            Error.errorhandler_wrapper(
                self._cursor.connection,
                self._cursor,
                OperationalError,
                {
                    "msg": "While getting file(s) there was an error: "
                    "the file does not exist.",
                    "errno": ER_FILE_NOT_EXISTS,
                },
            )
        self.transfer(self._file_metadata)

        # turn enum to string, in order to have backward compatible interface
        for result in self._results:
            result.result_status = result.result_status.value

    def transfer(self, metas: list[SnowflakeFileMeta]) -> None:
        max_concurrency = self._parallel
        network_tpe = ThreadPoolExecutor(max_concurrency)
        preprocess_tpe = ThreadPoolExecutor(min(len(metas), os.cpu_count()))
        postprocess_tpe = ThreadPoolExecutor(min(len(metas), os.cpu_count()))
        logger.debug(f"Chunk ThreadPoolExecutor size: {max_concurrency}")
        cv_main_thread = threading.Condition()  # to signal the main thread
        cv_chunk_process = (
            threading.Condition()
        )  # to get more chunks into the chunk_tpe
        files = [self._create_file_transfer_client(m) for m in metas]
        num_total_files = len(metas)
        transfer_metadata = TransferMetadata()  # this is protected by cv_chunk_process
        is_upload = self._command_type == CMD_TYPE_UPLOAD
        exception_caught_in_callback: Exception | None = None

        def notify_file_completed():
            # Increment the number of completed files, then notify the main thread.
            with cv_main_thread:
                transfer_metadata.num_files_completed += 1
                cv_main_thread.notify()

        def preprocess_done_cb(
            success: bool,
            result: Any,
            file_meta: SnowflakeFileMeta,
            done_client: SnowflakeStorageClient,
        ):
            if not success:
                logger.debug(f"Failed to prepare {done_client.meta.name}.")
                if is_upload:
                    done_client.finish_upload()
                    done_client.delete_client_data()
                else:
                    done_client.finish_download()
                notify_file_completed()
            elif done_client.meta.result_status == ResultStatus.SKIPPED:
                # this case applies to upload only
                notify_file_completed()
            else:
                logger.debug(f"Finished preparing file {done_client.meta.name}")
                with cv_chunk_process:
                    while transfer_metadata.chunks_in_queue > 2 * max_concurrency:
                        logger.debug(
                            "Chunk queue busy, waiting in file done callback..."
                        )
                        cv_chunk_process.wait()
                    for _chunk_id in range(done_client.num_of_chunks):
                        _callback = partial(
                            transfer_done_cb,
                            done_client=done_client,
                            chunk_id=_chunk_id,
                        )
                        if is_upload:
                            network_tpe.submit(
                                function_and_callback_wrapper,
                                # Work fn
                                done_client.upload_chunk,
                                # Callback fn
                                _callback,
                                file_meta,
                                # Arguments for work fn
                                _chunk_id,
                            )
                        else:
                            network_tpe.submit(
                                function_and_callback_wrapper,
                                # Work fn
                                done_client.download_chunk,
                                # Callback fn
                                _callback,
                                file_meta,
                                # Arguments for work fn
                                _chunk_id,
                            )
                        transfer_metadata.chunks_in_queue += 1
                    cv_chunk_process.notify()

        def transfer_done_cb(
            success: bool,
            result: Any,
            file_meta: SnowflakeFileMeta,
            done_client: SnowflakeStorageClient,
            chunk_id: int,
        ):
            # Note: chunk_id is 0 based while num_of_chunks is count
            logger.debug(
                f"Chunk {chunk_id}/{done_client.num_of_chunks} of file {done_client.meta.name} reached callback"
            )
            with cv_chunk_process:
                transfer_metadata.chunks_in_queue -= 1
                cv_chunk_process.notify()

            with done_client.lock:
                if not success:
                    # TODO: Cancel other chunks?
                    done_client.failed_transfers += 1
                    logger.debug(
                        f"Chunk {chunk_id} of file {done_client.meta.name} failed to transfer for unexpected exception {result}"
                    )
                else:
                    done_client.successful_transfers += 1
                logger.debug(
                    f"Chunk progress: {done_client.meta.name}: completed: {done_client.successful_transfers} failed: {done_client.failed_transfers} total: {done_client.num_of_chunks}"
                )
                if (
                    done_client.successful_transfers + done_client.failed_transfers
                    == done_client.num_of_chunks
                ):
                    if is_upload:
                        done_client.finish_upload()
                        done_client.delete_client_data()
                        notify_file_completed()
                    else:
                        postprocess_tpe.submit(
                            function_and_callback_wrapper,
                            # Work fn
                            done_client.finish_download,
                            # Callback fn
                            partial(postprocess_done_cb, done_client=done_client),
                            transfer_metadata,
                        )
                        logger.debug(
                            f"submitting {done_client.meta.name} to done_postprocess"
                        )

        def postprocess_done_cb(
            success: bool,
            result: Any,
            file_meta: SnowflakeFileMeta,
            done_client: SnowflakeStorageClient,
        ):
            logger.debug(f"File {done_client.meta.name} reached postprocess callback")

            with done_client.lock:
                if not success:
                    done_client.failed_transfers += 1
                    logger.debug(
                        f"File {done_client.meta.name} failed to transfer for unexpected exception {result}"
                    )
                # Whether there was an exception or not, we're done the file.
                notify_file_completed()

        _T = TypeVar("_T")

        def function_and_callback_wrapper(
            work: Callable[..., _T],
            _callback: Callable[[bool, _T | Exception, SnowflakeFileMeta], None],
            file_meta: SnowflakeFileMeta,
            *args: Any,
            **kwargs: Any,
        ) -> None:
            """This wrapper makes sure that callbacks are called from the TPEs.

            If the main thread adds a callback to a future that has already been
            fulfilled then the callback is executed by the main thread. This can
            lead to unexpected slowdowns and behavior.
            """
            try:
                result: tuple[bool, _T | Exception] = (
                    True,
                    work(*args, **kwargs),
                )
            except Exception as e:
                logger.error(f"An exception was raised in {repr(work)}", exc_info=True)
                file_meta.error_details = e
                result = (False, e)
            try:
                _callback(*result, file_meta)
            except Exception as e:
                # TODO: if an exception happens in a callback, the exception will not
                #  propagate to the main thread. We need to save these Exceptions
                #  somewhere and then re-raise by the main thread. For now let's log
                #  this exception, but for a long term solution see my
                #  TODO comment for SnowflakeFileMeta
                with cv_main_thread:
                    nonlocal exception_caught_in_callback
                    exception_caught_in_callback = e
                    cv_main_thread.notify()
                if not result[0]:
                    # Re-raising the exception from the work function, it would already
                    #  be logged at this point
                    logger.error(
                        f"An exception was raised in {repr(callback)}", exc_info=True
                    )

        for file_client in files:
            callback = partial(preprocess_done_cb, done_client=file_client)
            if is_upload:
                preprocess_tpe.submit(
                    function_and_callback_wrapper,
                    # Work fn
                    file_client.prepare_upload,
                    # Callback fn
                    callback,
                    file_client.meta,
                )
            else:
                preprocess_tpe.submit(
                    function_and_callback_wrapper,
                    # Work fn
                    file_client.prepare_download,
                    # Callback fn
                    callback,
                    file_client.meta,
                )
            transfer_metadata.num_files_started += 1  # TODO: do we need this?

        with cv_main_thread:
            while transfer_metadata.num_files_completed < num_total_files:
                cv_main_thread.wait()
                if exception_caught_in_callback is not None:
                    raise exception_caught_in_callback

        self._results = metas

    def _create_file_transfer_client(
        self, meta: SnowflakeFileMeta
    ) -> SnowflakeStorageClient:
        from .constants import AZURE_CHUNK_SIZE, S3_CHUNK_SIZE

        if self._stage_location_type == LOCAL_FS:
            return SnowflakeLocalStorageClient(
                meta,
                self._stage_info,
                4 * megabyte,
                use_s3_regional_url=self._use_s3_regional_url,
            )
        elif self._stage_location_type == AZURE_FS:
            return SnowflakeAzureRestClient(
                meta,
                self._credentials,
                AZURE_CHUNK_SIZE,
                self._stage_info,
                use_s3_regional_url=self._use_s3_regional_url,
            )
        elif self._stage_location_type == S3_FS:
            return SnowflakeS3RestClient(
                meta,
                self._credentials,
                self._stage_info,
                S3_CHUNK_SIZE,
                use_accelerate_endpoint=self._use_accelerate_endpoint,
                use_s3_regional_url=self._use_s3_regional_url,
            )
        elif self._stage_location_type == GCS_FS:
            return SnowflakeGCSRestClient(
                meta,
                self._credentials,
                self._stage_info,
                self._cursor._connection,
                self._command,
                use_s3_regional_url=self._use_s3_regional_url,
            )
        raise Exception(f"{self._stage_location_type} is an unknown stage type")

    def _transfer_accelerate_config(self) -> None:
        if self._stage_location_type == S3_FS and self._file_metadata:
            client = self._create_file_transfer_client(self._file_metadata[0])
            self._use_accelerate_endpoint = client.transfer_accelerate_config()

    def result(self):
        converter_class = self._cursor._connection.converter_class
        rowset = []
        if self._command_type == CMD_TYPE_UPLOAD:
            if hasattr(self, "_results"):
                for meta in self._results:
                    if meta.src_compression_type is not None:
                        src_compression_type = meta.src_compression_type.name
                    else:
                        src_compression_type = "NONE"

                    if meta.dst_compression_type is not None:
                        dst_compression_type = meta.dst_compression_type.name
                    else:
                        dst_compression_type = "NONE"

                    error_details: str = (
                        repr(meta.error_details)
                        if meta.error_details is not None
                        else ""
                    )

                    src_file_size = (
                        meta.src_file_size
                        if converter_class != SnowflakeConverterSnowSQL
                        else str(meta.src_file_size)
                    )

                    dst_file_size = (
                        meta.dst_file_size
                        if converter_class != SnowflakeConverterSnowSQL
                        else str(meta.dst_file_size)
                    )

                    logger.debug(
                        f"raise_put_get_error: {self._raise_put_get_error}, "
                        f"{meta.result_status}, {type(meta.result_status)}, "
                        f"{ResultStatus.ERROR}, {type(ResultStatus.ERROR)}",
                    )
                    if self._raise_put_get_error and error_details:
                        Error.errorhandler_wrapper(
                            self._cursor.connection,
                            self._cursor,
                            OperationalError,
                            {
                                "msg": "While putting file(s) there was an error: "
                                f"'{error_details}', this might be caused by "
                                f"your access to the blob storage provider, "
                                f"or by Snowflake.",
                                "errno": ER_FAILED_TO_UPLOAD_TO_STAGE,
                            },
                        )
                    rowset.append(
                        [
                            meta.name,
                            meta.dst_file_name,
                            src_file_size,
                            dst_file_size,
                            src_compression_type,
                            dst_compression_type,
                            meta.result_status,
                            error_details,
                        ]
                    )
            return {
                "rowtype": [
                    result_text_column_desc("source"),
                    result_text_column_desc("target"),
                    result_fixed_column_desc("source_size"),
                    result_fixed_column_desc("target_size"),
                    result_text_column_desc("source_compression"),
                    result_text_column_desc("target_compression"),
                    result_text_column_desc("status"),
                    result_text_column_desc("message"),
                ],
                "rowset": sorted(rowset),
            }
        else:  # DOWNLOAD
            if hasattr(self, "_results"):
                for meta in self._results:
                    dst_file_size = (
                        meta.dst_file_size
                        if converter_class != SnowflakeConverterSnowSQL
                        else str(meta.dst_file_size)
                    )

                    error_details: str = (
                        repr(meta.error_details)
                        if meta.error_details is not None
                        else ""
                    )

                    if self._raise_put_get_error and error_details:
                        Error.errorhandler_wrapper(
                            self._cursor.connection,
                            self._cursor,
                            OperationalError,
                            {
                                "msg": "While getting file(s) there was an error: "
                                f"'{error_details}', this might be caused by "
                                f"your access to the blob storage provider, "
                                f"or by Snowflake.",
                                "errno": ER_FAILED_TO_DOWNLOAD_FROM_STAGE,
                            },
                        )

                    rowset.append(
                        [
                            meta.dst_file_name,
                            dst_file_size,
                            meta.result_status,
                            error_details,
                        ]
                    )
            return {
                "rowtype": [
                    result_text_column_desc("file"),
                    result_fixed_column_desc("size"),
                    result_text_column_desc("status"),
                    result_text_column_desc("message"),
                ],
                "rowset": sorted(rowset),
            }

    def _expand_filenames(self, locations):
        canonical_locations = []
        for file_name in locations:
            if self._command_type == CMD_TYPE_UPLOAD:
                file_name = os.path.expanduser(file_name)
                if not os.path.isabs(file_name):
                    file_name = os.path.join(GET_CWD(), file_name)
                if (
                    IS_WINDOWS
                    and len(file_name) > 2
                    and file_name[0] == "/"
                    and file_name[2] == ":"
                ):
                    # Windows path: /C:/data/file1.txt where it starts with slash
                    # followed by a drive letter and colon.
                    file_name = file_name[1:]
                files = glob.glob(file_name)
                canonical_locations += files
            else:
                canonical_locations.append(file_name)

        return canonical_locations

    def _init_encryption_material(self) -> None:
        self._encryption_material = []
        if self._ret["data"].get("encryptionMaterial") is not None:
            root_node = self._ret["data"]["encryptionMaterial"]
            logger.debug(self._command_type)

            if self._command_type == CMD_TYPE_UPLOAD:
                self._encryption_material.append(
                    SnowflakeFileEncryptionMaterial(
                        query_stage_master_key=root_node["queryStageMasterKey"],
                        query_id=root_node["queryId"],
                        smk_id=root_node["smkId"],
                    )
                )
            else:
                for elem in root_node:
                    if elem is not None:
                        self._encryption_material.append(
                            SnowflakeFileEncryptionMaterial(
                                query_stage_master_key=elem["queryStageMasterKey"],
                                query_id=elem["queryId"],
                                smk_id=elem["smkId"],
                            )
                        )

    def _parse_command(self) -> None:

        if "data" not in self._ret:
            Error.errorhandler_wrapper(
                self._cursor.connection,
                self._cursor,
                DatabaseError,
                {
                    "msg": "Failed to parse server's response",
                    "errno": ER_INVALID_STAGE_LOCATION,
                },
            )

        response = self._ret["data"]

        self._command_type = response["command"]

        self._init_encryption_material()

        if not isinstance(response.get("src_locations"), list):
            Error.errorhandler_wrapper(
                self._cursor.connection,
                self._cursor,
                DatabaseError,
                {
                    "msg": "Failed to parse the location",
                    "errno": ER_INVALID_STAGE_LOCATION,
                },
            )

        self._src_locations = response["src_locations"]

        self._parallel = response.get("parallel", 1)
        self._overwrite = self._force_put_overwrite or response.get("overwrite", False)
        self._stage_location_type = response["stageInfo"]["locationType"].upper()

        if self._stage_location_type not in VALID_STORAGE:
            Error.errorhandler_wrapper(
                self._cursor.connection,
                self._cursor,
                OperationalError,
                {
                    "msg": f"Destination location type is not valid: {self._stage_location_type}",
                    "errno": ER_INVALID_STAGE_FS,
                },
            )

        self._stage_location = response["stageInfo"]["location"]
        self._stage_info = response["stageInfo"]
        self._credentials = StorageCredential(
            self._stage_info["creds"], self._cursor.connection, self._command
        )
        self._presigned_urls = self._ret["data"].get("presignedUrls")

        if self._command_type == CMD_TYPE_UPLOAD:
            if self._source_from_stream:
                self._src_files = self._src_locations
            else:
                self._src_files = list(self._expand_filenames(self._src_locations))
            self._auto_compress = (
                "autoCompress" not in response or response["autoCompress"]
            )
            self._source_compression = (
                response["sourceCompression"].lower()
                if "sourceCompression" in response
                else ""
            )
        else:
            self._src_files = list(self._src_locations)
            self._src_file_to_encryption_material = {}
            if len(response["src_locations"]) == len(self._encryption_material):
                for idx, src_file in enumerate(self._src_files):
                    logger.debug(src_file)
                    self._src_file_to_encryption_material[
                        src_file
                    ] = self._encryption_material[idx]
            elif len(self._encryption_material) != 0:
                # some encryption material exists. Zero means no encryption
                Error.errorhandler_wrapper(
                    self._cursor.connection,
                    self._cursor,
                    InternalError,
                    {
                        "msg": (
                            "The number of downloading files doesn't match "
                            f"the encryption materials: files={len(response['src_locations'])}, "
                            f"encmat={len(self._encryption_material)}"
                        ),
                        "errno": ER_INTERNAL_NOT_MATCH_ENCRYPT_MATERIAL,
                    },
                )

            self._local_location = os.path.expanduser(response["localLocation"])
            if not os.path.isdir(self._local_location):
                # NOTE: isdir follows the symlink
                Error.errorhandler_wrapper(
                    self._cursor.connection,
                    self._cursor,
                    ProgrammingError,
                    {
                        "msg": f"The local path is not a directory: {self._local_location}",
                        "errno": ER_LOCAL_PATH_NOT_DIRECTORY,
                    },
                )

    def _init_file_metadata(self) -> None:
        logger.debug(f"command type: {self._command_type}")

        if self._command_type == CMD_TYPE_UPLOAD:
            if not self._src_files:
                file_name = (
                    self._ret["data"]["src_locations"]
                    if "data" in self._ret and "src_locations" in self._ret["data"]
                    else "None"
                )
                Error.errorhandler_wrapper(
                    self._cursor.connection,
                    self._cursor,
                    ProgrammingError,
                    {
                        "msg": f"File doesn't exist: {file_name}",
                        "errno": ER_FILE_NOT_EXISTS,
                    },
                )
            if self._source_from_stream:
                self._file_metadata.append(
                    SnowflakeFileMeta(
                        name=os.path.basename(self._src_files[0]),
                        src_file_name=self._src_files[0],
                        intermediate_stream=self._source_from_stream,
                        src_file_size=self._source_from_stream.seek(0, os.SEEK_END),
                        stage_location_type=self._stage_location_type,
                        encryption_material=self._encryption_material[0]
                        if len(self._encryption_material) > 0
                        else None,
                    )
                )
                self._source_from_stream.seek(0)
            else:
                for file_name in self._src_files:
                    if not os.path.exists(file_name):
                        Error.errorhandler_wrapper(
                            self._cursor.connection,
                            self._cursor,
                            ProgrammingError,
                            {
                                "msg": f"File doesn't exist: {file_name}",
                                "errno": ER_FILE_NOT_EXISTS,
                            },
                        )
                    elif os.path.isdir(file_name):
                        Error.errorhandler_wrapper(
                            self._cursor.connection,
                            self._cursor,
                            ProgrammingError,
                            {
                                "msg": f"Not a file but a directory: {file_name}",
                                "errno": ER_FILE_NOT_EXISTS,
                            },
                        )
                    statinfo = os.stat(file_name)
                    self._file_metadata.append(
                        SnowflakeFileMeta(
                            name=os.path.basename(file_name),
                            src_file_name=file_name,
                            src_file_size=statinfo.st_size,
                            stage_location_type=self._stage_location_type,
                            encryption_material=self._encryption_material[0]
                            if len(self._encryption_material) > 0
                            else None,
                        )
                    )
        elif self._command_type == CMD_TYPE_DOWNLOAD:
            for idx, file_name in enumerate(self._src_files):
                if not file_name:
                    continue
                first_path_sep = file_name.find("/")
                dst_file_name = (
                    file_name[first_path_sep + 1 :]
                    if first_path_sep >= 0
                    else file_name
                )
                url = None
                if self._presigned_urls and idx < len(self._presigned_urls):
                    url = self._presigned_urls[idx]
                self._file_metadata.append(
                    SnowflakeFileMeta(
                        name=os.path.basename(file_name),
                        src_file_name=file_name,
                        dst_file_name=dst_file_name,
                        stage_location_type=self._stage_location_type,
                        local_location=self._local_location,
                        presigned_url=url,
                        encryption_material=self._src_file_to_encryption_material[
                            file_name
                        ]
                        if file_name in self._src_file_to_encryption_material
                        else None,
                    )
                )

    def _process_file_compression_type(self) -> None:
        user_specified_source_compression = None
        if self._source_compression == "auto_detect":
            auto_detect = True
        elif self._source_compression == "none":
            auto_detect = False
        else:
            user_specified_source_compression = lookup_by_mime_sub_type(
                self._source_compression
            )
            if (
                user_specified_source_compression is None
                or not user_specified_source_compression.is_supported
            ):
                Error.errorhandler_wrapper(
                    self._cursor.connection,
                    self._cursor,
                    ProgrammingError,
                    {
                        "msg": f"Feature is not supported: {user_specified_source_compression}",
                        "errno": ER_COMPRESSION_NOT_SUPPORTED,
                    },
                )

            auto_detect = False

        for m in self._file_metadata:
            file_name = m.src_file_name

            current_file_compression_type: CompressionType | None = None
            if auto_detect:
                mimetypes.init()
                _, encoding = mimetypes.guess_type(file_name)

                if encoding is None:
                    test = None
                    if not self._source_from_stream:
                        with open(file_name, "rb") as f:
                            test = f.read(4)
                    else:
                        test = self._source_from_stream.read(4)
                        self._source_from_stream.seek(0)
                    if file_name.endswith(".br"):
                        encoding = "br"
                    elif test and test[:3] == b"ORC":
                        encoding = "orc"
                    elif test and test == b"PAR1":
                        encoding = "parquet"
                    elif test and (int(binascii.hexlify(test), 16) == 0x28B52FFD):
                        encoding = "zstd"

                if encoding is not None:
                    logger.debug(
                        f"detected the encoding {encoding}: file={file_name}",
                    )
                    current_file_compression_type = lookup_by_mime_sub_type(encoding)
                else:
                    logger.debug(f"no file encoding was detected: file={file_name}")

                if (
                    current_file_compression_type is not None
                    and not current_file_compression_type.is_supported
                ):
                    Error.errorhandler_wrapper(
                        self._cursor.connection,
                        self._cursor,
                        ProgrammingError,
                        {
                            "msg": f"Feature is not supported: {current_file_compression_type}",
                            "errno": ER_COMPRESSION_NOT_SUPPORTED,
                        },
                    )
            else:
                current_file_compression_type = user_specified_source_compression

            if current_file_compression_type is not None:
                m.src_compression_type = current_file_compression_type
                if current_file_compression_type.is_supported:
                    m.dst_compression_type = current_file_compression_type
                    m.require_compress = False
                    m.dst_file_name = m.name
                else:
                    Error.errorhandler_wrapper(
                        self._cursor.connection,
                        self._cursor,
                        ProgrammingError,
                        {
                            "msg": f"Feature is not supported: {current_file_compression_type}",
                            "errno": ER_COMPRESSION_NOT_SUPPORTED,
                        },
                    )
            else:
                # src is not compressed but the destination want to be
                # compressed unless the users disable it
                m.require_compress = self._auto_compress
                m.src_compression_type = None
                if self._auto_compress:
                    m.dst_file_name = m.name + CompressionTypes["GZIP"].file_extension
                    m.dst_compression_type = CompressionTypes["GZIP"]
                else:
                    m.dst_file_name = m.name
                    m.dst_compression_type = None
