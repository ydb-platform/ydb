#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from collections import defaultdict
from enum import Enum, auto, unique
from typing import Any, Callable, DefaultDict, NamedTuple

from .options import installed_pandas
from .options import pyarrow as pa

if installed_pandas:
    DataType = pa.DataType
else:
    DataType = None


DBAPI_TYPE_STRING = 0
DBAPI_TYPE_BINARY = 1
DBAPI_TYPE_NUMBER = 2
DBAPI_TYPE_TIMESTAMP = 3


class FieldType(NamedTuple):
    name: str
    dbapi_type: list[int]
    pa_type: Callable[[], DataType]


# This type mapping holds column type definitions.
#  Be careful to not change the ordering as the index is what Snowflake
#  gives to as schema
FIELD_TYPES: tuple[FieldType] = (
    FieldType(name="FIXED", dbapi_type=[DBAPI_TYPE_NUMBER], pa_type=lambda: pa.int64()),
    FieldType(
        name="REAL", dbapi_type=[DBAPI_TYPE_NUMBER], pa_type=lambda: pa.float64()
    ),
    FieldType(name="TEXT", dbapi_type=[DBAPI_TYPE_STRING], pa_type=lambda: pa.string()),
    FieldType(
        name="DATE", dbapi_type=[DBAPI_TYPE_TIMESTAMP], pa_type=lambda: pa.date64()
    ),
    FieldType(
        name="TIMESTAMP",
        dbapi_type=[DBAPI_TYPE_TIMESTAMP],
        pa_type=lambda: pa.time64("ns"),
    ),
    FieldType(
        name="VARIANT", dbapi_type=[DBAPI_TYPE_BINARY], pa_type=lambda: pa.string()
    ),
    FieldType(
        name="TIMESTAMP_LTZ",
        dbapi_type=[DBAPI_TYPE_TIMESTAMP],
        pa_type=lambda: pa.timestamp("ns"),
    ),
    FieldType(
        name="TIMESTAMP_TZ",
        dbapi_type=[DBAPI_TYPE_TIMESTAMP],
        pa_type=lambda: pa.timestamp("ns"),
    ),
    FieldType(
        name="TIMESTAMP_NTZ",
        dbapi_type=[DBAPI_TYPE_TIMESTAMP],
        pa_type=lambda: pa.timestamp("ns"),
    ),
    FieldType(
        name="OBJECT", dbapi_type=[DBAPI_TYPE_BINARY], pa_type=lambda: pa.string()
    ),
    FieldType(
        name="ARRAY", dbapi_type=[DBAPI_TYPE_BINARY], pa_type=lambda: pa.string()
    ),
    FieldType(
        name="BINARY", dbapi_type=[DBAPI_TYPE_BINARY], pa_type=lambda: pa.binary()
    ),
    FieldType(
        name="TIME", dbapi_type=[DBAPI_TYPE_TIMESTAMP], pa_type=lambda: pa.time64("ns")
    ),
    FieldType(name="BOOLEAN", dbapi_type=[], pa_type=lambda: pa.bool_()),
    FieldType(
        name="GEOGRAPHY", dbapi_type=[DBAPI_TYPE_STRING], pa_type=lambda: pa.string()
    ),
)

FIELD_NAME_TO_ID: DefaultDict[Any, int] = defaultdict(int)
FIELD_ID_TO_NAME: DefaultDict[int, str] = defaultdict(str)

__binary_types: list[int] = []
__binary_type_names: list[str] = []
__string_types: list[int] = []
__string_type_names: list[str] = []
__number_types: list[int] = []
__number_type_names: list[str] = []
__timestamp_types: list[int] = []
__timestamp_type_names: list[str] = []

for idx, field_type in enumerate(FIELD_TYPES):
    FIELD_ID_TO_NAME[idx] = field_type.name
    FIELD_NAME_TO_ID[field_type.name] = idx

    dbapi_types = field_type.dbapi_type
    for dbapi_type in dbapi_types:
        if dbapi_type == DBAPI_TYPE_BINARY:
            __binary_types.append(idx)
            __binary_type_names.append(field_type.name)
        elif dbapi_type == DBAPI_TYPE_TIMESTAMP:
            __timestamp_types.append(idx)
            __timestamp_type_names.append(field_type.name)
        elif dbapi_type == DBAPI_TYPE_NUMBER:
            __number_types.append(idx)
            __number_type_names.append(field_type.name)
        elif dbapi_type == DBAPI_TYPE_STRING:
            __string_types.append(idx)
            __string_type_names.append(field_type.name)


def get_binary_types() -> list[int]:
    return __binary_types


def is_binary_type_name(type_name: str) -> bool:
    return type_name in __binary_type_names


def get_string_types() -> list[int]:
    return __string_types


def is_string_type_name(type_name) -> bool:
    return type_name in __string_type_names


def get_number_types() -> list[int]:
    return __number_types


def is_number_type_name(type_name) -> bool:
    return type_name in __number_type_names


def get_timestamp_types() -> list[int]:
    return __timestamp_types


def is_timestamp_type_name(type_name) -> bool:
    return type_name in __timestamp_type_names


def is_date_type_name(type_name) -> bool:
    return type_name == "DATE"


# Log format
LOG_FORMAT = (
    "%(asctime)s - %(filename)s:%(lineno)d - "
    "%(funcName)s() - %(levelname)s - %(message)s"
)

# String literals
UTF8 = "utf-8"
SHA256_DIGEST = "sha256_digest"

# PUT/GET related
S3_FS = "S3"
AZURE_FS = "AZURE"
GCS_FS = "GCS"
LOCAL_FS = "LOCAL_FS"
CMD_TYPE_UPLOAD = "UPLOAD"
CMD_TYPE_DOWNLOAD = "DOWNLOAD"
FILE_PROTOCOL = "file://"


@unique
class ResultStatus(Enum):
    ERROR = "ERROR"
    SUCCEEDED = "SUCCEEDED"
    UPLOADED = "UPLOADED"
    DOWNLOADED = "DOWNLOADED"
    COLLISION = "COLLISION"
    SKIPPED = "SKIPPED"
    RENEW_TOKEN = "RENEW_TOKEN"
    RENEW_PRESIGNED_URL = "RENEW_PRESIGNED_URL"
    NOT_FOUND_FILE = "NOT_FOUND_FILE"
    NEED_RETRY = "NEED_RETRY"
    NEED_RETRY_WITH_LOWER_CONCURRENCY = "NEED_RETRY_WITH_LOWER_CONCURRENCY"


class SnowflakeS3FileEncryptionMaterial(NamedTuple):
    query_id: str
    query_stage_master_key: str
    smk_id: int


class MaterialDescriptor(NamedTuple):
    smk_id: int
    query_id: str
    key_size: int


class EncryptionMetadata(NamedTuple):
    key: str
    iv: str
    matdesc: str


class FileHeader(NamedTuple):
    digest: str | None
    content_length: int | None
    encryption_metadata: EncryptionMetadata | None


PARAMETER_AUTOCOMMIT = "AUTOCOMMIT"
PARAMETER_CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY = (
    "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY"
)
PARAMETER_CLIENT_SESSION_KEEP_ALIVE = "CLIENT_SESSION_KEEP_ALIVE"
PARAMETER_CLIENT_PREFETCH_THREADS = "CLIENT_PREFETCH_THREADS"
PARAMETER_CLIENT_TELEMETRY_ENABLED = "CLIENT_TELEMETRY_ENABLED"
PARAMETER_CLIENT_TELEMETRY_OOB_ENABLED = "CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED"
PARAMETER_CLIENT_STORE_TEMPORARY_CREDENTIAL = "CLIENT_STORE_TEMPORARY_CREDENTIAL"
PARAMETER_CLIENT_REQUEST_MFA_TOKEN = "CLIENT_REQUEST_MFA_TOKEN"
PARAMETER_CLIENT_USE_SECURE_STORAGE_FOR_TEMPORARY_CREDENTIAL = (
    "CLIENT_USE_SECURE_STORAGE_FOR_TEMPORARY_CREDENTAIL"
)
PARAMETER_TIMEZONE = "TIMEZONE"
PARAMETER_SERVICE_NAME = "SERVICE_NAME"
PARAMETER_CLIENT_VALIDATE_DEFAULT_PARAMETERS = "CLIENT_VALIDATE_DEFAULT_PARAMETERS"
PARAMETER_PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT"
PARAMETER_ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1 = (
    "ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1"
)
PARAMETER_MULTI_STATEMENT_COUNT = "MULTI_STATEMENT_COUNT"

HTTP_HEADER_CONTENT_TYPE = "Content-Type"
HTTP_HEADER_CONTENT_ENCODING = "Content-Encoding"
HTTP_HEADER_ACCEPT_ENCODING = "Accept-Encoding"
HTTP_HEADER_ACCEPT = "accept"
HTTP_HEADER_USER_AGENT = "User-Agent"
HTTP_HEADER_SERVICE_NAME = "X-Snowflake-Service"

HTTP_HEADER_VALUE_OCTET_STREAM = "application/octet-stream"


@unique
class OCSPMode(Enum):
    """OCSP Mode enumerator for all the available modes.

    OCSP mode descriptions:
        FAIL_CLOSED: If the client or driver does not receive a valid OCSP CA response for any reason,
            the connection fails.
        FAIL_OPEN: A response indicating a revoked certificate results in a failed connection. A response with any
            other certificate errors or statuses allows the connection to occur, but denotes the message in the logs
            at the WARNING level with the relevant details in JSON format.
        INSECURE: The connection will occur anyway.
    """

    FAIL_CLOSED = "FAIL_CLOSED"
    FAIL_OPEN = "FAIL_OPEN"
    INSECURE = "INSECURE"


@unique
class FileTransferType(Enum):
    """This enum keeps track of the possible file transfer types."""

    PUT = auto()
    GET = auto()


@unique
class QueryStatus(Enum):
    RUNNING = 0
    ABORTING = 1
    SUCCESS = 2
    FAILED_WITH_ERROR = 3
    ABORTED = 4
    QUEUED = 5
    FAILED_WITH_INCIDENT = 6
    DISCONNECTED = 7
    RESUMING_WAREHOUSE = 8
    # purposeful typo. Is present in QueryDTO.java
    QUEUED_REPARING_WAREHOUSE = 9
    RESTARTED = 10
    BLOCKED = 11
    NO_DATA = 12


# Size constants
kilobyte = 1024
megabyte = kilobyte * 1024
gigabyte = megabyte * 1024


# ArrowResultChunk constants the unit in this iterator
# EMPTY_UNIT: default
# ROW_UNIT: fetch row by row if the user call `fetchone()`
# TABLE_UNIT: fetch one arrow table if the user call `fetch_pandas()`
@unique
class IterUnit(Enum):
    ROW_UNIT = "row"
    TABLE_UNIT = "table"


S3_CHUNK_SIZE = 8388608  # boto3 default
AZURE_CHUNK_SIZE = 4 * megabyte

DAY_IN_SECONDS = 60 * 60 * 24

# TODO: all env variables definitions should be here
ENV_VAR_PARTNER = "SF_PARTNER"
