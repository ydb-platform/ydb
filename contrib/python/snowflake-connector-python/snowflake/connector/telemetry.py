#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
from enum import Enum, unique
from threading import Lock
from typing import TYPE_CHECKING, Any

from .description import CLIENT_NAME, SNOWFLAKE_CONNECTOR_VERSION
from .secret_detector import SecretDetector
from .test_util import ENABLE_TELEMETRY_LOG, rt_plain_logger

if TYPE_CHECKING:
    from .connection import SnowflakeConnection
    from .network import SnowflakeRestful

logger = logging.getLogger(__name__)


@unique
class TelemetryField(Enum):
    # Fields which can be logged to telemetry
    TIME_CONSUME_FIRST_RESULT = "client_time_consume_first_result"
    TIME_CONSUME_LAST_RESULT = "client_time_consume_last_result"
    TIME_DOWNLOADING_CHUNKS = "client_time_downloading_chunks"
    TIME_PARSING_CHUNKS = "client_time_parsing_chunks"
    SQL_EXCEPTION = "client_sql_exception"
    GET_PARTITIONS_USED = "client_get_partitions_used"
    EMPTY_SEQ_INTERPOLATION = "client_pyformat_empty_seq_interpolation"
    # fetch_pandas_* usage
    PANDAS_FETCH_ALL = "client_fetch_pandas_all"
    PANDAS_FETCH_BATCHES = "client_fetch_pandas_batches"
    # fetch_arrow_* usage
    ARROW_FETCH_ALL = "client_fetch_arrow_all"
    ARROW_FETCH_BATCHES = "client_fetch_arrow_batches"
    # write_pandas usage
    PANDAS_WRITE = "client_write_pandas"
    # imported packages along with client
    IMPORTED_PACKAGES = "client_imported_packages"
    # multi-statement usage
    MULTI_STATEMENT = "client_multi_statement_query"
    # Keys for telemetry data sent through either in-band or out-of-band telemetry
    KEY_TYPE = "type"
    KEY_SOURCE = "source"
    KEY_SFQID = "query_id"
    KEY_SQLSTATE = "sql_state"
    KEY_DRIVER_TYPE = "driver_type"
    KEY_DRIVER_VERSION = "driver_version"
    KEY_REASON = "reason"
    KEY_VALUE = "value"
    KEY_EXCEPTION = "exception"
    # Reserved UpperCamelName keys
    KEY_ERROR_NUMBER = "ErrorNumber"
    KEY_ERROR_MESSAGE = "ErrorMessage"
    KEY_STACKTRACE = "Stacktrace"
    # OOB camelName keys
    KEY_OOB_DRIVER = "driver"
    KEY_OOB_VERSION = "version"
    KEY_OOB_TELEMETRY_SERVER_DEPLOYMENT = "telemetryServerDeployment"
    KEY_OOB_CONNECTION_STRING = "connectionString"
    KEY_OOB_EXCEPTION_MESSAGE = "exceptionMessage"
    KEY_OOB_ERROR_MESSAGE = "errorMessage"
    KEY_OOB_EXCEPTION_STACK_TRACE = "exceptionStackTrace"
    KEY_OOB_EVENT_TYPE = "eventType"
    KEY_OOB_ERROR_CODE = "errorCode"
    KEY_OOB_SQL_STATE = "sqlState"
    KEY_OOB_REQUEST = "request"
    KEY_OOB_RESPONSE = "response"
    KEY_OOB_RESPONSE_STATUS_LINE = "responseStatusLine"
    KEY_OOB_RESPONSE_STATUS_CODE = "responseStatusCode"
    KEY_OOB_RETRY_TIMEOUT = "retryTimeout"
    KEY_OOB_RETRY_COUNT = "retryCount"
    KEY_OOB_EVENT_SUB_TYPE = "eventSubType"
    KEY_OOB_SFC_PEER_HOST = "sfcPeerHost"
    KEY_OOB_CERT_ID = "certId"
    KEY_OOB_OCSP_REQUEST_BASE64 = "ocspRequestBase64"
    KEY_OOB_OCSP_RESPONDER_URL = "ocspResponderURL"
    KEY_OOB_INSECURE_MODE = "insecureMode"
    KEY_OOB_FAIL_OPEN = "failOpen"
    KEY_OOB_CACHE_ENABLED = "cacheEnabled"
    KEY_OOB_CACHE_HIT = "cacheHit"


class TelemetryData:
    """An instance of telemetry data which can be sent to the server."""

    TRUE = 1
    FALSE = 0

    def __init__(self, message, timestamp):
        self.message = message
        self.timestamp = timestamp

    @classmethod
    def from_telemetry_data_dict(
        cls,
        from_dict: dict,
        timestamp: int,
        connection: SnowflakeConnection | None = None,
        is_oob_telemetry: bool = False,
    ):
        """
        Generate telemetry data with driver info from given dict and timestamp.
        It takes an optional connection object to read data from.
        It also takes a boolean is_oob_telemetry to indicate whether it's for out-of-band telemetry, as
        naming of keys for driver and version is different from the ones of in-band telemetry.
        """
        return cls(
            generate_telemetry_data_dict(
                from_dict=(from_dict or {}),
                connection=connection,
                is_oob_telemetry=is_oob_telemetry,
            ),
            timestamp,
        )

    def to_dict(self):
        return {"message": self.message, "timestamp": str(self.timestamp)}

    def __repr__(self):
        return str(self.to_dict())


class TelemetryClient:
    """Client to enqueue and send metrics to the telemetry endpoint in batch."""

    SF_PATH_TELEMETRY = "/telemetry/send"
    DEFAULT_FORCE_FLUSH_SIZE = 100

    def __init__(self, rest: SnowflakeRestful, flush_size=None):
        self._rest: SnowflakeRestful | None = rest
        self._log_batch = []
        self._flush_size = flush_size or TelemetryClient.DEFAULT_FORCE_FLUSH_SIZE
        self._lock = Lock()
        self._enabled = True

    def add_log_to_batch(self, telemetry_data: TelemetryData) -> None:
        if self.is_closed:
            raise Exception("Attempted to add log when TelemetryClient is closed")
        elif not self._enabled:
            logger.debug("TelemetryClient disabled. Ignoring log.")
            return

        with self._lock:
            self._log_batch.append(telemetry_data)

        if len(self._log_batch) >= self._flush_size:
            self.send_batch()

    def try_add_log_to_batch(self, telemetry_data: TelemetryData) -> None:
        try:
            self.add_log_to_batch(telemetry_data)
        except Exception:
            logger.warning("Failed to add log to telemetry.", exc_info=True)

    def send_batch(self):
        if self.is_closed:
            raise Exception("Attempted to send batch when TelemetryClient is closed")
        elif not self._enabled:
            logger.debug("TelemetryClient disabled. Not sending logs.")
            return

        with self._lock:
            to_send = self._log_batch
            self._log_batch = []

        if not to_send:
            logger.debug("Nothing to send to telemetry.")
            return

        body = {"logs": [x.to_dict() for x in to_send]}
        logger.debug(
            "Sending %d logs to telemetry. Data is %s.",
            len(body),
            SecretDetector.mask_secrets(str(body))[1],
        )
        if ENABLE_TELEMETRY_LOG:
            # This logger guarantees the payload won't be masked. Testing purpose.
            rt_plain_logger.debug(f"Inband telemetry data being sent is {body}")
        try:
            ret = self._rest.request(
                TelemetryClient.SF_PATH_TELEMETRY,
                body=body,
                method="post",
                client=None,
                timeout=5,
            )
            if not ret["success"]:
                logger.info(
                    "Non-success response from telemetry server: %s. "
                    "Disabling telemetry.",
                    str(ret),
                )
                self._enabled = False
            else:
                logger.debug("Successfully uploading metrics to telemetry.")
        except Exception:
            self._enabled = False
            logger.debug("Failed to upload metrics to telemetry.", exc_info=True)

    @property
    def is_closed(self):
        return self._rest is None

    def close(self, send_on_close=True):
        if not self.is_closed:
            logger.debug("Closing telemetry client.")
            if send_on_close:
                self.send_batch()
            self._rest = None

    def disable(self):
        self._enabled = False

    def is_enabled(self):
        return self._enabled

    def buffer_size(self):
        return len(self._log_batch)


def generate_telemetry_data_dict(
    from_dict: dict | None = None,
    connection: SnowflakeConnection | None = None,
    is_oob_telemetry: bool = False,
) -> dict[str, Any]:
    """
    Generate telemetry data with driver info.
    The method also takes an optional dict to update from and optional connection object to read data from.
    It also takes a boolean is_oob_telemetry to indicate whether it's for out-of-band telemetry, as
    naming of keys for driver and version is different from the ones of in-band telemetry.
    """
    from_dict = from_dict or {}
    return (
        {
            TelemetryField.KEY_DRIVER_TYPE.value: CLIENT_NAME,
            TelemetryField.KEY_DRIVER_VERSION.value: SNOWFLAKE_CONNECTOR_VERSION,
            TelemetryField.KEY_SOURCE.value: connection.application
            if connection
            else CLIENT_NAME,
            **from_dict,
        }
        if not is_oob_telemetry
        else {
            TelemetryField.KEY_OOB_DRIVER.value: CLIENT_NAME,
            TelemetryField.KEY_OOB_VERSION.value: SNOWFLAKE_CONNECTOR_VERSION,
            **from_dict,
        }
    )
