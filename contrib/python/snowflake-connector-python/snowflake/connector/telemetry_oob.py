#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import datetime
import json
import logging
import uuid
from collections import namedtuple
from queue import Queue

from .compat import OK
from .description import CLIENT_NAME, SNOWFLAKE_CONNECTOR_VERSION
from .secret_detector import SecretDetector
from .telemetry import TelemetryField, generate_telemetry_data_dict
from .test_util import ENABLE_TELEMETRY_LOG, rt_plain_logger
from .vendored import requests

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10
DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY = 10
REQUEST_TIMEOUT = 3

TelemetryAPI = namedtuple("TelemetryAPI", ["url", "api_key"])
TelemetryServer = namedtuple("TelemetryServer", ["name", "url", "api_key"])
TelemetryEventBase = namedtuple(
    "TelemetryEventBase", ["name", "tags", "urgent", "value"]
)


class TelemetryAPIEndpoint:
    SFCTEST = TelemetryAPI(
        url="https://sfctest.client-telemetry.snowflakecomputing.com/enqueue",
        api_key="rRNY3EPNsB4U89XYuqsZKa7TSxb9QVX93yNM4tS6",
    )
    SFCDEV = TelemetryAPI(
        url="https://sfcdev.client-telemetry.snowflakecomputing.com/enqueue",
        api_key="kyTKLWpEZSaJnrzTZ63I96QXZHKsgfqbaGmAaIWf",
    )
    PROD = TelemetryAPI(
        url="https://client-telemetry.snowflakecomputing.com/enqueue",
        api_key="wLpEKqnLOW9tGNwTjab5N611YQApOb3t9xOnE1rX",
    )


class TelemetryServerDeployments:
    DEV = TelemetryServer(
        "dev", TelemetryAPIEndpoint.SFCTEST.url, TelemetryAPIEndpoint.SFCTEST.api_key
    )
    REG = TelemetryServer(
        "reg", TelemetryAPIEndpoint.SFCTEST.url, TelemetryAPIEndpoint.SFCTEST.api_key
    )
    QA1 = TelemetryServer(
        "qa1", TelemetryAPIEndpoint.SFCDEV.url, TelemetryAPIEndpoint.SFCDEV.api_key
    )
    PREPROD3 = TelemetryServer(
        "preprod3", TelemetryAPIEndpoint.SFCDEV.url, TelemetryAPIEndpoint.SFCDEV.api_key
    )
    PROD = TelemetryServer(
        "prod", TelemetryAPIEndpoint.PROD.url, TelemetryAPIEndpoint.PROD.api_key
    )


ENABLED_DEPLOYMENTS = (
    TelemetryServerDeployments.DEV.name,
    TelemetryServerDeployments.REG.name,
    TelemetryServerDeployments.QA1.name,
    TelemetryServerDeployments.PREPROD3.name,
    TelemetryServerDeployments.PROD.name,
)


class TelemetryEvent(TelemetryEventBase):
    """Base class for log and metric telemetry events.

    This class has all of the logic except for the 'type' of the telemetry event.
    That must be defined by the child class.
    """

    def get_type(self):
        """Gets the telemetry event type."""
        raise NotImplementedError

    def to_dict(self):
        """Transform this event into a dictionary."""
        event = dict()
        event["Name"] = self.name
        event["Urgent"] = self.urgent
        event["Value"] = self.value
        event["Tags"] = self.generate_tags()
        event.update(
            {
                "UUID": str(uuid.uuid4()),
                "Created_On": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "Type": self.get_type(),
                "SchemaVersion": 1,
            }
        )
        return event

    def get_deployment(self):
        """Gets the deployment field specified in tags if it exists."""
        tags = self.tags
        if tags:
            for tag in tags:
                if tag.get("Name", None) == "deployment":
                    return tag.get("Value")

        return "Unknown"

    def generate_tags(self):
        """Generates the tags to send as part of the telemetry event. Parts of the tags are user defined."""
        tags = dict()
        # Add in tags that were added to the event
        if self.tags and len(self.tags) > 0:
            for k, v in self.tags.items():
                if v is not None:
                    tags[str(k).lower()] = str(v)

        telemetry = TelemetryService.get_instance()
        # Add telemetry service generated tags
        tags[TelemetryField.KEY_OOB_DRIVER] = CLIENT_NAME
        tags[TelemetryField.KEY_OOB_VERSION] = str(SNOWFLAKE_CONNECTOR_VERSION)
        tags[
            TelemetryField.KEY_OOB_TELEMETRY_SERVER_DEPLOYMENT
        ] = telemetry.deployment.name
        tags[
            TelemetryField.KEY_OOB_CONNECTION_STRING
        ] = telemetry.get_connection_string()
        if telemetry.context and len(telemetry.context) > 0:
            for k, v in telemetry.context.items():
                if v is not None:
                    tags["ctx_" + str(k).lower()] = str(v)

        return tags


class TelemetryLogEvent(TelemetryEvent):
    def get_type(self):
        return "Log"


class TelemetryMetricEvent(TelemetryEvent):
    def get_type(self):
        return "Metric"


class TelemetryService:
    __instance = None

    @staticmethod
    def get_instance():
        """Static access method."""
        if TelemetryService.__instance is None:
            TelemetryService()
        return TelemetryService.__instance

    def __init__(self):
        """Virtually private constructor."""
        if TelemetryService.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            TelemetryService.__instance = self
        self._enabled = True
        self._queue = Queue()
        self.batch_size = DEFAULT_BATCH_SIZE
        self.num_of_retry_to_trigger_telemetry = (
            DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY
        )
        self.context = dict()
        self.connection_params = dict()
        self.deployment = TelemetryServerDeployments.PROD

    def __del__(self):
        """Tries to flush all events left in the queue. Ignores all exceptions."""
        try:
            self.close()
        except Exception:
            pass

    @property
    def enabled(self):
        """Whether the Telemetry service is enabled or not."""
        return self._enabled

    def enable(self):
        """Enable Telemetry Service."""
        self._enabled = True

    def disable(self):
        """Disable Telemetry Service."""
        self._enabled = False

    @property
    def queue(self):
        """Get the queue that holds all of the telemetry events."""
        return self._queue

    @property
    def context(self):
        """Returns the context of the current connection."""
        return self._context

    @context.setter
    def context(self, value):
        """Sets the context of the current connection."""
        self._context = value

    @property
    def connection_params(self):
        """Returns the connection parameters from the current connection."""
        return self._connection_params

    @connection_params.setter
    def connection_params(self, value):
        """Sets the connection parameters from the current connection."""
        self._connection_params = value

    @property
    def batch_size(self):
        """Returns the batch size for uploading results."""
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value):
        """Sets the batch size for uploading results."""
        self._batch_size = value

    @property
    def num_of_retry_to_trigger_telemetry(self):
        """Returns the number of HTTP retries before we submit a telemetry event."""
        return self._num_of_retry_to_trigger_telemetry

    @num_of_retry_to_trigger_telemetry.setter
    def num_of_retry_to_trigger_telemetry(self, value):
        """Sets the number of HTTP retries before we submit a telemetry event."""
        self._num_of_retry_to_trigger_telemetry = value

    @property
    def deployment(self):
        """Returns the deployment that we are sending the telemetry information to."""
        return self._deployment

    @deployment.setter
    def deployment(self, value):
        """Sets the deployment that we are sending the telemetry information to."""
        self._deployment = value

    def is_deployment_enabled(self):
        """Returns whether or not this deployment is enabled."""
        return self.deployment.name in ENABLED_DEPLOYMENTS

    def get_connection_string(self):
        """Returns the URL used to connect to Snowflake."""
        return (
            self.connection_params.get("protocol", "")
            + "://"
            + self.connection_params.get("host", "")
            + ":"
            + str(self.connection_params.get("port", ""))
        )

    def add(self, event):
        """Adds a telemetry event to the queue. If the event is urgent, upload all telemetry events right away."""
        if not self.enabled:
            return

        self.queue.put(event)
        if self.queue.qsize() > self.batch_size or event.urgent:
            payload = self.export_queue_to_string()
            if payload is None:
                return
            self._upload_payload(payload)

    def flush(self):
        """Flushes all telemetry events in the queue and submit them to the back-end."""
        if not self.enabled:
            return

        if not self.queue.empty():
            payload = self.export_queue_to_string()
            if payload is None:
                return
            self._upload_payload(payload)

    def update_context(self, connection_params):
        """Updates the telemetry service context. Remove any passwords or credentials."""
        self.configure_deployment(connection_params)
        self.context = dict()

        for key, value in connection_params.items():
            if (
                "password" not in key
                and "passcode" not in key
                and "privateKey" not in key
            ):
                self.context[key] = value

    def configure_deployment(self, connection_params):
        """Determines which deployment we are sending Telemetry OOB messages to."""
        self.connection_params = connection_params
        account = (
            self.connection_params.get("account")
            if self.connection_params.get("account")
            else ""
        )
        host = (
            self.connection_params.get("host")
            if self.connection_params.get("host")
            else ""
        )
        port = self.connection_params.get("port", None)

        # Set as PROD by default
        deployment = TelemetryServerDeployments.PROD
        if "reg" in host or "local" in host:
            deployment = TelemetryServerDeployments.REG
            if port == 8080:
                deployment = TelemetryServerDeployments.DEV
        elif "qa1" in host or "qa1" in account:
            deployment = TelemetryServerDeployments.QA1
        elif "preprod3" in host:
            deployment = TelemetryServerDeployments.PREPROD3

        self.deployment = deployment

    def log_ocsp_exception(
        self,
        event_type,
        telemetry_data,
        exception=None,
        stack_trace=None,
        tags=None,
        urgent=False,
    ):
        """Logs an OCSP Exception and adds it to the queue to be uploaded."""
        if tags is None:
            tags = dict()
        try:
            if self.enabled:
                event_name = "OCSPException"
                if exception is not None:
                    telemetry_data[
                        TelemetryField.KEY_OOB_EXCEPTION_MESSAGE.value
                    ] = str(exception)
                if stack_trace is not None:
                    telemetry_data[
                        TelemetryField.KEY_OOB_EXCEPTION_STACK_TRACE.value
                    ] = stack_trace

                if tags is None:
                    tags = dict()

                tags[TelemetryField.KEY_OOB_EVENT_TYPE.value] = event_type

                log_event = TelemetryLogEvent(
                    name=event_name, tags=tags, urgent=urgent, value=telemetry_data
                )

                self.add(log_event)
        except Exception:
            # Do nothing on exception, just log
            logger.debug("Failed to log OCSP exception", exc_info=True)

    def log_http_request_error(
        self,
        event_name,
        url,
        method,
        sqlstate,
        errno,
        response=None,
        retry_timeout=None,
        retry_count=None,
        exception=None,
        stack_trace=None,
        tags=None,
        urgent=False,
    ):
        """Logs an HTTP Request error and adds it to the queue to be uploaded."""
        if tags is None:
            tags = dict()
        try:
            if self.enabled:
                response_status_code = -1
                # This mimics the output of HttpRequestBase.toString() from JBDC
                telemetry_data = generate_telemetry_data_dict(
                    from_dict={
                        TelemetryField.KEY_OOB_REQUEST.value: f"{method} {url}",
                        TelemetryField.KEY_OOB_SQL_STATE.value: sqlstate,
                        TelemetryField.KEY_OOB_ERROR_CODE.value: errno,
                    },
                    is_oob_telemetry=True,
                )
                if response:
                    telemetry_data[
                        TelemetryField.KEY_OOB_RESPONSE.value
                    ] = response.json()
                    telemetry_data[
                        TelemetryField.KEY_OOB_RESPONSE_STATUS_LINE.value
                    ] = str(response.reason)
                    if response.status_code:
                        response_status_code = str(response.status_code)
                        telemetry_data[
                            TelemetryField.KEY_OOB_RESPONSE_STATUS_CODE.value
                        ] = response_status_code
                if retry_timeout:
                    telemetry_data[TelemetryField.KEY_OOB_RETRY_TIMEOUT.value] = str(
                        retry_timeout
                    )
                if retry_count:
                    telemetry_data[TelemetryField.KEY_OOB_RETRY_COUNT.value] = str(
                        retry_count
                    )
                if exception:
                    telemetry_data[
                        TelemetryField.KEY_OOB_EXCEPTION_MESSAGE.value
                    ] = str(exception)
                if stack_trace:
                    telemetry_data[
                        TelemetryField.KEY_OOB_EXCEPTION_STACK_TRACE.value
                    ] = stack_trace

                if tags is None:
                    tags = dict()

                tags[
                    TelemetryField.KEY_OOB_RESPONSE_STATUS_CODE.value
                ] = response_status_code
                tags[TelemetryField.KEY_OOB_SQL_STATE.value] = str(sqlstate)
                tags[TelemetryField.KEY_OOB_ERROR_CODE.value] = errno

                log_event = TelemetryLogEvent(
                    name=event_name, tags=tags, value=telemetry_data, urgent=urgent
                )

                self.add(log_event)
        except Exception:
            # Do nothing on exception, just log
            logger.debug("Failed to log HTTP request error", exc_info=True)

    def log_general_exception(
        self,
        event_name: str,
        telemetry_data: dict,
        tags: dict | None = None,
        urgent: bool | None = False,
    ):
        """Sends any type of exception through OOB telemetry."""
        if tags is None:
            tags = dict()
        try:
            if self.enabled:
                log_event = TelemetryLogEvent(
                    name=event_name, tags=tags, value=telemetry_data, urgent=urgent
                )
                self.add(log_event)
        except Exception:
            # Do nothing on exception, just log
            logger.debug("Failed to log general exception", exc_info=True)

    def _upload_payload(self, payload):
        """Uploads the JSON-formatted string payload to the telemetry backend.

        Ignore any exceptions that may arise.
        """
        success = True
        response = None
        try:
            if not self.is_deployment_enabled():
                logger.debug("Skip the disabled deployment: %s", self.deployment.name)
                return
            logger.debug(f"Sending OOB telemetry data. Payload: {payload}")
            if ENABLE_TELEMETRY_LOG:
                # This logger guarantees the payload won't be masked. Testing purpose.
                rt_plain_logger.debug(f"OOB telemetry data being sent is {payload}")

            with requests.Session() as session:
                headers = {
                    "Content-type": "application/json",
                    "x-api-key": self.deployment.api_key,
                }
                response = session.post(
                    self.deployment.url,
                    data=payload,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                )
                if (
                    response.status_code == OK
                    and json.loads(response.text).get("statusCode", 0) == OK
                ):
                    logger.debug(
                        "telemetry server request success: %d", response.status_code
                    )
                else:
                    logger.debug(
                        "telemetry server request error: %d", response.status_code
                    )
                    success = False
        except Exception as e:
            logger.debug(
                "Telemetry request failed, Exception response: %s, exception: %s",
                response,
                str(e),
            )
            success = False
        finally:
            logger.debug("Telemetry request success=%s", success)

    def export_queue_to_string(self):
        """Exports all events in the queue into a JSON formatted string with secrets masked."""
        logs = list()
        while not self._queue.empty():
            logs.append(self._queue.get().to_dict())
        # We may get an exception trying to serialize a python object to JSON
        try:
            payload = json.dumps(logs)
        except Exception:
            logger.debug(
                "Failed to generate a JSON dump from the passed in telemetry OOB events. String representation of logs: %s"
                % str(logs),
                exc_info=True,
            )
            payload = None
        _, masked_text, _ = SecretDetector.mask_secrets(payload)
        return masked_text

    def close(self):
        """Closes the telemetry service."""
        self.flush()
        self.disable()

    def size(self):
        """Returns the size of the queue."""
        return self.queue.qsize()
