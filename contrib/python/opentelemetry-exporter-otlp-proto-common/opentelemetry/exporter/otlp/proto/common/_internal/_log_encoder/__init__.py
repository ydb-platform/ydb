# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from collections import defaultdict
from typing import List, Sequence

from opentelemetry.exporter.otlp.proto.common._internal import (
    _encode_attributes,
    _encode_instrumentation_scope,
    _encode_resource,
    _encode_span_id,
    _encode_trace_id,
    _encode_value,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord as PB2LogRecord
from opentelemetry.proto.logs.v1.logs_pb2 import (
    ResourceLogs,
    ScopeLogs,
)
from opentelemetry.sdk._logs import ReadableLogRecord


def encode_logs(
    batch: Sequence[ReadableLogRecord],
) -> ExportLogsServiceRequest:
    return ExportLogsServiceRequest(resource_logs=_encode_resource_logs(batch))


def _encode_log(readable_log_record: ReadableLogRecord) -> PB2LogRecord:
    span_id = (
        None
        if readable_log_record.log_record.span_id == 0
        else _encode_span_id(readable_log_record.log_record.span_id)
    )
    trace_id = (
        None
        if readable_log_record.log_record.trace_id == 0
        else _encode_trace_id(readable_log_record.log_record.trace_id)
    )
    body = readable_log_record.log_record.body
    return PB2LogRecord(
        time_unix_nano=readable_log_record.log_record.timestamp,
        observed_time_unix_nano=readable_log_record.log_record.observed_timestamp,
        span_id=span_id,
        trace_id=trace_id,
        flags=int(readable_log_record.log_record.trace_flags),
        body=_encode_value(body, allow_null=True),
        severity_text=readable_log_record.log_record.severity_text,
        attributes=_encode_attributes(
            readable_log_record.log_record.attributes, allow_null=True
        ),
        dropped_attributes_count=readable_log_record.dropped_attributes,
        severity_number=getattr(
            readable_log_record.log_record.severity_number, "value", None
        ),
        event_name=readable_log_record.log_record.event_name,
    )


def _encode_resource_logs(
    batch: Sequence[ReadableLogRecord],
) -> List[ResourceLogs]:
    sdk_resource_logs = defaultdict(lambda: defaultdict(list))

    for readable_log in batch:
        sdk_resource = readable_log.resource
        sdk_instrumentation = readable_log.instrumentation_scope or None
        pb2_log = _encode_log(readable_log)

        sdk_resource_logs[sdk_resource][sdk_instrumentation].append(pb2_log)

    pb2_resource_logs = []

    for sdk_resource, sdk_instrumentations in sdk_resource_logs.items():
        scope_logs = []
        for sdk_instrumentation, pb2_logs in sdk_instrumentations.items():
            scope_logs.append(
                ScopeLogs(
                    scope=(_encode_instrumentation_scope(sdk_instrumentation)),
                    log_records=pb2_logs,
                    schema_url=sdk_instrumentation.schema_url
                    if sdk_instrumentation
                    else None,
                )
            )
        pb2_resource_logs.append(
            ResourceLogs(
                resource=_encode_resource(sdk_resource),
                scope_logs=scope_logs,
                schema_url=sdk_resource.schema_url,
            )
        )

    return pb2_resource_logs
