# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import traceback

from opentelemetry._logs import LogRecord
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.semconv.attributes import exception_attributes
from opentelemetry.util.types import AnyValue, _ExtendedAttributes


def _get_exception_attributes(
    exception: BaseException,
) -> dict[str, AnyValue]:
    stacktrace = "".join(
        traceback.format_exception(
            type(exception), value=exception, tb=exception.__traceback__
        )
    )
    module = type(exception).__module__
    qualname = type(exception).__qualname__
    exception_type = (
        f"{module}.{qualname}" if module and module != "builtins" else qualname
    )
    return {
        exception_attributes.EXCEPTION_TYPE: exception_type,
        exception_attributes.EXCEPTION_MESSAGE: str(exception),
        exception_attributes.EXCEPTION_STACKTRACE: stacktrace,
    }


def _get_attributes_with_exception(
    attributes: _ExtendedAttributes | None,
    exception: BaseException | None,
) -> _ExtendedAttributes | None:
    if exception is None:
        return attributes

    exception_attributes_map = _get_exception_attributes(exception)
    if attributes is None:
        attributes_map: _ExtendedAttributes = {}
    else:
        attributes_map = attributes

    if isinstance(attributes_map, BoundedAttributes):
        bounded_attributes = attributes_map
        merged = BoundedAttributes(
            maxlen=bounded_attributes.maxlen,
            attributes=bounded_attributes,
            immutable=False,
            max_value_len=bounded_attributes.max_value_len,
            extended_attributes=bounded_attributes._extended_attributes,  # pylint: disable=protected-access
        )
        merged.dropped = bounded_attributes.dropped
        for key, value in exception_attributes_map.items():
            if key not in merged:
                merged[key] = value
        return merged

    return exception_attributes_map | dict(attributes_map.items())


def _copy_log_record(
    record: LogRecord,
    attributes: _ExtendedAttributes | None,
) -> LogRecord:
    copied_record = LogRecord(
        timestamp=record.timestamp,
        observed_timestamp=record.observed_timestamp,
        context=record.context,
        severity_text=record.severity_text,
        severity_number=record.severity_number,
        body=record.body,
        attributes=attributes,
        event_name=record.event_name,
        exception=getattr(record, "exception", None),
    )
    copied_record.trace_id = record.trace_id
    copied_record.span_id = record.span_id
    copied_record.trace_flags = record.trace_flags
    return copied_record


def _copy_log_record_with_exception(record: LogRecord) -> LogRecord:
    return _copy_log_record(
        record,
        _get_attributes_with_exception(record.attributes, record.exception),
    )


def _set_log_record_exception_attributes(record: LogRecord) -> None:
    record.attributes = _get_attributes_with_exception(
        record.attributes,
        record.exception,
    )


def _create_log_record_with_exception(
    *,
    timestamp: int | None = None,
    observed_timestamp: int | None = None,
    context=None,
    severity_number=None,
    severity_text: str | None = None,
    body: AnyValue | None = None,
    attributes: _ExtendedAttributes | None = None,
    event_name: str | None = None,
    exception: BaseException | None = None,
) -> LogRecord:
    return LogRecord(
        timestamp=timestamp,
        observed_timestamp=observed_timestamp,
        context=context,
        severity_number=severity_number,
        severity_text=severity_text,
        body=body,
        attributes=_get_attributes_with_exception(attributes, exception),
        event_name=event_name,
        exception=exception,
    )
