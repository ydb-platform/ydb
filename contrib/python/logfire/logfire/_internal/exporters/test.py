from __future__ import annotations

import json
import os
import re
import sys
import typing
from collections.abc import Mapping, Sequence
from functools import partial
from json import JSONDecodeError
from pathlib import Path
from typing import Any, cast

from opentelemetry import trace
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs._internal.export import LogRecordExportResult
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from ..constants import ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY, ATTRIBUTES_SPAN_TYPE_KEY


class TestExporter(SpanExporter):
    """A SpanExporter that stores exported spans in a list for asserting in tests."""

    # NOTE: Avoid test discovery by pytest.
    __test__ = False

    def __init__(self) -> None:
        self.exported_spans: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Exports a batch of telemetry data."""
        self.exported_spans.extend(spans)
        return SpanExportResult.SUCCESS

    def clear(self) -> None:
        """Clears the collected spans."""
        self.exported_spans = []

    def exported_spans_as_dict(
        self,
        fixed_line_number: int | None = 123,
        strip_filepaths: bool = True,
        include_resources: bool = False,
        include_instrumentation_scope: bool = False,
        _include_pending_spans: bool = False,
        _strip_function_qualname: bool = True,
        parse_json_attributes: bool = False,
    ) -> list[dict[str, Any]]:
        """The exported spans as a list of dicts.

        Args:
            fixed_line_number: The line number to use for all spans.
            strip_filepaths: Whether to strip the filepaths from the exported spans.
            include_resources: Whether to include the resource attributes in the exported spans.
            include_instrumentation_scope: Whether to include the instrumentation scope in the exported spans.
            parse_json_attributes: Whether to parse strings containing JSON arrays/objects.

        Returns:
            A list of dicts representing the exported spans.
        """
        _build_attributes = partial(
            build_attributes,
            fixed_line_number=fixed_line_number,
            strip_filepaths=strip_filepaths,
            strip_function_qualname=_strip_function_qualname,
            parse_json_attributes=parse_json_attributes,
        )

        def build_context(context: trace.SpanContext) -> dict[str, Any]:
            return {'trace_id': context.trace_id, 'span_id': context.span_id, 'is_remote': context.is_remote}

        def build_link(link: trace.Link) -> dict[str, Any]:
            context = link.context or trace.INVALID_SPAN_CONTEXT
            return {'context': build_context(context), 'attributes': _build_attributes(link.attributes)}

        def build_event(event: Event) -> dict[str, Any]:
            res: dict[str, Any] = {'name': event.name, 'timestamp': event.timestamp}
            if event.attributes:  # pragma: no branch
                res['attributes'] = attributes = _build_attributes(event.attributes)
                assert attributes
                if 'exception.stacktrace' in attributes:
                    last_line = next(  # pragma: no branch
                        line.strip()
                        for line in reversed(cast(str, event.attributes['exception.stacktrace']).split('\n'))
                        if line.strip()
                    )
                    attributes['exception.stacktrace'] = last_line

            return res

        def build_instrumentation_scope(span: ReadableSpan) -> dict[str, Any]:
            if include_instrumentation_scope:
                return {'instrumentation_scope': span.instrumentation_scope and span.instrumentation_scope.name}
            else:
                return {}

        def build_span(span: ReadableSpan) -> dict[str, Any]:
            context = span.context or trace.INVALID_SPAN_CONTEXT
            res: dict[str, Any] = {
                'name': span.name,
                'context': build_context(context),
                'parent': build_context(span.parent) if span.parent else None,
                'start_time': span.start_time,
                'end_time': span.end_time,
                **build_instrumentation_scope(span),
                'attributes': _build_attributes(span.attributes),
            }
            if span.links:
                res['links'] = [build_link(link) for link in span.links]
            if span.events:
                res['events'] = [build_event(event) for event in span.events]
            if include_resources:
                resource_attributes = _build_attributes(span.resource.attributes)
                res['resource'] = {
                    'attributes': resource_attributes,
                }
            return res

        spans = [build_span(span) for span in self.exported_spans]
        return [
            span
            for span in spans
            if _include_pending_spans is True
            or (span.get('attributes', {}).get(ATTRIBUTES_SPAN_TYPE_KEY, 'span') != 'pending_span')
        ]


def process_attribute(
    name: str,
    value: Any,
    strip_filepaths: bool,
    fixed_line_number: int | None,
    strip_function_qualname: bool,
    parse_json_attributes: bool = False,
) -> Any:
    if name == 'code.filepath' and strip_filepaths:
        try:
            return Path(value).name
        except ValueError:  # pragma: no cover
            return value
    if name == 'code.lineno' and fixed_line_number is not None:
        return fixed_line_number
    if name == 'code.function':
        if sys.version_info >= (3, 11) and strip_function_qualname:
            return value.split('.')[-1]
    if name == 'process.pid':
        assert value == os.getpid()
        return 1234
    if name == 'service.instance.id':
        if re.match(r'^[0-9a-f]{32}$', value):
            return '0' * 32
    if parse_json_attributes and isinstance(value, str) and (value.startswith('{') or value.startswith('[')):
        try:
            return json.loads(value)
        except JSONDecodeError:  # pragma: no cover
            pass
    return value


def build_attributes(
    attributes: Mapping[str, Any] | None,
    strip_filepaths: bool,
    fixed_line_number: int | None,
    strip_function_qualname: bool,
    parse_json_attributes: bool,
) -> dict[str, Any] | None:
    if attributes is None:  # pragma: no cover
        return None
    attributes = {
        k: process_attribute(k, v, strip_filepaths, fixed_line_number, strip_function_qualname, parse_json_attributes)
        for k, v in attributes.items()
    }
    if ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY in attributes:
        attributes[ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY] = '0' * 64
    if 'telemetry.sdk.version' in attributes:
        attributes['telemetry.sdk.version'] = '0.0.0'
    return attributes


class TestLogExporter(InMemoryLogRecordExporter):
    """A LogExporter that stores exported logs in a list for asserting in tests."""

    # NOTE: Avoid test discovery by pytest.
    __test__ = False

    def __init__(self, ns_timestamp_generator: typing.Callable[[], int]) -> None:
        super().__init__()
        self.ns_timestamp_generator = ns_timestamp_generator

    def export(self, batch: typing.Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        for log in batch:
            log.log_record.timestamp = self.ns_timestamp_generator()
            log.log_record.observed_timestamp = self.ns_timestamp_generator()
        return super().export(batch)

    def exported_logs_as_dicts(
        self,
        fixed_line_number: int | None = 123,
        strip_filepaths: bool = True,
        include_resources: bool = False,
        include_instrumentation_scope: bool = False,
        _strip_function_qualname: bool = True,
        parse_json_attributes: bool = False,
    ) -> list[dict[str, Any]]:
        _build_attributes = partial(
            build_attributes,
            fixed_line_number=fixed_line_number,
            strip_filepaths=strip_filepaths,
            strip_function_qualname=_strip_function_qualname,
            parse_json_attributes=parse_json_attributes,
        )

        def build_log(log_data: ReadableLogRecord) -> dict[str, Any]:
            log_record = log_data.log_record
            res = {
                'body': log_record.body,
                'severity_number': log_record.severity_number.value if log_record.severity_number is not None else None,
                'severity_text': log_record.severity_text,
                'attributes': _build_attributes(log_record.attributes),
                'timestamp': log_record.timestamp,
                'observed_timestamp': log_record.observed_timestamp,
                'trace_id': log_record.trace_id,
                'span_id': log_record.span_id,
                'trace_flags': log_record.trace_flags,
            }

            if include_resources:  # pragma: no branch
                resource_attributes = _build_attributes(log_data.resource.attributes)
                res['resource'] = {
                    'attributes': resource_attributes,
                }

            if include_instrumentation_scope and log_data.instrumentation_scope:  # pragma: no branch
                res['instrumentation_scope'] = log_data.instrumentation_scope.name

            return res

        return [build_log(log) for log in self.get_finished_logs()]

    def shutdown(self) -> None:
        self.clear()
