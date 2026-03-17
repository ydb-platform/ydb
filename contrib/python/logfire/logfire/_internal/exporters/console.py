"""Console exporter for OpenTelemetry.

Inspired by https://opentelemetry-python.readthedocs.io/en/latest/_modules/opentelemetry/sdk/trace/export.html#ConsoleSpanExporter
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from textwrap import indent as indent_text
from typing import Any, Literal, TextIO, cast

from opentelemetry._logs import LogRecord
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import LogRecordExporter, LogRecordExportResult
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from rich.columns import Columns
from rich.console import Console, Group
from rich.syntax import Syntax
from rich.text import Text

from ..constants import (
    ATTRIBUTES_JSON_SCHEMA_KEY,
    ATTRIBUTES_LOG_LEVEL_NUM_KEY,
    ATTRIBUTES_MESSAGE_KEY,
    ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY,
    ATTRIBUTES_SPAN_TYPE_KEY,
    ATTRIBUTES_TAGS_KEY,
    DISABLE_CONSOLE_KEY,
    LEVEL_NUMBERS,
    NUMBER_TO_LEVEL,
    ONE_SECOND_IN_NANOSECONDS,
    LevelName,
)
from ..json_formatter import json_args_value_formatter
from ..utils import truncate_string

ConsoleColorsValues = Literal['auto', 'always', 'never']
_INFO_LEVEL = LEVEL_NUMBERS['info']
_WARN_LEVEL = LEVEL_NUMBERS['warn']
_ERROR_LEVEL = LEVEL_NUMBERS['error']

# A list of (text, style) pairs that can be passed to rich's `Text.assemble`.
# When logging without colors, just the text is used in a plain `print`.
TextParts = list[tuple[str, str]]


@dataclass
class Record:
    attributes: Mapping[str, object]
    timestamp: int
    message: str
    events: Sequence[Event]
    span_id: int | None
    parent_span_id: int | None
    kind: str
    level: int

    @classmethod
    def from_span(cls, span: ReadableSpan) -> Record:
        attributes = span.attributes or {}
        return cls(
            attributes=attributes,
            timestamp=span.start_time or 0,
            message=attributes.get(ATTRIBUTES_MESSAGE_KEY) or span.name,  # type: ignore
            events=span.events,
            span_id=span.context and span.context.span_id,
            parent_span_id=span.parent and span.parent.span_id,
            kind=attributes.get(ATTRIBUTES_SPAN_TYPE_KEY, 'span'),  # type: ignore
            level=attributes.get(ATTRIBUTES_LOG_LEVEL_NUM_KEY, _INFO_LEVEL),  # type: ignore
        )

    @classmethod
    def from_log(cls, log: LogRecord) -> Record:
        attributes = log.attributes or {}
        message: str = attributes.get(ATTRIBUTES_MESSAGE_KEY)  # type: ignore
        if not message:
            # TODO: this message could be better, for now we just want to have *something*
            # TODO: this message should be constructed in a wrapper processor so that it's also used in the UI
            parts: list[str] = []
            if event_name := (getattr(log, 'event_name', None) or attributes.get('event.name')):
                parts.append(str(event_name))
            if body := log.body:
                parts.append(truncate_string(str(body), max_length=100))
            else:
                other_attributes = {k: v for k, v in attributes.items() if k != 'event.name'}
                parts.append(truncate_string(str(other_attributes), max_length=100))
            message = ': '.join(parts)
        return cls(
            attributes=attributes,
            timestamp=log.timestamp or log.observed_timestamp or 0,
            message=message,
            events=[],
            span_id=None,
            parent_span_id=log.span_id,
            kind='log',
            level=log.severity_number.value if log.severity_number else _INFO_LEVEL,
        )


class SimpleConsoleSpanExporter(SpanExporter):
    """The ConsoleSpanExporter prints spans to the console.

    This simple version does not indent spans based on their parent(s), instead spans are printed as a
    flat list.
    """

    def __init__(
        self,
        output: TextIO | None = None,
        colors: ConsoleColorsValues = 'auto',
        include_timestamp: bool = True,
        include_tags: bool = True,
        verbose: bool = False,
        min_log_level: LevelName = 'info',
    ) -> None:
        self._output = output or sys.stdout
        if colors == 'auto':
            force_terminal = None
        else:
            force_terminal = colors == 'always'
        self._console = Console(
            color_system='standard' if os.environ.get('PYTEST_VERSION') else 'auto',
            file=self._output,
            force_terminal=force_terminal,
            highlight=False,
            markup=False,
            soft_wrap=True,
        )
        if not self._console.is_terminal:
            # Use plain `print` to `self._output` instead of rich when we don't need colors
            self._console = None

        self._include_timestamp = include_timestamp
        self._include_tags = include_tags
        # timestamp len('12:34:56.789') 12 + space (1)
        self._timestamp_indent = 13 if include_timestamp else 0
        self._verbose = verbose
        self._min_log_level_num = LEVEL_NUMBERS[min_log_level]

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Export the spans to the console."""
        for span in spans:
            self.export_record(Record.from_span(span))

        return SpanExportResult.SUCCESS

    def export_record(self, span: Record) -> None:
        """Print a summary of the span, this method can be overridden to customize how spans are displayed.

        In this simple case we just print the span if its type is not "span" - e.g. the message at the end of a span.
        """
        self._print_span(span)

    def _print_span(self, span: Record, indent: int = 0):
        """Build up a summary of the span, including formatting for rich, then print it."""
        _msg, parts = self._span_text_parts(span, indent)

        # only print for "pending_span" (received at the start of a span) and "log" (spans with no duration)
        if span.kind == 'span' or span.attributes.get(DISABLE_CONSOLE_KEY):
            return
        if span.level < self._min_log_level_num:
            return

        indent_str = (self._timestamp_indent + indent * 2) * ' '
        details_parts = self._details_parts(span, indent_str)
        if details_parts:
            parts += [('\n', '')] + details_parts

        if self._console:
            self._console.print(Text.assemble(*parts))
        else:
            print(''.join(text for text, _style in parts), file=self._output)

        # This uses a separate system for color vs no-color because it's not simple text:
        # in the rich case it uses syntax highlighting and columns for layout.
        self._print_arguments(span, indent_str)

        exc_event = next((event for event in span.events or [] if event.name == 'exception'), None)
        self._print_exc_info(exc_event, indent_str)

    def _span_text_parts(self, span: Record, indent: int) -> tuple[str, TextParts]:
        """Return the formatted message or span name and parts containing basic span information.

        The following information is included:
        * timestamp
        * message (maybe indented)
        * tags (if `self._include_tags` is True)

        The log level may be indicated by the color of the message.
        """
        parts: TextParts = []
        if self._include_timestamp:
            ts = datetime.fromtimestamp(span.timestamp / ONE_SECOND_IN_NANOSECONDS)
            # ugly though it is, `[:-3]` is the simplest way to convert microseconds -> milliseconds
            ts_str = f'{ts:%H:%M:%S.%f}'[:-3]
            parts += [(ts_str, 'green'), (' ', '')]

        if indent:
            parts += [(indent * '  ', '')]

        msg = span.message
        level = span.level
        if level >= _ERROR_LEVEL:
            # add the message in red if it's an error or worse
            parts += [(msg, 'red')]
        elif level >= _WARN_LEVEL:
            # add the message in yellow if it's a warning
            parts += [(msg, 'yellow')]
        else:
            parts += [(msg, '')]

        if self._include_tags:
            if tags := span.attributes.get(ATTRIBUTES_TAGS_KEY):
                tags_str = ','.join(cast('list[str]', tags))
                parts += [(' ', ''), (f'[{tags_str}]', 'cyan')]

        return msg, parts

    def _details_parts(self, span: Record, indent_str: str) -> TextParts:
        """Return parts containing details for the span if `self._verbose` is True.

        The following details are returned:
        * filename and line number
        * the log level name
        """
        if not self._verbose or not span.attributes:
            return []

        file_location_raw = span.attributes.get('code.filepath')
        file_location = None if file_location_raw in (None, 'null') else str(file_location_raw)
        if file_location:
            lineno = span.attributes.get('code.lineno')
            if lineno not in (None, 'null'):
                file_location += f':{lineno}'

        log_level_num: int = span.attributes.get(ATTRIBUTES_LOG_LEVEL_NUM_KEY)  # type: ignore
        log_level = NUMBER_TO_LEVEL.get(log_level_num)

        if file_location or log_level:
            parts: TextParts = [(indent_str, ''), ('│', 'blue')]
            if file_location:
                parts.append((f' {file_location}', 'cyan'))
            if log_level:
                parts.append((f' {log_level}', ''))
            return parts
        else:
            return []

    def _print_arguments(self, span: Record, indent_str: str):
        """Pretty-print formatted logfire arguments for the span if `self._verbose` is True."""
        if not self._verbose or not span.attributes:
            return

        arguments: dict[str, Any] = {}
        json_schema = cast('dict[str, Any]', json.loads(span.attributes.get(ATTRIBUTES_JSON_SCHEMA_KEY, '{}')))  # type: ignore
        for key, schema in json_schema.get('properties', {}).items():
            value = span.attributes.get(key)
            if schema and isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    schema = None
            value = json_args_value_formatter(value, schema=schema)
            arguments[key] = value

        if not arguments:
            return

        if self._console:
            self._print_arguments_rich(arguments, indent_str)
        else:
            self._print_arguments_plain(arguments, indent_str)

    def _print_arguments_rich(self, arguments: dict[str, Any], indent_str: str) -> None:
        """Print logfire arguments in color using rich, particularly with syntax highlighting."""
        assert self._console is not None

        chunks: list[Columns] = []
        for k, value_code in arguments.items():
            key = Text(f'{k}=', style='blue')
            value = Syntax(value_code, 'python', background_color='default')
            barrier = Text(('│ \n' * (value_code.count('\n') + 1))[:-1], style='blue')
            chunks.append(
                Columns(
                    (
                        # Don't have a column for empty indent_str as it will still take space
                        *[indent_str] * bool(indent_str),
                        barrier,
                        key,
                        value,
                    ),
                    padding=(0, 0),
                )
            )

        self._console.print(Group(*chunks))

    def _print_arguments_plain(self, arguments: dict[str, Any], indent_str: str) -> None:
        """Print logfire arguments without color using the built-in `print` function."""
        out: list[str] = []
        for k, value_code in arguments.items():
            value_lines = value_code.splitlines()
            out += [f'{indent_str}│ {k}={value_lines[0]}']
            prefix = f'{indent_str}│ {" " * len(k)} '
            for line in value_lines[1:]:
                out += [f'{prefix}{line}']
        print('\n'.join(out), file=self._output)

    def _print_exc_info(self, exc_event: Event | None, indent_str: str) -> None:
        """Print exception information if an exception event is present."""
        if exc_event is None or not exc_event.attributes:
            return

        exc_type = cast(str, exc_event.attributes.get('exception.type'))
        exc_msg = cast(str, exc_event.attributes.get('exception.message'))
        exc_tb = cast(str, exc_event.attributes.get('exception.stacktrace'))

        if self._console:
            barrier = Text(indent_str + '│ ', style='blue', end='')
            exc_type = Text(f'{exc_type}: ', end='', style='bold red')
            exc_msg = Text(exc_msg)
            indented_code = indent_text(exc_tb, indent_str + '│ ')
            exc_tb = Syntax(indented_code, 'python', background_color='default')
            self._console.print(Group(barrier, exc_type, exc_msg), exc_tb)
        else:
            out = [f'{indent_str}│ {exc_type}: {exc_msg}']
            out += [indent_text(exc_tb, indent_str + '│ ')]
            print('\n'.join(out), file=self._output)

    def force_flush(self, timeout_millis: int = 0) -> bool:  # pragma: no cover
        """Force flush all spans, does nothing for this exporter."""
        return True


class IndentedConsoleSpanExporter(SimpleConsoleSpanExporter):
    """The ConsoleSpanExporter exports spans to the console, indented.

    Spans are intended based simply on how many parents they have. This will work well when spans don't overlap,
    but will be hard to understand when multiple spans are in progress at the same time.
    """

    def __init__(
        self,
        output: TextIO | None = None,
        colors: ConsoleColorsValues = 'auto',
        include_timestamp: bool = True,
        include_tags: bool = True,
        verbose: bool = False,
        min_log_level: LevelName = 'info',
    ) -> None:
        super().__init__(output, colors, include_timestamp, include_tags, verbose, min_log_level)
        # lookup from span ID to indent level
        self._indent_level: dict[int, int] = {}

    def export_record(self, span: Record) -> None:
        """Get the span indent based on `self._indent_level`, then print the span with that indent."""
        if span.kind == 'span':
            # this is the end of a span, remove it from `self._indent_level` and don't print
            if span.span_id is not None:  # pragma: no branch
                self._indent_level.pop(span.span_id, None)
            return

        block_span_id = span.parent_span_id
        if span.kind == 'pending_span':
            parent_id = _pending_span_parent(span.attributes)
            indent = self._indent_level.get(parent_id, 0) if parent_id else 0

            # block_span_id will be the parent_id for all subsequent spans and logs in this block
            if block_span_id is not None:  # pragma: no branch
                self._indent_level[block_span_id] = indent + 1
        else:
            # this is a log, we just get the indent level from the parent span
            indent = self._indent_level.get(block_span_id, 0) if block_span_id else 0

        self._print_span(span, indent)


class ShowParentsConsoleSpanExporter(SimpleConsoleSpanExporter):
    """The ConsoleSpanExporter exports spans to the console, indented with parents displayed where necessary.

    Spans are intended based on how many parents they have, where multiple concurrent spans overlap and therefore
    the previously displayed span is not the parent or sibling of a span, parents are printed (with "dim" color)
    so it's easy (or as easy as possible in a terminal) to understand how nested spans are related.
    """

    def __init__(
        self,
        output: TextIO | None = None,
        colors: ConsoleColorsValues = 'auto',
        include_timestamp: bool = True,
        include_tags: bool = True,
        verbose: bool = False,
        min_log_level: LevelName = 'info',
    ) -> None:
        super().__init__(output, colors, include_timestamp, include_tags, verbose, min_log_level)

        # lookup from span_id to `(indent, span message, parent id)`
        self._span_history: dict[int, tuple[int, str, int]] = {}
        # current open span ids
        self._span_stack: list[int] = []

    def export_record(self, span: Record) -> None:
        """Print any parent spans which aren't in the current stack of displayed spans, then print this span."""
        if span.kind == 'span':
            # this is the end of a span, remove it from `self._span_history` and `self._span_stack`, don't print
            if span.span_id is not None:  # pragma: no branch
                self._span_history.pop(span.span_id, None)
                if self._span_stack and self._span_stack[-1] == span.span_id:
                    self._span_stack.pop()
            return

        self._print_span(span)

    def _span_text_parts(self, span: Record, indent: int) -> tuple[str, TextParts]:
        """Parts for any parent spans which aren't in the current stack of displayed spans, then parts for this span."""
        parts: TextParts = []
        block_span_id = span.parent_span_id
        if span.kind == 'pending_span':
            parent_id = _pending_span_parent(span.attributes)
            parts += self._parent_stack_text_parts(parent_id)

            indent = len(self._span_stack)
            msg, span_parts = super()._span_text_parts(span, indent)
            parts += span_parts

            if block_span_id is not None:  # pragma: no branch
                self._span_history[block_span_id] = (indent, msg, parent_id or 0)
                self._span_stack.append(block_span_id)
        else:
            # this is a log
            parts += self._parent_stack_text_parts(block_span_id)
            msg, span_parts = super()._span_text_parts(span, indent=len(self._span_stack))
            parts += span_parts
        return msg, parts

    def _parent_stack_text_parts(self, parent_id: int | None) -> TextParts:
        """Parts for "intermediate" parent spans - e.g., spans which are not parents of the currently displayed span.

        Also build up `self._span_stack` to correctly represent the path to the current span.
        """
        # (indent, msg, parent_id)
        parents: list[tuple[int, str, int]] = []
        clear_stack = True
        # find a stack of parent spans until we reach a span in self._span_stack
        while parent_id:
            try:
                indent, line, grand_parent_id = self._span_history[parent_id]
            except KeyError:  # pragma: no cover
                break
            else:
                try:
                    stack_index = self._span_stack.index(parent_id)
                except ValueError:
                    parents.append((indent, line, parent_id))
                    parent_id = grand_parent_id
                else:
                    self._span_stack = self._span_stack[: stack_index + 1]
                    clear_stack = False
                    break

        # if we haven't got to a span in self._span_stack, clear self._span_stack
        if clear_stack:
            self._span_stack.clear()

        parts: TextParts = []
        # parents are currently in the reverse order as they were built from innermost first, hence
        # iterate over them reversed
        for indent, msg, parent_id in reversed(parents):
            total_indent = self._timestamp_indent + indent * 2
            parts += [(f'{" " * total_indent}{msg}\n', 'dim')]
            if parent_id:  # pragma: no branch
                self._span_stack.append(parent_id)
        return parts


def _pending_span_parent(attributes: Mapping[str, object]) -> int | None:
    """Pending span marks the start of a span.

    Since they're nested within another span we haven't seen yet,
    we have to do a trick of getting the 'logfire.pending_parent_id' attribute to get the parent indent.

    Note that returning `0` is equivalent to returning `None` since top level spans get
    `ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY` encoded from `0`.
    """
    if parent_id_str := attributes.get(ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY):
        return int(parent_id_str, 16)  # type: ignore


@dataclass
class ConsoleLogExporter(LogRecordExporter):
    span_exporter: SimpleConsoleSpanExporter

    def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        for log_data in batch:
            self.span_exporter.export_record(Record.from_log(log_data.log_record))

        return LogRecordExportResult.SUCCESS

    def shutdown(self):
        self.span_exporter.shutdown()
