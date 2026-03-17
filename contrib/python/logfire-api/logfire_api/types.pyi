from dataclasses import dataclass
from logfire._internal.constants import ATTRIBUTES_LOG_LEVEL_NUM_KEY as ATTRIBUTES_LOG_LEVEL_NUM_KEY, LEVEL_NUMBERS as LEVEL_NUMBERS, LevelName as LevelName, NUMBER_TO_LEVEL as NUMBER_TO_LEVEL, log_level_attributes as log_level_attributes
from logfire._internal.tracer import get_parent_span as get_parent_span
from logfire._internal.utils import canonicalize_exception_traceback as canonicalize_exception_traceback
from opentelemetry.sdk.trace import ReadableSpan, Span
from opentelemetry.util import types as otel_types
from typing import Callable

@dataclass
class SpanLevel:
    """A convenience class for comparing span/log levels.

    Can be compared to log level names (strings) such as 'info' or 'error' using
    `<`, `>`, `<=`, or `>=`, so e.g. `level >= 'error'` is valid.

    Will raise an exception if compared to a non-string or an invalid level name.
    """
    number: int
    @classmethod
    def from_span(cls, span: ReadableSpan) -> SpanLevel:
        """Create a SpanLevel from an OpenTelemetry span.

        If the span has no level set, defaults to 'info'.
        """
    @property
    def name(self) -> LevelName | None:
        """The human-readable name of the level, or `None` if the number is invalid."""
    def __eq__(self, other: object): ...
    def __hash__(self): ...
    def __lt__(self, other: LevelName): ...
    def __gt__(self, other: LevelName): ...
    def __ge__(self, other: LevelName): ...
    def __le__(self, other: LevelName): ...

@dataclass
class ExceptionCallbackHelper:
    """Helper object passed to the exception callback.

    This is experimental and may change significantly in future releases.
    """
    span: Span
    exception: BaseException
    event_attributes: dict[str, otel_types.AttributeValue]
    @property
    def level(self) -> SpanLevel:
        """Convenient way to see and compare the level of the span.

        - When using `logfire.span` or `logfire.exception`, this is usually `error`.
        - Spans created directly by an OpenTelemetry tracer (e.g. from any `logfire.instrument_*()` method)
            typically don't have a level set, so this will return the default of `info`,
            but `level_is_unset` will be `True`.
        - Will be a different level if this is created by e.g. `logfire.info(..., _exc_info=True)`.
        """
    @level.setter
    def level(self, value: LevelName | int) -> None:
        """Override the level of the span.

        For example:

            helper.level = 'warning'
        """
    @property
    def level_is_unset(self) -> bool:
        """Determine if the level has not been explicitly set on the span (yet).

        For messy technical reasons, this is typically `True` for spans created directly by an OpenTelemetry tracer
        (e.g. from any `logfire.instrument_*()` method)
        although the level will usually still eventually be `error` by the time it's exported.

        Spans created by `logfire.span()` get the level set to `error` immediately when an exception passes through,
        so this will be `False` in that case.

        This is also typically `True` when calling `span.record_exception()` directly on any span
        instead of letting an exception bubble through.
        """
    @property
    def parent_span(self) -> ReadableSpan | None:
        """The parent span of the span the exception was recorded on.

        This is `None` if there is no parent span, or if the parent span is in a different process.
        """
    @property
    def issue_fingerprint_source(self) -> str:
        """Returns a string that will be hashed to create the issue fingerprint.

        By default this is a canonical representation of the exception traceback:

        - The source line is used, but not the line number, so that changes elsewhere in a file are irrelevant.
        - The module is used instead of the filename.
        - The same line appearing multiple times in a stack is ignored.
        - Exception group sub-exceptions are sorted and deduplicated.
        - If the exception has a cause or (not suppressed) context, it is included in the representation.
        - Cause and context are treated as different.
        """
    @issue_fingerprint_source.setter
    def issue_fingerprint_source(self, value: str):
        '''Override the string that will be hashed to create the issue fingerprint.

        For example, if you want all exceptions of a certain type to be grouped into the same issue,
        you could do something like:

            if isinstance(helper.exception, MyCustomError):
                helper.issue_fingerprint_source = "MyCustomError"

        Or if you want to add the exception message to make grouping more granular:

            helper.issue_fingerprint_source += str(helper.exception)

        Note that setting this property automatically sets `create_issue` to True.
        '''
    @property
    def create_issue(self) -> bool:
        '''Whether to create an issue for this exception.

        By default, issues are only created for exceptions on spans where:

        - The level is \'error\' or higher or is unset (see `level_is_unset` for details),
        - No parent span exists in the current process,

        Example:
            if helper.create_issue:
                helper.issue_fingerprint_source = "MyCustomError"
        '''
    @create_issue.setter
    def create_issue(self, value: bool):
        """Override whether to create an issue for this exception.

        For example, if you want to create issues for all exceptions, even warnings:

            helper.create_issue = True

        Issues can only be created if the exception is recorded on the span.
        """
    def no_record_exception(self) -> None:
        """Call this method to prevent recording the exception on the span.

        This improves performance and reduces noise in Logfire.
        This will also prevent creating an issue for this exception.
        The span itself will still be recorded, just without the exception information.
        This doesn't affect the level of the span, it will still be 'error' by default.
        To still record exception info without creating an issue, use `helper.create_issue = False` instead.
        To still record the exception info but at a different level, use `helper.level = 'warning'`
        or some other level instead.
        """
ExceptionCallback = Callable[[ExceptionCallbackHelper], None]
