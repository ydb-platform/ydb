import re
import typing_extensions
from .constants import ATTRIBUTES_JSON_SCHEMA_KEY as ATTRIBUTES_JSON_SCHEMA_KEY, ATTRIBUTES_LOGGING_NAME as ATTRIBUTES_LOGGING_NAME, ATTRIBUTES_LOG_LEVEL_NAME_KEY as ATTRIBUTES_LOG_LEVEL_NAME_KEY, ATTRIBUTES_LOG_LEVEL_NUM_KEY as ATTRIBUTES_LOG_LEVEL_NUM_KEY, ATTRIBUTES_MESSAGE_KEY as ATTRIBUTES_MESSAGE_KEY, ATTRIBUTES_MESSAGE_TEMPLATE_KEY as ATTRIBUTES_MESSAGE_TEMPLATE_KEY, ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY as ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY, ATTRIBUTES_SAMPLE_RATE_KEY as ATTRIBUTES_SAMPLE_RATE_KEY, ATTRIBUTES_SCRUBBED_KEY as ATTRIBUTES_SCRUBBED_KEY, ATTRIBUTES_SPAN_TYPE_KEY as ATTRIBUTES_SPAN_TYPE_KEY, ATTRIBUTES_TAGS_KEY as ATTRIBUTES_TAGS_KEY, MESSAGE_FORMATTED_VALUE_LENGTH_LIMIT as MESSAGE_FORMATTED_VALUE_LENGTH_LIMIT, RESOURCE_ATTRIBUTES_PACKAGE_VERSIONS as RESOURCE_ATTRIBUTES_PACKAGE_VERSIONS
from .stack_info import STACK_INFO_KEYS as STACK_INFO_KEYS
from .utils import ReadableSpanDict as ReadableSpanDict, truncate_string as truncate_string
from _typeshed import Incomplete
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from opentelemetry._logs import LogRecord
from opentelemetry.sdk.trace import Event
from typing import Any, Callable, TypedDict

DEFAULT_PATTERNS: Incomplete
JsonPath: typing_extensions.TypeAlias

@dataclass
class ScrubMatch:
    """An object passed to a [`ScrubbingOptions.callback`][logfire.ScrubbingOptions.callback] function."""
    path: JsonPath
    value: Any
    pattern_match: re.Match[str]
ScrubCallback = Callable[[ScrubMatch], Any]

class ScrubbedNote(TypedDict):
    path: JsonPath
    matched_substring: str

@dataclass
class ScrubbingOptions:
    """Options for redacting sensitive data."""
    callback: ScrubCallback | None = ...
    extra_patterns: Sequence[str] | None = ...

class BaseScrubber(ABC):
    SAFE_KEYS: Incomplete
    @abstractmethod
    def scrub_span(self, span: ReadableSpanDict): ...
    @abstractmethod
    def scrub_log(self, log: LogRecord) -> LogRecord: ...
    @abstractmethod
    def scrub_value(self, path: JsonPath, value: Any) -> tuple[Any, list[ScrubbedNote]]: ...

class NoopScrubber(BaseScrubber):
    def scrub_span(self, span: ReadableSpanDict): ...
    def scrub_log(self, log: LogRecord) -> LogRecord: ...
    def scrub_value(self, path: JsonPath, value: Any) -> tuple[Any, list[ScrubbedNote]]: ...

NOOP_SCRUBBER: Incomplete

class Scrubber(BaseScrubber):
    """Redacts potentially sensitive data."""
    def __init__(self, patterns: Sequence[str] | None, callback: ScrubCallback | None = None) -> None: ...
    def scrub_log(self, log: LogRecord) -> LogRecord: ...
    def scrub_span(self, span: ReadableSpanDict): ...
    def scrub_value(self, path: JsonPath, value: Any) -> tuple[Any, list[ScrubbedNote]]: ...

class SpanScrubber:
    """Does the actual scrubbing work.

    This class is separate from Scrubber so that it can be instantiated more regularly
    and hold and mutate state about the span being scrubbed, specifically the scrubbed notes.
    """
    scrubbed: list[ScrubbedNote]
    did_scrub: bool
    def __init__(self, parent: Scrubber) -> None: ...
    def scrub_span(self, span: ReadableSpanDict): ...
    def scrub_log(self, log: LogRecord) -> LogRecord: ...
    def scrub_event_attributes(self, event: Event, index: int): ...
    def scrub(self, path: JsonPath, value: Any) -> Any:
        """Redacts sensitive data from `value`, recursing into nested sequences and mappings.

        `path` is a list of keys and indices leading to `value` in the span.
        Similar to the truncation code, it should use the field names in the frontend, e.g. `otel_events`.
        """

class MessageValueCleaner:
    """Scrubs and truncates formatted field values to be included in the message attribute.

    Use to construct the message for a single span, e.g:

        cleaner = MessageValueCleaner(scrubber, check_keys=...)
        message_parts = [cleaner.clean_value(field_name, str(value)) for field_name, value in fields]
        message = <construct from message parts>
        attributes = {**other_attributes, **cleaner.extra_attrs(), ATTRIBUTES_MESSAGE_KEY: message}

    check_keys determines whether the key should be accounted for in scrubbing.
    Set to False if the user explicitly provided the key, e.g. `logfire.info(f'... {password} ...')`
    means that the password is clearly expected to be logged.
    The password will therefore not be scrubbed here and will appear in the message.
    However it may still be scrubbed out of the attributes, just because that process is independent.
    """
    scrubber: Incomplete
    scrubbed: list[ScrubbedNote]
    check_keys: Incomplete
    def __init__(self, scrubber: BaseScrubber, *, check_keys: bool) -> None: ...
    def clean_value(self, field_name: str, value: str) -> str: ...
    def truncate(self, value: str) -> str: ...
    def extra_attrs(self) -> dict[str, Any]: ...
