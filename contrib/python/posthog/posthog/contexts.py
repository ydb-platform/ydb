import contextvars
from contextlib import contextmanager
from typing import Optional, Any, Callable, Dict, TypeVar, cast, TYPE_CHECKING

if TYPE_CHECKING:
    # To avoid circular imports
    from posthog.client import Client


class ContextScope:
    def __init__(
        self,
        parent=None,
        fresh: bool = False,
        capture_exceptions: bool = True,
        client: Optional["Client"] = None,
    ):
        self.client: Optional[Client] = client
        self.parent = parent
        self.fresh = fresh
        self.capture_exceptions = capture_exceptions
        self.session_id: Optional[str] = None
        self.distinct_id: Optional[str] = None
        self.device_id: Optional[str] = None
        self.tags: Dict[str, Any] = {}
        self.capture_exception_code_variables: Optional[bool] = None
        self.code_variables_mask_patterns: Optional[list] = None
        self.code_variables_ignore_patterns: Optional[list] = None

    def set_session_id(self, session_id: str):
        self.session_id = session_id

    def set_distinct_id(self, distinct_id: str):
        self.distinct_id = distinct_id

    def set_device_id(self, device_id: str):
        self.device_id = device_id

    def add_tag(self, key: str, value: Any):
        self.tags[key] = value

    def set_capture_exception_code_variables(self, enabled: bool):
        self.capture_exception_code_variables = enabled

    def set_code_variables_mask_patterns(self, mask_patterns: list):
        self.code_variables_mask_patterns = mask_patterns

    def set_code_variables_ignore_patterns(self, ignore_patterns: list):
        self.code_variables_ignore_patterns = ignore_patterns

    def get_parent(self):
        return self.parent

    def get_session_id(self) -> Optional[str]:
        if self.session_id is not None:
            return self.session_id
        if self.parent is not None and not self.fresh:
            return self.parent.get_session_id()
        return None

    def get_distinct_id(self) -> Optional[str]:
        if self.distinct_id is not None:
            return self.distinct_id
        if self.parent is not None and not self.fresh:
            return self.parent.get_distinct_id()
        return None

    def get_device_id(self) -> Optional[str]:
        if self.device_id is not None:
            return self.device_id
        if self.parent is not None and not self.fresh:
            return self.parent.get_device_id()
        return None

    def collect_tags(self) -> Dict[str, Any]:
        if self.parent and not self.fresh:
            # We want child tags to take precedence over parent tags,
            # so collect parent tags first, then update with child tags.
            tags = self.parent.collect_tags()
            tags.update(self.tags)
            return tags
        return self.tags.copy()

    def get_capture_exception_code_variables(self) -> Optional[bool]:
        if self.capture_exception_code_variables is not None:
            return self.capture_exception_code_variables
        if self.parent is not None and not self.fresh:
            return self.parent.get_capture_exception_code_variables()
        return None

    def get_code_variables_mask_patterns(self) -> Optional[list]:
        if self.code_variables_mask_patterns is not None:
            return self.code_variables_mask_patterns
        if self.parent is not None and not self.fresh:
            return self.parent.get_code_variables_mask_patterns()
        return None

    def get_code_variables_ignore_patterns(self) -> Optional[list]:
        if self.code_variables_ignore_patterns is not None:
            return self.code_variables_ignore_patterns
        if self.parent is not None and not self.fresh:
            return self.parent.get_code_variables_ignore_patterns()
        return None


_context_stack: contextvars.ContextVar[Optional[ContextScope]] = contextvars.ContextVar(
    "posthog_context_stack", default=None
)


def _get_current_context() -> Optional[ContextScope]:
    return _context_stack.get()


@contextmanager
def new_context(
    fresh: bool = False,
    capture_exceptions: bool = True,
    client: Optional["Client"] = None,
):
    """
    Create a new context scope that will be active for the duration of the with block.
    Any tags set within this scope will be isolated to this context. Any exceptions raised
    or events captured within the context will be tagged with the context tags.

    Args:
        fresh: Whether to start with a fresh context (default: False).
               If False, inherits tags, identity and session id's from parent context.
               If True, starts with no state
        capture_exceptions: Whether to capture exceptions raised within the context (default: True).
               If True, captures exceptions and tags them with the context tags before propagating them.
               If False, exceptions will propagate without being tagged or captured.
        client: Optional client instance to use for capturing exceptions (default: None).
                If provided, the client will be used to capture exceptions within the context.
                If not provided, the default (global) client will be used. Note that the passed
                client is only used to capture exceptions within the context - other events captured
                within the context via `Client.capture` or `posthog.capture` will still carry the context
                state (tags, identity, session id), but will be captured by the client directly used (or
                the global one, in the case of `posthog.capture`)

    Examples:
        ```python
        # Inherit parent context tags
        with posthog.new_context():
            posthog.tag("request_id", "123")
            # Both this event and the exception will be tagged with the context tags
            posthog.capture("event_name", {"property": "value"})
            raise ValueError("Something went wrong")
        ```
        ```python
        # Start with fresh context (no inherited tags)
        with posthog.new_context(fresh=True):
            posthog.tag("request_id", "123")
            # Both this event and the exception will be tagged with the context tags
            posthog.capture("event_name", {"property": "value"})
            raise ValueError("Something went wrong")
        ```

    Category:
        Contexts
    """
    from posthog import capture_exception

    current_context = _get_current_context()
    new_context = ContextScope(current_context, fresh, capture_exceptions, client)
    _context_stack.set(new_context)

    try:
        yield
    except Exception as e:
        if new_context.capture_exceptions:
            if new_context.client:
                new_context.client.capture_exception(e)
            else:
                capture_exception(e)
        raise
    finally:
        _context_stack.set(new_context.get_parent())


def tag(key: str, value: Any) -> None:
    """
    Add a tag to the current context. All tags are added as properties to any event, including exceptions, captured
    within the context.

    Args:
        key: The tag key
        value: The tag value

    Example:
        ```python
        posthog.tag("user_id", "123")
        ```

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        current_context.add_tag(key, value)


def get_tags() -> Dict[str, Any]:
    """
    Get all tags from the current context. Note, modifying
    the returned dictionary will not affect the current context.

    Returns:
        Dict of all tags in the current context

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        return current_context.collect_tags()
    return {}


def identify_context(distinct_id: str) -> None:
    """
    Identify the current context with a distinct ID, associating all events captured in this or
    child contexts with the given distinct ID (unless identify_context is called again). This is overridden by
    distinct id's passed directly to posthog.capture and related methods (identify, set etc). Entering a
    fresh context will clear the context-level distinct ID. The distinct-id passed should be uniquely associated
    with one of your users. Events captured outside of a context, or in a context with no associated distinct
    ID, will be assigned a random UUID, and captured as "personless".

    Args:
        distinct_id: The distinct ID to associate with the current context and its children.

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        current_context.set_distinct_id(distinct_id)


def set_context_session(session_id: str) -> None:
    """
    Set the session ID for the current context, associating all events captured in this or
    child contexts with the given session ID (unless set_context_session is called again).
    Entering a fresh context will clear the context-level session ID.

    Args:
        session_id: The session ID to associate with the current context and its children. See https://posthog.com/docs/data/sessions

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        current_context.set_session_id(session_id)


def get_context_session_id() -> Optional[str]:
    """
    Get the session ID for the current context.

    Returns:
        The session ID if set, None otherwise

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        return current_context.get_session_id()
    return None


def get_context_distinct_id() -> Optional[str]:
    """
    Get the distinct ID for the current context.

    Returns:
        The distinct ID if set, None otherwise

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        return current_context.get_distinct_id()
    return None


def set_context_device_id(device_id: str) -> None:
    """
    Set the device ID for the current context, associating all feature flag requests in this or
    child contexts with the given device ID (unless set_context_device_id is called again).
    Entering a fresh context will clear the context-level device ID.

    Args:
        device_id: The device ID to associate with the current context and its children.

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        current_context.set_device_id(device_id)


def get_context_device_id() -> Optional[str]:
    """
    Get the device ID for the current context.

    Returns:
        The device ID if set, None otherwise

    Category:
        Contexts
    """
    current_context = _get_current_context()
    if current_context:
        return current_context.get_device_id()
    return None


def set_capture_exception_code_variables_context(enabled: bool) -> None:
    """
    Set whether code variables are captured for the current context.
    """
    current_context = _get_current_context()
    if current_context:
        current_context.set_capture_exception_code_variables(enabled)


def set_code_variables_mask_patterns_context(mask_patterns: list) -> None:
    """
    Variable names matching these patterns will be masked with *** when capturing code variables.
    """
    current_context = _get_current_context()
    if current_context:
        current_context.set_code_variables_mask_patterns(mask_patterns)


def set_code_variables_ignore_patterns_context(ignore_patterns: list) -> None:
    """
    Variable names matching these patterns will be ignored completely when capturing code variables.
    """
    current_context = _get_current_context()
    if current_context:
        current_context.set_code_variables_ignore_patterns(ignore_patterns)


def get_capture_exception_code_variables_context() -> Optional[bool]:
    current_context = _get_current_context()
    if current_context:
        return current_context.get_capture_exception_code_variables()
    return None


def get_code_variables_mask_patterns_context() -> Optional[list]:
    current_context = _get_current_context()
    if current_context:
        return current_context.get_code_variables_mask_patterns()
    return None


def get_code_variables_ignore_patterns_context() -> Optional[list]:
    current_context = _get_current_context()
    if current_context:
        return current_context.get_code_variables_ignore_patterns()
    return None


F = TypeVar("F", bound=Callable[..., Any])


def scoped(fresh: bool = False, capture_exceptions: bool = True):
    """
    Decorator that creates a new context for the function. Simply wraps
    the function in a with posthog.new_context(): block.

    Args:
        fresh: Whether to start with a fresh context (default: False)
        capture_exceptions: Whether to capture and track exceptions with posthog error tracking (default: True)

    Example:
        @posthog.scoped()
        def process_payment(payment_id):
            posthog.tag("payment_id", payment_id)
            posthog.tag("payment_method", "credit_card")

            # This event will be captured with tags
            posthog.capture("payment_started")
            # If this raises an exception, it will be captured with tags
            # and then re-raised
            some_risky_function()

    Category:
        Contexts
    """

    def decorator(func: F) -> F:
        from functools import wraps

        @wraps(func)
        def wrapper(*args, **kwargs):
            with new_context(fresh=fresh, capture_exceptions=capture_exceptions):
                return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator
