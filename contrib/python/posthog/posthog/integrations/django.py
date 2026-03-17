from typing import TYPE_CHECKING, cast
from posthog import contexts
from posthog.client import Client

try:
    from asgiref.sync import iscoroutinefunction, markcoroutinefunction
except ImportError:
    # Fallback for older Django versions without asgiref
    import asyncio

    iscoroutinefunction = asyncio.iscoroutinefunction

    # No-op fallback for markcoroutinefunction
    # Older Django versions without asgiref typically don't support async middleware anyway
    def markcoroutinefunction(func):
        return func


if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse  # noqa: F401
    from typing import Callable, Dict, Any, Optional, Union, Awaitable  # noqa: F401


class PosthogContextMiddleware:
    """Middleware to automatically track Django requests.

    This middleware wraps all calls with a posthog context. It attempts to extract the following from the request headers:
    - Session ID, (extracted from `X-POSTHOG-SESSION-ID`)
    - Distinct ID, (extracted from `X-POSTHOG-DISTINCT-ID`)
    - Request URL as $current_url
    - Request Method as $request_method

    The context will also auto-capture exceptions and send them to PostHog, unless you disable it by setting
    `POSTHOG_MW_CAPTURE_EXCEPTIONS` to `False` in your Django settings. The exceptions are captured using the
    global client, unless the setting `POSTHOG_MW_CLIENT` is set to a custom client instance

    The middleware behaviour is customisable through 3 additional functions:
    - `POSTHOG_MW_EXTRA_TAGS`, which is a Callable[[HttpRequest], Dict[str, Any]] expected to return a dictionary of additional tags to be added to the context.
    - `POSTHOG_MW_REQUEST_FILTER`, which is a Callable[[HttpRequest], bool] expected to return `False` if the request should not be tracked.
    - `POSTHOG_MW_TAG_MAP`, which is a Callable[[Dict[str, Any]], Dict[str, Any]], which you can use to modify the tags before they're added to the context.

    You can use the `POSTHOG_MW_TAG_MAP` function to remove any default tags you don't want to capture, or override them with your own values.

    Context tags are automatically included as properties on all events captured within a context, including exceptions.
    See the context documentation for more information. The extracted distinct ID and session ID, if found, are used to
    associate all events captured in the middleware context with the same distinct ID and session as currently active on the
    frontend. See the documentation for `set_context_session` and `identify_context` for more details.

    This middleware is hybrid-capable: it supports both WSGI (sync) and ASGI (async) Django applications. The middleware
    detects at initialization whether the next middleware in the chain is async or sync, and adapts its behavior accordingly.
    This ensures compatibility with both pure sync and pure async middleware chains, as well as mixed chains in ASGI mode.
    """

    sync_capable = True
    async_capable = True

    def __init__(self, get_response):
        # type: (Union[Callable[[HttpRequest], HttpResponse], Callable[[HttpRequest], Awaitable[HttpResponse]]]) -> None
        self.get_response = get_response
        self._is_coroutine = iscoroutinefunction(get_response)

        # Mark this instance as a coroutine function if get_response is async
        # This is required for Django to correctly detect async middleware
        if self._is_coroutine:
            markcoroutinefunction(self)

        from django.conf import settings

        if hasattr(settings, "POSTHOG_MW_EXTRA_TAGS") and callable(
            settings.POSTHOG_MW_EXTRA_TAGS
        ):
            self.extra_tags = cast(
                "Optional[Callable[[HttpRequest], Dict[str, Any]]]",
                settings.POSTHOG_MW_EXTRA_TAGS,
            )
        else:
            self.extra_tags = None

        if hasattr(settings, "POSTHOG_MW_REQUEST_FILTER") and callable(
            settings.POSTHOG_MW_REQUEST_FILTER
        ):
            self.request_filter = cast(
                "Optional[Callable[[HttpRequest], bool]]",
                settings.POSTHOG_MW_REQUEST_FILTER,
            )
        else:
            self.request_filter = None

        if hasattr(settings, "POSTHOG_MW_TAG_MAP") and callable(
            settings.POSTHOG_MW_TAG_MAP
        ):
            self.tag_map = cast(
                "Optional[Callable[[Dict[str, Any]], Dict[str, Any]]]",
                settings.POSTHOG_MW_TAG_MAP,
            )
        else:
            self.tag_map = None

        if hasattr(settings, "POSTHOG_MW_CAPTURE_EXCEPTIONS") and isinstance(
            settings.POSTHOG_MW_CAPTURE_EXCEPTIONS, bool
        ):
            self.capture_exceptions = settings.POSTHOG_MW_CAPTURE_EXCEPTIONS
        else:
            self.capture_exceptions = True

        if hasattr(settings, "POSTHOG_MW_CLIENT") and isinstance(
            settings.POSTHOG_MW_CLIENT, Client
        ):
            self.client = cast("Optional[Client]", settings.POSTHOG_MW_CLIENT)
        else:
            self.client = None

    def extract_tags(self, request):
        # type: (HttpRequest) -> Dict[str, Any]
        """Extract tags from request in sync context."""
        user_id, user_email = self.extract_request_user(request)
        return self._build_tags(request, user_id, user_email)

    def _build_tags(self, request, user_id, user_email):
        # type: (HttpRequest, Optional[str], Optional[str]) -> Dict[str, Any]
        """
        Build tags dict from request and user info.

        Centralized tag extraction logic used by both sync and async paths.
        """
        tags = {}

        # Extract session ID from X-POSTHOG-SESSION-ID header
        session_id = request.headers.get("X-POSTHOG-SESSION-ID")
        if session_id:
            contexts.set_context_session(session_id)

        # Extract distinct ID from X-POSTHOG-DISTINCT-ID header or request user id
        distinct_id = request.headers.get("X-POSTHOG-DISTINCT-ID") or user_id
        if distinct_id:
            contexts.identify_context(distinct_id)

        # Extract user email
        if user_email:
            tags["email"] = user_email

        # Extract current URL
        absolute_url = request.build_absolute_uri()
        if absolute_url:
            tags["$current_url"] = absolute_url

        # Extract request method
        if request.method:
            tags["$request_method"] = request.method

        # Extract request path
        if request.path:
            tags["$request_path"] = request.path

        # Extract IP address
        ip_address = request.headers.get("X-Forwarded-For")
        if ip_address:
            tags["$ip"] = ip_address

        # Extract user agent
        user_agent = request.headers.get("User-Agent")
        if user_agent:
            tags["$user_agent"] = user_agent

        # Apply extra tags if configured
        if self.extra_tags:
            extra = self.extra_tags(request)
            if extra:
                tags.update(extra)

        # Apply tag mapping if configured
        if self.tag_map:
            tags = self.tag_map(tags)

        return tags

    def extract_request_user(self, request):
        # type: (HttpRequest) -> tuple[Optional[str], Optional[str]]
        """Extract user ID and email from request in sync context."""
        user = getattr(request, "user", None)
        return self._resolve_user_details(user)

    async def aextract_tags(self, request):
        # type: (HttpRequest) -> Dict[str, Any]
        """
        Async version of extract_tags for use in async request handling.

        Uses await request.auser() instead of request.user to avoid
        SynchronousOnlyOperation in async context.

        Follows Django's naming convention for async methods (auser, asave, etc.).
        """
        user_id, user_email = await self.aextract_request_user(request)
        return self._build_tags(request, user_id, user_email)

    async def aextract_request_user(self, request):
        # type: (HttpRequest) -> tuple[Optional[str], Optional[str]]
        """
        Async version of extract_request_user for use in async request handling.

        Uses await request.auser() instead of request.user to avoid
        SynchronousOnlyOperation in async context.

        Follows Django's naming convention for async methods (auser, asave, etc.).
        """
        auser = getattr(request, "auser", None)
        if callable(auser):
            try:
                user = await auser()
                return self._resolve_user_details(user)
            except Exception:
                # If auser() fails, return empty - don't break the request
                # Real errors (permissions, broken auth) will be logged by Django
                return None, None

        # Fallback for test requests without auser
        return None, None

    def _resolve_user_details(self, user):
        # type: (Any) -> tuple[Optional[str], Optional[str]]
        """
        Extract user ID and email from a user object.

        Handles both authenticated and unauthenticated users, as well as
        legacy Django where is_authenticated was a method.
        """
        user_id = None
        email = None

        if user is None:
            return user_id, email

        # Handle is_authenticated (property in modern Django, method in legacy)
        is_authenticated = getattr(user, "is_authenticated", False)
        if callable(is_authenticated):
            is_authenticated = is_authenticated()

        if not is_authenticated:
            return user_id, email

        # Extract user primary key
        user_pk = getattr(user, "pk", None)
        if user_pk is not None:
            user_id = str(user_pk)

        # Extract user email
        user_email = getattr(user, "email", None)
        if user_email:
            email = str(user_email)

        return user_id, email

    def __call__(self, request):
        # type: (HttpRequest) -> Union[HttpResponse, Awaitable[HttpResponse]]
        """
        Unified entry point for both sync and async request handling.

        When sync_capable and async_capable are both True, Django passes requests
        without conversion. This method detects the mode and routes accordingly.
        """
        if self._is_coroutine:
            return self.__acall__(request)
        else:
            # Synchronous path
            if self.request_filter and not self.request_filter(request):
                return self.get_response(request)

            with contexts.new_context(self.capture_exceptions, client=self.client):
                for k, v in self.extract_tags(request).items():
                    contexts.tag(k, v)

                return self.get_response(request)

    async def __acall__(self, request):
        # type: (HttpRequest) -> Awaitable[HttpResponse]
        """
        Asynchronous entry point for async request handling.

        This method is called when the middleware chain is async.
        Uses aextract_tags() which calls request.auser() to avoid
        SynchronousOnlyOperation when accessing user in async context.
        """
        if self.request_filter and not self.request_filter(request):
            return await self.get_response(request)

        with contexts.new_context(self.capture_exceptions, client=self.client):
            for k, v in (await self.aextract_tags(request)).items():
                contexts.tag(k, v)

            return await self.get_response(request)

    def process_exception(self, request, exception):
        # type: (HttpRequest, Exception) -> None
        """
        Process exceptions from views and downstream middleware.

        Django calls this WHILE still inside the context created by __call__,
        so request tags have already been extracted and set. This method just
        needs to capture the exception directly.

        Django converts view exceptions into responses before they propagate through
        the middleware stack, so the context manager in __call__/__acall__ never sees them.

        Note: Django's process_exception is always synchronous, even for async views.
        """
        if self.request_filter and not self.request_filter(request):
            return

        if not self.capture_exceptions:
            return

        # Context and tags already set by __call__ or __acall__
        # Just capture the exception
        if self.client:
            self.client.capture_exception(exception)
        else:
            from posthog import capture_exception

            capture_exception(exception)
