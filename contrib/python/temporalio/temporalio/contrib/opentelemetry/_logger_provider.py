from __future__ import annotations

from typing import TYPE_CHECKING, Any

# The OpenTelemetry Logs API specification is stable, but opentelemetry-python
# exposes the logs bridge API only under the private opentelemetry._logs
# module path (there is no public opentelemetry.logs as of 1.44; upstream
# stabilization is tracked in opentelemetry-python#3361, open since 2023 with
# the module surface unchanged since 1.26 apart from additive parameters).
# _logs is therefore the import path OpenTelemetry itself documents, until
# the Python SIG promotes it to a public namespace.
from opentelemetry._logs import Logger, LoggerProvider

from temporalio import workflow

if TYPE_CHECKING:
    # _ExtendedAttributes is the annotation OpenTelemetry's own public
    # get_logger signature uses; there is no public alias.
    from opentelemetry.util.types import _ExtendedAttributes


def _skip_emitting() -> bool:
    # in_workflow() must be evaluated first: is_replaying_history_events()
    # requires an active workflow context. The history-events predicate is
    # deliberate (not is_replaying()): queries and update validators are live,
    # at-most-once-per-request operations even when they execute while the
    # workflow is replaying, so their emissions must not be dropped.
    return workflow.in_workflow() and workflow.unsafe.is_replaying_history_events()


class _ReplaySafeLogger(Logger):
    def __init__(
        self,
        logger: Logger,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
    ) -> None:
        super().__init__(name, version=version, schema_url=schema_url)
        self._logger = logger

    def __getattr__(self, name: str) -> object:
        return getattr(self._logger, name)

    def emit(self, *args: Any, **kwargs: Any) -> None:
        # emit's signature differs across the supported opentelemetry-api
        # range (a single positional LogRecord through 1.37, keyword fields
        # from 1.38), so forward arguments verbatim rather than pinning one
        # shape; the wrapped logger comes from the same installed API.
        if _skip_emitting():
            # Skip emitting log records during workflow replay to avoid duplicate telemetry
            return
        self._logger.emit(*args, **kwargs)


class ReplaySafeLoggerProvider(LoggerProvider):
    """A logger provider that is safe for use during workflow replay.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments. It wraps the
        OpenTelemetry Python logs bridge API, which upstream still ships
        under the private ``opentelemetry._logs`` namespace (stabilization
        is tracked in opentelemetry-python#3361); if that surface moves when
        upstream stabilizes it, this class will follow it in a minor release.

    This logger provider wraps an OpenTelemetry LoggerProvider and drops log
    records emitted (``Logger.emit()``) from workflow code while the workflow
    is replaying history events. Without this, libraries that emit
    OpenTelemetry log records from workflow code (e.g. ``google-adk``'s
    ``gen_ai.*`` events) re-emit every record on each replay, duplicating
    telemetry.

    Emissions are therefore first-execution-only: a workflow task retry
    re-executes live and can emit again. Queries and update validators are
    live, at-most-once-per-request operations even when they execute while the
    workflow is replaying, so their emissions are kept. Emissions outside
    workflows are unaffected.

    Install this as the process-global logger provider before any library
    (e.g. ``google-adk``) obtains loggers::

        opentelemetry._logs.set_logger_provider(
            ReplaySafeLoggerProvider(my_logger_provider)
        )

    OpenTelemetry proxy loggers late-bind, so calling ``set_logger_provider``
    after such libraries are imported still routes their loggers through this
    wrapper. However, ``set_logger_provider`` only takes effect once per
    process, so this wrapper must be the one and only global logger provider
    ever set.
    """

    def __init__(self, logger_provider: LoggerProvider) -> None:
        """Initialize the replay-safe logger provider.

        Args:
            logger_provider: The underlying OpenTelemetry LoggerProvider to wrap.
        """
        self._logger_provider = logger_provider

    def __getattr__(self, name: str) -> Any:
        """Delegate all other attributes (e.g. ``shutdown``, ``force_flush``)
        to the underlying logger provider.
        """
        return getattr(self._logger_provider, name)

    def get_logger(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
        attributes: _ExtendedAttributes | None = None,
    ) -> Logger:
        """Get a replay-safe logger from the underlying provider.

        Args:
            name: The name of the instrumenting module.
            version: The version string of the instrumenting library.
            schema_url: The schema URL for the logger.
            attributes: Instrumentation scope attributes for the logger.

        Returns:
            A replay-safe logger instance.
        """
        inner = self._logger_provider.get_logger(name, version, schema_url, attributes)
        return _ReplaySafeLogger(inner, name, version=version, schema_url=schema_url)
