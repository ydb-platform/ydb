"""
Instrumented YDB pool classes with automatic metrics collection through inheritance.

This module provides classes that inherit from ydb.QuerySessionPool and ydb.SessionPool,
which automatically collect metrics for all operations.

When YDB_STRESS_EXTENDED_RETRIES is enabled, omitted retry_settings are replaced with a
more aggressive stress retry policy (more retries + slower backoff). Without the env var,
SDK defaults are used unchanged.
"""

import inspect
import os
from typing import Optional, Callable
import ydb
from .publish_metrics import get_metrics_collector

# Enable with: YDB_STRESS_EXTENDED_RETRIES=1
EXTENDED_RETRIES_ENV = "YDB_STRESS_EXTENDED_RETRIES"

_EXTENDED_MAX_RETRIES = 50
_EXTENDED_FAST_BACKOFF = ydb.BackoffSettings(ceiling=6, slot_duration=0.5)
_EXTENDED_SLOW_BACKOFF = ydb.BackoffSettings(ceiling=6, slot_duration=2.0)


def extended_retries_enabled() -> bool:
    return os.getenv(EXTENDED_RETRIES_ENV, "").lower() in ("1", "true", "yes", "y")


def extended_retry_settings(**overrides) -> ydb.RetrySettings:
    params = dict(
        max_retries=_EXTENDED_MAX_RETRIES,
        fast_backoff_settings=_EXTENDED_FAST_BACKOFF,
        slow_backoff_settings=_EXTENDED_SLOW_BACKOFF,
    )
    params.update(overrides)
    return ydb.RetrySettings(**params)


def maybe_extended_retry_settings(retry_settings: Optional[ydb.RetrySettings] = None) -> Optional[ydb.RetrySettings]:
    """If env is set and retry_settings is omitted, return extended policy; else pass through."""
    if retry_settings is not None:
        return retry_settings
    if extended_retries_enabled():
        return extended_retry_settings()
    return None


class InstrumentedQuerySessionPool(ydb.QuerySessionPool):
    """
    Inherits from ydb.QuerySessionPool with automatic metrics collection.

    Fully compatible with ydb.QuerySessionPool, but adds automatic metrics collection.

    Usage:
        pool = InstrumentedQuerySessionPool(driver, size=100)
        assert isinstance(pool, ydb.QuerySessionPool)  # True!
    """

    def __init__(self, driver, size: Optional[int] = None, enable_metrics: bool = True, unique_suffix: Optional[str] = None):
        """
        Initialize instrumented pool.

        Args:
            driver: YDB Driver
            size: Session pool size
            enable_metrics: Enable metrics collection (default True)
        """
        super().__init__(driver, size=size)
        self.enable_metrics = enable_metrics
        self.metrics_collector = get_metrics_collector() if enable_metrics else None

        stack = inspect.stack()
        caller_frame = stack[1]
        try:
            self.full_name = os.path.dirname(caller_frame.filename).replace('/', '.') + (f'.{unique_suffix}' if unique_suffix else '')
        finally:
            del caller_frame

    def execute_with_retries(self, query: str, *args, parameters=None,
                             retry_settings=None, settings=None,
                             operation_name: Optional[str] = None,
                             **kwargs):
        """
        Executes query with automatic metrics collection.

        Args:
            query: SQL query
            parameters: Query parameters
            retry_settings: Retry settings
            settings: Additional settings
            operation_name: Operation name for metrics (optional)

        Returns:
            Query execution result
        """
        retry_settings = maybe_extended_retry_settings(retry_settings)
        if operation_name is None:
            operation_name = 'query_pool_execute'
        if not self.enable_metrics:
            return super(InstrumentedQuerySessionPool, self).execute_with_retries(query,
                                                                                  parameters=parameters,
                                                                                  retry_settings=retry_settings,
                                                                                  settings=settings,
                                                                                  *args,
                                                                                  **kwargs)

        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedQuerySessionPool, self).execute_with_retries(
                query, parameters=parameters, retry_settings=retry_settings, settings=settings,
                *args,
                **kwargs
            ),
            operation_name, self.full_name
        )

    def explain_with_retries(self, query: str, *args, retry_settings=None,
                             operation_name: str = 'explain_query',
                             **kwargs):
        """
        Executes EXPLAIN query with automatic metrics collection.

        Args:
            query: SQL query
            retry_settings: Retry settings
            operation_name: Operation name for metrics

        Returns:
            EXPLAIN result
        """
        retry_settings = maybe_extended_retry_settings(retry_settings)
        if not self.enable_metrics:
            return super(InstrumentedQuerySessionPool, self).explain_with_retries(query, retry_settings,
                                                                                  *args,
                                                                                  **kwargs)

        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedQuerySessionPool, self).explain_with_retries(query, retry_settings,
                                                                                   *args,
                                                                                   **kwargs),
            operation_name, self.full_name
        )

    def retry_operation_sync(self, callee: Callable, retry_settings=None, *args, **kwargs):
        return super().retry_operation_sync(
            callee, maybe_extended_retry_settings(retry_settings), *args, **kwargs
        )

    def retry_tx_sync(self, callee, tx_mode=None, retry_settings=None, *args, **kwargs):
        return super().retry_tx_sync(
            callee, tx_mode, maybe_extended_retry_settings(retry_settings), *args, **kwargs
        )

    def get_metrics_summary(self) -> str:
        """
        Returns summary of collected metrics.

        Returns:
            String with formatted statistics
        """
        if not self.enable_metrics:
            return "Metrics collection is disabled"
        return self.metrics_collector.get_summary()


class InstrumentedSessionPool(ydb.SessionPool):
    """
    Inherits from ydb.SessionPool with automatic metrics collection.

    Fully compatible with ydb.SessionPool, but adds automatic metrics collection.

    Usage:
        pool = InstrumentedSessionPool(driver, size=100)
        assert isinstance(pool, ydb.SessionPool)  # True!
    """

    def __init__(self, driver, size: Optional[int] = None, enable_metrics: bool = True, unique_suffix: Optional[str] = None):
        """
        Initialize instrumented pool.

        Args:
            driver: YDB Driver
            size: Session pool size
            enable_metrics: Enable metrics collection (default True)
        """
        super().__init__(driver, size=size)
        self.enable_metrics = enable_metrics
        self.metrics_collector = get_metrics_collector() if enable_metrics else None

        stack = inspect.stack()
        caller_frame = stack[1]
        try:
            self.full_name = os.path.dirname(caller_frame.filename).replace('/', '.') + (f'.{unique_suffix}' if unique_suffix else '')
        finally:
            del caller_frame

    def retry_operation_sync(self, callee: Callable, *args,
                             operation_name: Optional[str] = None, **kwargs):
        """
        Executes operation with retry and automatic metrics collection.

        Args:
            callee: Function to execute
            operation_name: Operation name for metrics (optional)
            *args, **kwargs: Arguments for callee

        Returns:
            Operation execution result
        """
        if operation_name is None:
            operation_name = 'session_pool_operation'

        # Parent signature: (callee, retry_settings=None, *args, **kwargs)
        if 'retry_settings' in kwargs:
            kwargs['retry_settings'] = maybe_extended_retry_settings(kwargs['retry_settings'])
        elif extended_retries_enabled():
            kwargs['retry_settings'] = extended_retry_settings()

        if not self.enable_metrics:
            return super(InstrumentedSessionPool, self).retry_operation_sync(callee, *args, **kwargs)

        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedSessionPool, self).retry_operation_sync(callee, *args, **kwargs),
            operation_name, self.full_name
        )

    def get_metrics_summary(self) -> str:
        """
        Returns summary of collected metrics.

        Returns:
            String with formatted statistics
        """
        if not self.enable_metrics:
            return "Metrics collection is disabled"
        return self.metrics_collector.get_summary()
