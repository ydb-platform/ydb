"""
Instrumented YDB pool classes with automatic metrics collection through inheritance.

This module provides classes that inherit from ydb.QuerySessionPool and ydb.SessionPool,
which automatically collect metrics for all operations.
"""

import inspect
import os
from typing import Optional, Callable
import ydb
from .publish_metrics import get_metrics_collector


class InstrumentedQuerySessionPool(ydb.QuerySessionPool):
    """
    Inherits from ydb.QuerySessionPool with automatic metrics collection.

    Fully compatible with ydb.QuerySessionPool, but adds automatic metrics collection.

    Usage:
        pool = InstrumentedQuerySessionPool(driver, size=100)
        assert isinstance(pool, ydb.QuerySessionPool)  # True!
    """

    def __init__(self, driver, size: Optional[int] = None, enable_metrics: bool = True):
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
            self.full_name = os.path.dirname(caller_frame.filename).replace('/', '.')
        finally:
            del caller_frame

    def execute_with_retries(self, query: str, parameters=None,
                             retry_settings=None, settings=None,
                             operation_name: Optional[str] = None):
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
        if operation_name is None:
            operation_name = 'query_pool_execute'
        if not self.enable_metrics:
            return super(InstrumentedQuerySessionPool, self).execute_with_retries(query, parameters, retry_settings, settings)

        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedQuerySessionPool, self).execute_with_retries(
                query, parameters, retry_settings, settings
            ),
            operation_name, self.full_name
        )

    def explain_with_retries(self, query: str, retry_settings=None,
                             operation_name: str = 'explain_query'):
        """
        Executes EXPLAIN query with automatic metrics collection.

        Args:
            query: SQL query
            retry_settings: Retry settings
            operation_name: Operation name for metrics

        Returns:
            EXPLAIN result
        """
        if not self.enable_metrics:
            return super(InstrumentedQuerySessionPool, self).explain_with_retries(query, retry_settings)

        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedQuerySessionPool, self).explain_with_retries(query, retry_settings),
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


class InstrumentedSessionPool(ydb.SessionPool):
    """
    Inherits from ydb.SessionPool with automatic metrics collection.

    Fully compatible with ydb.SessionPool, but adds automatic metrics collection.

    Usage:
        pool = InstrumentedSessionPool(driver, size=100)
        assert isinstance(pool, ydb.SessionPool)  # True!
    """

    def __init__(self, driver, size: Optional[int] = None, enable_metrics: bool = True):
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
            self.full_name = os.path.dirname(caller_frame.filename).replace('/', '.')
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
