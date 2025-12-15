"""
Instrumented YDB client with automatic metrics collection.

This module provides InstrumentedYdbClient class that inherits from YdbClient
and automatically collects metrics for all database operations.
"""

import inspect
import os
from typing import Optional
from .common import YdbClient
from .publish_metrics import get_metrics_collector


class InstrumentedYdbClient(YdbClient):
    """
    YDB client with automatic metrics collection for all operations.

    Inherits from YdbClient and adds instrumentation for tracking:
    - Query execution success/failure
    - Error types
    - Statistics by operation types

    Example usage:
        client = InstrumentedYdbClient(endpoint, database, use_query_service=True)
        client.query("CREATE TABLE ...", True, operation_name='create_table')
        client.query("INSERT INTO ...", False, operation_name='insert_data')
        print(client.get_metrics_summary())
    """

    def __init__(self, endpoint, database, use_query_service=False, sessions=100,
                 enable_metrics=True):
        """
        Initialize instrumented client.

        Args:
            endpoint: YDB endpoint
            database: Database name
            use_query_service: Use Query Service API
            sessions: Number of sessions in pool
            enable_metrics: Enable metrics collection (default True)
        """
        super().__init__(endpoint, database, use_query_service, sessions)

        self.enable_metrics = enable_metrics
        self.metrics_collector = get_metrics_collector() if enable_metrics else None

        stack = inspect.stack()
        caller_frame = stack[1]
        try:
            self.full_name = os.path.dirname(caller_frame.filename).replace('/', '.')
        finally:
            del caller_frame

    def query(self, statement: str, is_ddl: bool, retry_settings=None,
              operation_name: Optional[str] = None):
        """
        Executes query with automatic metrics collection.

        Args:
            statement: SQL query
            is_ddl: True for DDL queries, False for DML
            retry_settings: Retry settings
            operation_name: Operation name for metrics (optional)
                          If not specified, uses 'ddl' or 'dml'

        Returns:
            Query execution result
        """
        if operation_name is None:
            operation_name = 'ddl' if is_ddl else 'dml'
        if not self.enable_metrics:
            return super(InstrumentedYdbClient, self).query(statement, is_ddl, retry_settings)

        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedYdbClient, self).query(
                statement, is_ddl, retry_settings
            ),
            operation_name, self.full_name
        )

    def drop_table(self, path_to_table: str, operation_name: str = 'drop_table'):
        """
        Drops table with automatic metrics collection.

        Args:
            path_to_table: Table path
            operation_name: Operation name for metrics
        """
        if not self.enable_metrics:
            return super(InstrumentedYdbClient, self).drop_table(path_to_table)
        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedYdbClient, self).drop_table(path_to_table),
            operation_name, self.full_name
        )

    def replace_index(self, table: str, src: str, dst: str,
                      operation_name: str = 'replace_index'):
        """
        Replaces index with automatic metrics collection.

        Args:
            table: Table path
            src: Source index name
            dst: Target index name
            operation_name: Operation name for metrics
        """
        if not self.enable_metrics:
            return super(InstrumentedYdbClient, self).replace_index(table, src, dst)
        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedYdbClient, self).replace_index(table, src, dst),
            operation_name, self.full_name
        )

    def describe(self, path: str, operation_name: str = 'describe_path'):
        """
        Gets schema object description with automatic metrics collection.

        Args:
            path: Object path
            operation_name: Operation name for metrics
        """
        if not self.enable_metrics:
            return super(InstrumentedYdbClient, self).describe(path)
        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedYdbClient, self).describe(path),
            operation_name, self.full_name
        )

    def remove_recursively(self, path: str, operation_name: str = 'remove_recursively'):
        """
        Recursively removes directory with automatic metrics collection.

        Args:
            path: Directory path
            operation_name: Operation name for metrics
        """
        if not self.enable_metrics:
            return super(InstrumentedYdbClient, self).remove_recursively(path)
        return self.metrics_collector.wrap_call(
            lambda: super(InstrumentedYdbClient, self).remove_recursively(path),
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
