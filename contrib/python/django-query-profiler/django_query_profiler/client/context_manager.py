"""
Context manager which "starts" the profiling when it enters, and stops & return the profiled data when it exits
"""
from typing import Optional

from django_query_profiler.query_profiler_storage import QueryProfiledData, QueryProfilerLevel
from django_query_profiler.query_profiler_storage.data_collector import data_collector_thread_local_storage


class QueryProfiler:

    def __init__(self, query_profiler_level: QueryProfilerLevel, clear_thread_local: bool = False):
        if clear_thread_local:
            data_collector_thread_local_storage.reset()

        self.query_profiled_data: Optional[QueryProfiledData] = None
        self.query_profiler_level: QueryProfilerLevel = query_profiler_level

    def __enter__(self) -> 'QueryProfiler':
        data_collector_thread_local_storage.enter_profiler_mode(self.query_profiler_level)
        return self

    def __exit__(self, *_) -> None:
        self.query_profiled_data = data_collector_thread_local_storage.exit_profiler_mode()
