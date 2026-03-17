"""
This is the module that collects & stores data (in its data structures, maintained in a thread-local) when a query is
executed.

There are three main functions in this module:
1. enter_profiled_mode : Called from the context manager
2. exit_profiled_mode: Called from the context manager
3. add_query_profiler_data: Called from the Django hook, when any query is executed

Complexity in this module comes from the fact that context manager can be nested.  If we call one context manager after
another, we don't have any of the complexity about bookkeeping about which index we entered, which we exited etc.

Nested context manager results in following complexity:
1.  Every time we exit, we expect only that data to be returned which had happened since the start of the same block.
    That means we would have to segregate the data when storing, and aggregate data when returning for each nested
    invocation so that we can know when exiting the block of which data belong to this exit.
2.  We are supporting two types of QueryProfilerLevel - one of type Query, and one of QuerySignature.   Nesting makes it
    challenging since now, we have to deal with the case of what type of profiler we should run in each block.

    We also have to deal with issue of various combinations of {Query, QuerySignature} nesting.  Even though the user
    asks for a given queryProfilerLevel, we would have to decide based on nesting, which queryProfilerLevel would be
    active

Implementation Notes:
--------------------
1.  To segregate the data for each time we enter via the context manager, we are going to use a List.  Data at an
    index represents the query profiled data that was collected when that "index" was active.  Lets say we are at 4th
    nested context manager => we would be writing data at list[3]
2.  When we exit from a context manger, we are going to return all the data that we had started collecting, from the
    index at which we entered.  ie. Lets assume we enter at index 2, and now list has grown to index 5.  When we exit
    from index 2, we would want to get all data from index 2 to 5.
    Its very clear from above that we would need to keep track of index when we had entered.  For that, we are going
    to use a stack of indices A stack would work because when we exit a block, we would pop from the stack
3.  "QueryProfilerData" is the container that collects all the data that we collect.   When we enter a block, we
    initialize with an empty container.  Any time Django hook calls to register a query, we add existing data to the
    new data, create "QueryProfiledData" object which has combined data, and use this new object in the list
4.  For finding which QueryProfilerType would be active, we are again using a stack.  And using the fact that if anyone
    above the current call has used QUERY_SIGNATURE, the active one would be QUERY_SIGNATURE
"""

import re
import threading
from binascii import hexlify
from collections import Counter
from time import time
from typing import List, Optional, Union

import django.db.models as django_base_model
import mmh3 as mmh3
from django.conf import settings

from . import QueryProfiledData, QueryProfilerLevel, QuerySignature, QuerySignatureStatistics
from .stack_tracer import find_stack_trace

RE_NORMALIZE_REPEATED_PARAMS_PERCENT = re.compile(r'%s(, %s)+')


class DataCollectorThreadLocalStorage(threading.local):

    def __init__(self):
        self._query_profiler_enabled: bool = False
        self._query_profiled_data_list: List[QueryProfiledData] = []
        self._entry_index_stack: List[int] = []
        self._query_profiler_level_stack: List[QueryProfilerLevel] = []
        self._current_query_profiler_level: Optional[QueryProfilerLevel] = None

    def reset(self) -> None:
        self.__init__()

    def __str__(self):
        return f'_query_profiler_enabled={self._query_profiler_enabled}, ' \
            f'query_profiled_data_list={self._query_profiled_data_list}, _entry_index_stack={self._entry_index_stack}'

    def enter_profiler_mode(self, query_profiler_level: QueryProfilerLevel) -> None:
        self._query_profiler_enabled = True

        # Put index to use in stack
        current_active_index = len(self._query_profiled_data_list)
        self._entry_index_stack.append(current_active_index)

        # Activate a new index in the list, by appending.  Use empty container object here
        empty_query_profiled_data: QueryProfiledData = QueryProfiledData()
        self._query_profiled_data_list.append(empty_query_profiled_data)

        # Append the passed query_profiler_level to the stack that maintains
        self._query_profiler_level_stack.append(query_profiler_level)
        self._current_query_profiler_level = sum(self._query_profiler_level_stack)

    def exit_profiler_mode(self) -> QueryProfiledData:
        if not self._entry_index_stack:
            raise Exception(f'Looks like exit profiler is called before enter was called. {str(self)}')

        last_enter_index = self._entry_index_stack.pop()
        combined_query_profiler_data = sum(self._query_profiled_data_list[last_enter_index:])

        if self._entry_index_stack:
            self._query_profiler_level_stack.pop()
            self._current_query_profiler_level = sum(self._query_profiler_level_stack)
        else:
            # If it is the last exit, reset everything
            self.reset()
        return combined_query_profiler_data

    def add_query_profiler_data(self, query_without_params: str, params: Union[list, str, None], target_db: str,
                                query_execution_time_in_micros: int, db_row_count: Optional[int]) -> None:
        """ This function adds to the bucket in the last index of the list, if the profiler is on """

        if not self._query_profiler_enabled:
            return

        start_time = time()
        if self._current_query_profiler_level.normalize_sql and params:
            sql_normalized = re.sub(RE_NORMALIZE_REPEATED_PARAMS_PERCENT, '%s', query_without_params)
        else:
            sql_normalized = query_without_params

        app_stack_trace, django_stack_trace = find_stack_trace(
                app_module_names_to_exclude=settings.DJANGO_QUERY_PROFILER_APP_MODULES_TO_EXCLUDE,
                django_module_names_to_include=(django_base_model.__name__, ),
                max_depth=self._current_query_profiler_level.stack_trace_depth)

        # New query_signature & query_signature_statistics instances
        query_signature = QuerySignature(
            query_without_params=sql_normalized,
            app_stack_trace=app_stack_trace,
            django_stack_trace=django_stack_trace,
            target_db=target_db)
        query_signature_statistics = QuerySignatureStatistics(
            frequency=1,  # Number of sql calls would be 1, when we entered this block
            query_execution_time_in_micros=query_execution_time_in_micros,
            db_row_count=db_row_count)

        query_params_db_key = (query_without_params, params or '', target_db)
        query_params_db_key_hash = hexlify(mmh3.hash_bytes(str(query_params_db_key)))

        new_query_profiled_data = QueryProfiledData(
            query_signature_to_query_signature_statistics={query_signature: query_signature_statistics},
            _query_params_db_hash_counter=Counter({query_params_db_key_hash: 1}),
            time_spent_profiling_in_micros=int((time() - start_time) * 1000 * 1000))

        # Add to existing data and set it back
        existing_query_profiled_data: QueryProfiledData = self._query_profiled_data_list[-1]
        combined_query_profiled_data: QueryProfiledData = existing_query_profiled_data + new_query_profiled_data
        self._query_profiled_data_list[-1] = combined_query_profiled_data


#######################################################################################################################
# The public instance.  Using the fact that module level instances are singleton
#######################################################################################################################
data_collector_thread_local_storage = DataCollectorThreadLocalStorage()
