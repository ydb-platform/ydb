"""
This module defines a mixin, which can be used by all implementations for all databases.
All the databases have a different hierarchy of DatabaseWrapper, but all of them derive from BaseDatabaseWrapper
"""

from abc import ABC
from typing import Optional

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import CursorDebugWrapper, CursorWrapper

from .cursor_wrapper_instrumentation import QueryProfilerCursorDebugWrapper, QueryProfilerCursorWrapper


class QueryProfilerDatabaseWrapperMixin(BaseDatabaseWrapper, ABC):

    def cursor(self):
        cursor_wrapper = super().cursor()
        kwargs = dict(
            cursor=cursor_wrapper.cursor,
            db=cursor_wrapper.db,
            db_row_count=self.db_row_count(cursor_wrapper.cursor))

        if isinstance(cursor_wrapper, CursorDebugWrapper):
            return QueryProfilerCursorDebugWrapper(**kwargs)
        elif isinstance(cursor_wrapper, CursorWrapper):
            return QueryProfilerCursorWrapper(**kwargs)
        else:
            raise Exception("cursor_wrapper is not of either of {CursorWrapper, CursorDebugWrapper}.  Is it because of "
                            "new version of django?  Did you run the tests in the django_query_profiler - they must  "
                            "have failed")

    @staticmethod
    def db_row_count(cursor) -> Optional[int]:
        """
        Implementation varies by database types, having it as a function allows it to be overriden
        """
        return cursor.rowcount
