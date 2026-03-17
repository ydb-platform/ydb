from typing import Optional

import django.db.backends.mysql.base as mysql_base

from django_query_profiler.django.db.backends.database_wrapper_mixin import QueryProfilerDatabaseWrapperMixin


class DatabaseWrapper(mysql_base.DatabaseWrapper, QueryProfilerDatabaseWrapperMixin):

    @staticmethod
    def db_row_count(cursor) -> Optional[int]:
        try:
            if cursor.connection.errno():  # Not all mysql drivers have this attribute.  See Issue#19
                return -1
        except AttributeError:
            return cursor.rowcount
