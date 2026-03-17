import django.db.backends.sqlite3.base as sqlite_base

from django_query_profiler.django.db.backends.database_wrapper_mixin import QueryProfilerDatabaseWrapperMixin


class DatabaseWrapper(sqlite_base.DatabaseWrapper, QueryProfilerDatabaseWrapperMixin):

    @staticmethod
    def db_row_count(cursor) -> None:
        # sqlite does not return rowcount
        return None
