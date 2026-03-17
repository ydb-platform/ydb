import django.db.backends.oracle.base as oracle_base

from django_query_profiler.django.db.backends.database_wrapper_mixin import QueryProfilerDatabaseWrapperMixin


class DatabaseWrapper(oracle_base.DatabaseWrapper, QueryProfilerDatabaseWrapperMixin):
    pass
