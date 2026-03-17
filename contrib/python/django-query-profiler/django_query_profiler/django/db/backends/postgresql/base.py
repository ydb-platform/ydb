import django.db.backends.postgresql.base as postgresql_base

from django_query_profiler.django.db.backends.database_wrapper_mixin import QueryProfilerDatabaseWrapperMixin


class DatabaseWrapper(postgresql_base.DatabaseWrapper, QueryProfilerDatabaseWrapperMixin):
    pass
