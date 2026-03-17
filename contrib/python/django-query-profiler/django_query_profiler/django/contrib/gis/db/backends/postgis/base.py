import django.contrib.gis.db.backends.postgis.base as postgis_base

from django_query_profiler.django.db.backends.database_wrapper_mixin import QueryProfilerDatabaseWrapperMixin


class DatabaseWrapper(postgis_base.DatabaseWrapper, QueryProfilerDatabaseWrapperMixin):
    pass
