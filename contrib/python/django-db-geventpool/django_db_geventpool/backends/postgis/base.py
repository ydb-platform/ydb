# coding=utf-8

from django.contrib.gis.db.backends.postgis.base import DatabaseWrapper as OriginalDatabaseWrapper

from django_db_geventpool.backends.postgresql_psycopg2.base import DatabaseWrapperMixin


class DatabaseWrapper(DatabaseWrapperMixin, OriginalDatabaseWrapper):
    pass
