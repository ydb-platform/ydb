# coding: utf-8

from __future__ import unicode_literals

try:
    from django.db.backends.postgresql.base import DatabaseWrapper as BaseWrapper
except ImportError:
    from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as BaseWrapper

from .schema import DatabaseSchemaEditor


class DatabaseWrapper(BaseWrapper):
    SchemaEditorClass = DatabaseSchemaEditor
