# coding: utf-8

from __future__ import unicode_literals

import re
import sys
import inspect

from packaging.version import Version

try:
    from django.db.backends.postgresql.schema import DatabaseSchemaEditor as BaseEditor
except ImportError:
    from django.db.backends.postgresql_psycopg2.schema import DatabaseSchemaEditor as BaseEditor

import django
from django.db.models.fields import NOT_PROVIDED
from django.db.models.fields.related import RelatedField
from django.db import transaction
from django.db.migrations.questioner import InteractiveMigrationQuestioner

from zero_downtime_migrations.backend.sql_template import (
    SQL_ESTIMATE_COUNT_IN_TABLE,
    SQL_CHECK_COLUMN_STATUS,
    SQL_COUNT_IN_TABLE,
    SQL_COUNT_IN_TABLE_WITH_NULL,
    SQL_UPDATE_BATCH,
    SQL_CREATE_UNIQUE_INDEX,
    SQL_ADD_UNIQUE_CONSTRAINT_FROM_INDEX,
    SQL_CHECK_INDEX_STATUS,
)

from zero_downtime_migrations.backend.exceptions import InvalidIndexError

DJANGO_VERISON = Version(django.get_version())
TABLE_SIZE_FOR_MAX_BATCH = 500000
MAX_BATCH_SIZE = 10000
MIN_BATCH_SIZE = 1000

_getargspec = getattr(inspect, 'getfullargspec', getattr(inspect, 'getargspec', None))

class ZeroDownTimeMixin(object):
    RETRY_QUESTION_TEMPLATE = ('It look like column "{}" in table "{}" already exist with following '
                               'parameters: TYPE: "{}", DEFAULT: "{}", NULLABLE: "{}".'
                               )
    RETRY_CHOICES = (
        'abort migration',
        'drop column and run migration from beginning',
        'manually choose action to start from',
        'show how many rows still need to be updated',
        'mark operation as successful and proceed to next operation',
        'drop column and run migration from standard SchemaEditor',
    )

    ADD_FIELD_WITH_DEFAULT_ACTIONS = [
        'add field with default',
        'update existing rows',
        'set not null for field',
        'drop default',
    ]

    def alter_field(self, model, old_field, new_field, strict=False):

        if DJANGO_VERISON >= Version('2.1'):
            from django.db.backends.ddl_references import IndexName
            if self._unique_should_be_added(old_field, new_field):
                table = model._meta.db_table
                index_name = str(IndexName(table, [new_field.column], '_uniq', self._create_index_name))
                self.execute(
                    self._create_index_sql(model, [new_field], name=index_name, sql=SQL_CREATE_UNIQUE_INDEX)
                )
                self.execute(self._create_unique_constraint_from_index_sql(table, index_name))
                self.already_added_unique = True

        return super(ZeroDownTimeMixin, self).alter_field(model, old_field, new_field, strict=strict)

    def _field_supported(self, field):
        supported = True
        if isinstance(field, RelatedField):
            supported = False
        elif field.default is NOT_PROVIDED:
            supported = False

            if (DJANGO_VERISON >= Version('1.10') and
                (getattr(field, 'auto_now', False) or
                 getattr(field, 'auto_now_add', False))
            ):
                self.date_default = True
                supported = True
        return supported

    def add_field(self, model, field):
        if not self._field_supported(field=field):
            return super(ZeroDownTimeMixin, self).add_field(model, field)

        # Checking which actions we should perform - maybe this operation was run
        # before and it had crashed for some reason
        actions = self.get_actions_to_perform(model, field)
        if not actions:
            return

        # Saving initial values
        default_effective_value = self.effective_default(field)
        nullable = field.null
        # Update the values to the required ones
        field.default = None if DJANGO_VERISON < Version('1.11') else NOT_PROVIDED
        if getattr(self, 'date_default', False):
            if getattr(field, 'auto_now', False):
                field.auto_now = False
            if getattr(field, 'auto_now_add', False):
                field.auto_now_add = False
        if nullable is False:
            field.null = True

        # For Django < 1.10
        atomic = getattr(self, 'atomic_migration', True)

        if self.connection.in_atomic_block:
            self.atomic.__exit__(None, None, None)

        available_args = {
            'model': model,
            'field': field,
            'nullable': nullable,
            'default_effective_value': default_effective_value,
        }
        # Performing needed actions
        for action in actions:
            action = '_'.join(action.split())
            func = getattr(self, action)
            func_args = {arg: available_args[arg] for arg in
                         _getargspec(func).args if arg != 'self'
                         }
            func(**func_args)

        # If migrations was atomic=True initially
        # entering atomic block again
        if atomic:
            self.atomic = transaction.atomic(self.connection.alias)
            self.atomic.__enter__()

    def add_field_with_default(self, model, field, default_effective_value):
        """
        Adding field with default in two separate
        operations, so we can avoid rewriting the
        whole table
        """
        with transaction.atomic():
            super(ZeroDownTimeMixin, self).add_field(model, field)
            self.add_default(model, field, default_effective_value)

    def update_existing_rows(self, model, field, default_effective_value):
        """
        Updating existing rows in table by (relatively) small batches
        to avoid long locks on table
        """
        if default_effective_value is None:
            return
        objects_in_table = self.count_objects_in_table(model=model)
        if objects_in_table > 0:
            objects_in_batch_count = self.get_objects_in_batch_count(objects_in_table)
            while True:
                with transaction.atomic():
                    updated = self.update_batch(model=model, field=field,
                                                objects_in_batch_count=objects_in_batch_count,
                                                value=default_effective_value,
                                                )
                    print('Update {} rows in {}'.format(updated, model._meta.db_table))
                    if updated is None or updated == 0:
                        break

    def set_not_null_for_field(self, model, field, nullable):
        # If field was not null - adding
        # this knowledge to table
        if nullable is False:
            self.set_not_null(model, field)

    def get_column_info(self, model, field):
        sql = SQL_CHECK_COLUMN_STATUS % {
            "table": model._meta.db_table,
            "column": field.name,
        }
        return self.get_query_result(sql)

    def get_actions_to_perform(self, model, field):
        actions = self.ADD_FIELD_WITH_DEFAULT_ACTIONS
        # Checking maybe this column already exists
        # if so asking user what to do next
        column_info = self.get_column_info(model, field)

        if column_info is not None:
            existed_nullable, existed_type, existed_default = column_info

            questioner = InteractiveMigrationQuestioner()

            question = self.RETRY_QUESTION_TEMPLATE.format(
                field.name, model._meta.db_table,
                existed_type, existed_default,
                existed_nullable,
            )

            result = questioner._choice_input(question, self.RETRY_CHOICES)
            if result == 1:
                sys.exit(1)
            elif result == 2:
                self.remove_field(model, field)
            elif result == 3:
                question = 'Now choose from which action process should continue'
                result = questioner._choice_input(question, actions)
                actions = actions[result - 1:]
            elif result == 4:
                question = 'Rows in table where column is null: "{}"'
                need_to_update = self.need_to_update(model=model, field=field)
                questioner._choice_input(question.format(need_to_update),
                                         ('Continue',)
                                         )
                return self.get_actions_to_perform(model, field)
            elif result == 5:
                actions = []
            elif result == 6:
                self.remove_field(model, field)
                super(ZeroDownTimeMixin, self).add_field(model, field)
                actions = []
        return actions

    def get_pk_column_name(self, model):
        _, db_column = model._meta.pk.get_attname_column()
        return db_column

    def update_batch(self, model, field, objects_in_batch_count, value):
        pk_column_name = self.get_pk_column_name(model)
        sql = SQL_UPDATE_BATCH % {
            "table": model._meta.db_table,
            "column": field.name,
            "batch_size": objects_in_batch_count,
            "pk_column_name": pk_column_name,
            "value": "%s",
        }
        params = [value]
        return self.get_query_result(sql, params, row_count=True)

    def get_objects_in_batch_count(self, model_count):
        """
        Calculate batch size
        :param model_count: int
        :return: int
        """
        if model_count > TABLE_SIZE_FOR_MAX_BATCH:
            value = MAX_BATCH_SIZE
        else:
            value = int((model_count / 100) * 5)
        return max(MIN_BATCH_SIZE, value)

    def get_query_result(self, sql, params=(), row_count=False):
        """
        Default django backend execute function does not
        return any result so we use this custom where needed
        """
        if self.collect_sql:
            # in collect_sql case use django function logic
            return self.execute(sql, params)

        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            if row_count:
                return cursor.rowcount
            return cursor.fetchone()

    def parse_cursor_result(self, cursor_result, place=0, collect_sql_value=1, ):
        result = None
        if self.collect_sql:
            result = collect_sql_value  # For sqlmigrate purpose
        elif cursor_result:
            result = cursor_result[place]
        return result

    def execute_table_query(self, sql, model):
        sql = sql % {
            "table": model._meta.db_table
        }
        cursor_result = self.get_query_result(sql)
        return self.parse_cursor_result(cursor_result=cursor_result)

    def count_objects_in_table(self, model):
        count = self.execute_table_query(
            sql=SQL_ESTIMATE_COUNT_IN_TABLE,
            model=model,
        )
        if count == 0:
            # Check, maybe statistic is outdated?
            # Because previous count return 0 it will be fast query
            count = self.execute_table_query(
                sql=SQL_COUNT_IN_TABLE,
                model=model,
            )
        return count

    def need_to_update(self, model, field):
        sql = SQL_COUNT_IN_TABLE_WITH_NULL % {
            "table": model._meta.db_table,
            "column": field.name,
        }
        cursor_result = self.get_query_result(sql)
        return self.parse_cursor_result(cursor_result=cursor_result)

    def drop_default(self, model, field):
        set_default_sql, params = self._alter_column_default_sql_local(field, drop=True)
        self.execute_alter_column(model, set_default_sql, params)

    def add_default(self, model, field, default_value):
        set_default_sql, params = self._alter_column_default_sql_local(field, default_value)
        self.execute_alter_column(model, set_default_sql, params)

    def set_not_null(self, model, field):
        set_not_null_sql = self.generate_set_not_null(field)
        self.execute_alter_column(model, set_not_null_sql)

    def execute_alter_column(self, model, changes_sql, params=()):
        sql = self.sql_alter_column % {
            "table": self.quote_name(model._meta.db_table),
            "changes": changes_sql,
        }
        self.execute(sql, params)

    def generate_set_not_null(self, field):
        new_db_params = field.db_parameters(connection=self.connection)
        sql = self.sql_alter_column_not_null
        return sql % {
            'column': self.quote_name(field.column),
            'type': new_db_params['type'],
        }

    def _alter_column_default_sql_local(self, field, default_value=None, drop=False):
        """
        Copy this method from django2.0
        https://github.com/django/django/blob/master/django/db/backends/base/schema.py#L787
        """
        default = '%s'
        params = [default_value]

        if drop:
            params = []

        new_db_params = field.db_parameters(connection=self.connection)
        sql = self.sql_alter_column_no_default if drop else self.sql_alter_column_default
        return (
            sql % {
                'column': self.quote_name(field.column),
                'type': new_db_params['type'],
                'default': default,
            },
            params,
        )

    def _unique_should_be_added(self, old_field, new_field):
        if getattr(self, 'already_added_unique', False):
            return False
        return super(ZeroDownTimeMixin, self)._unique_should_be_added(old_field, new_field)

    def _create_unique_constraint_from_index_sql(self, table, index_name):
        return SQL_ADD_UNIQUE_CONSTRAINT_FROM_INDEX % {
            "table": table,
            "name": index_name,
            "index_name": index_name,
        }

    def _check_index_sql(self, index_name):
        return SQL_CHECK_INDEX_STATUS % {
            "index_name": index_name,
        }

    def _check_valid_index(self, sql):
        """
        Return index_name if it's invalid
        """
        index_match = re.match(r'.* "(?P<index_name>.+)" ON .+', sql)
        if index_match:
            index_name = index_match.group('index_name')
            check_index_sql = self._check_index_sql(index_name)
            cursor_result = self.get_query_result(check_index_sql)
            if self.parse_cursor_result(cursor_result=cursor_result):
                return index_name

    def _create_unique_failed(self, exc):
        return (DJANGO_VERISON >= Version('2.1')
                and 'could not create unique index' in repr(exc)
                )

    def execute(self, sql, params=()):
        exit_atomic = False
        # Account for non-string statement objects.
        sql = str(sql)

        if re.search('(CREATE|DROP).+INDEX', sql):
            exit_atomic = True
            if 'CONCURRENTLY' not in sql:
                sql = sql.replace('INDEX', 'INDEX CONCURRENTLY')
        atomic = self.connection.in_atomic_block
        if exit_atomic and atomic:
            self.atomic.__exit__(None, None, None)
        try:
            super(ZeroDownTimeMixin, self).execute(sql, params)
        except django.db.utils.IntegrityError as exc:
            # create unique index should be treated differently
            # because it raises error, instead of quiet exit
            if not self._create_unique_failed(exc):
                raise

        if exit_atomic and not self.collect_sql:
            invalid_index_name = self._check_valid_index(sql)
            if invalid_index_name:
                # index was build, but invalid, we need to delete it
                self.execute(self.sql_delete_index % {'name': invalid_index_name})
                raise InvalidIndexError(
                    'Unsuccessful attempt to create an index, fix data if needed and restart.'
                    'Sql was: {}'.format(sql)
                )
        if exit_atomic and atomic:
            self.atomic = transaction.atomic(self.connection.alias)
            self.atomic.__enter__()


class DatabaseSchemaEditor(ZeroDownTimeMixin, BaseEditor):
    pass
