import six

from .models import Model, BufferModel
from .fields import DateField, StringField
from .engines import MergeTree
from .utils import escape

from six.moves import zip
from six import iteritems

import logging
logger = logging.getLogger('migrations')


class Operation(object):
    '''
    Base class for migration operations.
    '''

    def apply(self, database):
        raise NotImplementedError()   # pragma: no cover


class CreateTable(Operation):
    '''
    A migration operation that creates a table for a given model class.
    '''

    def __init__(self, model_class):
        self.model_class = model_class

    def apply(self, database):
        logger.info('    Create table %s', self.model_class.table_name())
        if issubclass(self.model_class, BufferModel):
            database.create_table(self.model_class.engine.main_model)
        database.create_table(self.model_class)


class AlterTable(Operation):
    '''
    A migration operation that compares the table of a given model class to
    the model's fields, and alters the table to match the model. The operation can:
      - add new columns
      - drop obsolete columns
      - modify column types
    Default values are not altered by this operation.
    '''

    def __init__(self, model_class):
        self.model_class = model_class

    def _get_table_fields(self, database):
        query = "DESC `%s`.`%s`" % (database.db_name, self.model_class.table_name())
        return [(row.name, row.type) for row in database.select(query)]

    def _alter_table(self, database, cmd):
        cmd = "ALTER TABLE `%s`.`%s` %s" % (database.db_name, self.model_class.table_name(), cmd)
        logger.debug(cmd)
        database._send(cmd)

    def apply(self, database):
        logger.info('    Alter table %s', self.model_class.table_name())

        # Note that MATERIALIZED and ALIAS fields are always at the end of the DESC,
        # ADD COLUMN ... AFTER doesn't affect it
        table_fields = dict(self._get_table_fields(database))

        # Identify fields that were deleted from the model
        deleted_fields = set(table_fields.keys()) - set(self.model_class.fields())
        for name in deleted_fields:
            logger.info('        Drop column %s', name)
            self._alter_table(database, 'DROP COLUMN %s' % name)
            del table_fields[name]

        # Identify fields that were added to the model
        prev_name = None
        for name, field in iteritems(self.model_class.fields()):
            is_regular_field = not (field.materialized or field.alias)
            if name not in table_fields:
                logger.info('        Add column %s', name)
                assert prev_name, 'Cannot add a column to the beginning of the table'
                cmd = 'ADD COLUMN %s %s' % (name, field.get_sql(db=database))
                if is_regular_field:
                    cmd += ' AFTER %s' % prev_name
                self._alter_table(database, cmd)

            if is_regular_field:
                # ALIAS and MATERIALIZED fields are not stored in the database, and raise DatabaseError
                # (no AFTER column). So we will skip them
                prev_name = name

        # Identify fields whose type was changed
        # The order of class attributes can be changed any time, so we can't count on it
        # Secondly, MATERIALIZED and ALIAS fields are always at the end of the DESC, so we can't expect them to save
        # attribute position. Watch https://github.com/Infinidat/infi.clickhouse_orm/issues/47
        model_fields = {name: field.get_sql(with_default_expression=False, db=database)
                        for name, field in iteritems(self.model_class.fields())}
        for field_name, field_sql in self._get_table_fields(database):
            # All fields must have been created and dropped by this moment
            assert field_name in model_fields, 'Model fields and table columns in disagreement'

            if field_sql != model_fields[field_name]:
                logger.info('        Change type of column %s from %s to %s', field_name, field_sql,
                            model_fields[field_name])
                self._alter_table(database, 'MODIFY COLUMN %s %s' % (field_name, model_fields[field_name]))


class AlterTableWithBuffer(Operation):
    '''
    A migration operation for altering a buffer table and its underlying on-disk table.
    The buffer table is dropped, the on-disk table is altered, and then the buffer table
    is re-created.
    '''

    def __init__(self, model_class):
        self.model_class = model_class

    def apply(self, database):
        if issubclass(self.model_class, BufferModel):
            DropTable(self.model_class).apply(database)
            AlterTable(self.model_class.engine.main_model).apply(database)
            CreateTable(self.model_class).apply(database)
        else:
            AlterTable(self.model_class).apply(database)


class DropTable(Operation):
    '''
    A migration operation that drops the table of a given model class.
    '''

    def __init__(self, model_class):
        self.model_class = model_class

    def apply(self, database):
        logger.info('    Drop table %s', self.model_class.table_name())
        database.drop_table(self.model_class)


class RunPython(Operation):
    '''
    A migration operation that executes given python function on database
    '''
    def __init__(self, func):
        assert callable(func), "'func' parameter must be function"
        self._func = func

    def apply(self, database):
        logger.info('    Executing python operation %s', self._func.__name__)
        self._func(database)


class RunSQL(Operation):
    '''
    A migration operation that executes given SQL on database
    '''

    def __init__(self, sql):
        if isinstance(sql, six.string_types):
            sql = [sql]

        assert isinstance(sql, list), "'sql' parameter must be string or list of strings"
        self._sql = sql

    def apply(self, database):
        logger.info('    Executing raw SQL operations')
        for item in self._sql:
            database.raw(item)


class MigrationHistory(Model):
    '''
    A model for storing which migrations were already applied to the containing database.
    '''

    package_name = StringField()
    module_name = StringField()
    applied = DateField()

    engine = MergeTree('applied', ('package_name', 'module_name'))

    @classmethod
    def table_name(cls):
        return 'infi_clickhouse_orm_migrations'
