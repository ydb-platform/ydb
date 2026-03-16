from .models import Model, BufferModel
from .fields import DateField, StringField
from .engines import MergeTree
from .utils import escape, get_subclass_names

import logging
logger = logging.getLogger('migrations')


class Operation():
    '''
    Base class for migration operations.
    '''

    def apply(self, database):
        raise NotImplementedError()   # pragma: no cover


class ModelOperation(Operation):
    '''
    Base class for migration operations that work on a specific model.
    '''

    def __init__(self, model_class):
        '''
        Initializer.
        '''
        self.model_class = model_class
        self.table_name = model_class.table_name()

    def _alter_table(self, database, cmd):
        '''
        Utility for running ALTER TABLE commands.
        '''
        cmd = "ALTER TABLE $db.`%s` %s" % (self.table_name, cmd)
        logger.debug(cmd)
        database.raw(cmd)


class CreateTable(ModelOperation):
    '''
    A migration operation that creates a table for a given model class.
    '''

    def apply(self, database):
        logger.info('    Create table %s', self.table_name)
        if issubclass(self.model_class, BufferModel):
            database.create_table(self.model_class.engine.main_model)
        database.create_table(self.model_class)


class AlterTable(ModelOperation):
    '''
    A migration operation that compares the table of a given model class to
    the model's fields, and alters the table to match the model. The operation can:
      - add new columns
      - drop obsolete columns
      - modify column types
    Default values are not altered by this operation.
    '''

    def _get_table_fields(self, database):
        query = "DESC `%s`.`%s`" % (database.db_name, self.table_name)
        return [(row.name, row.type) for row in database.select(query)]

    def apply(self, database):
        logger.info('    Alter table %s', self.table_name)

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
        for name, field in self.model_class.fields().items():
            is_regular_field = not (field.materialized or field.alias)
            if name not in table_fields:
                logger.info('        Add column %s', name)
                cmd = 'ADD COLUMN %s %s' % (name, field.get_sql(db=database))
                if is_regular_field:
                    if prev_name:
                        cmd += ' AFTER %s' % prev_name
                    else:
                        cmd += ' FIRST'
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
                        for name, field in self.model_class.fields().items()}
        for field_name, field_sql in self._get_table_fields(database):
            # All fields must have been created and dropped by this moment
            assert field_name in model_fields, 'Model fields and table columns in disagreement'

            if field_sql != model_fields[field_name]:
                logger.info('        Change type of column %s from %s to %s', field_name, field_sql,
                            model_fields[field_name])
                self._alter_table(database, 'MODIFY COLUMN %s %s' % (field_name, model_fields[field_name]))


class AlterTableWithBuffer(ModelOperation):
    '''
    A migration operation for altering a buffer table and its underlying on-disk table.
    The buffer table is dropped, the on-disk table is altered, and then the buffer table
    is re-created.
    '''

    def apply(self, database):
        if issubclass(self.model_class, BufferModel):
            DropTable(self.model_class).apply(database)
            AlterTable(self.model_class.engine.main_model).apply(database)
            CreateTable(self.model_class).apply(database)
        else:
            AlterTable(self.model_class).apply(database)


class DropTable(ModelOperation):
    '''
    A migration operation that drops the table of a given model class.
    '''

    def apply(self, database):
        logger.info('    Drop table %s', self.table_name)
        database.drop_table(self.model_class)


class AlterConstraints(ModelOperation):
    '''
    A migration operation that adds new constraints from the model to the database
    table, and drops obsolete ones. Constraints are identified by their names, so
    a change in an existing constraint will not be detected unless its name was changed too.
    ClickHouse does not check that the constraints hold for existing data in the table.
    '''

    def apply(self, database):
        logger.info('    Alter constraints for %s', self.table_name)
        existing = self._get_constraint_names(database)
        # Go over constraints in the model
        for constraint in self.model_class._constraints.values():
            # Check if it's a new constraint
            if constraint.name not in existing:
                logger.info('        Add constraint %s', constraint.name)
                self._alter_table(database, 'ADD %s' % constraint.create_table_sql())
            else:
                existing.remove(constraint.name)
        # Remaining constraints in `existing` are obsolete
        for name in existing:
            logger.info('        Drop constraint %s', name)
            self._alter_table(database, 'DROP CONSTRAINT `%s`' % name)

    def _get_constraint_names(self, database):
        '''
        Returns a set containing the names of existing constraints in the table.
        '''
        import re
        table_def = database.raw('SHOW CREATE TABLE $db.`%s`' % self.table_name)
        matches = re.findall(r'\sCONSTRAINT\s+`?(.+?)`?\s+CHECK\s', table_def)
        return set(matches)


class AlterIndexes(ModelOperation):
    '''
    A migration operation that adds new indexes from the model to the database
    table, and drops obsolete ones. Indexes are identified by their names, so
    a change in an existing index will not be detected unless its name was changed too.
    '''

    def __init__(self, model_class, reindex=False):
        '''
        Initializer.
        By default ClickHouse does not build indexes over existing data, only for
        new data. Passing `reindex=True` will run `OPTIMIZE TABLE` in order to build
        the indexes over the existing data.
        '''
        super().__init__(model_class)
        self.reindex = reindex

    def apply(self, database):
        logger.info('    Alter indexes for %s', self.table_name)
        existing = self._get_index_names(database)
        logger.info(existing)
        # Go over indexes in the model
        for index in self.model_class._indexes.values():
            # Check if it's a new index
            if index.name not in existing:
                logger.info('        Add index %s', index.name)
                self._alter_table(database, 'ADD %s' % index.create_table_sql())
            else:
                existing.remove(index.name)
        # Remaining indexes in `existing` are obsolete
        for name in existing:
            logger.info('        Drop index %s', name)
            self._alter_table(database, 'DROP INDEX `%s`' % name)
        # Reindex
        if self.reindex:
            logger.info('        Build indexes on table')
            database.raw('OPTIMIZE TABLE $db.`%s` FINAL' % self.table_name)

    def _get_index_names(self, database):
        '''
        Returns a set containing the names of existing indexes in the table.
        '''
        import re
        table_def = database.raw('SHOW CREATE TABLE $db.`%s`' % self.table_name)
        matches = re.findall(r'\sINDEX\s+`?(.+?)`?\s+', table_def)
        return set(matches)


class RunPython(Operation):
    '''
    A migration operation that executes a Python function.
    '''
    def __init__(self, func):
        '''
        Initializer. The given Python function will be called with a single
        argument - the Database instance to apply the migration to.
        '''
        assert callable(func), "'func' argument must be function"
        self._func = func

    def apply(self, database):
        logger.info('    Executing python operation %s', self._func.__name__)
        self._func(database)


class RunSQL(Operation):
    '''
    A migration operation that executes arbitrary SQL statements.
    '''

    def __init__(self, sql):
        '''
        Initializer. The given sql argument must be a valid SQL statement or
        list of statements.
        '''
        if isinstance(sql, str):
            sql = [sql]
        assert isinstance(sql, list), "'sql' argument must be string or list of strings"
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


# Expose only relevant classes in import *
__all__ = get_subclass_names(locals(), Operation)
