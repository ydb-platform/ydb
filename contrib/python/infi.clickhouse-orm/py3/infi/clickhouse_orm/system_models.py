"""
This file contains system readonly models that can be got from the database
https://clickhouse.tech/docs/en/system_tables/
"""
from __future__ import unicode_literals

from .database import Database
from .fields import *
from .models import Model
from .utils import comma_join


class SystemPart(Model):
    """
    Contains information about parts of a table in the MergeTree family.
    This model operates only fields, described in the reference. Other fields are ignored.
    https://clickhouse.tech/docs/en/system_tables/system.parts/
    """
    OPERATIONS = frozenset({'DETACH', 'DROP', 'ATTACH', 'FREEZE', 'FETCH'})

    _readonly = True
    _system = True

    database = StringField()  # Name of the database where the table that this part belongs to is located.
    table = StringField()  # Name of the table that this part belongs to.
    engine = StringField()  # Name of the table engine, without parameters.
    partition = StringField()  # Name of the partition, in the format YYYYMM.
    name = StringField()  # Name of the part.

    # This field is present in the docs (https://clickhouse.tech/docs/en/single/index.html#system-parts),
    # but is absent in ClickHouse (in version 1.1.54245)
    # replicated = UInt8Field()  # Whether the part belongs to replicated data.

    # Whether the part is used in a table, or is no longer needed and will be deleted soon.
    # Inactive parts remain after merging.
    active = UInt8Field()

    # Number of marks - multiply by the index granularity (usually 8192)
    # to get the approximate number of rows in the part.
    marks = UInt64Field()

    bytes = UInt64Field()  # Number of bytes when compressed.

    # Time the directory with the part was modified. Usually corresponds to the part's creation time.
    modification_time = DateTimeField()
    remove_time = DateTimeField()  # For inactive parts only - the time when the part became inactive.

    # The number of places where the part is used. A value greater than 2 indicates
    # that this part participates in queries or merges.
    refcount = UInt32Field()

    @classmethod
    def table_name(cls):
        return 'parts'

    """
    Next methods return SQL for some operations, which can be done with partitions
    https://clickhouse.tech/docs/en/query_language/queries/#manipulations-with-partitions-and-parts
    """
    def _partition_operation_sql(self, operation, settings=None, from_part=None):
        """
        Performs some operation over partition

        - `db`: Database object to execute operation on
        - `operation`: Operation to execute from SystemPart.OPERATIONS set
        - `settings`: Settings for executing request to ClickHouse over db.raw() method

        Returns: Operation execution result
        """
        operation = operation.upper()
        assert operation in self.OPERATIONS, "operation must be in [%s]" % comma_join(self.OPERATIONS)

        sql = "ALTER TABLE `%s`.`%s` %s PARTITION %s" % (self._database.db_name, self.table, operation, self.partition)
        if from_part is not None:
            sql += " FROM %s" % from_part
        self._database.raw(sql, settings=settings, stream=False)

    def detach(self, settings=None):
        """
        Move a partition to the 'detached' directory and forget it.

        - `settings`: Settings for executing request to ClickHouse over db.raw() method

        Returns: SQL Query
        """
        return self._partition_operation_sql('DETACH', settings=settings)

    def drop(self, settings=None):
        """
        Delete a partition

        - `settings`: Settings for executing request to ClickHouse over db.raw() method

        Returns: SQL Query
        """
        return self._partition_operation_sql('DROP', settings=settings)

    def attach(self, settings=None):
        """
         Add a new part or partition from the 'detached' directory to the table.

        - `settings`: Settings for executing request to ClickHouse over db.raw() method

        Returns: SQL Query
        """
        return self._partition_operation_sql('ATTACH', settings=settings)

    def freeze(self, settings=None):
        """
        Create a backup of a partition.

        - `settings`: Settings for executing request to ClickHouse over db.raw() method

        Returns: SQL Query
        """
        return self._partition_operation_sql('FREEZE', settings=settings)

    def fetch(self, zookeeper_path, settings=None):
        """
        Download a partition from another server.

        - `zookeeper_path`: Path in zookeeper to fetch from
        - `settings`: Settings for executing request to ClickHouse over db.raw() method

        Returns: SQL Query
        """
        return self._partition_operation_sql('FETCH', settings=settings, from_part=zookeeper_path)

    @classmethod
    def get(cls, database, conditions=""):
        """
        Get all data from system.parts table

        - `database`: A database object to fetch data from.
        - `conditions`: WHERE clause conditions. Database condition is added automatically

        Returns: A list of SystemPart objects
        """
        assert isinstance(database, Database), "database must be database.Database class instance"
        assert isinstance(conditions, str), "conditions must be a string"
        if conditions:
            conditions += " AND"
        field_names = ','.join(cls.fields())
        return database.select("SELECT %s FROM `system`.%s WHERE %s database='%s'" %
                               (field_names, cls.table_name(), conditions, database.db_name), model_class=cls)

    @classmethod
    def get_active(cls, database, conditions=""):
        """
        Gets active data from system.parts table

        - `database`: A database object to fetch data from.
        - `conditions`: WHERE clause conditions. Database and active conditions are added automatically

        Returns: A list of SystemPart objects
        """
        if conditions:
            conditions += ' AND '
        conditions += 'active'
        return SystemPart.get(database, conditions=conditions)


# Expose only relevant classes in import *
__all__ = [c.__name__ for c in [SystemPart]]
