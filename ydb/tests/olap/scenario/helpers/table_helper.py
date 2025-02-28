from __future__ import annotations
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import (
    ScenarioTestHelper,
    TestContext,
)
from abc import abstractmethod, ABC
from typing import override, Dict, Iterable, Optional
from datetime import timedelta


class CreateTableLikeObject(ScenarioTestHelper.IYqlble):
    """The base class for all requests for creating table-like objects.

    Table-like objects are Tables and TableStore.
    See {ScenarioTestHelper.IYqlble}, {ScenarioTestHelper.execute_scheme_query}."""

    def __init__(self, name: str) -> None:
        """Constructor.

        Args:
            name: Name (relative path) of the object being created."""

        super().__init__(name)
        self._schema = None
        self._partitions = 64
        self._existing_ok = False

    def with_schema(self, schema: ScenarioTestHelper.Schema) -> CreateTableLikeObject:
        """Specify the schema of the created object.

         Args:
            schema: Schema of the created object.

        Returns:
            self."""

        self._schema = schema
        return self

    def with_partitions_count(self, partitions: int) -> CreateTableLikeObject:
        """Set the number of partitions of the created object.

         Args:
            partitions: Number of partitions of the created object.

        Returns:
            self."""

        self._partitions = partitions
        return self

    def existing_ok(self, value: bool = True) -> CreateTableLikeObject:
        """Set existing_ok value.

         Args:
            value: existing_ok.

        Returns:
            self."""

        self._existing_ok = value
        return self

    @override
    def params(self) -> Dict[str, str]:
        return {self._type(): self._name}

    @override
    def title(self):
        return f'Create {self._type()}'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        schema_str = ',\n    '.join([c.to_yql() for c in self._schema.columns])
        column_families_str = ',\n'.join([c.to_yql() for c in self._schema.column_families])
        keys = ', '.join(self._schema.key_columns)
        return f'''CREATE {self._type().upper()}{' IF NOT EXISTS' if self.existing_ok else ''} `{ScenarioTestHelper(ctx).get_full_path(self._name)}` (
    {schema_str},
    PRIMARY KEY({keys})
    {"" if not column_families_str else f", {column_families_str}"}
)
{self._partition_by()}
WITH(
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {self._partitions}
)
'''

    @abstractmethod
    def _type(self) -> str:
        """Type of object being created."""

        pass

    @abstractmethod
    def _partition_by(self) -> str:
        """Break into partitions according to..."""

        pass


class CreateTable(CreateTableLikeObject):
    """Create Table.

    Example:
        schema = (
            ScenarioTestHelper.Schema()
            .with_column(name='a', type=PrimitiveType.Int32, not_null=True)
            .with_column(name='b', type=PrimitiveType.Utf8)
            .with_column(name='c', type=PrimitiveType.Utf8)
            .with_key_columns('a')
        )

        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(CreateTable(testTable).with_schema(schema))
    """

    @override
    def _type(self) -> str:
        return 'table'

    @override
    def _partition_by(self) -> str:
        keys = ', '.join(self._schema.key_columns)
        return f'PARTITION BY HASH({keys})'


class CreateTableStore(CreateTableLikeObject):
    """Create TableStore.

    Example:
        schema = (
            ScenarioTestHelper.Schema()
            .with_column(name='a', type=PrimitiveType.Int32, not_null=True)
            .with_column(name='b', type=PrimitiveType.Utf8)
            .with_column(name='c', type=PrimitiveType.Utf8)
            .with_key_columns('a')
        )

        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(CreateTableStore('testStore').with_schema(schema))
        sth.execute_scheme_query(CreateTable(testStore/testTable).with_schema(schema))
    """

    @override
    def _type(self) -> str:
        return 'tablestore'

    @override
    def _partition_by(self) -> str:
        return ''


class Action(ABC):
    """The base class for all actions."""

    @abstractmethod
    def to_yql(self) -> str:
        """Convert to YQL."""
        pass

    @abstractmethod
    def title(self) -> str:
        """Title to display in Allure."""
        pass


class AlterTableAction(Action):
    """The base class for all actions when changing table-like objects.

    Table-like objects are Tables and TableStore.
    See {AlterTableLikeObject}.
    """

    @override
    def to_yql(self) -> str:
        """Convert to YQL."""

        pass

    @override
    def title(self) -> str:
        """Title to display in Allure."""

        pass


class AddColumn(AlterTableAction):
    """Add a column to a table-like object.

    Table-like objects are Tables and TableStore.
    See {AlterTableLikeObject}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable')
                .action(AddColumn(sth.Column('not_level', PrimitiveType.Uint32)))
        )
    """

    def __init__(self, column: ScenarioTestHelper.Column) -> None:
        """Constructor.

        Args:
            column: Column description."""

        super().__init__()
        self._column = column

    @override
    def to_yql(self) -> str:
        return f'ADD COLUMN {self._column.to_yql()}'

    @override
    def title(self) -> str:
        return f'add column `{self._column.name}`'


class AlterColumnBase(Action):
    """The base class for all actions on a column."""

    @override
    def to_yql(self) -> str:
        """Convert to YQL."""
        pass

    @override
    def title(self) -> str:
        """Title to display in Allure."""
        pass


class AlterFamily(AlterColumnBase):
    """Alter family for a column."""

    def __init__(self, colum_family_name: str) -> None:
        super().__init__()
        self._colum_family_name = colum_family_name

    @override
    def to_yql(self) -> str:
        return f'SET FAMILY {self._colum_family_name}'

    @override
    def title(self) -> str:
        return f'set family {self._colum_family_name}'


class AlterColumn(AlterTableAction):
    """Alter a column in a table-like object.

    Table-like objects are Tables and TableStore.
    See {AlterTableLikeObject}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable')
                .action(AlterColumn("column", AlterFamily("family2")))
        )
    """

    def __init__(self, column_name: str, action: AlterColumnBase) -> None:
        """Constructor.

        Args:
            column_name: Column name for alter
            action: Action description."""

        super().__init__()
        self._column_name = column_name
        self._action = action

    @override
    def to_yql(self) -> str:
        return f'ALTER COLUMN {self._column_name} {self._action.to_yql()}'

    @override
    def title(self) -> str:
        return f'altert column {self._column_name}`{self._action.title()}`'


class DropColumn(AlterTableAction):
    """Remove a column from a table-like object.

     Table-like objects are Tables and TableStore.
     See {AlterTableLikeObject}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable').action(DropColumn('not_level'))
        )
    """

    def __init__(self, column: str) -> None:
        """Constructor.

        Args:
            column: Name of the column to be deleted."""

        super().__init__()
        self._column = column

    @override
    def to_yql(self) -> str:
        return f'DROP COLUMN {self._column}'

    @override
    def title(self) -> str:
        return f'drop column `{self._column}`'


class SetSetting(AlterTableAction):
    """Set a setting value for a table-like object.

     Table-like objects are Tables and TableStore.
     See {AlterTableLikeObject}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable').action(SetSetting('TIERING', 'tiering1))
        )
    """

    def __init__(self, setting: str, value_literal: str) -> None:
        """Constructor.

        Args:
            column: Name of the column to be deleted."""

        super().__init__()
        self._setting = setting
        self._value = value_literal

    @override
    def to_yql(self) -> str:
        return f'SET {self._setting} {self._value}'

    @override
    def title(self) -> str:
        return f'set {self._setting} = {self._value}'


class ResetSetting(AlterTableAction):
    """Reset value of a setting for a table-like object.

     Table-like objects are Tables and TableStore.
     See {AlterTableLikeObject}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable').action(ResetSetting('TIERING'))
        )
    """

    def __init__(self, setting: str) -> None:
        """Constructor.

        Args:
            setting: Name of altered setting."""

        super().__init__()
        self._setting = setting

    @override
    def to_yql(self) -> str:
        return f'RESET ({self._setting})'

    @override
    def title(self) -> str:
        return f'reset {self._setting}'


class AddColumnFamily(AlterTableAction):
    """Add a column family to a table-like object.

    Table-like objects are Tables and TableStore.
    See {AlterTableLikeObject}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable')
                .action(AddColumnFamily(sth.ColumnFamily('family1', ScenarioTestHelper.Compression.LZ4, None))
                .action(AddColumnFamily(sth.ColumnFamily('family2', ScenarioTestHelper.Compression.ZSTD, 4))
                )
        )
    """

    def __init__(self, column_family: ScenarioTestHelper.ColumnFamily) -> None:
        """Constructor.

        Args:
            column_family: Column family description."""

        super().__init__()
        self._column_family = column_family

    @override
    def to_yql(self) -> str:
        return f'ADD {self._column_family.to_yql()}'

    @override
    def title(self) -> str:
        return f'add family `{self._column_family.name}`'


class ColumnFamilyAction(Action):
    """The base class for all actions when changing colum family."""

    @override
    def to_yql(self) -> str:
        """Convert to YQL."""

        pass

    @override
    def title(self) -> str:
        """Title to display in Allure."""

        pass


class AlterCompression(AlterTableAction):
    """Alter compression codec for a column family."""

    def __init__(self, compression: ScenarioTestHelper.Compression) -> None:
        super().__init__()
        self._compression = compression

    @override
    def to_yql(self) -> str:
        return f'SET COMPRESSION "{self._compression.name}"'

    @override
    def title(self) -> str:
        return f'set compression "{self._compression.name}"'


class AlterCompressionLevel(AlterTableAction):
    """Alter compression codec level for a column family."""

    def __init__(self, compression_level: int) -> None:
        super().__init__()
        self._compression_level = compression_level

    @override
    def to_yql(self) -> str:
        return f'SET COMPRESSION_LEVEL {self._compression_level}'

    @override
    def title(self) -> str:
        return f'set compression level {self._compression_level}'


class AlterColumnFamily(AlterTableAction):
    """Alter a column family to a table-like object.

    Table-like objects are Tables and TableStore.
    See {AlterTableLikeObject}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable')
                .action(AlterColumnFamily('family1', AlterCompression(ScenarioTestHelper.Compression.ZSTD)))
                .action(AlterColumnFamily('family2', AlterCompressionLevel(9)))
                )
        )
    """

    def __init__(self, column_family_name: str, action: ColumnFamilyAction) -> None:
        """Constructor.

        Args:
            column: Column description."""

        super().__init__()
        self._column_family_name = column_family_name
        self._action = action

    @override
    def to_yql(self) -> str:
        return f'ALTER FAMILY {self._column_family_name} {self._action.to_yql()}'

    @override
    def title(self) -> str:
        return f'alter family `{self._column_family_name}` {self._action.title()}'


class AlterTableLikeObject(ScenarioTestHelper.IYqlble):
    """The base class for all requests to change table-like objects.

     Table-like objects are Tables and TableStore.
     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, name: str) -> None:
        """Constructor.

        Args:
            name: Name (relative path) of the altered object."""

        super().__init__(name)
        self._actions = []

    def action(self, action: AlterTableAction) -> AlterTableLikeObject:
        """Add an action with an object.

         Args:
            action: Action on the object, such as creating or deleting a column.

        Returns:
            self."""

        self._actions.append(action)
        return self

    def __call__(self, action: AlterTableAction) -> AlterTableLikeObject:
        """See {AlterTableLikeObject.action}."""

        return self.action(action)

    def add_column(self, column: ScenarioTestHelper.Column) -> AlterTableLikeObject:
        """Add a column.

        The method is similar to calling {AlterTableLikeObject.action} with an {AddColumn} instance.

        Args:
            column: Description of the column.

        Returns:
            self."""

        return self(AddColumn(column))

    def drop_column(self, column: str) -> AlterTableLikeObject:
        """Delete a column.

        The method is similar to calling {AlterTableLikeObject.action} with a {DropColumn} instance.

        Args:
            column: Column name.

        Returns:
            self."""

        return self(DropColumn(column))

    def set_ttl(self, tiers: Iterable[(timedelta, Optional[str])], column: str) -> AlterTableLikeObject:
        """Set TTL for rows.

        The method is similar to calling {AlterTableLikeObject.action} with a {SetSetting} instance.

        Args:
            tiering_rule: Name of a TIERING_RULE object.

        Returns:
            self."""

        def make_tier_literal(delay: timedelta, storage_path: Optional[str]):
            delay_literal = f'Interval("PT{delay.total_seconds()}S")'
            if storage_path:
                return delay_literal + ' TO EXTERNAL DATA SOURCE `' + storage_path + '`'
            else:
                return delay_literal + ' DELETE'

        tiers_literal = ', '.join(map(lambda x: make_tier_literal(*x), tiers))
        return self(SetSetting('TTL', f'{tiers_literal} ON {column}'))

    def add_column_family(self, column_family: ScenarioTestHelper.ColumnFamily) -> AlterTableLikeObject:
        """Add a column_family.

        The method is similar to calling {AlterTableLikeObject.action} with an {AddColumnFamily} instance.

        Args:
            column: Description of the column_family.

        Returns:
            self."""

        return self(AddColumnFamily(column_family))

    @override
    def params(self) -> Dict[str, str]:
        return {self._type(): self._name, 'actions': ', '.join([a.title() for a in self._actions])}

    @override
    def title(self):
        return f'Alter {self._type()}'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        actions = ', '.join([a.to_yql() for a in self._actions])
        return f'ALTER {self._type().upper()} `{ScenarioTestHelper(ctx).get_full_path(self._name)}` {actions};'

    @abstractmethod
    def _type(self) -> str:
        pass


class AlterTable(AlterTableLikeObject):
    """Alter Table.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTable('testTable')
                .add_column(sth.Column('not_level', PrimitiveType.Uint32))
                .drop_column('level')
        )
    """

    @override
    def _type(self) -> str:
        return 'table'


class AlterTableStore(AlterTableLikeObject):
    """Alter TableStore.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(
            AlterTableStore('testStore')
                .add_column(sth.Column('not_level', PrimitiveType.Uint32))
                .drop_column('level')
        )
    """

    @override
    def _type(self) -> str:
        return 'tablestore'
