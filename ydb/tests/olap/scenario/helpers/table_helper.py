from __future__ import annotations
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import (
    ScenarioTestHelper,
    TestContext,
)
from abc import abstractmethod, ABC
from typing import override, Dict


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

    @override
    def params(self) -> Dict[str, str]:
        return {self._type(): self._name}

    @override
    def title(self):
        return f'Create {self._type()}'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        schema_str = ',\n    '.join([c.to_yql() for c in self._schema.columns])
        keys = ', '.join(self._schema.key_columns)
        return f'''CREATE {self._type().upper()} `{ScenarioTestHelper(ctx).get_full_path(self._name)}` (
    {schema_str},
    PRIMARY KEY({keys})
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


class AlterTableAction(ABC):
    """The base class for all actions when changing table-like objects.

    Table-like objects are Tables and TableStore.
    See {AlterTableLikeObject}.
    """

    @abstractmethod
    def to_yql(self) -> str:
        """Convert to YQL."""

        pass

    @abstractmethod
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

    def set_tiering(self, tiering_rule: str) -> AlterTableLikeObject:
        """Set a tiering policy.

        The method is similar to calling {AlterTableLikeObject.action} with a {SetSetting} instance.

        Args:
            tiering_rule: Name of a TIERING_RULE object.

        Returns:
            self."""

        return self(SetSetting('TIERING', f'"{tiering_rule}"'))

    def reset_tiering(self) -> AlterTableLikeObject:
        """Remove a tiering policy.

        The method is similar to calling {AlterTableLikeObject.action} with a {SetSetting} instance.

        Returns:
            self."""

        return self(ResetSetting('TIERING'))

    def set_ttl(self, interval: str, column: str) -> AlterTableLikeObject:
        """Set TTL for rows.

        The method is similar to calling {AlterTableLikeObject.action} with a {SetSetting} instance.

        Args:
            tiering_rule: Name of a TIERING_RULE object.

        Returns:
            self."""

        return self(SetSetting('TTL', f'Interval("{interval}") ON `{column}`'))

    @override
    def params(self) -> Dict[str, str]:
        return {self._type(): self._name, 'actions': ', '.join([a.title() for a in self._actions])}

    @override
    def title(self):
        return f'Alter {self._type()}'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        actions = ', '.join([a.to_yql() for a in self._actions])
        return f'ALTER {self._type().upper()} `{ScenarioTestHelper(ctx).get_full_path(self._name)}` {actions}'

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
