from __future__ import annotations
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper
from ydb.types import BulkUpsertColumns
from abc import abstractmethod, ABC
from ydb import PrimitiveType
from typing import override, Any, List, Dict
import random


class IColumnValueGenerator(ABC):
    """Column value generator interface.

    As a rule, there is no need to implement; it is better to use those available in this library.
    Used by the constructor of the {DataGeneratorPerColumn.__init__} class.
    See {DataGeneratorPerColumn} for examples.
    """

    @abstractmethod
    def generate_value(self, column: ScenarioTestHelper.Column) -> Any:
        """Generate column value.

        Args:
            column: Column description.

        Returns:
            Generated value. Type depends of column type.

        Example:
            @override
            def generate_value(column: ScenarioTestHelper.Column) -> Any:
                return None
        """

        pass

    def next_row(self) -> None:
        """Go to next line.

         The method is called after generating data for all columns in the current row.
        """

        pass


class ColumnValueGeneratorConst(IColumnValueGenerator):
    """Const column value generator.

    Allways generate specified value."""

    def __init__(self, value: Any) -> None:
        """Constructor.

         Args:
            value: Value to generate.
        Example:
            DataGeneratorPerColumn(
                self.schema2, 10,
                ColumnValueGeneratorDefault(init_value=10))
                    .with_column('not_level', ColumnValueGeneratorConst(42)
            )
        """

        super().__init__()
        self._value = value

    @override
    def generate_value(self, column: ScenarioTestHelper.Column) -> Any:
        return self._value


class ColumnValueGeneratorRandom(IColumnValueGenerator):
    """Random column value generator.

    Generates a random value of the appropriate type for the columns.
    Can generate a NULL value for columns that allow it."""

    def __init__(self, null_probability: float = 0.5) -> None:
        """Constructor.

         Args:
            null_probability: Probability of being NULL for columns that allow it.
        Example:
            DataGeneratorPerColumn(
                self.schema2, 10,
                ColumnValueGeneratorDefault(init_value=10))
                    .with_column('not_level', ColumnValueGeneratorRandom(0.1)
            )
        """

        super().__init__()
        self._null_propabitity = null_probability

    @override
    def generate_value(self, column: ScenarioTestHelper.Column) -> Any:
        if not column.not_null and random.random() <= self._null_propabitity:
            return None
        if column.type == PrimitiveType.Bool:
            return random.random() < 0.5
        elif column.type == PrimitiveType.Int8:
            return random.randint(-(2**7), 2**7 - 1)
        elif column.type == PrimitiveType.Int16:
            return random.randint(-(2**15), 2**15 - 1)
        elif column.type == PrimitiveType.Int32:
            return random.randint(-(2**31), 2**31 - 1)
        elif column.type == PrimitiveType.Int64:
            return random.randint(-(2**63), 2**63 - 1)
        elif column.type == PrimitiveType.Uint8:
            return random.randint(0, 2**8 - 1)
        elif column.type == PrimitiveType.Uint16:
            return random.randint(0, 2**16 - 1)
        elif column.type == PrimitiveType.Uint32:
            return random.randint(0, 2**32 - 1)
        elif column.type == PrimitiveType.Uint64:
            return random.randint(0, 2**64 - 1)
        elif column.type in {PrimitiveType.Float, PrimitiveType.Double}:
            return random.uniform(-1e6, 1e6)
        elif column.type in {PrimitiveType.String, PrimitiveType.Utf8}:
            return random.randbytes(15)
        elif column.type in {PrimitiveType.Json, PrimitiveType.JsonDocument}:
            return f'"{random.randbytes(15)}"'
        elif column.type == PrimitiveType.Timestamp:
            return random.randint(0, 1711700102000000)
        raise TypeError(f'Unsupported type {column.type}')


class ColumnValueGeneratorSequential(IColumnValueGenerator):
    """Serial column value generator.

    Generates sequential integer values and casts them to the desired type."""

    def __init__(self, init_value: int = 0, step: int = 1) -> None:
        """Constructor.

        Args:
            init_value: Initial value.
            step: Step.

        Example:
            # geneate 10, 13, 16, etc.
            ColumnValueGeneratorSequential(init_value=10, step=3)
        """

        super().__init__()
        self._value = init_value
        self._step = step

    @override
    def next_row(self) -> None:
        self._value += self._step

    @override
    def generate_value(self, column: ScenarioTestHelper.Column) -> Any:
        if column.type == PrimitiveType.Bool:
            return self._value % 2 == 0
        elif column.type in {
            PrimitiveType.Int8,
            PrimitiveType.Int16,
            PrimitiveType.Int32,
            PrimitiveType.Int64,
            PrimitiveType.Uint8,
            PrimitiveType.Uint16,
            PrimitiveType.Uint32,
            PrimitiveType.Uint64,
        }:
            return self._value
        elif column.type in {PrimitiveType.Float, PrimitiveType.Double}:
            return float(self._value)
        elif column.type in {PrimitiveType.String, PrimitiveType.Utf8}:
            return str(self._value)
        elif column.type in {PrimitiveType.Json, PrimitiveType.JsonDocument}:
            return f'"{str(self._value)}"'
        raise TypeError(f'Unsupported type {column.type}')


class ColumnValueGeneratorDefault(ColumnValueGeneratorSequential):
    """Default column value generator.

     Generates sequential integer values and casts them to the desired type for non-nullable columns
     and NULL for admitting."""

    @override
    def generate_value(self, column: ScenarioTestHelper.Column) -> Any:
        return super().generate_value(column) if column.not_null else None


class ColumnValueGeneratorData(IColumnValueGenerator):
    """Generator of column values from a dictionary.

    Returns data from the dictionary if it is specified for the corresponding column and row, otherwise NULL.

     Example:
        # For column a will generate values 1, 2, NULL, NULL...
        # For column b, generates the values 'one', 'two', NULL, NULL...
        # For column c, generate values NULL, NULL, 'value', NULL...
        ColumnValueGeneratorData([
            {'a': 1, 'b': 'one'},
            {'a': 2, 'b': 'two'},
            {'b': 'three', 'c': 'value'},
        ])
    """

    def __init__(self, data: List[Dict[str, Any]]) -> None:
        """Constructor.

        Args:
            data: Dictianary with data.
        """

        super().__init__()
        self._data = data
        self._position = 0

    @override
    def next_row(self) -> None:
        self._position += 1

    @override
    def generate_value(self, column: ScenarioTestHelper.Column) -> Any:
        if self._position >= len(self._data):
            return None
        return self._data[self._position].get(column.name)


class DataGeneratorPerColumn(ScenarioTestHelper.IDataGenerator):
    """Column-based data generator.

    Can generate different data for different columns.
    См. {ScenarioTestHelper.IDataGenerator}
    Example:
        schema = (
            ScenarioTestHelper.Schema()
            .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
            .with_column(name='level', type=PrimitiveType.Uint32)
            .with_column(name='not_level', type=PrimitiveType.Uint32)
            .with_key_columns('id')
        )

        # Will generate 10 lines, with:
        # the id column will be filled with consecutive natural numbers starting from 10;
        # the level column will be filled with NULL;
        # the not_level column will be filled with random numbers, with a 30% chance of generating NULL.
        DataGeneratorPerColumn(
            schema, 10,
            ColumnValueGeneratorDefault(init_value=10)
        ).with_column('not_level', ColumnValueGeneratorRandom(0.3)
    """

    def __init__(
        self,
        schema: ScenarioTestHelper.Schema,
        rows_count: int,
        default_column_generator: IColumnValueGenerator = None,
    ) -> None:
        """Constructor.

        Args:
            schema: Table schema for generating data.
            rows_count: Number of rows to generate.
            default_column_generator: Value generator for columns that do not have separate generators specified.
                If not specified, a ColumnValueGeneratorDefault will be created.
                See {IColumnValueGenerator}.
        """

        super().__init__()
        self._default_generator = (
            default_column_generator if default_column_generator is not None else ColumnValueGeneratorDefault()
        )
        self._column_generators: Dict[str, IColumnValueGenerator] = {}
        self._row_count = rows_count
        self._schema = schema

    @override
    def get_bulk_upsert_columns(self) -> BulkUpsertColumns:
        return self._schema.build_bulk_columns_types()

    def with_column(self, name: str, generator: IColumnValueGenerator) -> DataGeneratorPerColumn:
        """Set a value generator for the column.

        Args:
            name: Column name.
            generator: Value generator for the column. See {IColumnValueGenerator}.
        Returns:
            self."""

        assert name in {c.name for c in self._schema.columns}
        self._column_generators[name] = generator
        return self

    @override
    def EOF(self) -> bool:
        return self._row_count <= 0

    @override
    def generate_data_portion(self, rows_count: int) -> List[Dict[str, Any]]:
        count = min(rows_count, self._row_count)
        self._row_count -= count
        result = []
        for _ in range(count):
            result.append(
                {
                    c.name: self._column_generators.get(c.name, self._default_generator).generate_value(c)
                    for c in self._schema.columns
                }
            )
            for _, g in self._column_generators.items():
                g.next_row()
            self._default_generator.next_row()

        return result


class DataGeneratorConst(DataGeneratorPerColumn):
    """Constant data generator.

    Generates data from the given dictionary and stops.
    See {ScenarioTestHelper.IDataGenerator}.
    Example:
        schema = (
            ScenarioTestHelper.Schema()
            .with_column(name='a', type=PrimitiveType.Int32, not_null=True)
            .with_column(name='b', type=PrimitiveType.Utf8)
            .with_column(name='c', type=PrimitiveType.Utf8)
            .with_key_columns('a')
        )

        # Generates 3 lines, with:
        # column a will be filled with consecutive natural numbers starting from 1;
        # column b will be filled with the values 'one', 'two', NULL;
        # column c will be filled with NULL, NULL, 'value'.
        DataGeneratorConst(schema, [
            {'a': 1, 'b': 'one'},
            {'a': 2, 'b': 'two'},
            {'a': 3, 'c': 'value'},
        ])
    """

    def __init__(self, schema: ScenarioTestHelper.Schema, data: List[Dict[str, Any]]) -> None:
        """Constructor.

        Args:
            schema: Table schema for generating data.
            data: dictionary with data.
        """
        super().__init__(schema, len(data), ColumnValueGeneratorData(data))
