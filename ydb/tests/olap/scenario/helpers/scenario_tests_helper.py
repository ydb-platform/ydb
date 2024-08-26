from __future__ import annotations
import os
import allure
import allure_commons
import pytest
import ydb
import json
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from abc import abstractmethod, ABC
from typing import Set, List, Dict, Any, Callable


class TestContext:
    """Scenario test execution context.

    The class is created by the test execution system and used by {ScenarioTestHelper}."""

    def __init__(self, suite_name: str, test_name: str, scenario: Callable) -> None:
        """Constructor.

        Args:
            suite_name: Test suite name (scenario suite class name).
            test_name: Test name (scenario function name minus the scenario_ prefix).
            scenario: Scenario function.
        """

        self.executable = scenario
        self.suite = suite_name
        self.test = test_name


class ScenarioTestHelper:
    """The main script test helper.

    Contains functions for accessing the cluster under test and auxiliary interfaces.

    Example:
        schema = (
            ScenarioTestHelper.Schema()
            .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
            .with_column(name='level', type=PrimitiveType.Uint32)
            .with_key_columns('id')
        )

        sth = ScenarioTestHelper(ctx)
        table_name = 'testTable'
        sth.execute_scheme_query(CreateTable(table_name).with_schema(schema))
        sth.bulk_upsert_data(
            table_name,
            schema,
            [
                {'id': 1, 'level': 3},
                {'id': 2, 'level': None},
            ],
            comment="with ok scheme",
        )
        assert sth.get_table_rows_count(table_name) == 2
        sth.bulk_upsert(table_name, dg.DataGeneratorPerColumn(schema, 100), comment="100 sequetial ids")
        sth.execute_scheme_query(DropTable(table_name))
    """

    class Column:
        """A class that describes a table column."""

        def __init__(self, name: str, type: ydb.PrimitiveType, not_null: bool = False) -> None:
            """Constructor.

            Args:
                name: Column name.
                type: Column type.
                not_null: Whether the entry in the column can be NULL.
            """

            self._name = name
            self._type = type
            self._not_null = not_null

        def to_yql(self) -> str:
            """Convert to YQL"""

            return f'{self._name} {self._type}{" NOT NULL" if self._not_null else ""}'

        @property
        def bulk_upsert_type(self) -> ydb.OptionalType | ydb.PrimitiveType:
            """Column type for BulKUpsert."""

            if self._not_null:
                return self._type
            return ydb.OptionalType(self._type)

        @property
        def name(self) -> str:
            """Column name."""

            return self._name

        @property
        def type(self) -> ydb.PrimitiveType:
            """Column type."""

            return self._type

        @property
        def not_null(self) -> bool:
            """Whether the entry in the column can be NULL."""

            return self._not_null

    class Schema:
        """Class describing the table schema.

        Example:
            schema = (
                ScenarioTestHelper.Schema()
                .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
                .with_column(name='level', type=PrimitiveType.Uint32)
                .with_key_columns('id')
            )
        """

        def __init__(self) -> None:
            """Constructor."""

            self.columns = []
            self.key_columns = []

        def with_column(self, *vargs, **kargs) -> ScenarioTestHelper.Schema:
            """Add a column.

            The method arguments are the same as {ScenarioTestHelper.Column.__init__}.

            Returns:
                self.
            """

            self.columns.append(ScenarioTestHelper.Column(*vargs, **kargs))
            return self

        def with_key_columns(self, *vargs: str) -> ScenarioTestHelper.Schema:
            """Add columns to the PRIMARY KEY of table.

            Args:
                *vargs: strings, column names.

            Returns:
                self."""

            self.key_columns += vargs
            return self

        def build_bulk_columns_types(self) -> ydb.BulkUpsertColumns:
            """Convert to ydb.BulkUpsertColumns"""

            result = ydb.BulkUpsertColumns()
            for c in self.columns:
                result.add_column(c.name, c.bulk_upsert_type)
            return result

    class IYqlble(ABC):
        """An interface for entities that can be represented as a YQL query.

        Such as operations to create and modify tables, etc.
        See {ydb.tests.olap.scenario.helpers.table_helper} and {ydb.tests.olap.scenario.helpers.drop_helper} for examples."""

        def __init__(self, name: str) -> None:
            self._name = name

        @abstractmethod
        def to_yql(self, ctx: TestContext) -> str:
            """Generate YQL query.

            Args:
                ctx: Test execution context."""

            pass

        @abstractmethod
        def title(self) -> str:
            """Text to display in Allure headers."""

            pass

        @abstractmethod
        def params(self) -> Dict[str, str]:
            """Options for display in Allure."""

            pass

    class IDataGenerator(ABC):
        """Interface for data generation. see {ydb.tests.olap.scenario.helpers.data_generators}.

         For example for {ScenarioTestHelper.bulk_upsert}."""

        @abstractmethod
        def get_bulk_upsert_columns(self) -> ydb.BulkUpsertColumns:
            """Data schema in ydb.BulkUpsertColumns format."""

            pass

        @abstractmethod
        def generate_data_portion(self, rows_count: int) -> List[Dict[str, Any]]:
            """Generate a piece of data

            Args:
                rows_count: Number of rows requested. The generator has the right to return less.
            """
            pass

        @abstractmethod
        def EOF(self) -> bool:
            """Checks that the generation limit has been reached.

            Returns:
                True if the limit is reached, otherwise False."""

            pass

    def __init__(self, context: TestContext) -> None:
        """Constructor.

        Args:
            context: The execution context of the scenario test.
        """

        self.test_context = context

    def get_full_path(self, path: str) -> str:
        """Construct the full path to the object in the test database, taking into account the execution context.

        Args:
            path - relative path.

        Returns:
            Full path.
        """

        def _add_not_empty(p: str, dir: str):
            if dir is None or dir == '':
                return p
            return os.path.join(p, dir)

        result = os.path.join('/', YdbCluster.ydb_database, YdbCluster.tables_path)
        if self.test_context is not None:
            result = _add_not_empty(result, self.test_context.suite)
            result = _add_not_empty(result, self.test_context.test)
        result = _add_not_empty(result, path)
        return result

    @staticmethod
    def _run_with_expected_status(operation: callable, expected_status: ydb.StatusCode | Set[ydb.StatusCode]):
        if isinstance(expected_status, ydb.StatusCode):
            expected_status = {expected_status}
        try:
            result = operation()
            if ydb.StatusCode.SUCCESS not in expected_status:
                pytest.fail(
                    f'Unexpected status: must be in {repr(expected_status)}, but get {repr(ydb.StatusCode.SUCCESS)}'
                )
            return result
        except ydb.issues.Error as e:
            allure.attach(f'{repr(e.status)}: {e}', 'request status', allure.attachment_type.TEXT)
            if e.status not in expected_status:
                pytest.fail(f'Unexpected status: must be in {repr(expected_status)}, but get {repr(e)}')
            return None

    def _bulk_upsert_impl(
        self, tablename: str, data_generator: ScenarioTestHelper.IDataGenerator, expected_status: ydb.StatusCode | Set[ydb.StatusCode]
    ):
        fullpath = self.get_full_path(tablename)

        def _upsert():
            data = data_generator.generate_data_portion(1000)
            allure.attach(repr(data), 'data', allure.attachment_type.TEXT)
            YdbCluster.get_ydb_driver().table_client.bulk_upsert(
                fullpath, data, data_generator.get_bulk_upsert_columns()
            )

        while not data_generator.EOF():
            self._run_with_expected_status(
                lambda: _upsert(),
                expected_status,
            )

    @staticmethod
    def check_if_ydb_alive(timeout: float = 10) -> bool:
        """Check that the YDB being tested is alive.

        Args:
            timeout - waiting time for a database response in seconds.

        Returns:
            True - alive, False - dead.

        Example:
            sth = ScenarioTestHelper(ctx)
            assert sth.check_if_ydb_alive()
        """

        return YdbCluster.check_if_ydb_alive(timeout)

    def execute_scheme_query(
        self,
        yqlble: ScenarioTestHelper.IYqlble,
        expected_status: ydb.StatusCode | Set[ydb.StatusCode] = ydb.StatusCode.SUCCESS,
        comment: str = '',
    ) -> None:
        """Run a schema query on the database under test.

        Args:
            yqlble: Query generator.
            expected_status: Expected status or set of database response statuses. If the response status is not in the expected set, an exception is thrown.
            comment: Comment to display in the Allure header.

        Example:
            schema = (
                ScenarioTestHelper.Schema()
                .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
                .with_column(name='level', type=PrimitiveType.Uint32)
                .with_key_columns('id')
            )

            sth = ScenarioTestHelper(ctx)
            table_name = 'testTable'
            sth.execute_scheme_query(CreateTable(table_name).with_schema(schema), comment='Create first table')
        """

        with allure_commons._allure.StepContext(
            f'{yqlble.title()} {comment}', dict(yqlble.params(), expected_status=repr(expected_status))
        ):
            yql = yqlble.to_yql(self.test_context)
            allure.attach(yql, 'request', allure.attachment_type.TEXT)
            self._run_with_expected_status(
                lambda: YdbCluster.get_ydb_driver().table_client.session().create().execute_scheme(yql), expected_status
            )

    @classmethod
    @allure.step('Execute scan query')
    def execute_scan_query(
        cls, yql: str, expected_status: ydb.StatusCode | Set[ydb.StatusCode] = ydb.StatusCode.SUCCESS
    ):
        """Run a scanning query on the tested database.

        Args:
            yql: Query text.
            expected_status: Expected status or set of database response statuses. If the response status is not in the expected set, an exception is thrown.

        Returns:
            ydb.ResultSet with the result of the request.

        Example:
            tablename = 'testTable'
            sth = ScenarioTestHelper(ctx)
            result_set = sth.execute_scan_query(f'SELECT count(*) FROM `{sth.get_full_path(tablename)}`')
            print(f'There are {result_set.result_set.rows[0][0]} rows in talbe {tablename}')
        """

        allure.attach(yql, 'request', allure.attachment_type.TEXT)
        it = cls._run_with_expected_status(
            lambda: YdbCluster.get_ydb_driver().table_client.scan_query(yql), expected_status
        )
        rows = None
        ret = None
        for result_set in it:
            if ret is None:
                ret = result_set
                rows = result_set.result_set.rows
            else:
                rows += result_set.result_set.rows
        allure.attach(json.dumps(rows), 'result', allure.attachment_type.JSON)
        return ret

    def drop_if_exist(self, names: List[str], operation) -> None:
        """Erase entities in the tested database, if it exists.

        Args:
            names: list of names (relative paths) of entities to delete.
            operation: class - inheritance from IYqlble for deleting corresponding entities.
                See {ydb.tests.olap.scenario.helpers.drop_helper}.
        """

        for name in names:
            self.execute_scheme_query(operation(name), {ydb.StatusCode.SUCCESS, ydb.StatusCode.SCHEME_ERROR})

    def bulk_upsert_data(
        self,
        tablename: str,
        schema: ScenarioTestHelper.Schema,
        data: List[Dict[str, Any]],
        expected_status: ydb.StatusCode | Set[ydb.StatusCode] = ydb.StatusCode.SUCCESS,
        comment: str = '',
    ) -> None:
        """Perform stream data insertion into the tested database.

        Args:
            tablename: Name (relative path) of the table to insert.
            schema: Table schema.
            data: Array of data to insert. The data schema must match the table schema.
            expected_status: Expected status or set of database response statuses. If the response status is not in the expected set, an exception is thrown.
            comment: Comment to display in the Allure header.

        Example:
            schema = (
                ScenarioTestHelper.Schema()
                .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
                .with_column(name='level', type=PrimitiveType.Uint32)
                .with_key_columns('id')
            )

            table_name = 'testTable'
            sth = ScenarioTestHelper(ctx)
            sth.execute_scheme_query(CreateTable(table_name).with_schema(schema))
            sth.bulk_upsert_data(
                table_name,
                self.schema1,
                [
                    {'id': 1, 'level': 3},
                    {'id': 2, 'level': None},
                ],
                comment="with ok scheme",
            )
            assert sth.get_table_rows_count(table_name) == 2

        Example:
            schema1 = (
                ScenarioTestHelper.Schema()
                .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
                .with_column(name='level', type=PrimitiveType.Uint32)
                .with_key_columns('id')
            )
            schema2 = (
                ScenarioTestHelper.Schema()
                .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
                .with_column(name='not_level', type=PrimitiveType.Uint32)
                .with_key_columns('id')
            )

            table_name = 'testTable'
            sth = ScenarioTestHelper(ctx)
            sth.execute_scheme_query(CreateTable(table_name).with_schema(schema1))
            sth.bulk_upsert_data(
                table_name,
                self.schema2,
                [
                    {'id': 3, 'not_level': 3},
                ],
                StatusCode.SCHEME_ERROR,
                comment='with wrong scheme',
            )
        """

        with allure_commons._allure.StepContext(
            f'Bulk upsert {comment}', {'table': tablename, 'expected_status': repr(expected_status)}
        ):
            allure.attach(repr(data), 'data', allure.attachment_type.TEXT)
            from ydb.tests.olap.scenario.helpers.data_generators import DataGeneratorConst

            self._bulk_upsert_impl(tablename, DataGeneratorConst(schema, data), expected_status)

    def bulk_upsert(
        self,
        tablename: str,
        data_generator: ScenarioTestHelper.IDataGenerator,
        expected_status: ydb.StatusCode | Set[ydb.StatusCode] = ydb.StatusCode.SUCCESS,
        comment: str = '',
    ) -> None:
        """Perform stream data insertion into the tested database.

        Args:
            tablename: Name (relative path) of the table to insert.
            data_generator: Data generator for insertion.
            expected_status: Expected status or set of database response statuses. If the response status is not in the expected set, an exception is thrown.
            comment: Comment to display in the Allure header.

        Example:
            schema = (
                ScenarioTestHelper.Schema()
                .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
                .with_column(name='level', type=PrimitiveType.Uint32)
                .with_key_columns('id')
            )

            table_name = 'testTable'
            sth = ScenarioTestHelper(ctx)
            sth.execute_scheme_query(CreateTable(table_name).with_schema(schema))
            sth.bulk_upsert(table_name, dg.DataGeneratorPerColumn(schema, 100), comment="100 sequetial ids")
            sth.bulk_upsert(
                table_name,
                dg.DataGeneratorPerColumn(schema, 100, dg.ColumnValueGeneratorRandom()),
                comment="100 random rows"
            )
        """

        with allure_commons._allure.StepContext(
            f'Bulk upsert {comment}',
            {
                'table': tablename,
                'expected_status': repr(expected_status),
            },
        ):
            self._bulk_upsert_impl(tablename, data_generator, expected_status)

    def get_table_rows_count(self, tablename: str, comment: str = '') -> int:
        """Get the number of rows in the table.

        Args:
            tablename: Name (relative path) of the table.
            comment: Comment to display in the Allure header.

        Returns:
            Number of lines.

        Example:
            table_name = 'testTable'
            sth = ScenarioTestHelper(ctx)
            assert sth.get_table_rows_count(table_name) == 10
        """

        with allure_commons._allure.StepContext(
            f'Get table rows count {comment}',
            {
                'table': tablename,
            },
        ):
            result_set = self.execute_scan_query(f'SELECT count(*) FROM `{self.get_full_path(tablename)}`')
            return result_set.result_set.rows[0][0]
    
    @allure.step('Describe table {path}')
    def describe_table(self, path: str, settings: ydb.DescribeTableSettings = None) -> List[ydb.SchemeEntry]:
        """Get table description.

        Args:
            path: Relative path to a table.
            settings: DescribeTableSettings.

        Returns:
            TableSchemeEntry object.
        """

        return self._run_with_expected_status(
            lambda: YdbCluster.get_ydb_driver().table_client.session().create().describe_table(self.get_full_path(path), settings), ydb.StatusCode.SUCCESS
        )

    @allure.step('List path {path}')
    def list_path(self, path: str) -> List[ydb.SchemeEntry]:
        """Recursively describe the path in the database under test.

        If the path is a directory or TableStore, then all subpaths are included in the description.

        Args:
            path: Relative path for the description.

        Returns:
            A ydb.SchemeEntry list, where each entry corresponds to one path, starting at the leaves and ending at the root of the path tree.
            If the path does not exist, an empty list is returned.
        """

        root_path = self.get_full_path('')
        result = []
        self_descr = YdbCluster._describe_path_impl(os.path.join(root_path, path))
        if self_descr is not None:
            self_descr.name = path
            if self_descr.is_directory():
                result = YdbCluster._list_directory_impl(root_path, path)
            result.append(self_descr)
        allure.attach('\n'.join([f'{e.name}: {repr(e.type)}' for e in result]), 'result', allure.attachment_type.TEXT)
        return result

    @allure.step('Remove path {path}')
    def remove_path(self, path: str) -> None:
        """Recursively delete a path in the tested database.

        If the path is a directory or TableStore, then all nested paths are removed.
        If the path does not exist, nothing happens.

        Args:
            path: Relative path to delete.

        Example:
            ScenarioTestHelper(ctx).remove_path('testDir')
        """

        import ydb.tests.olap.scenario.helpers.drop_helper as dh

        root_path = self.get_full_path('')
        for e in self.list_path(path):
            if e.is_any_table():
                self.execute_scheme_query(dh.DropTable(e.name))
            elif e.is_column_store():
                self.execute_scheme_query(dh.DropTableStore(e.name))
            elif e.is_directory():
                self._run_with_expected_status(
                    lambda: YdbCluster.get_ydb_driver().scheme_client.remove_directory(os.path.join(root_path, e.name)),
                    ydb.StatusCode.SUCCESS,
                )
            else:
                pytest.fail(f'Cannot remove type {repr(e.type)} for path {os.path.join(root_path, e.name)}')
