from __future__ import annotations
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import (
    ScenarioTestHelper,
    TestContext,
)
from abc import abstractmethod
from typing import override, Dict


class DropObject(ScenarioTestHelper.IYqlble):
    """The base class for all requests to delete an object.

    See {ScenarioTestHelper.execute_scheme_query}.
    """

    def __init__(self, name: str) -> None:
        """Constructor.

        Args:
            name: Name (relative path) of the object to be deleted.
        """
        super().__init__(name)

    @override
    def params(self) -> Dict[str, str]:
        return {self._type(): self._name}

    @override
    def title(self):
        return f'Drop {self._type()}'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'DROP {self._type().upper()} `{ScenarioTestHelper(ctx).get_full_path(self._name)}`'

    @abstractmethod
    def _type(self) -> str:
        """Тип объекта."""

        pass


class DropTable(DropObject):
    """Class of requests to delete a table.

    See {ScenarioTestHelper.execute_scheme_query}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(DropTable('testTable'))
    """

    @override
    def _type(self) -> str:
        return 'table'


class DropTableStore(DropObject):
    """Class of requests to delete a tablestore.

    See {ScenarioTestHelper.execute_scheme_query}.

    Example:
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(DropTable('testStore'))
    """

    @override
    def _type(self) -> str:
        return 'tablestore'
