from abc import ABC, abstractmethod
from typing import Iterable, Tuple, Any

import alembic


class EnumLifecycleOp(alembic.operations.ops.MigrateOperation, ABC):
    def __init__(
        self,
        schema: str,
        name: str,
        enum_values: Iterable[str],
    ):
        self.schema = schema
        self.name = name
        self.enum_values = enum_values

    @property
    @abstractmethod
    def operation_name(self) -> str:
        pass

    def to_diff_tuple(self) -> Tuple[Any, ...]:
        return self.operation_name, self.name, self.schema, self.enum_values
