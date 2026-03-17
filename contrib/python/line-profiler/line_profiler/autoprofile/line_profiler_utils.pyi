from typing import Callable
from typing import Type
from types import ModuleType


def add_imported_function_or_module(
        self, item: Callable | Type | ModuleType) -> None:
    ...
