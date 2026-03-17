from collections.abc import Callable, Hashable
from typing import Any, TypeVar

from magic_filter import MagicFilter

from aiogram_dialog.api.protocols.manager import DialogManager

T = TypeVar("T")
Selector = Callable[[dict, T, DialogManager], Hashable]


def new_case_field(fieldname: str) -> Selector[T]:
    def case_field(
        data: dict, widget: T, manager: DialogManager,
    ) -> Hashable:
        return data.get(fieldname)

    return case_field


def new_magic_selector(f: MagicFilter) -> Selector[T]:
    def when_magic(
        data: dict, widget: T, manager: DialogManager,
    ) -> Any:
        return f.resolve(data)

    return when_magic
