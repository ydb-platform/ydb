from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from magic_filter import MagicFilter

from aiogram_dialog.api.protocols import DialogManager


class Predicate(Protocol):
    @abstractmethod
    def __call__(
            self,
            data: dict,
            widget: Whenable,
            dialog_manager: DialogManager,
            /,
    ) -> bool:
        """
        Check if widget should be shown.

        :param data: Data received from getter
        :param widget: Widget we are working with
        :param dialog_manager: Dialog manager to access current context
        :return: ``True`` if widget has to be shown, ``False`` otherwise
        """
        raise NotImplementedError


WhenCondition = str | MagicFilter | Predicate | None


def new_when_field(fieldname: str) -> Predicate:
    def when_field(
            data: dict, widget: Whenable, manager: DialogManager,
    ) -> bool:
        return bool(data.get(fieldname))

    return when_field


def new_when_magic(f: MagicFilter) -> Predicate:
    def when_magic(
            data: dict, widget: Whenable, manager: DialogManager,
    ) -> bool:
        return f.resolve(data)

    return when_magic


def true_condition(data: dict, widget: Whenable, manager: DialogManager):
    return True


class Whenable:
    def __init__(self, when: WhenCondition = None):
        self.condition: Predicate
        if when is None:
            self.condition = true_condition
        elif isinstance(when, str):
            self.condition = new_when_field(when)
        elif isinstance(when, MagicFilter):
            self.condition = new_when_magic(when)
        else:
            self.condition = when

    def is_(self, data: dict, manager: DialogManager):
        return self.condition(data, self, manager)
