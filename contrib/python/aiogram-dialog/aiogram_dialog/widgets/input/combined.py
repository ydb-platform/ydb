from collections.abc import Callable
from typing import Any

from aiogram.dispatcher.event.handler import FilterObject
from aiogram.types import Message

from aiogram_dialog.api.protocols import (
    DialogManager,
    DialogProtocol,
)
from .base import BaseInput


class CombinedInput(BaseInput):
    def __init__(
            self,
            *inputs: BaseInput,
            filter: Callable[..., Any] | None = None,
    ):
        super().__init__()
        self.inputs = inputs
        self.filters = []
        if filter is not None:
            self.filters.append(FilterObject(filter))

    async def process_message(
            self,
            message: Message,
            dialog: DialogProtocol,
            manager: DialogManager,
    ) -> bool:
        for handler_filter in self.filters:
            if not await handler_filter.call(
                    manager.event, **manager.middleware_data,
            ):
                return False
        for input_widget in self.inputs:
            if await input_widget.process_message(message, dialog, manager):
                return True
        return False
