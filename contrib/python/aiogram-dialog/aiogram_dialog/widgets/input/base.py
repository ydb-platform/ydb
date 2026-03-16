from abc import abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from aiogram import F
from aiogram.dispatcher.event.handler import FilterObject
from aiogram.types import ContentType, Message

from aiogram_dialog.api.internal import InputWidget
from aiogram_dialog.api.protocols import (
    DialogManager,
    DialogProtocol,
)
from aiogram_dialog.widgets.common import Actionable
from aiogram_dialog.widgets.widget_event import (
    WidgetEventProcessor,
    ensure_event_processor,
)

MessageHandlerFunc = Callable[
    [Message, "MessageInput", DialogManager],
    Awaitable,
]


class BaseInput(Actionable, InputWidget):
    @abstractmethod
    async def process_message(
            self, message: Message, dialog: DialogProtocol,
            manager: DialogManager,
    ) -> bool:
        raise NotImplementedError


class MessageInput(BaseInput):
    def __init__(
            self,
            func: MessageHandlerFunc | WidgetEventProcessor | None,
            content_types: Sequence[str] | str = ContentType.ANY,
            filter: Callable[..., Any] | None = None,
            id: str | None = None,
    ):
        super().__init__(id=id)
        self.func = ensure_event_processor(func)

        filters = []
        if isinstance(content_types, str):
            if content_types != ContentType.ANY:
                filters.append(FilterObject(F.content_type == content_types))
        elif ContentType.ANY not in content_types:
            filters.append(FilterObject(F.content_type.in_(content_types)))
        if filter is not None:
            filters.append(FilterObject(filter))
        self.filters = filters

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
        await self.func.process_event(message, self, manager)
        return True
