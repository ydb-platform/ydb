from abc import abstractmethod
from collections.abc import Callable
from typing import Any

from aiogram_dialog.api.entities import ChatEvent
from aiogram_dialog.api.protocols import DialogManager


class WidgetEventProcessor:
    @abstractmethod
    async def process_event(
            self,
            event: ChatEvent,
            source: Any,
            manager: DialogManager,
            *args,
            **kwargs,
    ):
        raise NotImplementedError


class SimpleEventProcessor(WidgetEventProcessor):
    def __init__(self, callback: Callable):
        self.callback = callback

    async def process_event(
            self,
            event: ChatEvent,
            source: Any,
            manager: DialogManager,
            *args,
            **kwargs,
    ):
        if self.callback:
            await self.callback(event, source, manager, *args, **kwargs)


def ensure_event_processor(
        processor: Callable | WidgetEventProcessor | None,
) -> WidgetEventProcessor:
    if isinstance(processor, WidgetEventProcessor):
        return processor
    else:
        return SimpleEventProcessor(processor)
