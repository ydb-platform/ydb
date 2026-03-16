from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from aiogram_dialog.api.entities import ChatEvent
from aiogram_dialog.api.internal import Widget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.utils import add_exception_note
from aiogram_dialog.widgets.widget_event import (
    WidgetEventProcessor,
    ensure_event_processor,
)
from .action import Actionable
from .managed import ManagedWidget


class Scroll(Widget, Protocol):
    @abstractmethod
    async def get_page_count(self, data: dict, manager: DialogManager) -> int:
        raise NotImplementedError

    @abstractmethod
    async def get_page(self, manager: DialogManager) -> int:
        raise NotImplementedError

    @abstractmethod
    async def set_page(
            self, event: ChatEvent, page: int, manager: DialogManager,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def managed(self, manager: DialogManager) -> "ManagedScroll":
        raise NotImplementedError


class ManagedScroll(ManagedWidget[Scroll]):
    @add_exception_note
    async def get_page_count(self, data: dict) -> int:
        return await self.widget.get_page_count(data, self.manager)

    async def get_page(self) -> int:
        return await self.widget.get_page(self.manager)

    async def set_page(self, page: int) -> None:
        return await self.widget.set_page(
            self.manager.event, page, self.manager,
        )


OnPageChanged = Callable[
    [ChatEvent, ManagedScroll, DialogManager],
    Awaitable,
]
OnPageChangedVariants = OnPageChanged | WidgetEventProcessor | None


class BaseScroll(Actionable, Scroll, ABC):
    def __init__(
            self,
            id: str,
            on_page_changed: OnPageChangedVariants = None,
    ):
        super().__init__(id=id)
        self.on_page_changed = ensure_event_processor(on_page_changed)

    async def get_page(self, manager: DialogManager) -> int:
        return self.get_widget_data(manager, 0)

    async def set_page(
            self, event: ChatEvent, page: int, manager: DialogManager,
    ) -> None:
        self.set_widget_data(manager, page)
        await self.on_page_changed.process_event(
            event,
            self.managed(manager),
            manager,
        )

    def managed(self, manager: DialogManager) -> ManagedScroll:
        return ManagedScroll(self, manager)


def sync_scroll(
    scroll_id: str | Sequence[str],
    on_page_chaged: OnPageChanged | None = None,
) -> OnPageChanged:
    async def sync_scroll_on_page_changed(
        event: ChatEvent,
        widget: ManagedScroll,
        dialog_manager: DialogManager,
    ) -> None:
        if on_page_chaged is not None:
            await on_page_chaged(event, widget, dialog_manager)
        page = await widget.get_page()
        scroll_ids = scroll_id
        if isinstance(scroll_id, str):
            scroll_ids = (scroll_id, )
        for id_ in scroll_ids:
            other_scroll: ManagedScroll = dialog_manager.find(id_)
            await other_scroll.set_page(page=page)
    return sync_scroll_on_page_changed
