from collections.abc import Callable, Sequence
from typing import Any

from aiogram.types import CallbackQuery

from aiogram_dialog.api.entities import ChatEvent
from aiogram_dialog.api.internal import RawKeyboard, Widget
from aiogram_dialog.api.protocols import (
    DialogManager,
    DialogProtocol,
)
from aiogram_dialog.manager.sub_manager import SubManager
from aiogram_dialog.widgets.common import (
    BaseScroll,
    ManagedScroll,
    ManagedWidget,
    OnPageChangedVariants,
    WhenCondition,
)
from aiogram_dialog.widgets.common.items import (
    ItemsGetterVariant,
    get_items_getter,
)
from aiogram_dialog.widgets.widget_event import ensure_event_processor
from .base import Keyboard

ItemIdGetter = Callable[[Any], str | int]


class ListGroup(Keyboard, BaseScroll):
    def __init__(
            self,
            *buttons: Keyboard,
            id: str,
            item_id_getter: ItemIdGetter,
            items: ItemsGetterVariant,
            when: WhenCondition = None,
            page_size: int = 0,
            on_page_changed: OnPageChangedVariants = None,
    ):
        super().__init__(id=id, when=when)
        self.buttons = buttons
        self.item_id_getter = item_id_getter
        self.items_getter = get_items_getter(items)
        self.page_size = page_size
        self.on_page_changed = ensure_event_processor(on_page_changed)

    def _get_page_count(self, items: Sequence[object]) -> int:
        if self.page_size == 0:
            return 1
        total = len(items)
        return total // self.page_size + bool(total % self.page_size)

    async def get_page_count(self, data: dict, manager: DialogManager) -> int:
        if self.page_size == 0:
            return 1
        return self._get_page_count(self.items_getter(data))


    async def _render_keyboard(
        self, data: dict, manager: DialogManager,
    ) -> RawKeyboard:
        kbd: RawKeyboard = []
        items = self.items_getter(data)
        if self.page_size > 0:
            page = await self.get_page(manager)
            total_pages = self._get_page_count(items)
            offset = min(total_pages - 1, page) * self.page_size
            items = items[offset:offset+self.page_size]
        else:
            offset = 0

        for pos, item in enumerate(items, offset):
            kbd.extend(await self._render_item(pos, item, data, manager))
        return kbd

    async def _render_item(
            self,
            pos: int,
            item: Any,
            data: dict,
            manager: DialogManager,
    ) -> RawKeyboard:
        kbd: RawKeyboard = []
        data = {"data": data, "item": item, "pos": pos + 1, "pos0": pos}
        item_id = str(self.item_id_getter(item))
        sub_manager = SubManager(
            widget=self,
            manager=manager,
            widget_id=self.widget_id,
            item_id=item_id,
        )
        for b in self.buttons:
            b_kbd = await b.render_keyboard(data, sub_manager)
            for row in b_kbd:
                for btn in row:
                    if btn.callback_data:
                        btn.callback_data = self._item_callback_data(
                            f"{item_id}:{btn.callback_data}",
                        )
            kbd.extend(b_kbd)
        return kbd

    def find(self, widget_id: str) -> Widget | None:
        if widget_id == self.widget_id:
            return self
        for btn in self.buttons:
            widget = btn.find(widget_id)
            if widget:
                return widget
        return None

    async def _process_item_callback(
            self,
            callback: CallbackQuery,
            data: str,
            dialog: DialogProtocol,
            manager: DialogManager,
    ) -> bool:
        item_id, callback_data = data.split(":", maxsplit=1)
        callback = callback.model_copy(update={
            "data": callback_data,
        })
        sub_manager = SubManager(
            widget=self,
            manager=manager,
            widget_id=self.widget_id,
            item_id=item_id,
        )
        for b in self.buttons:
            if await b.process_callback(callback, dialog, sub_manager):
                return True
        return False

    def managed(self, manager: DialogManager) -> "ManagedListGroup":
        return ManagedListGroup(self, manager)

    async def get_page(self, manager: DialogManager) -> int:
        sub_manager = SubManager(
            widget=self,
            manager=manager,
            widget_id=self.widget_id,
            item_id="",
        )
        return self.get_widget_data(sub_manager, 0)

    async def set_page(
        self, event: ChatEvent, page: int, manager: DialogManager,
    ) -> None:
        sub_manager = SubManager(
            widget=self,
            manager=manager,
            widget_id=self.widget_id,
            item_id="",
        )
        self.set_widget_data(sub_manager, page)
        await self.on_page_changed.process_event(
            event,
            self.managed(manager),
            manager,
        )


class ManagedListGroup(ManagedScroll, ManagedWidget[ListGroup]):
    def find_for_item(self, widget_id: str, item_id: str) -> Any | None:
        """Find widget for specific item_id."""
        widget = self.widget.find(widget_id)
        if widget:
            return widget.managed(
                SubManager(
                    widget=self.widget,
                    manager=self.manager,
                    widget_id=self.widget.widget_id,
                    item_id=item_id,
                ),
            )
        return None
