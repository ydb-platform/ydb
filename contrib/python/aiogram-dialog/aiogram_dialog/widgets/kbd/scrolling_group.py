
from aiogram.types import CallbackQuery, InlineKeyboardButton

from aiogram_dialog.api.internal import RawKeyboard
from aiogram_dialog.api.protocols import DialogManager, DialogProtocol
from aiogram_dialog.widgets.common import (
    BaseScroll,
    OnPageChangedVariants,
    WhenCondition,
)
from .base import Keyboard
from .group import Group


class ScrollingGroup(Group, BaseScroll):
    def __init__(
            self,
            *buttons: Keyboard,
            id: str,
            width: int | None = None,
            height: int = 0,
            when: WhenCondition = None,
            on_page_changed: OnPageChangedVariants = None,
            hide_on_single_page: bool = False,
            hide_pager: bool = False,
    ):
        Group.__init__(self, *buttons, id=id, width=width, when=when)
        BaseScroll.__init__(self, id=id, on_page_changed=on_page_changed)
        self.height = height
        self.hide_on_single_page = hide_on_single_page
        self.hide_pager = hide_pager

    def _get_page_count(
            self,
            keyboard: RawKeyboard,
    ) -> int:
        return len(keyboard) // self.height + bool(len(keyboard) % self.height)

    async def _render_contents(
            self,
            data: dict,
            manager: DialogManager,
    ) -> RawKeyboard:
        return await super()._render_keyboard(data, manager)

    async def _render_pager(
            self,
            pages: int,
            manager: DialogManager,
    ) -> RawKeyboard:
        if self.hide_pager:
            return []
        if pages == 0 or (pages == 1 and self.hide_on_single_page):
            return []

        last_page = pages - 1
        current_page = min(last_page, await self.get_page(manager))
        next_page = min(last_page, current_page + 1)
        prev_page = max(0, current_page - 1)

        return [
            [
                InlineKeyboardButton(
                    text="1", callback_data=self._item_callback_data("0"),
                ),
                InlineKeyboardButton(
                    text="<",
                    callback_data=self._item_callback_data(prev_page),
                ),
                InlineKeyboardButton(
                    text=str(current_page + 1),
                    callback_data=self._item_callback_data(current_page),
                ),
                InlineKeyboardButton(
                    text=">",
                    callback_data=self._item_callback_data(next_page),
                ),
                InlineKeyboardButton(
                    text=str(last_page + 1),
                    callback_data=self._item_callback_data(last_page),
                ),
            ],
        ]

    async def _render_page(
            self,
            page: int,
            keyboard: list[list[InlineKeyboardButton]],
    ) -> list[list[InlineKeyboardButton]]:
        pages = self._get_page_count(keyboard)
        last_page = pages - 1
        current_page = min(last_page, page)
        page_offset = current_page * self.height

        return keyboard[page_offset: page_offset + self.height]

    async def _render_keyboard(
            self,
            data: dict,
            manager: DialogManager,
    ) -> RawKeyboard:
        keyboard = await self._render_contents(data, manager)
        pages = self._get_page_count(keyboard)

        pager = await self._render_pager(pages, manager)
        page_keyboard = await self._render_page(
            page=await self.get_page(manager),
            keyboard=keyboard,
        )

        return page_keyboard + pager

    async def _process_item_callback(
            self,
            callback: CallbackQuery,
            data: str,
            dialog: DialogProtocol,
            manager: DialogManager,
    ) -> bool:
        await self.set_page(callback, int(data), manager)
        return True

    async def get_page_count(self, data: dict, manager: DialogManager) -> int:
        keyboard = await self._render_contents(data, manager)
        return self._get_page_count(keyboard=keyboard)
