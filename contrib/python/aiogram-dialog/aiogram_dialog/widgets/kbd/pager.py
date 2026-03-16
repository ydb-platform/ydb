from abc import ABC
from enum import Enum
from typing import TypedDict

from aiogram.types import CallbackQuery, InlineKeyboardButton

from aiogram_dialog.api.internal import RawKeyboard, StyleWidget, TextWidget
from aiogram_dialog.api.protocols import DialogManager, DialogProtocol
from aiogram_dialog.widgets.common import ManagedScroll, Scroll, WhenCondition
from aiogram_dialog.widgets.style import EMPTY_STYLE
from aiogram_dialog.widgets.text import Const, Format
from .base import Keyboard


class PageDirection(Enum):
    NEXT = "NEXT"
    PREV = "PREV"
    FIRST = "FIRST"
    LAST = "LAST"
    IGNORE = "IGNORE"


class PagerData(TypedDict):
    data: dict
    current_page: int
    current_page1: int
    pages: int


class PagerPageData(PagerData):
    target_page: int
    target_page1: int


DEFAULT_PAGER_ID = "__pager__"

DEFAULT_LAST_BUTTON_TEXT = Const(">>")
DEFAULT_FIRST_BUTTON_TEXT = Const("<<")
DEFAULT_PREV_BUTTON_TEXT = Const("<")
DEFAULT_NEXT_BUTTON_TEXT = Const(">")
DEFAULT_CURRENT_BUTTON_TEXT = Format("{current_page1}")
DEFAULT_PAGE_TEXT = Format("{target_page1}")
DEFAULT_CURRENT_PAGE_TEXT = Format("[ {current_page1} ]")


class BasePager(Keyboard, ABC):
    def __init__(
        self,
        scroll: str | Scroll | None,
        id: str,
        when: WhenCondition = None,
    ):
        super().__init__(id=id, when=when)
        if isinstance(scroll, str):
            self._scroll_id = scroll
            self._scroll = None
        else:
            self._scroll = scroll
            self._scroll_id = None

    def _find_scroll(self, manager: DialogManager) -> ManagedScroll:
        if self._scroll:
            return self._scroll.managed(manager)
        else:
            return manager.find(self._scroll_id)

    async def _process_item_callback(
        self,
        callback: CallbackQuery,
        data: str,
        dialog: DialogProtocol,
        manager: DialogManager,
    ) -> bool:
        scroll = self._find_scroll(manager)
        await scroll.set_page(int(data))
        return True


class SwitchPage(BasePager):
    def __init__(
        self,
        page: int | PageDirection,
        scroll: str | Scroll | None,
        id: str,
        text: TextWidget,
        when: WhenCondition = None,
        style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(id=id, scroll=scroll, when=when)
        self.page = page
        self.text = text
        self.style = style

    async def _get_target_page(
        self,
        current_page: int,
        pages: int,
    ) -> int:
        if isinstance(self.page, int):
            return self.page
        if self.page is PageDirection.FIRST:
            return 0

        last_page = pages - 1
        if self.page is PageDirection.PREV:
            return max(0, current_page - 1)
        elif self.page is PageDirection.NEXT:
            return min(last_page, current_page + 1)
        elif self.page is PageDirection.LAST:
            return max(0, last_page)
        else:
            return min(last_page, current_page)

    async def _prepare_data(
        self,
        data: dict,
        target_page: int,
        current_page: int,
        pages: int,
    ) -> PagerPageData:
        return {
            "data": data,
            "target_page": target_page,
            "target_page1": target_page + 1,
            "current_page": current_page,
            "current_page1": current_page + 1,
            "pages": pages,
        }

    async def render_keyboard(
        self,
        data: dict,
        manager: DialogManager,
    ) -> RawKeyboard:
        scroll = self._find_scroll(manager)
        pages = await scroll.get_page_count(data)
        current_page = await scroll.get_page()
        target_page = await self._get_target_page(current_page, pages)
        button_data = await self._prepare_data(
            data=data,
            target_page=target_page,
            current_page=current_page,
            pages=pages,
        )
        return await super().render_keyboard(button_data, manager)

    async def _render_keyboard(
        self,
        data: PagerPageData,
        manager: DialogManager,
    ) -> RawKeyboard:
        return [
            [
                InlineKeyboardButton(
                    text=await self.text.render_text(data, manager),
                    callback_data=self._item_callback_data(
                        data["target_page"],
                    ),
                    style=await self.style.render_style(data, manager),
                    icon_custom_emoji_id=await self.style.render_emoji(
                        data, manager,
                    ),
                ),
            ],
        ]


class LastPage(SwitchPage):
    def __init__(
        self,
        scroll: str | Scroll | None,
        id: str = DEFAULT_PAGER_ID,
        text: TextWidget = DEFAULT_LAST_BUTTON_TEXT,
        when: WhenCondition = None,
        style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            id=id,
            text=text,
            page=PageDirection.LAST,
            scroll=scroll,
            when=when,
            style=style,
        )


class NextPage(SwitchPage):
    def __init__(
        self,
        scroll: str | Scroll | None,
        id: str = DEFAULT_PAGER_ID,
        text: TextWidget = DEFAULT_NEXT_BUTTON_TEXT,
        when: WhenCondition = None,
        style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            id=id,
            text=text,
            page=PageDirection.NEXT,
            scroll=scroll,
            when=when,
            style=style,
        )


class PrevPage(SwitchPage):
    def __init__(
        self,
        scroll: str | Scroll | None,
        id: str = DEFAULT_PAGER_ID,
        text: TextWidget = DEFAULT_PREV_BUTTON_TEXT,
        when: WhenCondition = None,
        style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            id=id,
            text=text,
            page=PageDirection.PREV,
            scroll=scroll,
            when=when,
            style=style,
        )


class FirstPage(SwitchPage):
    def __init__(
        self,
        scroll: str | Scroll | None,
        id: str = DEFAULT_PAGER_ID,
        text: TextWidget = DEFAULT_FIRST_BUTTON_TEXT,
        when: WhenCondition = None,
        style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            id=id,
            text=text,
            page=PageDirection.FIRST,
            scroll=scroll,
            when=when,
            style=style,
        )


class CurrentPage(SwitchPage):
    def __init__(
        self,
        scroll: str | Scroll | None,
        id: str = DEFAULT_PAGER_ID,
        text: TextWidget = DEFAULT_CURRENT_BUTTON_TEXT,
        when: WhenCondition = None,
        style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            id=id,
            text=text,
            page=PageDirection.IGNORE,
            scroll=scroll,
            when=when,
            style=style,
        )


class NumberedPager(BasePager):
    def __init__(
        self,
        scroll: str | Scroll | None,
        id: str = DEFAULT_PAGER_ID,
        page_text: TextWidget = DEFAULT_PAGE_TEXT,
        current_page_text: TextWidget = DEFAULT_CURRENT_PAGE_TEXT,
        when: WhenCondition = None,
        length: int | None = None,
        style: StyleWidget = EMPTY_STYLE,
        current_page_style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(id=id, scroll=scroll, when=when)
        self.page_text = page_text
        self.current_page_text = current_page_text
        self.length = length
        self.style = style
        self.current_page_style = current_page_style

    async def _prepare_data(
        self,
        data: dict,
        current_page: int,
        pages: int,
    ) -> PagerData:
        return {
            "data": data,
            "current_page": current_page,
            "current_page1": current_page + 1,
            "pages": pages,
        }

    async def _prepare_page_data(
        self,
        data: dict,
        target_page: int,
    ) -> PagerData:
        data = data.copy()
        data["target_page"] = target_page
        data["target_page1"] = target_page + 1
        return data

    async def render_keyboard(
        self,
        data: dict,
        manager: DialogManager,
    ) -> RawKeyboard:
        scroll = self._find_scroll(manager)
        pages = await scroll.get_page_count(data)
        current_page = await scroll.get_page()
        pager_data = await self._prepare_data(
            data=data,
            current_page=current_page,
            pages=pages,
        )
        return await super().render_keyboard(pager_data, manager)

    async def _render_keyboard(
        self,
        data: PagerData,
        manager: DialogManager,
    ) -> RawKeyboard:
        buttons = []
        pages = data["pages"]
        current_page = data["current_page"]
        final_buttons = []

        for target_page in range(pages):
            if self.length is not None and len(buttons) >= self.length:
                final_buttons.append(buttons)
                buttons = []
            button_data = await self._prepare_page_data(
                data=data,
                target_page=target_page,
            )
            if target_page == current_page:
                text_widget = self.current_page_text
                style = self.current_page_style
            else:
                text_widget = self.page_text
                style = self.style
            text = await text_widget.render_text(button_data, manager)
            buttons.append(
                InlineKeyboardButton(
                    text=text,
                    callback_data=self._item_callback_data(target_page),
                    style=await style.render_style(button_data, manager),
                    icon_custom_emoji_id=await style.render_emoji(
                        button_data, manager,
                    ),
                ),
            )
        if buttons:
            final_buttons.append(buttons)
        return final_buttons
