from aiogram_dialog.api.internal import TextWidget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import (
    BaseScroll,
    OnPageChangedVariants,
    WhenCondition,
)
from .base import Text


class ScrollingText(Text, BaseScroll):
    def __init__(
            self,
            text: TextWidget,
            id: str,
            page_size: int = 0,
            when: WhenCondition = None,
            on_page_changed: OnPageChangedVariants = None,
    ):
        Text.__init__(self, when=when)
        BaseScroll.__init__(self, id=id, on_page_changed=on_page_changed)
        self.text = text
        self.page_size = page_size

    def _get_page_count(
            self,
            text: str,
    ) -> int:
        return len(text) // self.page_size + bool(len(text) % self.page_size)

    async def _render_contents(
            self,
            data: dict,
            manager: DialogManager,
    ) -> str:
        return await self.text.render_text(data, manager)

    async def _render_text(self, data, manager: DialogManager) -> str:
        text = await self._render_contents(data, manager)
        pages = self._get_page_count(text)
        page = await self.get_page(manager)
        last_page = pages - 1
        current_page = min(last_page, page)
        page_offset = current_page * self.page_size

        return text[page_offset: page_offset + self.page_size]

    async def get_page_count(self, data: dict, manager: DialogManager) -> int:
        text = await self._render_contents(data, manager)
        return self._get_page_count(text)
