from collections.abc import Sequence
from typing import Any

from aiogram_dialog.api.internal import TextWidget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import (
    BaseScroll,
    OnPageChangedVariants,
    WhenCondition,
)
from aiogram_dialog.widgets.common.items import (
    ItemsGetterVariant,
    get_items_getter,
)
from .base import Text


class List(Text, BaseScroll):
    def __init__(
            self,
            field: TextWidget,
            items: ItemsGetterVariant,
            sep: str = "\n",
            when: WhenCondition = None,
            id: str | None = None,
            page_size: int | None = None,
            on_page_changed: OnPageChangedVariants = None,
    ):
        Text.__init__(self, when=when)
        BaseScroll.__init__(self, id=id, on_page_changed=on_page_changed)
        self.field = field
        self.sep = sep
        self.items_getter = get_items_getter(items)
        self.page_size = page_size

    async def _render_text(
            self, data: dict, manager: DialogManager,
    ) -> str:
        items = self.items_getter(data)
        pages = self._get_page_count(items)
        if self.page_size is None:
            current_page = 0
            start = 0
        else:
            last_page = pages - 1
            current_page = min(last_page, await self.get_page(manager))
            start = current_page * self.page_size
            items = items[start:start + self.page_size]

        texts = [
            await self.field.render_text(
                {
                    "current_page": current_page,
                    "current_page1": current_page + 1,
                    "pages": pages,
                    "data": data,
                    "item": item,
                    "pos": pos + 1,
                    "pos0": pos,
                },
                manager,
            )
            for pos, item in enumerate(items, start)
        ]
        return self.sep.join(filter(None, texts))

    async def get_page_count(self, data: dict, manager: DialogManager) -> int:
        items = self.items_getter(data)
        return self._get_page_count(items)

    def _get_page_count(self, items: Sequence[Any]) -> int:
        if not items:
            return 0
        if self.page_size is None:
            return 1
        return len(items) // self.page_size + bool(len(items) % self.page_size)
