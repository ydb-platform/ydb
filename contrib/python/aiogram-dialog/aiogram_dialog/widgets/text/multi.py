from typing import Any

from magic_filter import MagicFilter

from aiogram_dialog.api.internal import TextWidget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import (
    Selector,
    WhenCondition,
    new_case_field,
    new_magic_selector,
)
from .base import Text


class Case(Text):
    def __init__(
        self,
        texts: dict[Any, TextWidget],
        selector: str | Selector["Case"] | MagicFilter,
        when: WhenCondition = None,
    ):
        super().__init__(when=when)
        self.texts = texts
        self._has_default = ... in self.texts

        self.selector: Selector[Case]
        if isinstance(selector, str):
            self.selector = new_case_field(selector)
        elif isinstance(selector, MagicFilter):
            self.selector = new_magic_selector(selector)
        else:
            self.selector = selector

    async def _render_text(self, data, manager: DialogManager) -> str:
        selection = self.selector(data, self, manager)
        if selection not in self.texts:
            if self._has_default:
                selection = ...
            elif manager.is_preview():
                selection = next(iter(self.texts))
        return await self.texts[selection].render_text(data, manager)

    def find(self, widget_id: str) -> TextWidget | None:
        for text in self.texts.values():
            if found := text.find(widget_id):
                return found
        return None
