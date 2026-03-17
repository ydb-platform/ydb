from typing import Any

from magic_filter import MagicFilter

from aiogram_dialog.api.internal.widgets import StyleWidget
from aiogram_dialog.api.protocols.manager import DialogManager
from aiogram_dialog.widgets.common import (
    Selector,
    WhenCondition,
    new_case_field,
    new_magic_selector,
)
from .base import BaseStyle


class StyleCase(BaseStyle):
    def __init__(
        self,
        styles: dict[Any, StyleWidget],
        selector: str | Selector["StyleCase"] | MagicFilter,
        when: WhenCondition = None,
    ):
        super().__init__(when=when)
        self.styles = styles
        self._has_default = ... in self.styles

        self.selector: Selector[StyleCase]
        if isinstance(selector, str):
            self.selector = new_case_field(selector)
        elif isinstance(selector, MagicFilter):
            self.selector = new_magic_selector(selector)
        else:
            self.selector = selector

    def _get_selection(self, data: dict, manager: DialogManager) -> Any:
        selection = self.selector(data, self, manager)
        if selection not in self.styles:
            if self._has_default:
                selection = ...
            elif manager.is_preview():
                selection = next(iter(self.styles))
        return selection

    async def _render_style(
        self,
        data: dict,
        manager: DialogManager,
    ) -> str | None:
        selection = self._get_selection(data, manager)
        return await self.styles[selection].render_style(data, manager)

    async def _render_emoji(
        self,
        data: dict,
        manager: DialogManager,
    ) -> str | None:
        selection = self._get_selection(data, manager)
        return await self.styles[selection].render_emoji(data, manager)
