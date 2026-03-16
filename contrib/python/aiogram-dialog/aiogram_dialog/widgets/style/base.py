from abc import abstractmethod
from typing import Any

from aiogram_dialog.api.internal.widgets import (
    ButtonStyle,
    StyleWidget,
    Widget,
)
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.utils import add_exception_note
from aiogram_dialog.widgets.common import (
    BaseWidget,
    Whenable,
    WhenCondition,
)


class BaseStyle(Whenable, BaseWidget, StyleWidget):
    def __init__(self, when: WhenCondition = None):
        super().__init__(when=when)

    @add_exception_note
    async def render_style(
        self,
        data: dict,
        manager: DialogManager,
    ) -> ButtonStyle | None:
        """
        Create button style.

        When inheriting override `_render_style` method instead
        if you want to keep processing of `when` condition
        """
        if not self.is_(data, manager):
            return None
        return await self._render_style(data, manager)

    @abstractmethod
    async def _render_style(self, data, manager: DialogManager) -> str | None:
        """
        Create button style.

        Called if widget is not hidden only (regarding `when`-condition)
        """
        raise NotImplementedError

    @add_exception_note
    async def render_emoji(
        self,
        data: dict,
        manager: DialogManager,
    ) -> str | None:
        """
        Add custom emoji shown before the text of the button.

        When inheriting override `_render_emoji` method instead
        if you want to keep processing of `when` condition
        """
        if not self.is_(data, manager):
            return None
        return await self._render_emoji(data, manager)

    @abstractmethod
    async def _render_emoji(self, data, manager: DialogManager) -> str | None:
        """
        Add custom emoji shown before the text of the button.

        Called if widget is not hidden only (regarding `when`-condition)
        """
        raise NotImplementedError

    def __or__(self, other: StyleWidget) -> "StyleWidget":
        return Or(self, other)

    def __ror__(self, other: StyleWidget) -> "StyleWidget":
        return Or(other, self)


class Style(BaseStyle):
    def __init__(
        self,
        style: ButtonStyle | None = None,
        emoji_id: str | None = None,
        when: WhenCondition = None,
    ):
        super().__init__(when=when)
        self.style = style
        self.emoji_id = emoji_id

    async def _render_style(
        self,
        data: dict,
        manager: DialogManager,
    ) -> ButtonStyle | None:
        return self.style

    async def _render_emoji(
        self,
        data: dict,
        manager: DialogManager,
    ) -> str | None:
        return self.emoji_id


class Or(StyleWidget):
    def __init__(self, *styles: StyleWidget):
        super().__init__()
        self.styles = styles

    def managed(self, manager: DialogManager) -> Any:
        return self

    def find(self, widget_id: str) -> Widget | None:
        for style in self.styles:
            if found := style.find(widget_id):
                return found
        return None


    async def render_style(
            self, data: dict, manager: DialogManager,
    ) -> ButtonStyle | None:
        for style in self.styles:
            res = await style.render_style(data, manager)
            if res:
                return res
        return None

    async def render_emoji(self, data: dict,
                           manager: DialogManager) -> str | None:
        for style in self.styles:
            res = await style.render_emoji(data, manager)
            if res:
                return res
        return None

    def __ior__(self, other: StyleWidget) -> "Or":
        self.styles += (other,)
        return self

    def __or__(self, other: StyleWidget) -> "Or":
        # reduce nesting
        return Or(*self.styles, other)

    def __ror__(self, other: StyleWidget) -> "Or":
        # reduce nesting
        return Or(other, *self.styles)

EMPTY_STYLE = Style(style=None, emoji_id=None, when=None)
