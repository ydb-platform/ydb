from typing import Any

from aiogram.types import CopyTextButton, InlineKeyboardButton

from aiogram_dialog import DialogManager
from aiogram_dialog.api.internal import RawKeyboard, StyleWidget, TextWidget
from aiogram_dialog.widgets.common import WhenCondition
from aiogram_dialog.widgets.kbd import Keyboard
from aiogram_dialog.widgets.style import EMPTY_STYLE


class CopyText(Keyboard):
    def __init__(
        self,
        text: TextWidget,
        copy_text: TextWidget,
        when: WhenCondition = None,
        style: StyleWidget = EMPTY_STYLE,
    ) -> None:
        super().__init__(when=when)
        self._text = text
        self._copy_text = copy_text
        self.style = style

    async def _render_keyboard(
        self,
        data: dict[str, Any],
        manager: DialogManager,
    ) -> RawKeyboard:
        style = await self.style.render_style(data, manager)
        icon_custom_emoji_id = await self.style.render_emoji(data, manager)

        return [
            [
                InlineKeyboardButton(
                    text=await self._text.render_text(data, manager),
                    copy_text=CopyTextButton(
                        text=await self._copy_text.render_text(data, manager),
                    ),
                    style=style,
                    icon_custom_emoji_id=icon_custom_emoji_id,
                ),
            ],
        ]
