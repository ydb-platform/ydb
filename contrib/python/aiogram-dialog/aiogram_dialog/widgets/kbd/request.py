from collections.abc import Callable

from aiogram.types import KeyboardButton, KeyboardButtonPollType

from aiogram_dialog.api.internal import RawKeyboard, StyleWidget, TextWidget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.style import EMPTY_STYLE
from .base import Keyboard


class RequestContact(Keyboard):
    def __init__(
            self,
            text: TextWidget,
            when: str | Callable | None = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(when=when)
        self.text = text
        self.style = style

    async def _render_keyboard(
            self,
            data: dict,
            manager: DialogManager,
    ) -> RawKeyboard:
        style = await self.style.render_style(data, manager)
        icon_custom_emoji_id = await self.style.render_emoji(data, manager)

        return [
            [
                KeyboardButton(
                    text=await self.text.render_text(data, manager),
                    request_contact=True,
                    style=style,
                    icon_custom_emoji_id=icon_custom_emoji_id,
                ),
            ],
        ]


class RequestLocation(Keyboard):
    def __init__(
            self,
            text: TextWidget,
            when: str | Callable | None = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(when=when)
        self.text = text
        self.style = style

    async def _render_keyboard(
            self,
            data: dict,
            manager: DialogManager,
    ) -> RawKeyboard:
        style = await self.style.render_style(data, manager)
        icon_custom_emoji_id = await self.style.render_emoji(data, manager)

        return [
            [
                KeyboardButton(
                    text=await self.text.render_text(data, manager),
                    request_location=True,
                    style=style,
                    icon_custom_emoji_id=icon_custom_emoji_id,
                ),
            ],
        ]


class RequestPoll(Keyboard):
    def __init__(
            self,
            text: TextWidget,
            poll_type: str | None = None,
            when: str | Callable | None = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(when=when)
        self.text = text
        self.poll_type = poll_type
        self.style = style

    async def _render_keyboard(
            self,
            data: dict,
            manager: DialogManager,
    ) -> RawKeyboard:
        text = await self.text.render_text(data, manager)
        request_poll = KeyboardButtonPollType(type=self.poll_type)
        style = await self.style.render_style(data, manager)
        icon_custom_emoji_id = await self.style.render_emoji(data, manager)

        return [
            [
                KeyboardButton(
                    text=text,
                    request_poll=request_poll,
                    style=style,
                    icon_custom_emoji_id=icon_custom_emoji_id,
                ),
            ],
        ]
