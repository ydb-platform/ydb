from typing import Any

from aiogram.fsm.state import State
from aiogram.types import CallbackQuery

from aiogram_dialog.api.entities import ChatEvent, Data, ShowMode, StartMode
from aiogram_dialog.api.internal import StyleWidget, TextWidget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import WhenCondition
from aiogram_dialog.widgets.kbd.button import Button, OnClick
from aiogram_dialog.widgets.style import EMPTY_STYLE
from aiogram_dialog.widgets.text import Const
from aiogram_dialog.widgets.widget_event import WidgetEventProcessor

BACK_TEXT = Const("Back")
NEXT_TEXT = Const("Next")
CANCEL_TEXT = Const("Cancel")


class EventProcessorButton(Button, WidgetEventProcessor):
    async def process_event(
            self,
            event: ChatEvent,
            source: Any,
            manager: DialogManager,
            *args,
            **kwargs,
    ):
        await self._on_click(event, self, manager)

    async def _on_click(
            self, callback: CallbackQuery, button: Button,
            manager: DialogManager,
    ):
        raise NotImplementedError


class SwitchTo(EventProcessorButton):
    def __init__(
            self,
            text: TextWidget,
            id: str,
            state: State,
            on_click: OnClick | None = None,
            when: WhenCondition = None,
            show_mode: ShowMode | None = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            text=text, on_click=self._on_click,
            id=id, when=when, style=style,
        )
        self.text = text
        self.user_on_click = on_click
        self.state = state
        self.show_mode = show_mode

    async def _on_click(
            self, callback: CallbackQuery, button: Button,
            manager: DialogManager,
    ):
        if self.user_on_click:
            await self.user_on_click(callback, self, manager)
        await manager.switch_to(self.state, show_mode=self.show_mode)


class Next(EventProcessorButton):
    def __init__(
            self,
            text: TextWidget = NEXT_TEXT,
            id: str = "__next__",
            on_click: OnClick | None = None,
            show_mode: ShowMode | None = None,
            when: WhenCondition = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            text=text,
            on_click=self._on_click,
            id=id, when=when,
            style=style,
        )
        self.text = text
        self.show_mode = show_mode
        self.user_on_click = on_click

    async def _on_click(
            self, callback: CallbackQuery, button: Button,
            manager: DialogManager,
    ):
        if self.user_on_click:
            await self.user_on_click(callback, self, manager)
        await manager.next(self.show_mode)


class Back(EventProcessorButton):
    def __init__(
            self,
            text: TextWidget = BACK_TEXT,
            id: str = "__back__",
            on_click: OnClick | None = None,
            show_mode: ShowMode | None = None,
            when: WhenCondition = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            text=text, on_click=self._on_click,
            id=id, when=when, style=style,
        )
        self.text = text
        self.callback_data = id
        self.show_mode = show_mode
        self.user_on_click = on_click

    async def _on_click(
            self, callback: CallbackQuery, button: Button,
            manager: DialogManager,
    ):
        if self.user_on_click:
            await self.user_on_click(callback, self, manager)
        await manager.back(self.show_mode)


class Cancel(EventProcessorButton):
    def __init__(
            self,
            text: TextWidget = CANCEL_TEXT,
            id: str = "__cancel__",
            result: Any = None,
            on_click: OnClick | None = None,
            when: WhenCondition = None,
            show_mode: ShowMode | None = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            text=text, on_click=self._on_click,
            id=id, when=when, style=style,
        )
        self.text = text
        self.result = result
        self.user_on_click = on_click
        self.show_mode = show_mode

    async def _on_click(
            self, callback: CallbackQuery, button: Button,
            manager: DialogManager,
    ):
        if self.user_on_click:
            await self.user_on_click(callback, self, manager)
        await manager.done(self.result, show_mode=self.show_mode)


class Start(EventProcessorButton):
    def __init__(
            self,
            text: TextWidget,
            id: str,
            state: State,
            data: Data = None,
            on_click: OnClick | None = None,
            show_mode: ShowMode | None = None,
            mode: StartMode = StartMode.NORMAL,
            when: WhenCondition = None,
            style: StyleWidget = EMPTY_STYLE,
    ):
        super().__init__(
            text=text, on_click=self._on_click,
            id=id, when=when, style=style,
        )
        self.text = text
        self.start_data = data
        self.user_on_click = on_click
        self.show_mode = show_mode
        self.state = state
        self.mode = mode

    async def _on_click(
            self, callback: CallbackQuery, button: Button,
            manager: DialogManager,
    ):
        if self.user_on_click:
            await self.user_on_click(callback, self, manager)
        await manager.start(
            state=self.state,
            data=self.start_data,
            mode=self.mode,
            show_mode=self.show_mode,
        )
