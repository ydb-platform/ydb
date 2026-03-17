import html
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from aiogram import Router
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    Chat,
    ContentType,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
    User,
)
from jinja2 import Environment, PackageLoader, select_autoescape

from aiogram_dialog import (
    BaseDialogManager,
    Dialog,
    DialogManager,
    DialogProtocol,
)
from aiogram_dialog.api.entities import (
    EVENT_CONTEXT_KEY,
    AccessSettings,
    ChatEvent,
    Context,
    Data,
    DialogAction,
    DialogUpdateEvent,
    EventContext,
    MediaAttachment,
    NewMessage,
    ShowMode,
    Stack,
    StartMode,
)
from aiogram_dialog.api.exceptions import NoContextError
from aiogram_dialog.api.protocols import UnsetId
from aiogram_dialog.setup import collect_dialogs
from aiogram_dialog.utils import split_reply_callback

logger = logging.getLogger(__name__)


@dataclass
class RenderButton:
    state: str
    title: str


@dataclass
class RenderWindow:
    message: str
    state: str
    state_name: str
    keyboard: list[list[RenderButton]]
    reply_keyboard: list[list[RenderButton]]
    photo: str | None
    text_input: RenderButton | None
    attachment_input: RenderButton | None


@dataclass
class RenderDialog:
    state_group: str
    windows: list[RenderWindow]


class FakeManager(DialogManager):

    def __init__(self):
        self._event = DialogUpdateEvent(
            from_user=User(
                id=1, is_bot=False, first_name="Fake",
                language_code="en",
            ),
            chat=Chat(id=1, type="private"),
            action=DialogAction.UPDATE,
            data={},
            intent_id=None,
            stack_id=None,
            thread_id=None,
            business_connection_id=None,
        )
        self._context: Context | None = None
        self._dialog: DialogProtocol | None = None
        self._data = {
            "dialog_manager": self,
            "event_chat": self._event.chat,
            "event_from_user": self._event.from_user,
            EVENT_CONTEXT_KEY: EventContext(
                bot=None,
                thread_id=None,
                chat=self._event.chat,
                user=self._event.from_user,
                business_connection_id=None,
            ),
        }

    async def next(self, show_mode: ShowMode | None = None) -> None:
        states = self._dialog.states()
        current_index = states.index(self.current_context().state)
        if current_index + 1 >= len(states):
            raise ValueError(
                f"Cannot go to a non-existent state."
                f"The state of {current_index + 1} idx is requested, "
                f"but there are only {len(states)} states"
                f"current state is {self.current_context().state}",
            )
        new_state = states[current_index + 1]
        await self.switch_to(new_state, show_mode)

    async def back(self, show_mode: ShowMode | None = None) -> None:
        states = self._dialog.states()
        current_index = states.index(self.current_context().state)
        if current_index - 1 < 0:
            raise ValueError(
                f"Cannot go to a non-existent state."
                f"The state of {current_index - 1} idx is requested, "
                f"but states idx should be positive"
                f"current state is {self.current_context().state}",
            )
        new_state = states[current_index - 1]
        await self.switch_to(new_state, show_mode)


    @property
    def middleware_data(self) -> dict:
        return self._data

    @property
    def event(self) -> ChatEvent:
        return self._event

    async def load_data(self) -> dict:
        return {}

    async def close_manager(self) -> None:
        pass

    async def reset_stack(self, remove_keyboard: bool = True) -> None:
        self.reset_context()

    def set_dialog(self, dialog: Dialog):
        self._dialog = dialog
        self.reset_context()

    def set_state(self, state: State):
        self.current_context().state = state

    def is_preview(self) -> bool:
        return True

    @property
    def dialog_data(self) -> dict:
        return self.current_context().dialog_data

    def reset_context(self) -> None:
        self._context = Context(
            _intent_id="0",
            _stack_id="0",
            start_data={},
            widget_data={},
            dialog_data={},
            state=State(),
        )

    async def switch_to(
            self,
            state: State,
            show_mode: ShowMode | None = None,
    ) -> None:
        self.set_state(state)

    async def start(
            self,
            state: State,
            data: Data = None,
            mode: StartMode = StartMode.NORMAL,
            show_mode: ShowMode = ShowMode.AUTO,
            access_settings: AccessSettings | None = None,
    ) -> None:
        self.set_state(state)

    async def done(
            self,
            result: Any = None,
            show_mode: ShowMode | None = None,
    ) -> None:
        self.set_state(State("-"))

    def current_stack(self) -> Stack:
        return Stack()

    def current_context(self) -> Context:
        if not self._context:
            raise NoContextError
        return self._context

    def has_context(self) -> bool:
        return bool(self._context)

    async def show_raw(self) -> NewMessage:
        return await self._dialog.render(self)

    async def mark_closed(self) -> None:
        pass

    @property
    def start_data(self) -> Data:
        return {}

    @property
    def show_mode(self) -> ShowMode:
        return ShowMode.AUTO

    @show_mode.setter
    def show_mode(self, show_mode: ShowMode) -> None:
        return

    async def show(self, show_mode: ShowMode | None = None) -> None:
        pass

    def find(self, widget_id) -> Any | None:
        widget = self._dialog.find(widget_id)
        if not widget:
            return None
        return widget.managed(self)

    async def update(
            self,
            data: dict,
            show_mode: ShowMode | None = None,
    ) -> None:
        pass

    def bg(
            self,
            user_id: int | None = None,
            chat_id: int | None = None,
            stack_id: str | None = None,
            thread_id: int | None | UnsetId = UnsetId.UNSET,
            business_connection_id: str | None | UnsetId = UnsetId.UNSET,
            load: bool = False,
    ) -> BaseDialogManager:
        return self

    async def answer_callback(self) -> None:
        pass


def create_photo(media: MediaAttachment | None) -> str | None:
    if not media:
        return None
    if media.type != ContentType.PHOTO:
        return None
    if media.url:
        return media.url
    if media.path:
        return media.path
    if media.file_id:
        return str(media.file_id)
    return None


async def create_button(
        title: str,
        callback: str,
        manager: FakeManager,
        state: State,
        dialog: Dialog,
        simulate_events: bool,
) -> RenderButton:
    if not simulate_events:
        return RenderButton(title=title, state=state.state)
    callback_query = CallbackQuery(
        id="1",
        from_user=User(id=1, is_bot=False, first_name=""),
        chat_instance="",
        data=callback,
    )
    manager.set_state(state)
    try:
        await dialog._callback_handler(callback_query, dialog_manager=manager)
    except Exception:
        logger.debug("Click %s", callback)
    state = manager.current_context().state
    return RenderButton(title=title, state=state.state)


async def render_input(
        manager: FakeManager,
        state: State,
        dialog: Dialog,
        content_type: str,
        simulate_events: bool,
) -> RenderButton | None:
    if not simulate_events:
        return None
    if content_type == ContentType.PHOTO:
        data = {content_type: []}
    else:
        data = {content_type: "<stub>"}
    message = Message(
        message_id=1,
        date=datetime.now(),
        chat=Chat(id=1, type="private"),
        **data,
    )
    manager.set_state(state)
    try:
        await dialog._message_handler(message, dialog_manager=manager)
    except Exception:
        logger.debug("Input %s", content_type)

    if state == manager.current_context().state:
        logger.debug("State not changed")
        return None
    logger.debug(
        "State changed %s >> %s", state, manager.current_context().state,
    )
    return RenderButton(
        title=content_type,
        state=manager.current_context().state.state,
    )


async def render_inline_keyboard(
        state: State,
        reply_markup: InlineKeyboardMarkup,
        manager: FakeManager,
        dialog: Dialog,
        simulate_events: bool,
):
    return [
        [
            await create_button(
                title=button.text,
                callback=button.callback_data,
                manager=manager,
                dialog=dialog,
                state=state,
                simulate_events=simulate_events,
            )
            for button in row
        ]
        for row in reply_markup.inline_keyboard
    ]


async def render_reply_keyboard(
        state: State,
        reply_markup: ReplyKeyboardMarkup,
        manager: FakeManager,
        dialog: Dialog,
        simulate_events: bool,
):
    # TODO simulate events using keyboard
    keyboard = []
    for row in reply_markup.keyboard:
        keyboard_row = []
        for button in row:
            text, data = split_reply_callback(button.text)
            keyboard_row.append(
                await create_button(
                    title=text,
                    callback=data,
                    manager=manager,
                    dialog=dialog,
                    state=state,
                    simulate_events=simulate_events,
                ),
            )
        keyboard.append(keyboard_row)
    return keyboard


async def create_window(
        state: State,
        message: NewMessage,
        manager: FakeManager,
        dialog: Dialog,
        simulate_events: bool,
) -> RenderWindow:
    if message.parse_mode is None or message.parse_mode == "None":
        text = html.escape(message.text)
    else:
        text = message.text

    if isinstance(message.reply_markup, InlineKeyboardMarkup):
        keyboard = await render_inline_keyboard(
            state, message.reply_markup, manager, dialog, simulate_events,
        )
        reply_keyboard = []
    elif isinstance(message.reply_markup, ReplyKeyboardMarkup):
        keyboard = []
        reply_keyboard = await render_reply_keyboard(
            state, message.reply_markup, manager, dialog, simulate_events,
        )
    else:
        keyboard = []
        reply_keyboard = []

    return RenderWindow(
        message=text.replace("\n", "<br>"),
        state=state.state,
        state_name=state._state,
        photo=create_photo(media=message.media),
        keyboard=keyboard,
        reply_keyboard=reply_keyboard,
        text_input=await render_input(
            manager=manager,
            state=state,
            dialog=dialog,
            content_type=ContentType.TEXT,
            simulate_events=simulate_events,
        ),
        attachment_input=await render_input(
            manager=manager,
            state=state,
            dialog=dialog,
            content_type=ContentType.PHOTO,
            simulate_events=simulate_events,
        ),
    )


async def render_dialog(
        manager: FakeManager,
        group: type[StatesGroup],
        dialog: Dialog,
        simulate_events: bool,
) -> RenderDialog:
    manager.set_dialog(dialog)
    windows = []
    for state in group.__states__:
        manager.set_state(state)
        new_message = await manager.show_raw()
        windows.append(
            await create_window(
                manager=manager,
                state=state,
                dialog=dialog,
                message=new_message,
                simulate_events=simulate_events,
            ),
        )

    return RenderDialog(state_group=str(group.__qualname__), windows=windows)


async def render_preview_content(
        router: Router,
        simulate_events: bool = False,
) -> str:
    fake_manager = FakeManager()
    dialogs = [
        await render_dialog(
            manager=fake_manager,
            group=dialog.states_group(),
            dialog=dialog,
            simulate_events=simulate_events,
        )
        for dialog in collect_dialogs(router)
    ]
    env = Environment(
        loader=PackageLoader("aiogram_dialog.tools"),
        autoescape=select_autoescape(),
    )
    template = env.get_template("message.html")
    return template.render(dialogs=dialogs)


async def render_preview(
        router: Router,
        file: str,
        simulate_events: bool = False,
):
    res = await render_preview_content(router, simulate_events)
    with open(file, "w", encoding="utf-8") as f:
        f.write(res)
