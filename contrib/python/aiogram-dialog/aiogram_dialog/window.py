import warnings
from logging import getLogger
from typing import Any, cast

from aiogram.fsm.state import State
from aiogram.types import (
    UNSET_PARSE_MODE,
    CallbackQuery,
    LinkPreviewOptions,
    Message,
)

from aiogram_dialog.api.entities import (
    EVENT_CONTEXT_KEY,
    EventContext,
    MarkupVariant,
    MediaAttachment,
    NewMessage,
)
from aiogram_dialog.api.internal import Widget, WindowProtocol
from .api.entities import Data
from .api.internal.widgets import MarkupFactory
from .api.protocols import DialogManager, DialogProtocol
from .dialog import OnResultEvent
from .widgets.data import PreviewAwareGetter
from .widgets.kbd import Keyboard
from .widgets.link_preview import LinkPreview
from .widgets.markup.inline_keyboard import InlineKeyboardFactory
from .widgets.utils import (
    GetterVariant,
    WidgetSrc,
    ensure_data_getter,
    ensure_widgets,
)

logger = getLogger(__name__)

_DEFAULT_MARKUP_FACTORY = InlineKeyboardFactory()


class Window(WindowProtocol):
    def __init__(
            self,
            *widgets: WidgetSrc,
            state: State,
            getter: GetterVariant = None,
            on_process_result: OnResultEvent | None = None,
            markup_factory: MarkupFactory = _DEFAULT_MARKUP_FACTORY,
            parse_mode: str | None = UNSET_PARSE_MODE,
            disable_web_page_preview: bool | None = None,
            protect_content: bool | None = None,
            preview_add_transitions: list[Keyboard] | None = None,
            preview_data: GetterVariant = None,
    ):
        (
            self.text,
            self.keyboard,
            self.on_message,
            self.media,
            self.link_preview,
        ) = ensure_widgets(widgets)
        self.getter = PreviewAwareGetter(
            ensure_data_getter(getter),
            ensure_data_getter(preview_data),
        )
        self.state = state
        self.on_process_result = on_process_result
        self.markup_factory = markup_factory
        self.parse_mode = parse_mode
        self.protect_content = protect_content
        self.preview_add_transitions = preview_add_transitions
        if disable_web_page_preview is not None:
            if self.link_preview:
                raise ValueError(
                    "Cannot use LinkPreview widget "
                    "together with disable_web_page_preview",
                )
            warnings.warn(
                "disable_web_page_preview is deprecated, "
                "use `LinkPreview` widget instead",
                category=DeprecationWarning,
                stacklevel=2,
            )
            self.link_preview = LinkPreview(is_disabled=True)

    async def render_text(
            self, data: dict, manager: DialogManager,
    ) -> str:
        return await self.text.render_text(data, manager)

    async def render_media(
            self, data: dict, manager: DialogManager,
    ) -> MediaAttachment | None:
        if self.media:
            return await self.media.render_media(data, manager)
        return None

    async def render_kbd(
            self, data: dict, manager: DialogManager,
    ) -> MarkupVariant:
        keyboard = await self.keyboard.render_keyboard(data, manager)
        return await self.markup_factory.render_markup(
            data, manager, keyboard,
        )

    async def render_link_preview(
            self, data: dict, manager: DialogManager,
    ) -> LinkPreviewOptions | None:
        if self.link_preview:
            return await self.link_preview.render_link_preview(data, manager)
        return None

    async def load_data(
            self, dialog: "DialogProtocol",
            manager: DialogManager,
    ) -> dict:
        data = await dialog.load_data(manager)
        data.update(await self.getter(**manager.middleware_data))
        return data

    async def process_message(
            self, message: Message, dialog: DialogProtocol,
            manager: DialogManager,
    ) -> bool:
        if self.on_message:
            return await self.on_message.process_message(
                message, dialog, manager,
            )
        return False

    async def process_callback(
            self, callback: CallbackQuery, dialog: DialogProtocol,
            manager: DialogManager,
    ) -> bool:
        if self.keyboard:
            return await self.keyboard.process_callback(
                callback, dialog, manager,
            )
        return False

    async def process_result(
            self, start_data: Data, result: Any, manager: DialogManager,
    ) -> None:
        if self.on_process_result:
            await self.on_process_result(start_data, result, manager)

    async def render(
            self, dialog: DialogProtocol,
            manager: DialogManager,
    ) -> NewMessage:
        logger.debug("Show window: %s", self)
        chat = manager.middleware_data["event_chat"]
        try:
            current_data = await self.load_data(dialog, manager)
        except Exception:
            logger.error("Cannot get window data for state %s", self.state)
            raise
        try:
            event_context = cast(
                EventContext, manager.middleware_data.get(EVENT_CONTEXT_KEY),
            )
            return NewMessage(
                chat=chat,
                thread_id=event_context.thread_id,
                business_connection_id=event_context.business_connection_id,
                text=await self.render_text(current_data, manager),
                reply_markup=await self.render_kbd(current_data, manager),
                parse_mode=self.parse_mode,
                protect_content=self.protect_content,
                media=await self.render_media(current_data, manager),
                link_preview_options=await self.render_link_preview(
                    current_data, manager,
                ),
            )
        except Exception:
            logger.error("Cannot render window for state %s", self.state)
            raise

    def get_state(self) -> State:
        return self.state

    def find(self, widget_id) -> Widget | None:
        for root in (self.text, self.keyboard, self.on_message, self.media):
            if root and (found := root.find(widget_id)):
                return found
        return None

    def __repr__(self) -> str:
        return f"<{self.__class__.__qualname__}({self.state})>"
