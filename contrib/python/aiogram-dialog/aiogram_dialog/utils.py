import sys
from collections.abc import Callable
from logging import getLogger
from typing import Any, ParamSpec, TypeVar

from aiogram.types import (
    CallbackQuery,
    Chat,
    ChatJoinRequest,
    ChatMemberUpdated,
    ErrorEvent,
    InaccessibleMessage,
    InlineKeyboardButton,
    KeyboardButton,
    Message,
    User,
)

from aiogram_dialog.api.entities import (
    ChatEvent,
    DialogUpdateEvent,
    MediaId,
)
from aiogram_dialog.api.internal import RawKeyboard

logger = getLogger(__name__)

CB_SEP = "\x1D"

REPLY_CALLBACK_SYMBOLS: str = (
    "\u200C"
    "\u200D"
    "\u2060"
    "\u2061"
    "\u2062"
    "\u2063"
    "\u2064"
    "\u00AD"
    "\U0001D173"
    "\U0001D174"
    "\U0001D175"
    "\U0001D176"
    "\U0001D177"
    "\U0001D178"
    "\U0001D179"
    "\U0001D17A"
)


def _encode_reply_callback_byte(byte: int):
    return (
        REPLY_CALLBACK_SYMBOLS[byte % len(REPLY_CALLBACK_SYMBOLS)] +
        REPLY_CALLBACK_SYMBOLS[byte // len(REPLY_CALLBACK_SYMBOLS)]
    )


def encode_reply_callback(data: str) -> str:
    bytes_data = data.encode("utf-8")
    return "".join(
        _encode_reply_callback_byte(byte)
        for byte in bytes_data
    )


def _decode_reply_callback_byte(little: str, big: str) -> int:
    return (
        REPLY_CALLBACK_SYMBOLS.index(big) * len(REPLY_CALLBACK_SYMBOLS) +
        REPLY_CALLBACK_SYMBOLS.index(little)
    )


def join_reply_callback(text: str, callback_data: str) -> str:
    return text + encode_reply_callback(callback_data)


def split_reply_callback(
        data: str | None,
) -> tuple[str | None, str | None]:
    if not data:
        return None, None
    text = data.rstrip(REPLY_CALLBACK_SYMBOLS)
    callback = data[len(text):]
    return text, decode_reply_callback(callback)


def decode_reply_callback(data: str) -> str:
    bytes_data = bytes(
        _decode_reply_callback_byte(little, big)
        for little, big in zip(data[::2], data[1::2], strict=False)
    )
    return bytes_data.decode("utf-8")


def _transform_to_reply_button(
        button: InlineKeyboardButton | KeyboardButton,
) -> KeyboardButton:
    if isinstance(button, KeyboardButton):
        return button
    if button.web_app:
        return KeyboardButton(text=button.text, web_app=button.web_app)
    if not button.callback_data:
        raise ValueError(
            "Cannot convert inline button without callback_data or web_app",
        )

    style: str | None = getattr(
        button,
        "style",
        button.model_extra.get("style"),
    )
    emoji_id: str | None = getattr(
        button,
        "icon_custom_emoji_id",
        button.model_extra.get("icon_custom_emoji_id"),
    )

    return KeyboardButton(
        text=join_reply_callback(
            text=button.text,
            callback_data=button.callback_data,
        ),
        style=style,
        icon_custom_emoji_id=emoji_id,
    )


def transform_to_reply_keyboard(
        keyboard: list[list[InlineKeyboardButton | KeyboardButton]],
) -> list[list[KeyboardButton]]:
    return [
        [_transform_to_reply_button(button) for button in row]
        for row in keyboard
    ]


def get_chat(event: ChatEvent) -> Chat:
    if isinstance(
            event,
            (Message, DialogUpdateEvent, ChatMemberUpdated, ChatJoinRequest),
    ):
        return event.chat
    elif isinstance(event, CallbackQuery):
        if not event.message:
            return Chat(id=event.from_user.id, type="")
        return event.message.chat
    elif isinstance(event, ErrorEvent):
        upd_event = event.update.event
        if hasattr(upd_event, "chat"):
            return upd_event.chat
        elif hasattr(upd_event, "user"):
            return Chat(id=upd_event.user.id, type="")
        elif hasattr(upd_event, "from_user"):
            return Chat(id=upd_event.from_user.id, type="")
        else:
            raise AttributeError
    else:
        raise TypeError


def is_chat_loaded(chat: Chat) -> bool:
    """
    Check if chat is correctly loaded from telegram.

    For internal events it can be created with no data inside as a FakeChat
    """
    return not getattr(chat, "fake", False)


def is_user_loaded(user: User) -> bool:
    """
    Check if user is correctly loaded from telegram.

    For internal events it can be created with no data inside as a FakeUser
    """
    return not getattr(user, "fake", False)


def get_media_id(
    message: Message | InaccessibleMessage,
) -> MediaId | None:
    if isinstance(message, InaccessibleMessage):
        return None

    media = (
        message.audio or
        message.animation or
        message.document or
        (message.photo[-1] if message.photo else None) or
        message.video or
        message.voice
    )
    if not media:
        return None
    return MediaId(
        file_id=media.file_id,
        file_unique_id=media.file_unique_id,
    )


def intent_callback_data(
        intent_id: str, callback_data: str | None,
) -> str | None:
    if callback_data is None:
        return None
    prefix = intent_id + CB_SEP
    if callback_data.startswith(prefix):
        return callback_data
    return prefix + callback_data


def add_intent_id(keyboard: RawKeyboard, intent_id: str):
    for row in keyboard:
        for button in row:
            if isinstance(button, InlineKeyboardButton):
                button.callback_data = intent_callback_data(
                    intent_id, button.callback_data,
                )


def remove_intent_id(callback_data: str) -> tuple[str | None, str]:
    if CB_SEP in callback_data:
        intent_id, new_data = callback_data.split(CB_SEP, maxsplit=1)
        return intent_id, new_data
    return None, callback_data


P = ParamSpec("P")
R = TypeVar("R")

def add_exception_note(f: Callable[P, R]) -> Callable[P, R]:
    async def inner(self: Any, *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await f(self, *args, **kwargs)
        except Exception as e:
            # execute only on version >= 3.11
            if sys.version_info >= (3, 11):
                e.add_note(f"at {self!r}")
            raise
    return inner
