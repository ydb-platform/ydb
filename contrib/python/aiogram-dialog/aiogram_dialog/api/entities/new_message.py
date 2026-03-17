from dataclasses import dataclass
from enum import Enum

from aiogram.enums import ContentType
from aiogram.types import (
    Chat,
    ForceReply,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from aiogram_dialog.api.entities import MediaAttachment, ShowMode

MarkupVariant = (
    ForceReply
    | InlineKeyboardMarkup
    | ReplyKeyboardMarkup
    | ReplyKeyboardRemove
)


class UnknownText(Enum):
    UNKNOWN = object()


@dataclass
class OldMessage:
    chat: Chat
    message_id: int
    media_id: str | None
    media_uniq_id: str | None
    text: str | UnknownText | None = None
    has_protected_content: bool | None = None
    has_reply_keyboard: bool = False
    business_connection_id: str | None = None
    content_type: ContentType | None = None


@dataclass
class NewMessage:
    chat: Chat
    thread_id: int | None = None
    business_connection_id: str | None = None
    text: str | None = None
    reply_markup: MarkupVariant | None = None
    parse_mode: str | None = None
    protect_content: bool | None = None
    show_mode: ShowMode = ShowMode.AUTO
    media: MediaAttachment | None = None
    link_preview_options: LinkPreviewOptions | None = None
