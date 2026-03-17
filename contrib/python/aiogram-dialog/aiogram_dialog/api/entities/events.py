from dataclasses import dataclass

from aiogram import Bot
from aiogram.types import (
    CallbackQuery,
    Chat,
    ChatJoinRequest,
    ChatMemberUpdated,
    ErrorEvent,
    Message,
    User,
)

from .update_event import DialogUpdateEvent

ChatEvent = (
    CallbackQuery
    | ChatJoinRequest
    | ChatMemberUpdated
    | DialogUpdateEvent
    | ErrorEvent
    | Message
)


@dataclass
class EventContext:
    bot: Bot
    chat: Chat
    user: User
    thread_id: int | None
    business_connection_id: str | None


EVENT_CONTEXT_KEY = "aiogd_event_context"
