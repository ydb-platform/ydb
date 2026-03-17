from enum import Enum
from typing import Any

from aiogram.fsm.state import State
from aiogram.types import (
    Chat,
    TelegramObject,
    Update,
    User,
)
from pydantic import ConfigDict

from .modes import (
    ShowMode,
    StartMode,
)
from .stack import AccessSettings

DIALOG_EVENT_NAME = "aiogd_update"


class DialogAction(Enum):
    DONE = "DONE"
    START = "START"
    UPDATE = "UPDATE"
    SWITCH = "SWITCH"


class DialogUpdateEvent(TelegramObject):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        use_enum_values=False,
    )
    from_user: User
    chat: Chat
    action: DialogAction
    data: Any
    intent_id: str | None
    stack_id: str | None
    thread_id: int | None
    business_connection_id: str | None
    show_mode: ShowMode | None = None


class DialogStartEvent(DialogUpdateEvent):
    new_state: State
    mode: StartMode
    access_settings: AccessSettings | None = None


class DialogSwitchEvent(DialogUpdateEvent):
    new_state: State


class DialogUpdate(Update):
    aiogd_update: DialogUpdateEvent

    def __init__(self, aiogd_update: DialogUpdateEvent):
        super().__init__(update_id=0, aiogd_update=aiogd_update)

    @property
    def event_type(self) -> str:
        return DIALOG_EVENT_NAME

    @property
    def event(self) -> DialogUpdateEvent:
        return self.aiogd_update
