from logging import getLogger

from aiogram.enums import ChatType

from aiogram_dialog import ChatEvent
from aiogram_dialog.api.entities import (
    Context,
    Stack,
)
from aiogram_dialog.api.protocols import StackAccessValidator

logger = getLogger(__name__)


class DefaultAccessValidator(StackAccessValidator):
    async def is_allowed(
            self,
            stack: Stack,
            context: Context | None,
            event: ChatEvent,
            data: dict,
    ) -> bool:
        if context:
            access_settings = context.access_settings
        else:
            access_settings = stack.access_settings

        if not access_settings:
            return True
        chat = data["event_chat"]
        if chat.type is ChatType.PRIVATE:
            return True
        if access_settings.user_ids:
            user = data["event_from_user"]
            if user.id not in access_settings.user_ids:
                return False
        return True
