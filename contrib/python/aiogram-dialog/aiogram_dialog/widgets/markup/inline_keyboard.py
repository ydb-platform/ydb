from aiogram.types import InlineKeyboardMarkup

from aiogram_dialog import DialogManager
from aiogram_dialog.api.internal.widgets import (
    MarkupFactory,
    MarkupVariant,
    RawKeyboard,
)
from aiogram_dialog.utils import add_intent_id


class InlineKeyboardFactory(MarkupFactory):
    async def render_markup(
            self, data: dict, manager: DialogManager, keyboard: RawKeyboard,
    ) -> MarkupVariant:
        # TODO validate buttons
        add_intent_id(keyboard, manager.current_context().id)
        return InlineKeyboardMarkup(
            inline_keyboard=keyboard,
        )
