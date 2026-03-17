from typing import Any, Literal

from aiogram.methods import AnswerCallbackQuery
from aiogram.types import (
    CallbackQuery,
    Chat,
    Message,
    User,
)


class ReplyCallbackQuery(CallbackQuery):
    original_message: Message

    def answer(self, *args: Any, **kwargs: Any) -> AnswerCallbackQuery:
        raise ValueError(
            "This callback query is generated from ReplyButton click. "
            "Support of `.answer()` call is impossible.",
        )


class FakeUser(User):
    fake: Literal[True] = True


class FakeChat(Chat):
    fake: Literal[True] = True
