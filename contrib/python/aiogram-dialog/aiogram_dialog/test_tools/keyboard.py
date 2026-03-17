import re
from typing import Protocol

from aiogram.types import InlineKeyboardButton, Message


class InlineButtonLocator(Protocol):
    def find_button(
            self, message: Message,
    ) -> InlineKeyboardButton | None:
        raise NotImplementedError


class InlineButtonTextLocator:
    def __init__(self, regex: str):
        self.regex = re.compile(regex)

    def find_button(
            self, message: Message,
    ) -> InlineKeyboardButton | None:
        if not message.reply_markup:
            return None
        for row in message.reply_markup.inline_keyboard:
            for button in row:
                if self.regex.fullmatch(button.text):
                    return button
        return None

    def __repr__(self):
        return f"InlineButtonTextLocator({self.regex.pattern!r})"


class InlineButtonPositionLocator:
    def __init__(self, row: int, column: int):
        self.row = row
        self.column = column

    def find_button(
            self, message: Message,
    ) -> InlineKeyboardButton | None:
        if not message.reply_markup:
            return None
        try:
            return message.reply_markup.inline_keyboard[self.row][self.column]
        except IndexError:
            return None

    def __repr__(self):
        return f"InlineButtonPositionLocator" \
               f"(row={self.row}, column={self.column})"


class InlineButtonDataLocator:
    def __init__(self, regex: str):
        self.regex = re.compile(regex)

    def find_button(
            self, message: Message,
    ) -> InlineKeyboardButton | None:
        if not message.reply_markup:
            return None
        for row in message.reply_markup.inline_keyboard:
            for button in row:
                if not button.callback_data:
                    continue
                if self.regex.fullmatch(button.callback_data):
                    return button
        return None

    def __repr__(self):
        return f"InlineButtonDataLocator({self.regex.pattern!r})"
