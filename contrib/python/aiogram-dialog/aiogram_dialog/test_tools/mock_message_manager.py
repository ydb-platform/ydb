from copy import deepcopy
from datetime import datetime
from uuid import uuid4

from aiogram import Bot
from aiogram.types import (
    Audio,
    CallbackQuery,
    Document,
    Message,
    PhotoSize,
    ReplyKeyboardMarkup,
    Video,
)

from aiogram_dialog import ShowMode
from aiogram_dialog.api.entities import MediaAttachment, NewMessage, OldMessage
from aiogram_dialog.api.protocols import (
    MessageManagerProtocol,
    MessageNotModified,
)


def file_id(media: MediaAttachment) -> str:
    file_id_ = None
    if media.file_id:
        file_id_ = media.file_id.file_id
    return file_id_ or str(uuid4())


def file_unique_id(media: MediaAttachment) -> str:
    file_unique_id_ = None
    if media.file_id:
        file_unique_id_ = media.file_id.file_unique_id
    return file_unique_id_ or str(uuid4())


MEDIA_CLASSES = {
    "audio": lambda x: Audio(
        file_id=file_id(x), file_unique_id=file_unique_id(x),
        duration=1024,
    ),
    "document": lambda x: Document(
        file_id=file_id(x), file_unique_id=file_unique_id(x),
    ),
    "photo": lambda x: [PhotoSize(
        file_id=file_id(x), file_unique_id=file_unique_id(x),
        width=1024, height=1024,
    )],
    "video": lambda x: Video(
        file_id=file_id(x), file_unique_id=file_unique_id(x),
        width=1024, height=1024, duration=1024,
    ),
}


class MockMessageManager(MessageManagerProtocol):
    def __init__(self):
        self.answered_callbacks: set[str] = set()
        self.sent_messages = []
        self.last_message_id = 0

    def reset_history(self):
        self.sent_messages.clear()
        self.answered_callbacks.clear()

    def assert_one_message(self) -> None:
        assert len(self.sent_messages) == 1

    def last_message(self) -> Message:
        return self.sent_messages[-1]

    def first_message(self) -> Message:
        return self.sent_messages[0]

    def one_message(self) -> Message:
        self.assert_one_message()
        return self.first_message()

    async def remove_kbd(
            self,
            bot: Bot,
            show_mode: ShowMode,
            old_message: OldMessage | None,
    ) -> Message | None:
        if not old_message:
            return None
        if show_mode in (ShowMode.DELETE_AND_SEND, ShowMode.NO_UPDATE):
            return None
        assert isinstance(old_message, OldMessage)

        message = Message(
            message_id=old_message.message_id,
            date=datetime.now(),
            chat=old_message.chat,
            reply_markup=None,
        )
        self.sent_messages.append(message)
        return message

    async def answer_callback(
            self, bot: Bot, callback_query: CallbackQuery,
    ) -> None:
        self.answered_callbacks.add(callback_query.id)

    def assert_answered(self, callback_id: str) -> None:
        assert callback_id in self.answered_callbacks

    async def show_message(self, bot: Bot, new_message: NewMessage,
                           old_message: OldMessage | None) -> OldMessage:
        assert isinstance(new_message, NewMessage)
        assert isinstance(old_message, (OldMessage, type(None)))
        if new_message.show_mode is ShowMode.NO_UPDATE:
            raise MessageNotModified

        message_id = self.last_message_id + 1
        self.last_message_id = message_id

        if new_message.media:
            contents = {
                "caption": new_message.text,
                new_message.media.type: MEDIA_CLASSES[new_message.media.type](
                    new_message.media,
                ),
            }
        else:
            contents = {
                "text": new_message.text,
            }

        message = Message(
            message_id=message_id,
            date=datetime.now(),
            chat=new_message.chat,
            reply_markup=deepcopy(new_message.reply_markup),
            **contents,
        )
        self.sent_messages.append(message)

        return OldMessage(
            message_id=message_id,
            chat=new_message.chat,
            text=new_message.text,
            media_id=(
                file_id(new_message.media)
                if new_message.media
                else None
            ),
            media_uniq_id=(
                file_unique_id(new_message.media)
                if new_message.media
                else None
            ),
            has_reply_keyboard=isinstance(
                new_message.reply_markup, ReplyKeyboardMarkup,
            ),
            business_connection_id=None,
        )
