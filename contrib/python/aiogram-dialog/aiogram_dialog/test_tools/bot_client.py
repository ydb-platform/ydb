import uuid
from datetime import datetime
from typing import Any

from aiogram import Bot, Dispatcher
from aiogram.methods import AnswerCallbackQuery, TelegramMethod
from aiogram.types import (
    CallbackQuery,
    Chat,
    ChatJoinRequest,
    ChatMemberAdministrator,
    ChatMemberBanned,
    ChatMemberLeft,
    ChatMemberMember,
    ChatMemberOwner,
    ChatMemberRestricted,
    ChatMemberUpdated,
    InlineKeyboardButton,
    Message,
    Update,
    User,
)

from .keyboard import InlineButtonLocator


class FakeBot(Bot):
    def __init__(self):
        pass  # do not call super, so it is invalid bot, used only as a stub

    @property
    def id(self):
        return 1

    async def __call__(
            self, method: TelegramMethod[Any],
            request_timeout: int | None = None,
    ) -> Any:
        del request_timeout  # unused
        if isinstance(method, AnswerCallbackQuery):
            return True
        raise RuntimeError("Fake bot should not be used to call telegram")

    def __hash__(self) -> int:
        return 1

    def __eq__(self, other) -> bool:
        return self is other


ChatMember = (
    ChatMemberOwner |
    ChatMemberAdministrator |
    ChatMemberMember |
    ChatMemberRestricted |
    ChatMemberLeft |
    ChatMemberBanned
)

class BotClient:
    def __init__(
            self,
            dp: Dispatcher,
            user_id: int = 1,
            chat_id: int = 1,
            chat_type: str = "private",
            bot: Bot | None = None,
    ):
        self.chat = Chat(id=chat_id, type=chat_type)
        self.user = User(
            id=user_id, is_bot=False,
            first_name=f"User_{user_id}",
        )
        self.dp = dp
        self.last_update_id = 1
        self.last_message_id = 1
        self.bot = bot or FakeBot()

    def _new_update_id(self):
        self.last_update_id += 1
        return self.last_update_id

    def _new_message_id(self):
        self.last_message_id += 1
        return self.last_message_id

    def _new_message(
            self, text: str, reply_to: Message | None,
    ):
        return Message(
            message_id=self._new_message_id(),
            date=datetime.fromtimestamp(1234567890),
            chat=self.chat,
            from_user=self.user,
            text=text,
            reply_to_message=reply_to,
        )

    async def send(self, text: str, reply_to: Message | None = None):
        return await self.dp.feed_update(self.bot, Update(
            update_id=self._new_update_id(),
            message=self._new_message(text, reply_to),
        ))

    def _new_callback(
            self, message: Message, button: InlineKeyboardButton,
    ) -> CallbackQuery:
        if not button.callback_data:
            raise ValueError("Button has no callback data")
        return CallbackQuery(
            id=str(uuid.uuid4()),
            data=button.callback_data,
            chat_instance="--",
            from_user=self.user,
            message=message,
        )

    async def click(
            self, message: Message,
            locator: InlineButtonLocator,
    ) -> str:
        button = locator.find_button(message)
        if not button:
            raise ValueError(
                f"No button matching {locator} found",
            )

        callback = self._new_callback(message, button)
        await self.dp.feed_update(self.bot, Update(
            update_id=self._new_update_id(),
            callback_query=callback,
        ))
        return callback.id

    async def request_chat_join(self):
        return await self.dp.feed_update(self.bot, Update(
            update_id=self._new_update_id(),
            chat_join_request=ChatJoinRequest(
                chat=self.chat,
                from_user=self.user,
                date=datetime.fromtimestamp(1234567890),
                user_chat_id=self.user.id,
            ),
        ))

    async def my_chat_member_update(
            self, old_chat_member: ChatMember, new_chat_member: ChatMember,
    ):
        return await self.dp.feed_update(self.bot, Update(
            update_id=self._new_update_id(),
            my_chat_member=ChatMemberUpdated(
                chat=self.chat,
                from_user=self.user,
                date=datetime.fromtimestamp(1234567890),
                old_chat_member=old_chat_member,
                new_chat_member=new_chat_member,
            ),
        ))
