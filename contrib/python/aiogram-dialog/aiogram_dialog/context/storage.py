from contextlib import AsyncExitStack
from copy import copy

from aiogram import Bot
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import (
    BaseEventIsolation,
    BaseStorage,
    StorageKey,
)

from aiogram_dialog.api.entities import (
    DEFAULT_STACK_ID,
    AccessSettings,
    Context,
    Stack,
)
from aiogram_dialog.api.exceptions import UnknownIntent, UnknownState


class StorageProxy:
    def __init__(
            self,
            storage: BaseStorage,
            events_isolation: BaseEventIsolation,
            user_id: int | None,
            chat_id: int,
            thread_id: int | None,
            business_connection_id: str | None,
            bot: Bot,
            state_groups: dict[str, type[StatesGroup]],
    ):
        self.storage = storage
        self.events_isolation = events_isolation
        self.state_groups = state_groups
        self.user_id = user_id
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.business_connection_id = business_connection_id
        self.bot = bot
        self.lock_stack = AsyncExitStack()

    async def lock(self, key: StorageKey):
        await self.lock_stack.enter_async_context(
            self.events_isolation.lock(key),
        )

    async def unlock(self):
        await self.lock_stack.aclose()

    async def load_context(self, intent_id: str) -> Context:
        data = await self.storage.get_data(
            key=self._context_key(intent_id),
        )
        if not data:
            raise UnknownIntent(
                f"Context not found for intent id: {intent_id}",
            )
        data["access_settings"] = self._parse_access_settings(
            data.pop("access_settings", None),
        )
        data["state"] = self._state(data["state"])
        return Context(**data)

    def _default_access_settings(self, stack_id: str) -> AccessSettings:
        if stack_id == DEFAULT_STACK_ID and self.user_id:
            return AccessSettings(user_ids=[self.user_id])
        else:
            return AccessSettings(user_ids=[])

    async def load_stack(self, stack_id: str = DEFAULT_STACK_ID) -> Stack:
        fixed_stack_id = self._fixed_stack_id(stack_id)
        key = self._stack_key(fixed_stack_id)
        await self.lock(key)
        data = await self.storage.get_data(key)
        data.pop("access_settings", None)  # compat with 2.2a5
        access_settings = self._default_access_settings(stack_id)
        if not data:
            return Stack(_id=fixed_stack_id, access_settings=access_settings)
        return Stack(access_settings=access_settings, **data)

    async def save_context(self, context: Context | None) -> None:
        if not context:
            return
        data = copy(vars(context))
        data["state"] = data["state"].state
        data["access_settings"] = self._dump_access_settings(
            context.access_settings,
        )
        await self.storage.set_data(
            key=self._context_key(context.id),
            data=data,
        )

    async def remove_context(self, intent_id: str):
        await self.storage.set_data(
            key=self._context_key(intent_id),
            data={},
        )

    async def remove_stack(self, stack_id: str):
        await self.storage.set_data(
            key=self._stack_key(stack_id),
            data={},
        )

    async def save_stack(self, stack: Stack | None) -> None:
        if not stack:
            return
        if stack.empty() and not stack.last_message_id:
            await self.storage.set_data(
                key=self._stack_key(stack.id),
                data={},
            )
        else:
            data = {
                "_id": stack.id,
                "intents": stack.intents,
                "last_message_id": stack.last_message_id,
                "last_reply_keyboard": stack.last_reply_keyboard,
                "last_media_id": stack.last_media_id,
                "last_media_unique_id": stack.last_media_unique_id,
                "last_income_media_group_id": stack.last_income_media_group_id,
            }
            await self.storage.set_data(
                key=self._stack_key(stack.id),
                data=data,
            )

    def _context_key(self, intent_id: str) -> StorageKey:
        return StorageKey(
            bot_id=self.bot.id,
            chat_id=self.chat_id,
            user_id=self.chat_id,
            thread_id=self.thread_id,
            business_connection_id=self.business_connection_id,
            destiny=f"aiogd:context:{intent_id}",
        )

    def _fixed_stack_id(self, stack_id: str) -> str:
        if stack_id != DEFAULT_STACK_ID:
            return stack_id
        # private chat has chat_id=user_id and no business connection
        if (
                self.user_id in (None, self.chat_id) and
                self.business_connection_id is None
        ):
            return stack_id
        return f"<{self.user_id}>"

    def _stack_key(self, stack_id: str) -> StorageKey:
        stack_id = self._fixed_stack_id(stack_id)
        return StorageKey(
            bot_id=self.bot.id,
            chat_id=self.chat_id,
            user_id=self.chat_id,
            thread_id=self.thread_id,
            business_connection_id=self.business_connection_id,
            destiny=f"aiogd:stack:{stack_id}",
        )

    def _state(self, state: str) -> State:
        group, *_ = state.partition(":")
        try:
            for real_state in self.state_groups[group].__all_states__:
                if real_state.state == state:
                    return real_state
        except KeyError:
            raise UnknownState(f"Unknown state group {group}") from None
        raise UnknownState(f"Unknown state {state}")

    def _parse_access_settings(
            self, raw: dict | None,
    ) -> AccessSettings | None:
        if not raw:
            return None
        return AccessSettings(
            user_ids=raw.get("user_ids") or [],
            custom=raw.get("custom"),
        )

    def _dump_access_settings(
            self, access_settings: AccessSettings | None,
    ) -> dict | None:
        if not access_settings:
            return None
        return {
            "user_ids": access_settings.user_ids,
            "custom": access_settings.custom,
        }
