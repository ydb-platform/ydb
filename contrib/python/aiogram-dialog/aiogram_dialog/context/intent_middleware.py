from collections.abc import Awaitable, Callable
from logging import getLogger
from typing import Any

from aiogram import Router
from aiogram.dispatcher.event.bases import UNHANDLED
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.fsm.storage.base import BaseEventIsolation, BaseStorage
from aiogram.types import (
    CallbackQuery,
    ChatJoinRequest,
    ChatMemberUpdated,
    InaccessibleMessage,
    Message,
)
from aiogram.types.error_event import ErrorEvent

from aiogram_dialog.api.entities import (
    DEFAULT_STACK_ID,
    EVENT_CONTEXT_KEY,
    ChatEvent,
    Context,
    DialogUpdate,
    DialogUpdateEvent,
    EventContext,
    Stack,
)
from aiogram_dialog.api.exceptions import (
    InvalidStackIdError,
    OutdatedIntent,
    UnknownIntent,
    UnknownState,
)
from aiogram_dialog.api.internal import (
    CALLBACK_DATA_KEY,
    CONTEXT_KEY,
    EVENT_SIMULATED,
    STACK_KEY,
    STORAGE_KEY,
    ReplyCallbackQuery,
)
from aiogram_dialog.api.protocols import (
    DialogRegistryProtocol,
    StackAccessValidator,
)
from aiogram_dialog.utils import remove_intent_id, split_reply_callback
from .storage import StorageProxy

logger = getLogger(__name__)

FORBIDDEN_STACK_KEY = "aiogd_stack_forbidden"


def get_thread_id(message: Message) -> int | None:
    if not message.is_topic_message:
        return None
    return message.message_thread_id


def event_context_from_callback(event: CallbackQuery) -> EventContext:
    # native inaccessible message has no `business_connection_id`
    # we can do nothing here
    business_connection_id = getattr(
        event.message,
        "business_connection_id",
        None,
    )
    return EventContext(
        bot=event.bot,
        user=event.from_user,
        chat=event.message.chat,
        thread_id=(
            get_thread_id(event.message)
            if isinstance(
                event.message,
                (Message, InaccessibleBusinessMessage),
            )
            else None
        ),
        business_connection_id=business_connection_id,
    )


def event_context_from_chat_member(event: ChatMemberUpdated) -> EventContext:
    return EventContext(
        bot=event.bot,
        user=event.from_user,
        chat=event.chat,
        thread_id=None,
        business_connection_id=None,
    )


def event_context_from_chat_join(event: ChatJoinRequest) -> EventContext:
    return EventContext(
        bot=event.bot,
        user=event.from_user,
        chat=event.chat,
        thread_id=None,
        business_connection_id=None,
    )


def event_context_from_message(event: Message) -> EventContext:
    return EventContext(
        bot=event.bot,
        user=event.from_user,
        chat=event.chat,
        thread_id=get_thread_id(event),
        business_connection_id=event.business_connection_id,
    )


def event_context_from_aiogd(event: DialogUpdateEvent) -> EventContext:
    return EventContext(
        bot=event.bot,
        user=event.from_user,
        chat=event.chat,
        thread_id=event.thread_id,
        business_connection_id=event.business_connection_id,
    )


def event_context_from_error(event: ErrorEvent) -> EventContext:
    if event.update.message:
        return event_context_from_message(event.update.message)
    elif event.update.business_message:
        return event_context_from_message(event.update.business_message)
    elif event.update.my_chat_member:
        return event_context_from_chat_member(event.update.my_chat_member)
    elif event.update.chat_join_request:
        return event_context_from_chat_join(event.update.chat_join_request)
    elif event.update.callback_query:
        return event_context_from_callback(event.update.callback_query)
    elif isinstance(event.update, DialogUpdate) and event.update.aiogd_update:
        return event_context_from_aiogd(event.update.aiogd_update)
    raise ValueError("Unsupported event type in ErrorEvent.update")


class InaccessibleBusinessMessage(InaccessibleMessage):
    business_connection_id: str | None = None
    message_thread_id: int | None = None
    is_topic_message: bool | None = None


class IntentMiddlewareFactory:
    def __init__(
            self,
            registry: DialogRegistryProtocol,
            access_validator: StackAccessValidator,
            events_isolation: BaseEventIsolation,
    ):
        super().__init__()
        self.registry = registry
        self.access_validator = access_validator
        self.events_isolation = events_isolation

    def storage_proxy(
            self, event_context: EventContext, fsm_storage: BaseStorage,
    ) -> StorageProxy:
        return StorageProxy(
            bot=event_context.bot,
            storage=fsm_storage,
            events_isolation=self.events_isolation,
            state_groups=self.registry.states_groups(),
            user_id=event_context.user.id,
            chat_id=event_context.chat.id,
            thread_id=event_context.thread_id,
            business_connection_id=event_context.business_connection_id,
        )

    def _check_outdated(self, intent_id: str, stack: Stack):
        """Check if intent id is outdated for stack."""
        if stack.empty():
            raise OutdatedIntent(
                stack.id,
                f"Outdated intent id ({intent_id}) "
                f"for stack ({stack.id})",
            )
        if intent_id != stack.last_intent_id():
            raise OutdatedIntent(
                stack.id,
                f"Outdated intent id ({intent_id}) "
                f"for stack ({stack.id})",
            )

    async def _load_stack(
            self,
            event: ChatEvent,
            stack_id: str | None,
            proxy: StorageProxy,
            data: dict,
    ) -> Stack | None:
        if stack_id is None:
            raise InvalidStackIdError("Both stack id and intent id are None")
        return await proxy.load_stack(stack_id)

    async def _load_context_by_stack(
            self,
            event: ChatEvent,
            proxy: StorageProxy,
            stack_id: str | None,
            data: dict,
    ) -> None:
        logger.debug(
            "Loading context for stack: "
            "`%s`, user: `%s`, chat: `%s`, thread: `%s`",
            stack_id, proxy.user_id, proxy.chat_id, proxy.thread_id,
        )
        stack = await self._load_stack(event, stack_id, proxy, data)
        if not stack:
            return
        if stack.empty():
            context = None
        else:
            try:
                context = await proxy.load_context(stack.last_intent_id())
            except:
                await proxy.unlock()
                raise

        if not await self.access_validator.is_allowed(
                stack, context, event, data,
        ):
            logger.debug(
                "Stack %s is not allowed for user %s",
                stack.id, proxy.user_id,
            )
            data[FORBIDDEN_STACK_KEY] = True
            await proxy.unlock()
            return
        data[STORAGE_KEY] = proxy
        data[STACK_KEY] = stack
        data[CONTEXT_KEY] = context

    async def _load_context_by_intent(
            self,
            event: ChatEvent,
            proxy: StorageProxy,
            intent_id: str | None,
            data: dict,
    ) -> None:
        logger.debug(
            "Loading context for intent: `%s`, user: `%s`, chat: `%s`",
            intent_id, proxy.user_id, proxy.chat_id,
        )
        context = await proxy.load_context(intent_id)
        stack = await self._load_stack(event, context.stack_id, proxy, data)
        if not stack:
            return
        try:
            self._check_outdated(intent_id, stack)
        except:
            await proxy.unlock()
            raise

        if not await self.access_validator.is_allowed(
                stack, context, event, data,
        ):
            logger.debug(
                "Stack %s is not allowed for user %s",
                stack.id, proxy.user_id,
            )
            data[FORBIDDEN_STACK_KEY] = True
            await proxy.unlock()
            return
        data[STORAGE_KEY] = proxy
        data[STACK_KEY] = stack
        data[CONTEXT_KEY] = context

    async def _load_default_context(
            self, event: ChatEvent, data: dict, event_context: EventContext,
    ) -> None:
        return await self._load_context_by_stack(
            event=event,
            proxy=self.storage_proxy(event_context, data["fsm_storage"]),
            stack_id=DEFAULT_STACK_ID,
            data=data,
        )

    def _intent_id_from_reply(
            self, event: Message, data: dict,
    ) -> str | None:
        if not (
                event.reply_to_message and
                event.reply_to_message.from_user.id == data["bot"].id and
                event.reply_to_message.reply_markup and
                event.reply_to_message.reply_markup.inline_keyboard
        ):
            return None
        for row in event.reply_to_message.reply_markup.inline_keyboard:
            for button in row:
                if button.callback_data:
                    intent_id, _ = remove_intent_id(button.callback_data)
                    return intent_id
        return None

    async def process_message(
            self,
            handler: Callable,
            event: Message,
            data: dict,
    ):
        event_context = event_context_from_message(event)
        data[EVENT_CONTEXT_KEY] = event_context

        _, callback_data = split_reply_callback(event.text)
        if callback_data:
            query = ReplyCallbackQuery(
                id="",
                message=InaccessibleBusinessMessage(
                    chat=event.chat,
                    message_id=event.message_id,
                    business_connection_id=event.business_connection_id,
                    message_thread_id=event.message_thread_id,
                    is_topic_message=event.is_topic_message,
                ),
                original_message=event,
                data=callback_data,
                from_user=event.from_user,
                # we cannot know real chat instance
                chat_instance=str(event.chat.id),
            ).as_(data["bot"])
            router: Router = data["event_router"]
            return await router.propagate_event(
                "callback_query",
                query,
                **{EVENT_SIMULATED: True},
                **data,
            )

        if intent_id := self._intent_id_from_reply(event, data):
            await self._load_context_by_intent(
                event=event,
                proxy=self.storage_proxy(event_context, data["fsm_storage"]),
                intent_id=intent_id,
                data=data,
            )
        else:
            await self._load_default_context(event, data, event_context)
        return await handler(event, data)

    async def process_my_chat_member(
            self,
            handler: Callable,
            event: ChatMemberUpdated,
            data: dict,
    ) -> None:
        event_context = event_context_from_chat_member(event)
        data[EVENT_CONTEXT_KEY] = event_context

        await self._load_default_context(event, data, event_context)
        return await handler(event, data)

    async def process_chat_join_request(
            self,
            handler: Callable,
            event: ChatJoinRequest,
            data: dict,
    ) -> None:
        event_context = event_context_from_chat_join(event)
        data[EVENT_CONTEXT_KEY] = event_context

        await self._load_default_context(event, data, event_context)
        return await handler(event, data)

    async def process_aiogd_update(
            self,
            handler: Callable,
            event: DialogUpdateEvent,
            data: dict,
    ):
        event_context = event_context_from_aiogd(event)
        data[EVENT_CONTEXT_KEY] = event_context

        if event.intent_id:
            await self._load_context_by_intent(
                event=event,
                proxy=self.storage_proxy(event_context, data["fsm_storage"]),
                intent_id=event.intent_id,
                data=data,
            )
        else:
            await self._load_context_by_stack(
                event=event,
                proxy=self.storage_proxy(event_context, data["fsm_storage"]),
                stack_id=event.stack_id,
                data=data,
            )
        return await handler(event, data)

    async def process_callback_query(
            self,
            handler: Callable,
            event: CallbackQuery,
            data: dict,
    ):
        if "event_chat" not in data:
            return await handler(event, data)

        event_context = event_context_from_callback(event)
        data[EVENT_CONTEXT_KEY] = event_context
        original_data = event.data
        if event.data:
            intent_id, _ = remove_intent_id(event.data)
            if intent_id:
                await self._load_context_by_intent(
                    event=event,
                    proxy=self.storage_proxy(
                        event_context,
                        data["fsm_storage"],
                    ),
                    intent_id=intent_id,
                    data=data,
                )
            else:
                await self._load_default_context(event, data, event_context)
            data[CALLBACK_DATA_KEY] = original_data
        else:
            await self._load_default_context(event, data, event_context)
        result = await handler(event, data)
        if result is UNHANDLED and data.get(FORBIDDEN_STACK_KEY):
            await event.answer()
        return result


SUPPORTED_ERROR_EVENTS = {
    "message",
    "callback_query",
    "my_chat_member",
    "aiogd_update",
    "chat_join_request",
    "business_message",
}


async def context_saver_middleware(handler, event, data):
    result = await handler(event, data)
    proxy: StorageProxy = data.pop(STORAGE_KEY, None)
    if proxy:
        await proxy.save_context(data.pop(CONTEXT_KEY))
        await proxy.save_stack(data.pop(STACK_KEY))
    return result


async def context_unlocker_middleware(handler, event, data):
    proxy: StorageProxy = data.get(STORAGE_KEY, None)
    try:
        result = await handler(event, data)
    finally:
        if proxy:
            await proxy.unlock()
    return result


class IntentErrorMiddleware(BaseMiddleware):
    def __init__(
            self,
            registry: DialogRegistryProtocol,
            access_validator: StackAccessValidator,
            events_isolation: BaseEventIsolation,
    ):
        super().__init__()
        self.registry = registry
        self.events_isolation = events_isolation
        self.access_validator = access_validator

    def _is_error_supported(
            self, event: ErrorEvent, data: dict[str, Any],
    ) -> bool:
        if isinstance(event, InvalidStackIdError):
            return False
        if event.update.event_type not in SUPPORTED_ERROR_EVENTS:
            return False
        if "event_chat" not in data:
            return False
        return "event_from_user" in data

    async def _fix_broken_stack(
            self, storage: StorageProxy, stack: Stack,
    ) -> None:
        while not stack.empty():
            await storage.remove_context(stack.pop())
        await storage.save_stack(stack)

    async def _load_last_context(
            self, storage: StorageProxy, stack: Stack,
    ) -> Context | None:
        try:
            return await storage.load_context(stack.last_intent_id())
        except (UnknownIntent, OutdatedIntent):
            logger.warning(
                "Stack is broken for user %s, chat %s, resetting",
                storage.user_id, storage.chat_id,
            )
            await self._fix_broken_stack(storage, stack)
        return None

    async def _load_stack(
            self, proxy: StorageProxy, error: Exception,
    ) -> Stack:
        if isinstance(error, OutdatedIntent):
            return await proxy.load_stack(stack_id=error.stack_id)
        else:
            return await proxy.load_stack()

    async def __call__(
            self,
            handler: Callable[
                [ErrorEvent, dict[str, Any]], Awaitable[Any],
            ],
            event: ErrorEvent,
            data: dict[str, Any],
    ) -> Any:
        error = event.exception
        if not self._is_error_supported(event, data):
            return await handler(event, data)

        try:
            event_context = event_context_from_error(event)
            data[EVENT_CONTEXT_KEY] = event_context
            proxy = StorageProxy(
                bot=event_context.bot,
                storage=data["fsm_storage"],
                events_isolation=self.events_isolation,
                state_groups=self.registry.states_groups(),
                user_id=event_context.user.id,
                chat_id=event_context.chat.id,
                thread_id=event_context.thread_id,
                business_connection_id=event_context.business_connection_id,
            )
            data[STORAGE_KEY] = proxy
            stack = await self._load_stack(proxy, event)
            if stack.empty() or isinstance(error, UnknownState):
                context = None
            else:
                context = await self._load_last_context(
                    storage=proxy,
                    stack=stack,
                )

            if await self.access_validator.is_allowed(
                    stack, context, event.update.event, data,
            ):
                data[STACK_KEY] = stack
                data[CONTEXT_KEY] = context
            else:
                logger.debug(
                    "Stack %s is not allowed for user %s",
                    stack.id, proxy.user_id,
                )
                data[FORBIDDEN_STACK_KEY] = True
                del data[STORAGE_KEY]
                await proxy.unlock()
            return await handler(event, data)
        finally:
            proxy: StorageProxy = data.pop(STORAGE_KEY, None)
            if proxy:
                await proxy.unlock()
                context = data.pop(CONTEXT_KEY)
                if context is not None:
                    await proxy.save_context(context)
                await proxy.save_stack(data.pop(STACK_KEY))
