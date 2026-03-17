from logging import getLogger
from typing import Any

from aiogram import Bot, Router
from aiogram.fsm.state import State
from aiogram.types import Chat, User

from aiogram_dialog.api.entities import (
    DEFAULT_STACK_ID,
    AccessSettings,
    Data,
    DialogAction,
    DialogStartEvent,
    DialogSwitchEvent,
    DialogUpdate,
    DialogUpdateEvent,
    EventContext,
    ShowMode,
    StartMode,
)
from aiogram_dialog.api.internal import (
    FakeChat,
    FakeUser,
)
from aiogram_dialog.api.protocols import (
    BaseDialogManager,
    BgManagerFactory,
    UnsetId,
)
from aiogram_dialog.manager.updater import Updater
from aiogram_dialog.utils import is_chat_loaded, is_user_loaded

logger = getLogger(__name__)


def coalesce_business_connection_id(
        *,
        user: User,
        chat: Chat,
        business_connection_id: str | UnsetId | None,
        event_context: EventContext,
) -> str | None:
    if business_connection_id is not UnsetId.UNSET:
        return business_connection_id
    if user.id != event_context.user.id:
        return None
    if chat.id != event_context.chat.id:
        return None
    return event_context.business_connection_id


def coalesce_thread_id(
        *,
        user: User,
        chat: Chat,
        thread_id: str | UnsetId | None,
        event_context: EventContext,
) -> str | None:
    if thread_id is not UnsetId.UNSET:
        return thread_id
    if user.id != event_context.user.id:
        return None
    if chat.id != event_context.chat.id:
        return None
    return None


class BgManager(BaseDialogManager):
    def __init__(
            self,
            user: User,
            chat: Chat,
            bot: Bot,
            router: Router,
            intent_id: str | None,
            stack_id: str | None,
            thread_id: int | None = None,
            business_connection_id: str | None = None,
            load: bool = False,
    ):
        self._event_context = EventContext(
            chat=chat,
            user=user,
            bot=bot,
            thread_id=thread_id,
            business_connection_id=business_connection_id,
        )
        self._router = router
        self._updater = Updater(router)
        self.intent_id = intent_id
        self.stack_id = stack_id
        self.load = load

    def _get_fake_user(self, user_id: int | None = None) -> User:
        """Get User if we have info about him or FakeUser instead."""
        if user_id in (None, self._event_context.user.id):
            return self._event_context.user
        return FakeUser(id=user_id, is_bot=False, first_name="")

    def _get_fake_chat(self, chat_id: int | None = None) -> Chat:
        """Get Chat if we have info about him or FakeChat instead."""
        if chat_id in (None, self._event_context.chat.id):
            return self._event_context.chat
        return FakeChat(id=chat_id, type="")

    def bg(
            self,
            user_id: int | None = None,
            chat_id: int | None = None,
            stack_id: str | None = None,
            thread_id: str | UnsetId | None = UnsetId.UNSET,
            business_connection_id: str | UnsetId | None = UnsetId.UNSET,
            load: bool = False,
    ) -> "BaseDialogManager":
        chat = self._get_fake_chat(chat_id)
        user = self._get_fake_user(user_id)

        new_event_context = EventContext(
            bot=self._event_context.bot,
            chat=chat,
            user=user,
            thread_id=coalesce_thread_id(
                chat=chat,
                user=user,
                thread_id=thread_id,
                event_context=self._event_context,
            ),
            business_connection_id=coalesce_business_connection_id(
                chat=chat,
                user=user,
                business_connection_id=business_connection_id,
                event_context=self._event_context,
            ),
        )
        if stack_id is None:
            if self._event_context == new_event_context:
                stack_id = self.stack_id
                intent_id = self.intent_id
            else:
                stack_id = DEFAULT_STACK_ID
                intent_id = None
        else:
            intent_id = None

        return BgManager(
            user=new_event_context.user,
            chat=new_event_context.chat,
            bot=new_event_context.bot,
            router=self._router,
            intent_id=intent_id,
            stack_id=stack_id,
            thread_id=new_event_context.thread_id,
            business_connection_id=new_event_context.business_connection_id,
            load=load,
        )

    def _base_event_params(self):
        return {
            "from_user": self._event_context.user,
            "chat": self._event_context.chat,
            "intent_id": self.intent_id,
            "stack_id": self.stack_id,
            "thread_id": self._event_context.thread_id,
            "business_connection_id":
                self._event_context.business_connection_id,
        }

    async def _notify(self, event: DialogUpdateEvent):
        bot = self._event_context.bot
        update = DialogUpdate(aiogd_update=event.as_(bot)).as_(bot)
        await self._updater.notify(bot=bot, update=update)

    async def _load(self):
        if self.load:
            bot = self._event_context.bot
            if not is_chat_loaded(self._event_context.chat):
                logger.debug(
                    "load chat: %s", self._event_context.chat.id,
                )
                self._event_context.chat = await bot.get_chat(
                    self._event_context.chat.id,
                )
            if not is_user_loaded(self._event_context.user):
                logger.debug(
                    "load user %s from chat %s",
                    self._event_context.chat.id, self._event_context.user.id,
                )
                chat_member = await bot.get_chat_member(
                    self._event_context.chat.id, self._event_context.user.id,
                )
                self._event_context.user = chat_member.user

    async def done(
            self,
            result: Any = None,
            show_mode: ShowMode | None = None,
    ) -> None:
        await self._load()
        await self._notify(
            DialogUpdateEvent(
                action=DialogAction.DONE,
                data=result,
                show_mode=show_mode,
                **self._base_event_params(),
            ),
        )

    async def start(
            self,
            state: State,
            data: Data = None,
            mode: StartMode = StartMode.NORMAL,
            show_mode: ShowMode | None = None,
            access_settings: AccessSettings | None = None,
    ) -> None:
        await self._load()
        await self._notify(
            DialogStartEvent(
                action=DialogAction.START,
                data=data,
                new_state=state,
                mode=mode,
                show_mode=show_mode,
                access_settings=access_settings,
                **self._base_event_params(),
            ),
        )

    async def switch_to(
            self,
            state: State,
            show_mode: ShowMode | None = None,
    ) -> None:
        await self._load()
        await self._notify(
            DialogSwitchEvent(
                action=DialogAction.SWITCH,
                data={},
                new_state=state,
                show_mode=show_mode,
                **self._base_event_params(),
            ),
        )

    async def update(
            self,
            data: dict,
            show_mode: ShowMode | None = None,
    ) -> None:
        await self._load()
        await self._notify(
            DialogUpdateEvent(
                action=DialogAction.UPDATE, data=data, show_mode=show_mode,
                **self._base_event_params(),
            ),
        )


class BgManagerFactoryImpl(BgManagerFactory):
    def __init__(self, router: Router):
        self._router = router

    def bg(
            self,
            bot: Bot,
            user_id: int,
            chat_id: int,
            stack_id: str | None = None,
            thread_id: int | None = None,
            business_connection_id: str | None = None,
            load: bool = False,
    ) -> "BaseDialogManager":
        chat = FakeChat(id=chat_id, type="")
        user = FakeUser(id=user_id, is_bot=False, first_name="")
        if stack_id is None:
            stack_id = DEFAULT_STACK_ID

        return BgManager(
            user=user,
            chat=chat,
            bot=bot,
            router=self._router,
            intent_id=None,
            stack_id=stack_id,
            thread_id=thread_id,
            business_connection_id=business_connection_id,
            load=load,
        )
