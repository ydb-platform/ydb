from collections.abc import Callable, Iterable

from aiogram import Router
from aiogram.dispatcher.event.telegram import TelegramEventObserver
from aiogram.fsm.state import State, StatesGroup, any_state
from aiogram.fsm.storage.base import BaseEventIsolation
from aiogram.fsm.storage.memory import SimpleEventIsolation

from aiogram_dialog.api.entities import DIALOG_EVENT_NAME
from aiogram_dialog.api.exceptions import UnregisteredDialogError
from aiogram_dialog.api.internal import DialogManagerFactory
from aiogram_dialog.api.protocols import (
    BgManagerFactory,
    DialogProtocol,
    DialogRegistryProtocol,
    MediaIdStorageProtocol,
    MessageManagerProtocol,
    StackAccessValidator,
)
from aiogram_dialog.context.intent_middleware import (
    IntentErrorMiddleware,
    IntentMiddlewareFactory,
    context_saver_middleware,
    context_unlocker_middleware,
)
from aiogram_dialog.context.media_storage import MediaIdStorage
from aiogram_dialog.manager.bg_manager import BgManagerFactoryImpl
from aiogram_dialog.manager.manager_factory import DefaultManagerFactory
from aiogram_dialog.manager.manager_middleware import (
    BgFactoryMiddleware,
    ManagerMiddleware,
)
from aiogram_dialog.manager.message_manager import MessageManager
from aiogram_dialog.manager.update_handler import handle_update
from .about import about_dialog
from .context.access_validator import DefaultAccessValidator


def _setup_event_observer(router: Router) -> None:
    router.observers[DIALOG_EVENT_NAME] = TelegramEventObserver(
        router=router, event_name=DIALOG_EVENT_NAME,
    )


def _register_event_handler(router: Router, callback: Callable) -> None:
    handler = router.observers[DIALOG_EVENT_NAME]
    handler.register(callback, any_state)


class DialogRegistry(DialogRegistryProtocol):
    def __init__(self, router: Router):
        self.router = router
        self._loaded = False
        self._dialogs = {}
        self._states_groups = {}

    def _ensure_loaded(self):
        if not self._loaded:
            self.refresh()

    def find_dialog(self, state: State | str) -> DialogProtocol:
        self._ensure_loaded()
        try:
            return self._dialogs[state.group]
        except KeyError as e:
            raise UnregisteredDialogError(
                f"No dialog found for `{state.group}`"
                f" (looking by state `{state}`)",
            ) from e

    def states_groups(self) -> dict[str, type[StatesGroup]]:
        self._ensure_loaded()
        return self._states_groups

    def refresh(self):
        for dialog in collect_dialogs(self.router):
            states_group = dialog.states_group()
            if states_group in self._dialogs:
                existing_dialog = self._dialogs[states_group]
                raise ValueError(
                    f"StatesGroup '{states_group.__name__}' "
                    f"is used in multiple dialogs: "
                    f"'{existing_dialog}' and '{dialog}'",
                )
            self._dialogs[states_group] = dialog

        self._states_groups = {
            d.states_group_name(): d.states_group()
            for d in self._dialogs.values()
        }
        self._loaded = True


def _startup_callback(
        registry: DialogRegistry,
) -> Callable:
    async def _setup_dialogs(router):
        registry.refresh()

    return _setup_dialogs


def _register_middleware(
        router: Router,
        dialog_manager_factory: DialogManagerFactory,
        bg_manager_factory: BgManagerFactory,
        stack_access_validator: StackAccessValidator,
        events_isolation: BaseEventIsolation,
):
    registry = DialogRegistry(router)
    manager_middleware = ManagerMiddleware(
        dialog_manager_factory=dialog_manager_factory,
        router=router,
        registry=registry,
    )
    intent_middleware = IntentMiddlewareFactory(
        registry=registry,
        access_validator=stack_access_validator,
        events_isolation=events_isolation,
    )
    # delayed configuration of middlewares
    router.startup.register(_startup_callback(registry))
    update_handler = router.observers[DIALOG_EVENT_NAME]

    router.errors.middleware(IntentErrorMiddleware(
        registry=registry,
        events_isolation=events_isolation,
        access_validator=stack_access_validator,
    ))

    router.message.middleware(manager_middleware)
    router.business_message.middleware(manager_middleware)
    router.callback_query.middleware(manager_middleware)
    update_handler.middleware(manager_middleware)
    router.my_chat_member.middleware(manager_middleware)
    router.chat_join_request.middleware(manager_middleware)
    router.errors.middleware(manager_middleware)

    router.message.outer_middleware(intent_middleware.process_message)
    router.business_message.outer_middleware(intent_middleware.process_message)
    router.callback_query.outer_middleware(
        intent_middleware.process_callback_query,
    )
    update_handler.outer_middleware(
        intent_middleware.process_aiogd_update,
    )
    router.my_chat_member.outer_middleware(
        intent_middleware.process_my_chat_member,
    )
    router.chat_join_request.outer_middleware(
        intent_middleware.process_chat_join_request,
    )

    router.message.outer_middleware(context_unlocker_middleware)
    router.business_message.outer_middleware(context_unlocker_middleware)
    router.callback_query.outer_middleware(context_unlocker_middleware)
    update_handler.outer_middleware(context_unlocker_middleware)
    router.my_chat_member.outer_middleware(context_unlocker_middleware)
    router.chat_join_request.outer_middleware(context_unlocker_middleware)

    router.message.middleware(context_saver_middleware)
    router.business_message.middleware(context_saver_middleware)
    router.callback_query.middleware(context_saver_middleware)
    update_handler.middleware(context_saver_middleware)
    router.my_chat_member.middleware(context_saver_middleware)
    router.chat_join_request.middleware(context_saver_middleware)

    bg_factory_middleware = BgFactoryMiddleware(bg_manager_factory)
    for observer in router.observers.values():
        observer.outer_middleware(bg_factory_middleware)


def _prepare_dialog_manager_factory(
        dialog_manager_factory: DialogManagerFactory | None,
        message_manager: MessageManagerProtocol | None,
        media_id_storage: MediaIdStorageProtocol | None,
) -> DialogManagerFactory:
    if dialog_manager_factory is not None:
        return dialog_manager_factory
    if media_id_storage is None:
        media_id_storage = MediaIdStorage()
    if message_manager is None:
        message_manager = MessageManager()
    return DefaultManagerFactory(
        message_manager=message_manager,
        media_id_storage=media_id_storage,
    )


def _prepare_stack_access_validator(
        stack_access_validator: StackAccessValidator | None,
) -> StackAccessValidator:
    if stack_access_validator:
        return stack_access_validator
    else:
        return DefaultAccessValidator()


def _prepare_events_isolation(
        events_isolation: BaseEventIsolation | None,
) -> BaseEventIsolation:
    if events_isolation:
        return events_isolation
    else:
        return SimpleEventIsolation()


def collect_dialogs(router: Router) -> Iterable[DialogProtocol]:
    if isinstance(router, DialogProtocol):
        yield router
    for sub_router in router.sub_routers:
        yield from collect_dialogs(sub_router)


def _include_default_dialogs(router: Router):
    router.include_router(about_dialog())


def setup_dialogs(
        router: Router,
        *,
        dialog_manager_factory: DialogManagerFactory | None = None,
        message_manager: MessageManagerProtocol | None = None,
        media_id_storage: MediaIdStorageProtocol | None = None,
        stack_access_validator: StackAccessValidator | None = None,
        events_isolation: BaseEventIsolation | None = None,
) -> BgManagerFactory:
    _setup_event_observer(router)
    _register_event_handler(router, handle_update)
    _include_default_dialogs(router)

    dialog_manager_factory = _prepare_dialog_manager_factory(
        dialog_manager_factory=dialog_manager_factory,
        message_manager=message_manager,
        media_id_storage=media_id_storage,
    )
    stack_access_validator = _prepare_stack_access_validator(
        stack_access_validator,
    )
    events_isolation = _prepare_events_isolation(events_isolation)
    bg_manager_factory = BgManagerFactoryImpl(router)
    _register_middleware(
        router=router,
        dialog_manager_factory=dialog_manager_factory,
        bg_manager_factory=bg_manager_factory,
        stack_access_validator=stack_access_validator,
        events_isolation=events_isolation,
    )
    return bg_manager_factory
