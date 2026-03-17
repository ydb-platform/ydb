from contextlib import contextmanager
from functools import partial
from itertools import chain
from typing import List, Optional, Dict, Any, Type, Union, Callable

from django.contrib.auth.base_user import AbstractBaseUser

from ..exceptions import UnknownMessageTypeError, MessengerException
from ..models import Dispatch, Message, MessageTuple
from ..utils import Recipient, is_iterable, TypeRecipients

if False:  # pragma: nocover
    from ..messages.base import MessageBase  # noqa

TypeProxy = Optional[Union[Callable, dict]]


class DispatchProcessingHandler:
    """Context manager to facilitate exception handling on various
    messages processing stages.

    """
    def __init__(
        self,
        *,
        messenger: 'MessengerBase',
        messages: Optional[List[MessageTuple]] = None
    ):
        self.messenger = messenger

        self.dispatches = (
            chain.from_iterable((
                dispatch
                for dispatch in (item.dispatches for item in messages)
            ))
            if messages else None
        )

    def __enter__(self):
        messenger = self.messenger

        with messenger._exception_handling(self.dispatches):
            messenger._init_delivery_statuses_dict()
            messenger.before_send()

    def __exit__(self, exc_type, exc_val, exc_tb):
        messenger = self.messenger

        with messenger._exception_handling(self.dispatches):
            messenger.after_send()

        messenger._update_dispatches()

        return True


class MessengerBase:
    """Base class for messengers used by sitemessage.

    Custom messenger classes, implementing various message delivery
    mechanics, other messenger classes must inherit from this one.

    """
    alias: str = None
    """Messenger alias to address it from different places, Should rather be quite unique %)"""

    title: str = None
    """Title to show to user."""

    allow_user_subscription: bool = True
    """Makes subscription for this messenger messages available for users (see get_user_preferences_for_ui())"""

    address_attr: str = None
    """User object attribute containing address."""

    # Dispatches by status dict will be here runtime. See init_delivery_statuses_dict().
    _st = None

    @classmethod
    def get_alias(cls) -> str:
        """Returns messenger alias."""

        if cls.alias is None:
            cls.alias = cls.__name__

        return cls.alias

    def __str__(self) -> str:
        return self.__class__.get_alias()

    @contextmanager
    def before_after_send_handling(self, messages: Optional[List[MessageTuple]] = None):
        """Context manager that allows to execute send wrapped
        in before_send() and after_send().

        """
        with DispatchProcessingHandler(messenger=self, messages=messages):
            yield

    @contextmanager
    def _exception_handling(self, dispatches: Optional[List[Dispatch]]):
        """Propagates unhandled exceptions to dispatches log.

        :param dispatches:

        """
        try:
            yield

        except Exception as e:
            for dispatch in dispatches or []:
                self.mark_error(dispatch, e)

    def send_test_message(self, to: str, text: str) -> Any:
        """Sends a test message using messengers settings.

        :param to: an address to send test message to
        :param text: text to send

        """
        result = None  # noqa

        with self.before_after_send_handling():
            result = self._test_message(to, text)

        return result

    def _test_message(self, to: str, text: str) -> Any:
        """This method should be implemented by a heir to send a test message.

        :param to: an address to send test message to
        :param text: text to send

        """
        raise NotImplementedError  # pragma: nocover

    @classmethod
    def get_address(cls, recipient: Any) -> Any:
        """Returns recipient address.

        Heirs may override this to deduce address from `recipient` data
        (e.g. to get address from Django User model instance).

        :param recipient: any object passed to `recipients()`

        """
        address = recipient

        address_attr = cls.address_attr

        if address_attr:
            address = getattr(recipient, address_attr, None) or address

        return address

    @classmethod
    def structure_recipients_data(cls, recipients: TypeRecipients) -> List[Recipient]:
        """Converts recipients data into a list of Recipient objects.

        :param recipients: list of objects

        """
        if not is_iterable(recipients):
            recipients = (recipients,)

        objects = []
        for recipient in recipients:
            user = None

            if isinstance(recipient, AbstractBaseUser):
                user = recipient

            address = cls.get_address(recipient)

            objects.append(Recipient(cls.get_alias(), user, address))

        return objects

    def _init_delivery_statuses_dict(self):
        """Initializes a dict indexed by message delivery statuses."""
        self._st = {
            'pending': [],
            'sent': [],
            'error': [],
            'failed': []
        }

    def mark_pending(self, dispatch: Dispatch):
        """Marks a dispatch as pending.

        Should be used within send().

        :param dispatch: a Dispatch

        """
        self._st['pending'].append(dispatch)

    def mark_sent(self, dispatch: Dispatch):
        """Marks a dispatch as successfully sent.

        Should be used within send().

        :param dispatch: a Dispatch

        """
        self._st['sent'].append(dispatch)

    def mark_error(
            self,
            dispatch: Dispatch,
            error_log: Union[str, Exception],
            message_cls: Optional[Type['MessageBase']] = None
    ):
        """Marks a dispatch as having error or consequently as failed
        if send retry limit for that message type is exhausted.

        Should be used within send().

        :param dispatch: a Dispatch
        :param error_log: error message or exception object
        :param message_cls: MessageBase heir

        """
        if message_cls is None:
            message_cls = dispatch.message.get_type()

        if message_cls.send_retry_limit is not None and (dispatch.retry_count + 1) >= message_cls.send_retry_limit:
            self.mark_failed(dispatch, error_log)

        else:
            dispatch.error_log = error_log
            self._st['error'].append(dispatch)

    def mark_failed(self, dispatch: Dispatch, error_log: Union[str, Exception]):
        """Marks a dispatch as failed.

        Sitemessage won't try to deliver already failed messages.

        Should be used within send().

        :param dispatch: a Dispatch
        :param error_log: str - error message

        """
        dispatch.error_log = error_log
        self._st['failed'].append(dispatch)

    def before_send(self):
        """This one is called right before send procedure.
        Usually heir will implement some messenger warm up (connect) code.

        """

    def after_send(self):
        """This one is called right after send procedure.
        Usually heir will implement some messenger cool down (disconnect) code.

        """

    def process_messages(self, messages: Dict[int, MessageTuple], ignore_unknown_message_types: bool = False):
        """Performs message processing.

        :param messages: indexed by message id dict with messages data
        :param ignore_unknown_message_types: whether to silence exceptions

        :raises UnknownMessageTypeError:

        """
        send = self.send
        exception_handling = self._exception_handling

        messages = list(messages.values())

        with self.before_after_send_handling(messages=messages):

            for message, dispatches in messages:

                try:
                    message_cls = message.get_type()
                    compile_message = partial(message_cls.compile, message=message, messenger=self)

                except UnknownMessageTypeError:
                    if ignore_unknown_message_types:
                        continue
                    raise

                message_type_cache = None

                for dispatch in dispatches:

                    if dispatch.message_cache:
                        continue

                    # Create actual message text for further usage.
                    with exception_handling(dispatches=[dispatch]):
                        if message_type_cache is None and not message_cls.has_dynamic_context:
                            # If a message class doesn't depend upon a dispatch data for message compilation,
                            # we'd compile a message just once.
                            message_type_cache = compile_message(dispatch=dispatch)

                        dispatch.message_cache = message_type_cache or compile_message(dispatch=dispatch)

                with exception_handling(dispatches=dispatches):
                    # Batch send to cover wider messenger scenarios.
                    send(message_cls, message, dispatches)

    def _update_dispatches(self):
        """Updates dispatched data in DB according to information gather by `mark_*` methods,"""
        Dispatch.log_dispatches_errors(self._st['error'] + self._st['failed'])
        Dispatch.set_dispatches_statuses(**self._st)
        self._init_delivery_statuses_dict()

    def send(self, message_cls: Type['MessageBase'], message_model: Message, dispatch_models: List[Dispatch]):
        """Main send method must be implemented by all heirs.

        :param message_cls: a MessageBase heir
        :param message_model: message model
        :param dispatch_models: Dispatch models for this Message

        """
        raise NotImplementedError  # pragma: nocover


class RequestsMessengerBase(MessengerBase):
    """Shared requests-based messenger base class.

    Uses `requests` module: https://pypi.python.org/pypi/requests

    """
    timeout: int = 10
    """Request timeout."""

    def __init__(self, proxy: TypeProxy = None, **kwargs):
        """Configures messenger.

        :param proxy: Dictionary of proxy settings,
            or a callable returning such a dictionary.

        """
        import requests

        self.lib = requests
        self.proxy = proxy

    def _get_common_params(self) -> dict:
        """Returns common parameters for every request."""

        proxy = self.proxy

        if proxy:

            if callable(proxy):
                proxy = proxy()

        params = {
            'timeout': self.timeout,
            'proxies': proxy or None
        }

        return params

    def get(self, url: str, json: bool = True) -> Union[dict, str]:
        """Performs POST and returns data.

        :param url:
        :param json: Expect json data and return it as dict.

        """
        try:
            params = self._get_common_params()
            response = self.lib.get(url, **params)
            result = response.json() if json else response.text
            return result

        except self.lib.exceptions.RequestException as e:
            raise MessengerException(e)

    def post(self, url: str, data: dict) -> dict:
        """Performs POST and returns data.

        :param url:
        :param data: data to send.

        """
        try:
            params = self._get_common_params()
            response = self.lib.post(url, data=data, **params)
            result = response.json()
            return result

        except self.lib.exceptions.RequestException as e:
            raise MessengerException(e)

    def _test_message(self, to: str, text: str):
        return self._send_message(self._build_message(text, to=to))

    def _build_message(self, text: str, to: Optional[str] = None) -> str:
        """Builds a message before send.

        :param text: Contents to add to message.
        :param to: Recipient address.

        """
        return text

    def _send_message(self, msg: str, to: Optional[str] = None):
        """Should implement actual sending.

        :param msg: Contents to add to message.
        :param to: Recipient address.

        """
        raise NotImplementedError  # pragma: nocover

    def send(self, message_cls: Type['MessageBase'], message_model: Message, dispatch_models: List[Dispatch]):
        for dispatch_model in dispatch_models:
            try:
                recipient = dispatch_model.address
                msg = self._build_message(dispatch_model.message_cache, to=recipient)
                self._send_message(msg, to=recipient)
                self.mark_sent(dispatch_model)

            except Exception as e:
                self.mark_error(dispatch_model, e, message_cls)
