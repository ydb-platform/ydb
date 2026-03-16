from typing import Union, Optional, List, Iterable

from django.contrib.auth.base_user import AbstractBaseUser
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.template.loader import render_to_string
from django.urls import reverse, NoReverseMatch
from django.utils.crypto import salted_hmac
from django.utils.translation import gettext as _

from ..exceptions import UnknownMessengerError
from ..models import Message, Dispatch, Subscription, MessageTuple
from ..signals import sig_unsubscribe_success, sig_unsubscribe_failed, sig_mark_read_success, sig_mark_read_failed
from ..utils import Recipient, recipients, get_site_url, get_registered_messenger_object, TypeRecipients

if False:  # pragma: nocover
    from ..messengers.base import MessengerBase  # noqa


APP_URLS_ATTACHED = None


class MessageBase:
    """Base class for messages used by sitemessage.
    Customized message handling is available through inheritance.

    """

    supported_messengers: List[str] = []
    """List of supported messengers (aliases)."""

    alias: str = None
    """Message type alias to address it from different places, Should rather be quite unique %)"""

    title: str = _('Notification')
    """Title to show to user."""

    priority: Optional[int] = None
    """Number describing message priority. Can be overridden by `priority` provided with schedule_messages()."""

    has_dynamic_context: bool = False
    """This flag is used to optimize template compilation process.
    If True template will be compiled for every dispatch (and dispatch data will be available in it)
    instead of just once per message.

    """

    group_mark: str = ''
    """Group marker - a distinctive (grouping) string, allowing switching to the contribution mode.
    If provided, message content is added to an existing unsent message having this very mark,
    instead of a new message creation.

    """

    template_ext: str = 'tpl'
    """Template file extension. Considered when the below mentioned `template` field is not set."""

    template: str = None
    """Path to the template to be used for message rendering.
    If not set, will be deduced from message, messenger data (e.g. `sitemessage/plain_smtp.txt`)
    and `template_ext` (see above).

    """

    send_retry_limit: int = 10
    """This limits the number of send attempts before message delivery considered failed."""

    allow_user_subscription: bool = True
    """Makes subscription for this message type available for users (see get_user_preferences_for_ui())"""

    _message_model = None
    _dispatch_models = None

    SIMPLE_TEXT_ID = 'stext_'

    def __init__(self, context: Union[str, dict] = None, template_path: str = None):
        """Initializes a message.

        :param context: data to be used for message rendering (e.g. in templates)
        :param template_path: template path

        """
        context_base = {
            'tpl': None,  # Template path to use
            'use_tpl': False  # Use template boolean flag
        }

        if context is not None:
            self.update_context(context_base, context, template_path=template_path)

        self.context = context_base

    @classmethod
    def get_alias(cls) -> str:
        """Returns message type alias."""

        if cls.alias is None:
            cls.alias = cls.__name__

        return cls.alias

    def __str__(self) -> str:
        return self.__class__.get_alias()

    def get_context(self) -> dict:
        """Returns message context."""
        return self.context

    def schedule(
            self,
            recipients: Optional[Union[Iterable[Recipient], Recipient]] = None,
            sender: Optional[AbstractBaseUser] = None,
            priority: Optional[int] = None
    ) -> MessageTuple:
        """Schedules message for a delivery.
        Puts message (and dispatches if any) data into DB.
        Returns a tuple with message model and a list of dispatch models.

        :param recipients: recipient (or a list) or None.
            If `None` Dispatches should be created later but
            before send using `prepare_dispatches()` (e.g. in a background task).

        :param sender: Django User model heir instance

        :param priority: number describing message priority

        """
        if priority is None:
            priority = self.priority

        self._message_model, self._dispatch_models = Message.create(
            self.get_alias(),
            self.get_context(),
            recipients=recipients,
            sender=sender,
            priority=priority,
            group_mark=self.group_mark,
            context_merge_hook=self.merge_context,
        )
        return MessageTuple(message=self._message_model, dispatches=self._dispatch_models)

    @classmethod
    def merge_context(cls, context: dict, new_context: dict) -> dict:
        """Performs context merge for old and new message
        in cases when 'group_mark' is set and previous message
        is found in database.

        :param context:
        :param new_context:

        """
        stest_id = cls.SIMPLE_TEXT_ID
        unset = set()

        merged = {**context, **new_context}

        stext = context.get(stest_id, unset)

        if stext is not unset:  # in case of a simple string
            merged[stest_id] = f'{stext}\n{new_context.get(stest_id, "")}'

        return merged

    @classmethod
    def recipients(cls, messenger: Union[str, 'MessengerBase'], addresses: TypeRecipients) -> List[Recipient]:
        """Shortcut method. See `recipients()`,"""
        return recipients(messenger, addresses)

    @classmethod
    def get_subscribers(cls, active_only: bool = True) -> List[Recipient]:
        """Returns a list of Recipient objects subscribed for this message type.

        :param active_only: Flag whether to return only active subscribers.

        """
        subscribers_raw = Subscription.get_for_message_cls(cls.alias)
        subscribers = []

        for subscriber in subscribers_raw:
            messenger_cls = subscriber.messenger_cls
            address = subscriber.address
            recipient = subscriber.recipient

            # Do not send messages to inactive users.
            if active_only and recipient:
                if not getattr(recipient, 'is_active', False):
                    continue

            if address is None:
                try:
                    address = get_registered_messenger_object(messenger_cls).get_address(recipient)
                except UnknownMessengerError:
                    pass

            if address and isinstance(address, str):
                subscribers.append(Recipient(messenger_cls, recipient, address))

        return subscribers

    @classmethod
    def get_dispatch_hash(cls, dispatch_id: int, message_id: int) -> str:
        """Returns a hash string for validation purposes.

        :param dispatch_id:
        :param message_id:

        """
        return salted_hmac(f'{dispatch_id}', f'{message_id}|{dispatch_id}').hexdigest()

    @classmethod
    def get_mark_read_directive(cls, message_model: Message, dispatch_model: Dispatch) -> str:
        """Returns mark read directive (command, URL, etc.) string.

        :param message_model:
        :param dispatch_model:

        """
        return cls._get_url('sitemessage_mark_read', message_model, dispatch_model)

    @classmethod
    def get_unsubscribe_directive(cls, message_model: Message, dispatch_model: Dispatch) -> str:
        """Returns an unsubscribe directive (command, URL, etc.) string.

        :param message_model:
        :param dispatch_model:

        """
        return cls._get_url('sitemessage_unsubscribe', message_model, dispatch_model)

    @classmethod
    def _get_url(cls, name: str, message_model: Message, dispatch_model: Optional[Dispatch]) -> str:
        """Returns a common pattern sitemessage URL.

        :param name: URL name
        :param message_model:
        :param dispatch_model:

        """
        global APP_URLS_ATTACHED

        url = ''

        if dispatch_model is None:
            return url

        if APP_URLS_ATTACHED != False:  # sic!

            hashed = cls.get_dispatch_hash(dispatch_model.pk, message_model.pk)

            try:
                url = reverse(name, args=[message_model.pk, dispatch_model.pk, hashed])
                url = f'{get_site_url()}{url}'

            except NoReverseMatch:
                if APP_URLS_ATTACHED is None:
                    APP_URLS_ATTACHED = False

        return url

    @classmethod
    def handle_unsubscribe_request(
            cls,
            request: HttpRequest,
            message: Message,
            dispatch: Dispatch,
            hash_is_valid: bool,
            redirect_to: str
    ) -> HttpResponse:
        """Handles user subscription cancelling request.

        :param request: Request instance
        :param message: Message model instance
        :param dispatch: Dispatch model instance
        :param hash_is_valid: Flag indicating that user supplied request signature is correct
        :param redirect_to: Redirection URL

        """
        if hash_is_valid:
            Subscription.cancel(
                dispatch.recipient_id or dispatch.address, cls.alias, dispatch.messenger
            )
            signal = sig_unsubscribe_success

        else:
            signal = sig_unsubscribe_failed

        signal.send(cls, request=request, message=message, dispatch=dispatch)

        return redirect(redirect_to)

    @classmethod
    def handle_mark_read_request(
            cls,
            request: HttpRequest,
            message: Message,
            dispatch: Dispatch,
            hash_is_valid: bool,
            redirect_to: str
    ) -> HttpResponse:
        """Handles a request to mark a message as read.

        :param request: Request instance
        :param message: Message model instance
        :param dispatch: Dispatch model instance
        :param hash_is_valid: Flag indicating that user supplied request signature is correct
        :param redirect_to: Redirection URL

        """
        if hash_is_valid:
            dispatch.mark_read()
            dispatch.save()
            signal = sig_mark_read_success

        else:
            signal = sig_mark_read_failed

        signal.send(cls, request=request, message=message, dispatch=dispatch)

        return redirect(redirect_to)

    @classmethod
    def get_template(cls, message: Message, messenger: 'MessengerBase') -> str:
        """Get a template path to compile a message.

        1. `tpl` field of message context;
        2. `template` field of message class;
        3. deduced from message, messenger data and `template_ext` message type field
           (e.g. `sitemessage/messages/plain__smtp.txt` for `plain` message type).

        :param message: Message model
        :param messenger: a MessengerBase heir

        """
        template = message.context.get('tpl', None)

        if template:  # Template name is taken from message context.
            return template

        if cls.template is None:
            cls.template = f'sitemessage/messages/{cls.get_alias()}__{messenger.get_alias()}.{cls.template_ext}'

        return cls.template

    @classmethod
    def compile(cls, message: Message, messenger: 'MessengerBase', dispatch: Optional[Dispatch] = None) -> str:
        """Compiles and returns a message text.

        Considers `use_tpl` field from message context to decide whether
        template compilation is used.

        Otherwise a SIMPLE_TEXT_ID field from message context is used as message contents.

        :param message: model instance

        :param messenger: MessengerBase heir instance

        :param dispatch: model instance to consider context from

        """
        if message.context.get('use_tpl', False):
            context = message.context
            context.update({
                'SITE_URL': get_site_url(),
                'directive_unsubscribe': cls.get_unsubscribe_directive(message, dispatch),
                'directive_mark_read': cls.get_mark_read_directive(message, dispatch),
                'message_model': message,
                'dispatch_model': dispatch
            })
            context = cls.get_template_context(context)

            return render_to_string(cls.get_template(message, messenger), context)

        return message.context[cls.SIMPLE_TEXT_ID]

    @classmethod
    def get_template_context(cls, context: dict) -> dict:
        """Returns context dict for template compilation.

        This method might be reimplemented by a heir to add some data into
        context before template compilation.

        :param context: Initial context

        """
        return context

    @classmethod
    def update_context(cls, base_context: dict, str_or_dict: Union[dict, str], template_path: str = None):
        """Helper method to structure initial message context data.

        NOTE: updates `base_context` inplace.

        :param base_context: context dict to update

        :param str_or_dict: text representing a message, or a dict to be placed into message context.

        :param template_path: template path to be used for message rendering

        """
        if isinstance(str_or_dict, dict):
            base_context.update(str_or_dict)
            base_context['use_tpl'] = True

        else:
            base_context[cls.SIMPLE_TEXT_ID] = str_or_dict

        if cls.SIMPLE_TEXT_ID in str_or_dict:
            base_context['use_tpl'] = False

        base_context['tpl'] = template_path

    @classmethod
    def prepare_dispatches(cls, message: Message, recipients: Optional[List[Recipient]] = None) -> List[Dispatch]:
        """Creates Dispatch models for a given message and return them.

        :param message: Message model instance

        :param recipients: A list or Recipient objects

        """
        return Dispatch.create(message, recipients or cls.get_subscribers())
