import json
from typing import Type, List, Optional, Union, Tuple, Dict, Iterable, NamedTuple, Callable

from django.conf import settings
from django.contrib.auth.base_user import AbstractBaseUser
from django.core import exceptions
from django.db import models, transaction, DatabaseError, NotSupportedError
from django.db.models import QuerySet, Q
from django.db.transaction import atomic
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from .utils import get_registered_message_type, Recipient, TypeRecipient, TypeMessage, TypeMessenger

if False:  # pragma: nocover
    from .messages.base import MessageBase  # noqa
    from .messengers.base import MessengerBase

USER_MODEL = getattr(settings, 'AUTH_USER_MODEL', 'auth.User')


class MessageTuple(NamedTuple):

    message: 'Message'
    dispatches: List['Dispatch']


def _get_dispatches(filter_kwargs: dict) -> List['Dispatch']:
    """Simplified version. Not distributed friendly."""

    dispatches = Dispatch.objects.prefetch_related('message').filter(
        **filter_kwargs
    ).order_by('-message__time_created')

    return list(dispatches)


def _get_dispatches_for_update(filter_kwargs: dict) -> Optional[List['Dispatch']]:
    """Distributed friendly version using ``select for update``."""

    dispatches = Dispatch.objects.prefetch_related('message').filter(
        **filter_kwargs

    ).select_for_update(
        **GET_DISPATCHES_ARGS[1]

    ).order_by('-message__time_created')

    try:
        dispatches = list(dispatches)

    except NotSupportedError:
        return None

    except DatabaseError:  # Probably locked. That's fine.
        return []

    return dispatches


GET_DISPATCHES_ARGS = [
    _get_dispatches_for_update,
    {'skip_locked': True}
]
"""This could be set runtime in Dispatch.get_unsent()"""


class ContextField(models.TextField):

    @classmethod
    def parse_value(cls, value: str):
        try:
            return json.loads(value)

        except ValueError:
            raise exceptions.ValidationError(
                _('Value `%r` is not a valid context.') % value,
                code='invalid_context', params={'value': value})

    def from_db_value(self, *args):
        value, expression, connection = args[:3]

        if value is None:
            return {}

        return self.parse_value(value)

    def to_python(self, value: Union[dict, str]):
        if not value:
            return {}

        if isinstance(value, dict):
            return value

        return self.parse_value(value)

    def get_prep_value(self, value: dict):
        return json.dumps(value, ensure_ascii=False)


class Message(models.Model):

    time_created = models.DateTimeField(_('Time created'), auto_now_add=True, editable=False)

    sender = models.ForeignKey(
        USER_MODEL, verbose_name=_('Sender'), null=True, blank=True, on_delete=models.CASCADE)

    cls = models.CharField(
        _('Message class'), max_length=250, db_index=True,
        help_text=_('Message logic class identifier.'))

    group_mark = models.CharField(
        _('Group mark'), max_length=128, db_index=True, db_column='gmark',
        help_text=_('An identifier to group several messages into one.'))

    context = ContextField(_('Message context'))

    priority = models.PositiveIntegerField(
        _('Priority'), default=0, db_index=True,
        help_text=_('Number describing message sending priority. '
                    'Messages with different priorities can be sent with different periodicity.'))

    dispatches_ready = models.BooleanField(
        _('Dispatches ready'), db_index=True, default=False,
        help_text=_('Indicates whether dispatches for this message are already formed and ready to delivery.'))

    class Meta:
        verbose_name = _('Message')
        verbose_name_plural = _('Messages')

    def __str__(self) -> str:
        return self.cls

    def get_type(self) -> Type['MessageBase']:
        """Returns message type (class) associated with the message.

        :raises UnknownMessageTypeError:

        """
        return get_registered_message_type(self.cls)

    @classmethod
    def get_without_dispatches(cls) -> Union[List['Message'], QuerySet]:
        """Returns messages with no dispatches created."""
        return cls.objects.filter(dispatches_ready=False).all()

    @classmethod
    @transaction.atomic()
    def create(
            cls,
            message_class: str,
            context: dict,
            recipients: Optional[Union[Iterable[Recipient], Recipient]] = None,
            sender: Optional[AbstractBaseUser] = None,
            priority: Optional[int] = None,
            group_mark: str = '',
            context_merge_hook: Callable = None,
    ) -> Tuple['Message', List['Dispatch']]:
        """Creates a message (and dispatches).

        Returns a tuple: (message_model, list_of_dispatches)

        :param message_class: alias of MessageBase heir

        :param context: context for a message

        :param recipients: recipient (or a list) or None.
            If `None` Dispatches should be created before send using `prepare_dispatches()`.

        :param sender: Django User model heir instance

        :param priority: number describing message priority

        :param group_mark: A distinctive (grouping) string. If provided,
            message content is added to an existing unsent message having this very mark,
            instead of a new message creation.

        :param context_merge_hook: A function performing context merge
            for cases when 'group_mark' is set and previous message
            is found in database.

        """
        dispatches_ready = False

        if recipients is not None:
            dispatches_ready = True

        msg_kwargs = {
            'cls': message_class,
            'context': context,
            'sender': sender,
            'dispatches_ready': dispatches_ready,
            'group_mark': group_mark,
        }

        if priority is not None:
            msg_kwargs['priority'] = priority

        message_model = None
        dispatch_models = None

        if group_mark:
            # We'll try to get an unset message and contribute into it.
            filter_kwargs = {}

            if sender is None:
                filter_kwargs['sender__isnull'] = True

            message_model = cls.objects.filter(
                Q(dispatch__dispatch_status=Dispatch.DISPATCH_STATUS_PENDING) |
                Q(dispatches_ready=False),
                cls=message_class,
                group_mark=group_mark,
                **filter_kwargs,
            ).first()

            if message_model:

                get_context = context_merge_hook or (lambda ctx, newcxt: newcxt)
                message_model.context = get_context(message_model.context, context,)

                if message_model.dispatches_ready:
                    dispatch_models = message_model.dispatch_set.all()
                    dispatch_models.update(message_cache='')
                else:
                    dispatch_models = []

        if not message_model:
            message_model = cls(**msg_kwargs)

        message_model.save()

        if dispatch_models is None:
            dispatch_models = Dispatch.create(message_model, recipients)

        return message_model, dispatch_models


class Dispatch(models.Model):

    DISPATCH_STATUS_PENDING = 1
    DISPATCH_STATUS_SENT = 2
    DISPATCH_STATUS_ERROR = 3
    DISPATCH_STATUS_FAILED = 4
    DISPATCH_STATUS_PROCESSING = 5

    DISPATCH_STATUSES = (
        (DISPATCH_STATUS_PENDING, _('Pending')),
        (DISPATCH_STATUS_PROCESSING, _('Processing')),
        (DISPATCH_STATUS_SENT, _('Sent')),
        (DISPATCH_STATUS_ERROR, _('Error')),
        (DISPATCH_STATUS_FAILED, _('Failed')),
    )

    READ_STATUS_UNREAD = 0
    READ_STATUS_READ = 1

    READ_STATUSES = (
        (READ_STATUS_UNREAD, _('Unread')),
        (READ_STATUS_READ, _('Read')),
    )

    error_log = None
    """Dynamic attribute. Populated in .mark_error()."""

    time_created = models.DateTimeField(
        _('Time created'), auto_now_add=True, editable=False)

    time_dispatched = models.DateTimeField(
        _('Time dispatched'), editable=False, null=True, blank=True, help_text=_('Time of the last delivery attempt.'))

    message = models.ForeignKey(Message, verbose_name=_('Message'), on_delete=models.CASCADE)

    messenger = models.CharField(
        _('Messenger'), max_length=250, db_index=True, help_text=_('Messenger class identifier.'))

    recipient = models.ForeignKey(
        USER_MODEL, verbose_name=_('Recipient'), null=True, blank=True, on_delete=models.CASCADE)

    address = models.CharField(_('Address'), max_length=250, help_text=_('Recipient address.'))

    retry_count = models.PositiveIntegerField(
        _('Retry count'), default=0, help_text=_('A number of delivery retries has already been made.'))

    message_cache = models.TextField(_('Message cache'), null=True, editable=False)

    dispatch_status = models.PositiveIntegerField(
        _('Dispatch status'), choices=DISPATCH_STATUSES, default=DISPATCH_STATUS_PENDING)

    read_status = models.PositiveIntegerField(_('Read status'), choices=READ_STATUSES, default=READ_STATUS_UNREAD)

    class Meta:
        verbose_name = _('Dispatch')
        verbose_name_plural = _('Dispatches')

    def __str__(self) -> str:
        return f'{self.address} [{self.messenger}]'

    def is_read(self) -> bool:
        """Returns message read flag."""
        return self.read_status == self.READ_STATUS_READ

    def mark_read(self):
        """Marks message as read (doesn't save it)."""
        self.read_status = self.READ_STATUS_READ

    @classmethod
    def log_dispatches_errors(cls, dispatches: List['Dispatch']):
        """Batch logs dispatches delivery errors into DB.

        :param dispatches:

        """
        error_entries = []

        for dispatch in dispatches:
            # Saving message body cache for further usage.
            dispatch.save()
            error_entries.append(DispatchError(dispatch=dispatch, error_log=dispatch.error_log))

        DispatchError.objects.bulk_create(error_entries)

    @classmethod
    def set_dispatches_statuses(cls, **statuses: List['Dispatch']):
        """Batch set dispatches delivery statuses using a [kwargs] dictionary
        of dispatch lists indexed by statuses.

        :param statuses:

        """
        kwarg_status_map = {
            'sent': cls.DISPATCH_STATUS_SENT,
            'error': cls.DISPATCH_STATUS_ERROR,
            'failed': cls.DISPATCH_STATUS_FAILED,
            'pending': cls.DISPATCH_STATUS_PENDING,
        }

        for status_name, real_status in kwarg_status_map.items():

            if statuses.get(status_name, False):
                update_kwargs = {
                    'time_dispatched': timezone.now(),
                    'dispatch_status': real_status,
                    'retry_count': models.F('retry_count') + 1
                }

                cls.objects.filter(
                    id__in=[dispatch.pk for dispatch in statuses[status_name]]
                ).update(**update_kwargs)

    @staticmethod
    def group_by_messengers(dispatches: List['Dispatch']) -> Dict[str, Dict[int, MessageTuple]]:
        """Groups dispatches by messages.

        :param dispatches:

        """
        by_messengers = {}

        for dispatch in dispatches:
            message = dispatch.message
            by_messenger = by_messengers.setdefault(dispatch.messenger, {})
            message_data = by_messenger.setdefault(message.pk, MessageTuple(message=message, dispatches=[]))
            message_data.dispatches.append(dispatch)

        return by_messengers

    @classmethod
    def get_unsent(cls, priority: Optional[int] = None) -> Union[List['Dispatch'], QuerySet]:
        """Returns dispatches unsent (scheduled or with errors).

        .. warning:: This changes dispatch status to `Processing`.

        :param priority: Message priority filter

        """
        filter_kwargs = {
            'dispatch_status__in': (cls.DISPATCH_STATUS_PENDING, cls.DISPATCH_STATUS_ERROR),
        }

        if priority is not None:
            filter_kwargs['message__priority'] = priority

        with transaction.atomic():

            dispatches = GET_DISPATCHES_ARGS[0](filter_kwargs)

            if dispatches is None:
                # Try graceful degradation.
                # This branch normally runs only once to adapt to DB capabilities.

                # 1. drop skip_locked/no_wait
                GET_DISPATCHES_ARGS[1] = {}
                dispatches = GET_DISPATCHES_ARGS[0](filter_kwargs)

                if dispatches is None:
                    # 2. drop for update entirely
                    GET_DISPATCHES_ARGS[0] = _get_dispatches
                    dispatches = _get_dispatches(filter_kwargs)

            if not dispatches:
                return []

            # Trigger update for 'select_for_update' setting the processing state.
            cls.objects.filter(
                pk__in=[dispatch.pk for dispatch in dispatches]

            ).update(dispatch_status=cls.DISPATCH_STATUS_PROCESSING)

        return dispatches

    @classmethod
    def get_unread(cls) -> Union[List['Dispatch'], QuerySet]:
        """Returns unread dispatches."""
        return cls.objects.filter(read_status=cls.READ_STATUS_UNREAD).prefetch_related('message').all()

    @classmethod
    def create(
            cls,
            message_model: Message,
            recipients: Optional[Union[Iterable[Recipient], Recipient]]
    ) -> List['Dispatch']:
        """Creates dispatches for given recipients.

        NB: dispatch models are bulk created and do not have IDs.

        :param message_model:
        :param recipients:

        """
        objects = []

        if recipients:
            if not isinstance(recipients, (list, set)):
                recipients = (recipients,)

            for r in recipients:
                objects.append(cls(message=message_model, messenger=r.messenger, recipient=r.user, address=r.address))

            if objects:
                cls.objects.bulk_create(objects)

            if not message_model.dispatches_ready:
                message_model.dispatches_ready = True
                message_model.save()

        return objects


class DispatchError(models.Model):

    time_created = models.DateTimeField(_('Time created'), auto_now_add=True, editable=False)
    dispatch = models.ForeignKey(Dispatch, verbose_name=_('Dispatch'), on_delete=models.CASCADE)
    error_log = models.TextField(_('Text'))

    class Meta:
        verbose_name = _('Dispatch error')
        verbose_name_plural = _('Dispatch errors')

    def __str__(self) -> str:
        return f'Dispatch ID {self.dispatch_id} error entry'


class Subscription(models.Model):

    time_created = models.DateTimeField(_('Time created'), auto_now_add=True, editable=False)

    message_cls = models.CharField(
        _('Message class'), max_length=250, db_index=True, help_text=_('Message logic class identifier.'))

    messenger_cls = models.CharField(
        _('Messenger'), max_length=250, db_index=True, help_text=_('Messenger class identifier.'))

    recipient = models.ForeignKey(
        USER_MODEL, verbose_name=_('Recipient'), null=True, blank=True, on_delete=models.CASCADE)

    address = models.CharField(_('Address'), max_length=250, null=True, help_text=_('Recipient address.'))

    class Meta:
        verbose_name = _('Subscription')
        verbose_name_plural = _('Subscriptions')

    def __str__(self) -> str:
        recipient = self.recipient_id or self.address
        return f'{recipient} [{self.message_cls} - {self.messenger_cls}]'

    @classmethod
    def get_for_user(cls, user: AbstractBaseUser) -> Union[List['Subscription'], QuerySet]:
        """Returns subscriptions for a given user.

        :param user:

        """
        if user.pk is None:
            return []

        return cls.objects.filter(recipient=user)

    @classmethod
    def replace_for_user(
            cls,
            user: AbstractBaseUser,
            prefs: List[Tuple[Type['MessageBase'], Type['MessengerBase']]]
    ) -> bool:
        """Set subscription preferences for a given user.

        :param user:
        :param prefs: List of tuples (message_cls, messenger_cls)

        """
        uid = user.pk

        if uid is None:
            return False

        # Remove previous prefs.
        cls.objects.filter(recipient_id=uid).delete()

        new_prefs = []

        for pref in prefs:
            new_prefs.append(
                cls(**cls._get_base_kwargs(uid, pref[0], pref[1]))
            )

        if new_prefs:
            cls.objects.bulk_create(new_prefs)

        return True

    @classmethod
    def get_for_message_cls(cls, message_cls: str) -> Union[List['Subscription'], QuerySet]:
        """Returns subscriptions for a given message class alias.

        :param message_cls:

        """
        return cls.objects.select_related('recipient').filter(message_cls=message_cls)

    @classmethod
    def _get_base_kwargs(
            cls,
            recipient: TypeRecipient,
            message_cls: Type['MessageBase'],
            messenger_cls: Type['MessengerBase']
    ) -> dict:

        if not isinstance(message_cls, str):
            message_cls = message_cls.alias

        if not isinstance(messenger_cls, str):
            messenger_cls = messenger_cls.alias

        base_kwargs = {
            'message_cls': message_cls,
            'messenger_cls': messenger_cls,
        }

        if isinstance(recipient, str):
            base_kwargs['address'] = recipient

        else:
            if not isinstance(recipient, int):
                # user model presumed
                recipient = recipient.pk

            base_kwargs['recipient_id'] = recipient

        return base_kwargs

    @classmethod
    def create(
            cls,
            uid_or_address: Union[int, str],
            message_cls: TypeMessage,
            messenger_cls: TypeMessenger,
    ) -> 'Subscription':
        """Creates a subscription for a recipient.

        :param uid_or_address: User ID or address string.
        :param message_cls: Message type alias or class
        :param messenger_cls: Messenger type alias or class

        """
        obj = cls(**cls._get_base_kwargs(uid_or_address, message_cls, messenger_cls))
        obj.save()
        return obj

    @classmethod
    def cancel(
            cls,
            uid_or_address: Union[int, str],
            message_cls: TypeMessage,
            messenger_cls: TypeMessenger,
    ):
        """Cancels a subscription for a recipient.

        :param uid_or_address: User ID or address string.
        :param message_cls: Message type alias or class
        :param messenger_cls: Messenger type alias or class

        """
        cls.objects.filter(
            **cls._get_base_kwargs(uid_or_address, message_cls, messenger_cls)
        ).delete()
