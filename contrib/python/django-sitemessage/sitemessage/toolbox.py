from collections import defaultdict
from datetime import timedelta
from itertools import chain
from operator import itemgetter
from typing import Optional, List, Tuple, Union, Iterable, Any, Callable, Dict, Mapping

from django.conf import settings
from django.contrib.auth.base_user import AbstractBaseUser
from django.http import HttpRequest
from django.urls import re_path
from django.utils import timezone
from django.utils.translation import gettext as _

from .exceptions import UnknownMessengerError, UnknownMessageTypeError
# NB: Some of these unused imports are exposed as part of toolbox API.
from .messages import register_builtin_message_types  # noqa
from .messages.base import MessageBase
from .messages.plain import PlainTextMessage
from .models import Message, Dispatch, Subscription, MessageTuple
from .utils import (  # noqa
    is_iterable, import_project_sitemessage_modules, get_site_url, recipients,
    register_messenger_objects, get_registered_messenger_object, get_registered_messenger_objects,
    register_message_types, get_registered_message_type, get_registered_message_types,
    get_message_type_for_app, override_message_type_for_app, Recipient, TypeUser
)
from .views import mark_read, unsubscribe

_ALIAS_SEP = '|'
_PREF_POST_KEY = 'sm_user_pref'

TypeMessages = Union[str, MessageBase, List[Union[str, MessageBase]]]


def schedule_messages(
        messages: TypeMessages,
        recipients: Union[Iterable[Recipient], Recipient] = None,
        sender: TypeUser = None,
        priority: Optional[int] = None
) -> List[MessageTuple]:
    """Schedules a message or messages.

    :param messages: str or MessageBase heir or list - use str to create PlainTextMessage.

    :param recipients: recipients addresses or Django User model heir instances
        If `None` Dispatches should be created before send using `prepare_dispatches()`.

    :param sender: User model heir instance

    :param priority: number describing message priority. If set overrides priority provided with message type.

    """
    if not is_iterable(messages):
        messages = (messages,)

    results = []
    for message in messages:
        if isinstance(message, str):
            message = PlainTextMessage(message)

        resulting_priority = message.priority
        if priority is not None:
            resulting_priority = priority

        results.append(message.schedule(
            sender=sender,
            recipients=recipients,
            priority=resulting_priority,
        ))

    return results


def send_scheduled_messages(
        priority: Optional[int] = None,
        ignore_unknown_messengers: bool = False,
        ignore_unknown_message_types: bool = False
):
    """Sends scheduled messages.

    :param priority: number to limit sending message by this priority.

    :param ignore_unknown_messengers: to silence UnknownMessengerError

    :param ignore_unknown_message_types: to silence UnknownMessageTypeError

    :raises UnknownMessengerError:
    :raises UnknownMessageTypeError:

    """
    dispatches_by_messengers = Dispatch.group_by_messengers(Dispatch.get_unsent(priority=priority))

    for messenger_id, messages in dispatches_by_messengers.items():
        try:
            messenger_obj = get_registered_messenger_object(messenger_id)
            messenger_obj.process_messages(messages, ignore_unknown_message_types=ignore_unknown_message_types)

        except UnknownMessengerError:
            if ignore_unknown_messengers:
                continue
            raise


def send_test_message(messenger_id: str, to: Optional[str] = None) -> Any:
    """Sends a test message using the given messenger.

    :param messenger_id: Messenger alias.
    :param to: Recipient address (if applicable).

    """
    messenger_obj = get_registered_messenger_object(messenger_id)
    return messenger_obj.send_test_message(to=to, text='Test message from sitemessages.')


def check_undelivered(to: Optional[str] = None) -> int:
    """Sends a notification email if any undelivered dispatches.

    Returns undelivered (failed) dispatches count.

    :param to: Recipient address. If not set Django ADMINS setting is used.

    """
    failed_count = Dispatch.objects.filter(dispatch_status=Dispatch.DISPATCH_STATUS_FAILED).count()

    if failed_count:
        from sitemessage.shortcuts import schedule_email
        from sitemessage.messages.email import EmailTextMessage

        if to is None:
            admins = settings.ADMINS

            if admins:
                to = list(dict(admins).values())

        if to:
            priority = 999

            register_message_types(EmailTextMessage)

            schedule_email(
                _('You have %(count)s undelivered dispatch(es) at %(url)s') % {
                    'count': failed_count,
                    'url': get_site_url(),
                },
                subject=_('[SITEMESSAGE] Undelivered dispatches'),
                to=to, priority=priority)

            send_scheduled_messages(priority=priority)

    return failed_count


def cleanup_sent_messages(ago: Optional[int] = None, dispatches_only: bool = False):
    """Cleans up DB : removes delivered dispatches (and messages).

    :param ago: Days. Allows cleanup messages sent X days ago. Defaults to None (cleanup all sent).
    :param dispatches_only: Remove dispatches only (messages objects will stay intact).

    """
    filter_kwargs = {
        'dispatch_status': Dispatch.DISPATCH_STATUS_SENT,
    }

    objects = Dispatch.objects

    if ago:
        filter_kwargs['time_dispatched__lte'] = timezone.now() - timedelta(days=int(ago))

    dispatch_map = dict(objects.filter(**filter_kwargs).values_list('pk', 'message_id'))

    # Remove dispatches
    objects.filter(pk__in=list(dispatch_map.keys())).delete()

    if not dispatches_only:
        # Remove messages also.
        messages_ids = set(dispatch_map.values())

        if messages_ids:
            messages_blocked = set(chain.from_iterable(
                objects.filter(message_id__in=messages_ids).values_list('message_id')))

            messages_stale = messages_ids.difference(messages_blocked)

            if messages_stale:
                Message.objects.filter(pk__in=messages_stale).delete()


def prepare_dispatches() -> List[Dispatch]:
    """Automatically creates dispatches for messages without them."""

    dispatches = []
    target_messages = Message.get_without_dispatches()

    cache = {}

    for message_model in target_messages:

        if message_model.cls not in cache:
            message_cls = get_registered_message_type(message_model.cls)
            subscribers = message_cls.get_subscribers()
            cache[message_model.cls] = (message_cls, subscribers)
        else:
            message_cls, subscribers = cache[message_model.cls]

        dispatches.extend(message_cls.prepare_dispatches(message_model))

    return dispatches


def get_user_preferences_for_ui(
        user: AbstractBaseUser,
        message_filter: Optional[Callable] = None,
        messenger_filter: Optional[Callable] = None,
        new_messengers_titles: Optional[Dict[str, str]] = None
) -> Tuple[List[str], Mapping]:
    """Returns a two element tuple with user subscription preferences to render in UI.

    Message types with the same titles are merged into one row.

    First element:
        A list of messengers titles.

    Second element:
        User preferences dictionary indexed by message type titles.
        Preferences (dictionary values) are lists of tuples:
            (preference_alias, is_supported_by_messenger_flag, user_subscribed_flag)

        Example:
            {'My message type': [('test_message|smtp', True, False), ...]}

    :param user:

    :param message_filter: A callable accepting a message object to filter out message types

    :param messenger_filter: A callable accepting a messenger object to filter out messengers

    :param new_messengers_titles: Mapping of messenger aliases to a new titles.

    """
    if new_messengers_titles is None:
        new_messengers_titles = {}

    msgr_to_msg = defaultdict(set)
    msg_titles = {}
    msgr_titles = {}

    for msgr in get_registered_messenger_objects().values():
        if not (messenger_filter is None or messenger_filter(msgr)) or not msgr.allow_user_subscription:
            continue

        msgr_alias = msgr.alias
        msgr_title = new_messengers_titles.get(msgr.alias) or msgr.title

        for msg in get_registered_message_types().values():
            if not (message_filter is None or message_filter(msg)) or not msg.allow_user_subscription:
                continue

            msgr_supported = msg.supported_messengers
            is_supported = (not msgr_supported or msgr.alias in msgr_supported)

            if not is_supported:
                continue

            msg_alias = msg.alias
            msg_titles.setdefault(f'{msg.title}', []).append(msg_alias)

            msgr_to_msg[msgr_alias].update((msg_alias,))
            msgr_titles[msgr_title] = msgr_alias

    def sort_titles(titles):
        return dict(sorted([(k, v) for k, v in titles.items()], key=itemgetter(0)))

    msgr_titles = sort_titles(msgr_titles)

    user_prefs = {}

    user_subscriptions = [
        f'{pref.message_cls}{_ALIAS_SEP}{pref.messenger_cls}'
        for pref in Subscription.get_for_user(user)]

    for msg_title, msg_aliases in sort_titles(msg_titles).items():

        for __, msgr_alias in msgr_titles.items():
            msg_candidates = msgr_to_msg[msgr_alias].intersection(msg_aliases)

            alias = ''
            msg_supported = False
            subscribed = False

            if msg_candidates:
                alias = f'{msg_candidates.pop()}{_ALIAS_SEP}{msgr_alias}'
                msg_supported = True
                subscribed = alias in user_subscriptions

            user_prefs.setdefault(msg_title, []).append((alias, msg_supported, subscribed))

    return list(msgr_titles.keys()), user_prefs


def set_user_preferences_from_request(request: HttpRequest) -> bool:
    """Sets user subscription preferences using data from a request.

    Expects data sent by form built with `sitemessage_prefs_table` template tag.
    Returns a flag, whether prefs were found in the request.

    :param request:

    """
    prefs = []

    for pref in request.POST.getlist(_PREF_POST_KEY):
        message_alias, messenger_alias = pref.split(_ALIAS_SEP)

        try:
            get_registered_message_type(message_alias)
            get_registered_messenger_object(messenger_alias)

        except (UnknownMessengerError, UnknownMessageTypeError):
            pass

        else:
            prefs.append((message_alias, messenger_alias))

    Subscription.replace_for_user(request.user, prefs)

    return bool(prefs)


def get_sitemessage_urls() -> List:
    """Returns sitemessage urlpatterns, that can be attached to urlpatterns of a project:

        # Example from urls.py.

        from sitemessage.toolbox import get_sitemessage_urls

        urlpatterns = patterns('',
            # Your URL Patterns belongs here.

        ) + get_sitemessage_urls()  # Now attaching additional URLs.

    """
    url_unsubscribe = re_path(
        r'^messages/unsubscribe/(?P<message_id>\d+)/(?P<dispatch_id>\d+)/(?P<hashed>[^/]+)/$',
        unsubscribe,
        name='sitemessage_unsubscribe'
    )

    url_mark_read = re_path(
        r'^messages/ping/(?P<message_id>\d+)/(?P<dispatch_id>\d+)/(?P<hashed>[^/]+)/$',
        mark_read,
        name='sitemessage_mark_read'
    )

    return [url_unsubscribe, url_mark_read]
