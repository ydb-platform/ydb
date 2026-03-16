from django.dispatch import Signal
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.templatetags.static import static as get_static_url

from .exceptions import UnknownMessageTypeError
from .models import Dispatch
from .signals import sig_unsubscribe_failed, sig_mark_read_failed


def _generic_view(
        message_method: str,
        fail_signal: Signal,
        request: HttpRequest,
        message_id: int,
        dispatch_id: int,
        hashed: str,
        redirect_to: str = None
):

    if redirect_to is None:
        redirect_to = '/'

    try:
        dispatch = Dispatch.objects.select_related('message').get(pk=dispatch_id)

        if int(message_id) != dispatch.message_id:
            raise ValueError()

        message = dispatch.message

    except (Dispatch.DoesNotExist, ValueError):
        pass

    else:

        try:
            message_type = message.get_type()
            expected_hash = message_type.get_dispatch_hash(dispatch_id, message_id)

            method = getattr(message_type, message_method)

            return method(
                request, message, dispatch,
                hash_is_valid=(expected_hash == hashed),
                redirect_to=redirect_to
            )

        except UnknownMessageTypeError:
            pass

    fail_signal.send(None, request=request, message=message_id, dispatch=dispatch_id)

    return redirect(redirect_to)


def unsubscribe(
        request: HttpRequest,
        message_id: int,
        dispatch_id: int,
        hashed: str,
        redirect_to: str = None
) -> HttpResponse:
    """Handles an unsubscribe request.

    :param request:
    :param message_id:
    :param dispatch_id:
    :param hashed:
    :param redirect_to:

    """
    return _generic_view(
        'handle_unsubscribe_request', sig_unsubscribe_failed,
        request, message_id, dispatch_id, hashed, redirect_to=redirect_to
    )


def mark_read(
        request: HttpRequest,
        message_id: int,
        dispatch_id: int,
        hashed: str,
        redirect_to: str = None
) -> HttpResponse:
    """Handles mark message as read request.

    :param request:
    :param message_id:
    :param dispatch_id:
    :param hashed:
    :param redirect_to:

    """
    if redirect_to is None:
        redirect_to = get_static_url('img/sitemessage/blank.png')

    return _generic_view(
        'handle_mark_read_request', sig_mark_read_failed,
        request, message_id, dispatch_id, hashed, redirect_to=redirect_to
    )
