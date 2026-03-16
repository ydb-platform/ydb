# coding: utf-8

from django.conf import settings
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import load_backend, login
from django.core.exceptions import ObjectDoesNotExist
from django.http import Http404
from django.shortcuts import redirect
from django.utils.html import escape
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils.translation import gettext_lazy as _

from grappelli.settings import SWITCH_USER_ORIGINAL, SWITCH_USER_TARGET

try:
    from django.contrib.auth import get_user_model
    User = get_user_model()
except ImportError:
    from django.contrib.auth.models import User


@staff_member_required
def switch_user(request, object_id):

    # current/session user
    current_user = request.user
    session_user = request.session.get("original_user", {"id": current_user.id, "username": current_user.get_username()})

    # check redirect
    redirect_url = request.GET.get("redirect", None)
    if redirect_url is None or not \
        url_has_allowed_host_and_scheme(
            url=redirect_url,
            allowed_hosts={request.get_host()},
            require_https=request.is_secure(),
        ):
        raise Http404()
        
    # check original_user
    try:
        original_user = User.objects.get(pk=session_user["id"], is_staff=True)
        if not SWITCH_USER_ORIGINAL(original_user):
            messages.add_message(request, messages.ERROR, _("Permission denied."))
            return redirect(redirect_url)
    except ObjectDoesNotExist:
        msg = _('%(name)s object with primary key %(key)r does not exist.') % {'name': "User", 'key': escape(session_user["id"])}
        messages.add_message(request, messages.ERROR, msg)
        return redirect(redirect_url)

    # check new user
    try:
        target_user = User.objects.get(pk=object_id, is_staff=True)
        if target_user != original_user and not SWITCH_USER_TARGET(original_user, target_user):
            messages.add_message(request, messages.ERROR, _("Permission denied."))
            return redirect(redirect_url)
    except ObjectDoesNotExist:
        msg = _('%(name)s object with primary key %(key)r does not exist.') % {'name': "User", 'key': escape(object_id)}
        messages.add_message(request, messages.ERROR, msg)
        return redirect(redirect_url)

    # find backend
    if not hasattr(target_user, 'backend'):
        for backend in settings.AUTHENTICATION_BACKENDS:
            if target_user == load_backend(backend).get_user(target_user.pk):
                target_user.backend = backend
                break

    # target user login, set original as session
    if hasattr(target_user, 'backend'):
        login(request, target_user)
        if original_user.id != target_user.id:
            request.session["original_user"] = {"id": original_user.id, "username": original_user.get_username()}

    return redirect(redirect_url)
