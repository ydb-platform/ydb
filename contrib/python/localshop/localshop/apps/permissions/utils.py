from base64 import b64decode
from functools import wraps

from django.conf import settings
from django.contrib.auth import authenticate, login
from django.db import DatabaseError, DataError
from django.http import HttpResponseForbidden
from django.utils.decorators import available_attrs

from localshop.apps.permissions.models import CIDR
from localshop.http import HttpResponseUnauthorized


def decode_credentials(auth):
    auth = b64decode(auth.strip()).decode('utf-8')
    return auth.split(':', 1)


def split_auth(request):
    auth = request.META.get('HTTP_AUTHORIZATION')
    if auth:
        method, identity = auth.split(' ', 1)
    else:
        method, identity = None, None
    return method, identity


def get_basic_auth_data(request):
    method, identity = split_auth(request)
    if method is not None and method.lower() == 'basic':
        return decode_credentials(identity)
    return None, None


def authenticate_user(request):
    key, secret = get_basic_auth_data(request)
    if key and secret:
        try:
            user = authenticate(access_key=key, secret_key=secret)
        except (DatabaseError, DataError):
            # we fallback on django user auth in case of DB error
            user = None
        if not user:
            user = authenticate(username=key, password=secret)
        return user


def credentials_required(view_func):
    """
    This decorator should be used with views that need simple authentication
    against Django's authentication framework.
    """
    @wraps(view_func, assigned=available_attrs(view_func))
    def decorator(request, *args, **kwargs):
        if settings.LOCALSHOP_USE_PROXIED_IP:
            try:
                ip_addr = request.META['HTTP_X_FORWARDED_FOR']
            except KeyError:
                return HttpResponseForbidden('No permission')
            else:
                # HTTP_X_FORWARDED_FOR can be a comma-separated list of IPs.
                # The client's IP will be the first one.
                ip_addr = ip_addr.split(",")[0].strip()
        else:
            ip_addr = request.META['REMOTE_ADDR']

        if CIDR.objects.has_access(ip_addr, with_credentials=False):
            return view_func(request, *args, **kwargs)

        if not CIDR.objects.has_access(ip_addr, with_credentials=True):
            return HttpResponseForbidden('No permission')

        # Just return the original view because already logged in
        if request.user.is_authenticated():
            return view_func(request, *args, **kwargs)

        user = authenticate_user(request)
        if user is not None:
            login(request, user)
            return view_func(request, *args, **kwargs)

        return HttpResponseUnauthorized(content='Authorization Required')
    return decorator
