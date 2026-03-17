import django
from django.conf import settings
from django.contrib.auth import get_user_model  # noqa
from django.utils import timezone

try:
    from django.urls import NoReverseMatch, reverse, Resolver404, get_script_prefix  # noqa
except ImportError:  # 1.8 backwards compat
    from django.core.urlresolvers import NoReverseMatch, reverse, Resolver404, get_script_prefix  # noqa

# Django 4.0 had a private _sanitize_token function whose signature is/was different than
# the 4.1 version (_check_token_format) - import the correct one and define a compatability
# function.
if django.VERSION < (4, 1):
    from django.middleware.csrf import _sanitize_token
else:
    from django.middleware.csrf import _check_token_format

# Django 5.0 eliminated the former datetime_safe function, this provides
# some level of backwards compatability for existing tastypie use cases
if django.VERSION < (5, 0):
    from django.utils import datetime_safe  # noqa: F401
else:
    import datetime as datetime_safe  # noqa: F401
    # Django 5.0 removed this alias - restore it for backwards compatability.
    # Django 5.0 essentially completed a lot of the move to zoneinfo, prior to that
    # this was an alias that was added in 4.0.
    timezone.utc = datetime_safe.timezone.utc

AUTH_USER_MODEL = settings.AUTH_USER_MODEL


def get_username_field():
    return get_user_model().USERNAME_FIELD


def get_module_name(meta):
    return meta.model_name


def is_ajax(request):
    """
    Handle multiple ways of detecting an ajax request.  Probably nearly useless.
    """
    return request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'


atomic_decorator = django.db.transaction.atomic

# force_text deprecated in 2.2, removed in 3.0
# note that in 1.1.x, force_str and force_text both exist, but force_str behaves
# much differently on python 3 than python 2.
if django.VERSION < (2, 2):
    from django.utils.encoding import force_text as force_str  # noqa
else:
    from django.utils.encoding import force_str  # noqa

compare_sanitized_tokens = None

# django 4.0
try:
    from django.middleware.csrf import _does_token_match, InvalidTokenFormat

    compare_sanitized_tokens = _does_token_match
except ImportError:
    pass

# django 3.2
if compare_sanitized_tokens is None:
    try:
        from django.middleware.csrf import _compare_masked_tokens

        compare_sanitized_tokens = _compare_masked_tokens

        class InvalidTokenFormat(Exception):  # noqa
            pass
    except ImportError:
        pass

# django 2.2
if compare_sanitized_tokens is None:
    try:
        from django.middleware.csrf import _unsalt_cipher_token, constant_time_compare

        def compare_sanitized_tokens(request_csrf_token, csrf_token):
            return constant_time_compare(_unsalt_cipher_token(request_csrf_token),
                                         _unsalt_cipher_token(csrf_token))

        class InvalidTokenFormat(Exception):  # noqa
            pass
    except ImportError:  # pragma: no cover
        raise ImportError("Couldn't find a way to compare csrf tokens safely")  # pragma: no cover


def check_token_format(csrf_token):
    """
    Handle the pre-4.1 version of sanitizing the token as well as the post-4.0 version of the same.
    """
    if django.VERSION < (4, 1):
        csrf_token = _sanitize_token(csrf_token)
    else:
        _check_token_format(csrf_token)
    return csrf_token
