import base64
from hashlib import sha1
import hmac
import time
import uuid
import warnings

from django.conf import settings
from django.contrib.auth import authenticate
from django.core.exceptions import ImproperlyConfigured

from django.utils.translation import gettext as _

from urllib.parse import urlparse

from tastypie.compat import (
    get_user_model, get_username_field, compare_sanitized_tokens, InvalidTokenFormat, check_token_format
)
from tastypie.http import HttpUnauthorized

try:
    import python_digest
except ImportError:
    python_digest = None

try:
    import oauth2
except ImportError:
    oauth2 = None

try:
    import oauth_provider
except ImportError:
    oauth_provider = None


def same_origin(url1, url2):
    """
    Checks if two URLs are 'same-origin'
    """
    PROTOCOL_TO_PORT = {
        'http': 80,
        'https': 443,
    }
    p1, p2 = urlparse(url1), urlparse(url2)
    try:
        o1 = (p1.scheme, p1.hostname, p1.port or PROTOCOL_TO_PORT[p1.scheme])
        o2 = (p2.scheme, p2.hostname, p2.port or PROTOCOL_TO_PORT[p2.scheme])
        return o1 == o2
    except (ValueError, KeyError):
        return False


class Authentication(object):
    """
    A simple base class to establish the protocol for auth.

    By default, this indicates the user is always authenticated.
    """
    auth_type = 'none'

    def __init__(self, require_active=True):
        self.require_active = require_active

    def get_authorization_data(self, request):
        """
        Verifies that the HTTP Authorization header has the right auth type
        (matches self.auth_type) and returns the auth data.

        Raises ValueError when data could not be extracted.
        """
        authorization = request.META.get('HTTP_AUTHORIZATION', '')

        if not authorization:
            raise ValueError('Authorization header missing or empty.')

        try:
            auth_type, data = authorization.split(' ', 1)
        except ValueError:
            raise ValueError('Authorization header must have a space separating auth_type and data.')

        if auth_type.lower() != self.auth_type:
            raise ValueError('auth_type is not "%s".' % self.auth_type)

        return data

    def is_authenticated(self, request, **kwargs):
        """
        Identifies if the user is authenticated to continue or not.

        Should return either ``True`` if allowed, ``False`` if not or an
        ``HttpResponse`` if you need something custom.
        """
        return True

    def get_identifier(self, request):
        """
        Provides a unique string identifier for the requestor.

        This implementation returns a combination of IP address and hostname.
        """
        return "%s_%s" % (request.META.get('REMOTE_ADDR', 'noaddr'), request.META.get('REMOTE_HOST', 'nohost'))

    def check_active(self, user):
        """
        Ensures the user has an active account.

        Optimized for the ``django.contrib.auth.models.User`` case.
        """
        if not self.require_active:
            # Ignore & move on.
            return True

        return user.is_active


class BasicAuthentication(Authentication):
    """
    Handles HTTP Basic auth against a specific auth backend if provided,
    or against all configured authentication backends using the
    ``authenticate`` method from ``django.contrib.auth``.

    Optional keyword arguments:

    ``backend``
        If specified, use a specific ``django.contrib.auth`` backend instead
        of checking all backends specified in the ``AUTHENTICATION_BACKENDS``
        setting.
    ``realm``
        The realm to use in the ``HttpUnauthorized`` response.  Default:
        ``django-tastypie``.
    """
    auth_type = 'basic'

    def __init__(self, backend=None, realm='django-tastypie', **kwargs):
        super(BasicAuthentication, self).__init__(**kwargs)
        self.backend = backend
        self.realm = realm

    def _unauthorized(self):
        response = HttpUnauthorized()
        # FIXME: Sanitize realm.
        response['WWW-Authenticate'] = 'Basic Realm="%s"' % self.realm
        return response

    def extract_credentials(self, request):
        data = self.get_authorization_data(request)
        data = base64.b64decode(data).decode('utf-8')
        username, password = data.split(':', 1)

        return username, password

    def is_authenticated(self, request, **kwargs):
        """
        Checks a user's basic auth credentials against the current
        Django auth backend.

        Should return either ``True`` if allowed, ``False`` if not or an
        ``HttpResponse`` if you need something custom.
        """
        try:
            username, password = self.extract_credentials(request)
        except ValueError:
            return self._unauthorized()

        if not username or not password:
            return self._unauthorized()

        if self.backend:
            user = self.backend.authenticate(
                username=username,
                password=password
            )
        else:
            if not self.require_active and 'django.contrib.auth.backends.ModelBackend' in settings.AUTHENTICATION_BACKENDS:
                warnings.warn("Authenticating inactive users via ModelUserBackend not supported for Django >= 1.10")
            user = authenticate(username=username, password=password)

        if user is None:
            return self._unauthorized()

        # Kept for backwards-compatibility with Django < 1.10
        if not self.check_active(user):
            return False

        request.user = user
        return True

    def get_identifier(self, request):
        """
        Provides a unique string identifier for the requestor.

        This implementation returns the user's basic auth username.
        """
        try:
            username = self.extract_credentials(request)[0]
        except ValueError:
            username = ''
        return username or 'nouser'


class ApiKeyAuthentication(Authentication):
    """
    Handles API key auth, in which a user provides a username & API key.

    Uses the ``ApiKey`` model that ships with tastypie. If you wish to use
    a different model, override the ``get_key`` method to perform the key check
    as suits your needs.
    """
    auth_type = 'apikey'

    def _unauthorized(self):
        return HttpUnauthorized()

    def extract_credentials(self, request):
        try:
            data = self.get_authorization_data(request)
        except ValueError:
            username = request.GET.get('username') or request.POST.get('username')
            api_key = request.GET.get('api_key') or request.POST.get('api_key')
        else:
            username, api_key = data.split(':', 1)

        return username, api_key

    def is_authenticated(self, request, **kwargs):
        """
        Finds the user and checks their API key.

        Should return either ``True`` if allowed, ``False`` if not or an
        ``HttpResponse`` if you need something custom.
        """
        try:
            username, api_key = self.extract_credentials(request)
        except ValueError:
            return self._unauthorized()

        if not username or not api_key:
            return self._unauthorized()

        username_field = get_username_field()
        User = get_user_model()

        lookup_kwargs = {username_field: username}
        try:
            user = User.objects.select_related('api_key').get(**lookup_kwargs)
        except (User.DoesNotExist, User.MultipleObjectsReturned):
            return self._unauthorized()

        if not self.check_active(user):
            return False

        key_auth_check = self.get_key(user, api_key)
        if key_auth_check and not isinstance(key_auth_check, HttpUnauthorized):
            request.user = user

        return key_auth_check

    def get_key(self, user, api_key):
        """
        Attempts to find the API key for the user. Uses ``ApiKey`` by default
        but can be overridden.
        """
        from tastypie.models import ApiKey

        try:
            if user.api_key.key != api_key:
                return self._unauthorized()
        except ApiKey.DoesNotExist:
            return self._unauthorized()

        return True

    def get_identifier(self, request):
        """
        Provides a unique string identifier for the requestor.

        This implementation returns the user's username.
        """
        try:
            username = self.extract_credentials(request)[0]
        except ValueError:
            username = ''
        return username or 'nouser'


class SessionAuthentication(Authentication):
    """
    An authentication mechanism that piggy-backs on Django sessions.

    This is useful when the API is talking to Javascript on the same site.
    Relies on the user being logged in through the standard Django login
    setup.

    Requires a valid CSRF token.
    """

    def is_authenticated(self, request, **kwargs):
        """
        Checks to make sure the user is logged in & has a Django session.
        """
        # Cargo-culted from Django 1.3/1.4's ``django/middleware/csrf.py``.
        # We can't just use what's there, since the return values will be
        # wrong.
        # We also can't risk accessing ``request.POST``, which will break with
        # the serialized bodies.

        if request.method in ('GET', 'HEAD', 'OPTIONS', 'TRACE'):
            return request.user.is_authenticated

        if getattr(request, '_dont_enforce_csrf_checks', False):
            return request.user.is_authenticated
        csrf_token = request.COOKIES.get(settings.CSRF_COOKIE_NAME, '')

        try:
            csrf_token = check_token_format(csrf_token)
        except InvalidTokenFormat:
            return False

        if request.is_secure():
            referer = request.META.get('HTTP_REFERER')

            if referer is None:
                return False

            good_referer = 'https://%s/' % request.get_host()

            if not same_origin(referer, good_referer):
                return False

        request_csrf_token = request.META.get('HTTP_X_CSRFTOKEN', '')
        try:
            request_csrf_token = check_token_format(request_csrf_token)
        except InvalidTokenFormat:
            return False

        try:
            if not compare_sanitized_tokens(request_csrf_token, csrf_token):
                return False
        except AssertionError:
            return False

        return request.user.is_authenticated

    def get_identifier(self, request):
        """
        Provides a unique string identifier for the requestor.

        This implementation returns the user's username.
        """

        return getattr(request.user, get_username_field())


class DigestAuthentication(Authentication):
    """
    Handles HTTP Digest auth against a specific auth backend if provided,
    or against all configured authentication backends using the
    ``authenticate`` method from ``django.contrib.auth``. However, instead of
    the user's password, their API key should be used.

    Optional keyword arguments:

    ``backend``
        If specified, use a specific ``django.contrib.auth`` backend instead
        of checking all backends specified in the ``AUTHENTICATION_BACKENDS``
        setting.
    ``realm``
        The realm to use in the ``HttpUnauthorized`` response.  Default:
        ``django-tastypie``.
    """
    auth_type = 'digest'

    def __init__(self, backend=None, realm='django-tastypie', **kwargs):
        super(DigestAuthentication, self).__init__(**kwargs)
        self.backend = backend
        self.realm = realm

        if python_digest is None:
            raise ImproperlyConfigured(
                "The 'python_digest' package could not be imported. It is required for use with the 'DigestAuthentication' class.")

    def _unauthorized(self):
        response = HttpUnauthorized()
        new_uuid = uuid.uuid4()
        opaque = hmac.new(str(new_uuid).encode('utf-8'), digestmod=sha1).hexdigest()
        response['WWW-Authenticate'] = python_digest.build_digest_challenge(
            timestamp=time.time(),
            secret=settings.SECRET_KEY,
            realm=self.realm,
            opaque=opaque,
            stale=False
        )
        return response

    def is_authenticated(self, request, **kwargs):
        """
        Finds the user and checks their API key.

        Should return either ``True`` if allowed, ``False`` if not or an
        ``HttpResponse`` if you need something custom.
        """
        try:
            self.get_authorization_data(request)
        except ValueError:
            return self._unauthorized()

        digest_response = python_digest.parse_digest_credentials(request.META['HTTP_AUTHORIZATION'])

        # FIXME: Should the nonce be per-user?
        if not python_digest.validate_nonce(digest_response.nonce, settings.SECRET_KEY):
            return self._unauthorized()

        user = self.get_user(digest_response.username)
        api_key = self.get_key(user)

        if user is False or api_key is False:
            return self._unauthorized()

        expected = python_digest.calculate_request_digest(
            request.method,
            python_digest.calculate_partial_digest(digest_response.username, self.realm, api_key),
            digest_response)

        if not digest_response.response == expected:
            return self._unauthorized()

        if not self.check_active(user):
            return False

        request.user = user
        return True

    def get_user(self, username):
        username_field = get_username_field()
        User = get_user_model()

        try:
            lookup_kwargs = {username_field: username}
            user = User.objects.get(**lookup_kwargs)
        except (User.DoesNotExist, User.MultipleObjectsReturned):
            return False

        return user

    def get_key(self, user):
        """
        Attempts to find the API key for the user. Uses ``ApiKey`` by default
        but can be overridden.

        Note that this behaves differently than the ``ApiKeyAuthentication``
        method of the same name.
        """
        from tastypie.models import ApiKey

        try:
            key = ApiKey.objects.get(user=user)
        except ApiKey.DoesNotExist:
            return False

        return key.key

    def get_identifier(self, request):
        """
        Provides a unique string identifier for the requestor.

        This implementation returns the user's username.
        """
        if hasattr(request, 'user'):
            if hasattr(request.user, 'username'):
                return request.user.username

        return 'nouser'


class OAuthAuthentication(Authentication):
    """
    Handles OAuth, which checks a user's credentials against a separate service.
    Currently verifies against OAuth 1.0a services.

    This does *NOT* provide OAuth authentication in your API, strictly
    consumption.
    """

    def __init__(self, **kwargs):
        super(OAuthAuthentication, self).__init__(**kwargs)

        if oauth2 is None:
            raise ImproperlyConfigured(
                "The 'python-oauth2' package could not be imported. It is required for use with the 'OAuthAuthentication' class.")

        if oauth_provider is None:
            raise ImproperlyConfigured(
                "The 'django-oauth-plus' package could not be imported. It is required for use with the 'OAuthAuthentication' class.")

    def is_authenticated(self, request, **kwargs):
        from oauth_provider.store import store

        if self.is_valid_request(request):
            oauth_request = oauth_provider.utils.get_oauth_request(request)
            consumer = store.get_consumer(request, oauth_request, oauth_request.get_parameter('oauth_consumer_key'))

            try:
                token = store.get_access_token(request, oauth_request, consumer,
                                               oauth_request.get_parameter('oauth_token'))
            except oauth_provider.store.InvalidTokenError:
                return oauth_provider.utils.send_oauth_error(
                    oauth2.Error(_('Invalid access token: %s') % oauth_request.get_parameter('oauth_token')))

            try:
                self.validate_token(request, consumer, token)
            except oauth2.Error as e:
                return oauth_provider.utils.send_oauth_error(e)

            if consumer and token:
                user = store.get_user_for_access_token(request, oauth_request, token)
                if not self.check_active(user):
                    return False

                request.user = user
                return True

            return oauth_provider.utils.send_oauth_error(
                oauth2.Error(_('You are not allowed to access this resource.')))

        return oauth_provider.utils.send_oauth_error(oauth2.Error(_('Invalid request parameters.')))

    def is_in(self, params):
        """
        Checks to ensure that all the OAuth parameter names are in the
        provided ``params``.
        """
        from oauth_provider.consts import OAUTH_PARAMETERS_NAMES

        for param_name in OAUTH_PARAMETERS_NAMES:
            if param_name not in params:
                return False

        return True

    def is_valid_request(self, request):
        """
        Checks whether the required parameters are either in the HTTP
        ``Authorization`` header sent by some clients (the preferred method
        according to OAuth spec) or fall back to ``GET/POST``.
        """
        auth_params = request.META.get("HTTP_AUTHORIZATION", [])
        return (self.is_in(auth_params)
                or self.is_in(request.POST)
                or self.is_in(request.GET))

    def validate_token(self, request, consumer, token):
        oauth_server, oauth_request = oauth_provider.utils.initialize_server_request(request)
        return oauth_server.verify_request(oauth_request, consumer, token)


class MultiAuthentication(object):
    """
    An authentication backend that tries a number of backends in order.
    """

    def __init__(self, *backends, **kwargs):
        super(MultiAuthentication, self).__init__(**kwargs)
        self.backends = backends

    def is_authenticated(self, request, **kwargs):
        """
        Identifies if the user is authenticated to continue or not.

        Should return either ``True`` if allowed, ``False`` if not or an
        ``HttpResponse`` if you need something custom.
        """
        unauthorized = False

        for backend in self.backends:
            check = backend.is_authenticated(request, **kwargs)

            if check:
                if isinstance(check, HttpUnauthorized):
                    unauthorized = unauthorized or check
                else:
                    request._authentication_backend = backend
                    return check

        return unauthorized

    def get_identifier(self, request):
        """
        Provides a unique string identifier for the requestor.

        This implementation returns a combination of IP address and hostname.
        """
        try:
            return request._authentication_backend.get_identifier(request)
        except AttributeError:
            return 'nouser'
