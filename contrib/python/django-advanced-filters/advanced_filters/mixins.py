import inspect
import json

from django.conf import settings
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.contrib.auth.views import redirect_to_login
from django.core import serializers
from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.core.serializers.json import DjangoJSONEncoder
from django.http import HttpResponse, StreamingHttpResponse
from django.utils.decorators import method_decorator

try:
    from django.utils.encoding import force_str as force_string
except ImportError:
    from django.utils.encoding import force_text as force_string
from django.views.decorators.csrf import csrf_exempt


class CsrfExemptMixin:
    """
    Exempts the view from CSRF requirements.
    NOTE:
        This should be the left-most mixin of a view.
    """

    @method_decorator(csrf_exempt)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)


class AccessMixin:
    """
    'Abstract' mixin that gives access mixins the same customizable
    functionality.
    """
    login_url = None
    raise_exception = False
    redirect_field_name = REDIRECT_FIELD_NAME  # Set by django.contrib.auth
    redirect_unauthenticated_users = False

    def get_login_url(self):
        """
        Override this method to customize the login_url.
        """
        login_url = self.login_url or settings.LOGIN_URL
        if not login_url:
            raise ImproperlyConfigured(
                'Define {0}.login_url or settings.LOGIN_URL or override '
                '{0}.get_login_url().'.format(self.__class__.__name__))

        return force_string(login_url)

    def get_redirect_field_name(self):
        """
        Override this method to customize the redirect_field_name.
        """
        if self.redirect_field_name is None:
            raise ImproperlyConfigured(
                '{0} is missing the '
                'redirect_field_name. Define {0}.redirect_field_name or '
                'override {0}.get_redirect_field_name().'.format(
                    self.__class__.__name__))
        return self.redirect_field_name

    def handle_no_permission(self, request):
        if self.raise_exception:
            if (self.redirect_unauthenticated_users
                    and not request.user.is_authenticated):
                return self.no_permissions_fail(request)
            else:
                if (inspect.isclass(self.raise_exception)
                        and issubclass(self.raise_exception, Exception)):
                    raise self.raise_exception
                if callable(self.raise_exception):
                    ret = self.raise_exception(request)
                    if isinstance(ret, (HttpResponse, StreamingHttpResponse)):
                        return ret
                raise PermissionDenied

        return self.no_permissions_fail(request)

    def no_permissions_fail(self, request=None):
        """
        Called when the user has no permissions and no exception was raised.
        This should only return a valid HTTP response.
        By default we redirect to login.
        """
        return redirect_to_login(request.get_full_path(),
                                 self.get_login_url(),
                                 self.get_redirect_field_name())


class StaffuserRequiredMixin(AccessMixin):
    """
    Mixin allows you to require a user with `is_staff` set to True.
    """
    def dispatch(self, request, *args, **kwargs):
        if not request.user.is_staff:
            return self.handle_no_permission(request)

        return super().dispatch(
            request, *args, **kwargs)


class JSONResponseMixin:
    """
    A mixin that allows you to easily serialize simple data such as a dict or
    Django models.
    """
    content_type = None
    json_dumps_kwargs = None
    json_encoder_class = DjangoJSONEncoder

    def get_content_type(self):
        if (self.content_type is not None and
            not isinstance(self.content_type,
                           ((str,), str))):
            raise ImproperlyConfigured(
                '{0} is missing a content type. Define {0}.content_type, '
                'or override {0}.get_content_type().'.format(
                    self.__class__.__name__))
        return self.content_type or "application/json"

    def get_json_dumps_kwargs(self):
        if self.json_dumps_kwargs is None:
            self.json_dumps_kwargs = {}
        self.json_dumps_kwargs.setdefault('ensure_ascii', False)
        return self.json_dumps_kwargs

    def render_json_response(self, context_dict, status=200):
        """
        Limited serialization for shipping plain data. Do not use for models
        or other complex or custom objects.
        """
        json_context = json.dumps(
            context_dict,
            cls=self.json_encoder_class,
            **self.get_json_dumps_kwargs()).encode('utf-8')
        return HttpResponse(json_context,
                            content_type=self.get_content_type(),
                            status=status)

    def render_json_object_response(self, objects, **kwargs):
        """
        Serializes objects using Django's builtin JSON serializer. Additional
        kwargs can be used the same way for django.core.serializers.serialize.
        """
        json_data = serializers.serialize("json", objects, **kwargs)
        return HttpResponse(json_data, content_type=self.get_content_type())
