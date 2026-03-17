from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, DisallowedHost
from django.utils.deprecation import MiddlewareMixin
from django.urls import NoReverseMatch, set_urlconf, get_urlconf

from .resolvers import get_host_patterns, get_host


class HostsBaseMiddleware(MiddlewareMixin):
    """
    Adjust incoming request's urlconf based on hosts defined in
    settings.ROOT_HOSTCONF module.
    """
    new_hosts_middleware = 'django_hosts.middleware.HostsRequestMiddleware'
    toolbar_middleware = 'debug_toolbar.middleware.DebugToolbarMiddleware'

    def __init__(self, get_response=None):
        self.get_response = get_response
        self.current_urlconf = None
        self.host_patterns = get_host_patterns()
        try:
            self.default_host = get_host()
        except NoReverseMatch as exc:
            raise ImproperlyConfigured("Invalid DEFAULT_HOST setting: %s" %
                                       exc)

        middleware_setting = 'MIDDLEWARE' if getattr(settings, 'MIDDLEWARE', None) is not None else 'MIDDLEWARE_CLASSES'
        middlewares = list(getattr(settings, middleware_setting))

        show_exception = False

        if self.new_hosts_middleware in middlewares and self.toolbar_middleware in middlewares:
            show_exception = (middlewares.index(self.new_hosts_middleware) >
                              middlewares.index(self.toolbar_middleware))

        if show_exception:
            raise ImproperlyConfigured(
                "The django-hosts and django-debug-toolbar middlewares "
                "are in the wrong order. Make sure the django-hosts "
                "middleware comes before the django-debug-toolbar "
                "middleware in the %s setting." % middleware_setting)

    def get_host(self, request_host):
        for host in self.host_patterns:
            match = host.compiled_regex.match(request_host)
            if match:
                return host, match.groupdict()
        return self.default_host, {}


class HostsRequestMiddleware(HostsBaseMiddleware):
    def process_request(self, request):
        # Find best match, falling back to settings.DEFAULT_HOST
        host, kwargs = self.get_host(request.get_host())
        # This is the main part of this middleware
        request.urlconf = host.urlconf
        request.host = host
        # But we have to temporarily override the URLconf
        # already to allow correctly reversing host URLs in
        # the host callback, if needed.
        current_urlconf = get_urlconf()
        try:
            set_urlconf(host.urlconf)
            return host.callback(request, **kwargs)
        finally:
            # Reset URLconf for this thread on the way out for complete
            # isolation of request.urlconf
            set_urlconf(current_urlconf)


class HostsResponseMiddleware(HostsBaseMiddleware):
    def process_response(self, request, response):
        # Django resets the base urlconf when it starts to process
        # the response, so we need to set this again, in case
        # any of our middleware makes use of host, etc URLs.

        # Find best match, falling back to settings.DEFAULT_HOST
        try:
            host, kwargs = self.get_host(request.get_host())
        except DisallowedHost:
            # Bail out early, there is nothing to reset as HostsRequestMiddleware
            # never gets called with an invalid host.
            return response
        # This is the main part of this middleware
        request.urlconf = host.urlconf
        request.host = host

        set_urlconf(host.urlconf)
        return response
