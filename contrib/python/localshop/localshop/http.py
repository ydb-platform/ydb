from django.conf import settings
from django.http import HttpResponse

BASIC_AUTH_REALM = getattr(settings, 'BASIC_AUTH_REALM', 'pypi')


class HttpResponseUnauthorized(HttpResponse):
    status_code = 401

    def __init__(self, basic_auth_realm=None, *args, **kwargs):
        super(HttpResponseUnauthorized, self).__init__(*args, **kwargs)
        if basic_auth_realm is None:
            basic_auth_realm = BASIC_AUTH_REALM
        self['WWW-Authenticate'] = 'Basic realm="%s"' % basic_auth_realm
