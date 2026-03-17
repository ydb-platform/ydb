import logging

from django.http.response import HttpResponse
from django.urls import reverse

from . import constants
from .settings import scim_settings
from .utils import get_is_authenticated_predicate, get_loggable_body

logger = logging.getLogger(__name__)


class SCIMAuthCheckMiddleware(object):
    """
    Check to see if a prior middleware has logged the user in.

    This middleware should be place after auth middleware used to login a user.
    """

    def __init__(self, get_response=None):
        # One-time configuration and initialization per server start.
        self.get_response = get_response

    def __call__(self, request, *args, **kwargs):
        response = None
        if hasattr(self, 'process_request'):
            response = self.process_request(request)
        if not response:
            response = self.get_response(request, *args, **kwargs)
        if hasattr(self, 'process_response'):
            response = self.process_response(request, response)
        return response

    @property
    def reverse_url(self):
        if not hasattr(self, '_reverse_url'):
            self._reverse_url = reverse('scim:root')
        return self._reverse_url

    def should_log_request(self, request):
        """
        Return True if request should be logged.
        """
        return request.path.startswith(self.reverse_url)

    def process_request(self, request):
        if self.should_log_request(request):
            self.log_request(request)
        # If we've just passed through the auth middleware and there is no user
        # associated with the request we can assume permission
        # was denied and return a 401.
        if not hasattr(request, 'user') or not get_is_authenticated_predicate()(request.user):
            if request.path.startswith(self.reverse_url):
                response = HttpResponse(status=401)
                response['WWW-Authenticate'] = scim_settings.WWW_AUTHENTICATE_HEADER
                return response

    def process_response(self, request, response):
        if self.should_log_request(request):
            self.log_response(request, response)
        return response

    def get_loggable_content(self, content):
        try:
            body = get_loggable_body(content.decode(constants.ENCODING))
        except Exception as e:
            body = 'Could not parse request body\n' + str(e)

        return body

    def get_loggable_request_message(self, request):
        body = self.get_loggable_content(request.body)
        parts = [
            'PATH',
            request.path,
            'METHOD',
            request.method,
            'BODY',
            body,
        ]

        return '\n'.join(parts)

    def get_loggable_response_message(self, request, response):
        body = self.get_loggable_content(response.content)
        parts = [
            'PATH',
            request.path,
            'METHOD',
            request.method,
            'BODY',
            body,
            'STATUS_CODE',
            str(response.status_code),
        ]

        return '\n'.join(parts)

    def log_request(self, request):
        message = self.get_loggable_request_message(request)
        logger.debug(message)

    def log_response(self, request, response):
        message = self.get_loggable_response_message(request, response)
        logger.debug(message)
