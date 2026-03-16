# coding: utf-8

from __future__ import unicode_literals

from django.conf import settings
from six.moves import urllib_parse as urlparse
import uuid

from .base import BaseProvider


def _get_request_id(request):
    return request.META.get(
        'HTTP_X_REQ_ID',
        request.META.get(
            'HTTP_X_REQUEST_ID',
            request.GET.get(
                'request_id',
                'auto-' + uuid.uuid4().hex,
            )
        )
    )


class Provider(BaseProvider):
    required_kwargs = ['request']

    def _get_headers(self, request):
        for header, not_secure in settings.TOOLS_LOG_CONTEXT_ALLOWED_HEADERS:
            value = request.META.get(header)
            if value is not None:
                yield header, value if not_secure else '******'

    def _get_query_params(self, request):
        params = urlparse.parse_qs(request.META.get('QUERY_STRING') or request.GET.urlencode() or '')
        for blacklist_query_param in settings.TOOLS_LOG_CONTEXT_QUERY_PARAMS_BLACKLIST:
            if blacklist_query_param in params:
                params[blacklist_query_param] = '******'
        return params

    def request(self, request):
        return {
            'id': _get_request_id(request),
            'method': request.method,
            'path': request.path,
            'query_params': self._get_query_params(request),
            'headers': dict(self._get_headers(request)),
        }
