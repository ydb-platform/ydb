# -*- coding: utf-8 -*-
from .base import BaseMatcher


class HeadersMatcher(BaseMatcher):
    # Matches based on the headers of the request
    name = 'headers'

    def match(self, request, recorded_request):
        return dict(request.headers) == self.flatten_headers(recorded_request)

    def flatten_headers(self, request):
        from betamax.util import from_list
        headers = request['headers'].items()
        return dict((k, from_list(v)) for (k, v) in headers)
