# -*- coding: utf-8 -*-
from .base import BaseMatcher
from requests.compat import urlparse


class HostMatcher(BaseMatcher):
    # Matches based on the host of the request
    name = 'host'

    def match(self, request, recorded_request):
        request_host = urlparse(request.url).netloc
        recorded_host = urlparse(recorded_request['uri']).netloc
        return request_host == recorded_host
