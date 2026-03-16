# -*- coding: utf-8 -*-
from .base import BaseMatcher
from requests.compat import urlparse


class PathMatcher(BaseMatcher):
    # Matches based on the path of the request
    name = 'path'

    def match(self, request, recorded_request):
        request_path = urlparse(request.url).path
        recorded_path = urlparse(recorded_request['uri']).path
        return request_path == recorded_path
