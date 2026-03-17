# -*- coding: utf-8 -*-
from .base import BaseMatcher
from .query import QueryMatcher
from requests.compat import urlparse


class URIMatcher(BaseMatcher):
    # Matches based on the uri of the request
    name = 'uri'

    def on_init(self):
        # Get something we can use to match query strings with
        self.query_matcher = QueryMatcher().match

    def match(self, request, recorded_request):
        queries_match = self.query_matcher(request, recorded_request)
        request_url, recorded_url = request.url, recorded_request['uri']
        return self.all_equal(request_url, recorded_url) and queries_match

    def parse(self, uri):
        parsed = urlparse(uri)
        return {
            'scheme': parsed.scheme,
            'netloc': parsed.netloc,
            'path': parsed.path,
            'fragment': parsed.fragment
            }

    def all_equal(self, new_uri, recorded_uri):
        new_parsed = self.parse(new_uri)
        recorded_parsed = self.parse(recorded_uri)
        return (new_parsed == recorded_parsed)
