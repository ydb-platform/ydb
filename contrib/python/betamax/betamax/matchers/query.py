# -*- coding: utf-8 -*-
import sys

from .base import BaseMatcher

try:
    from urlparse import parse_qs, urlparse
except ImportError:
    from urllib.parse import parse_qs, urlparse


isPY2 = (2, 6) <= sys.version_info < (3, 0)


class QueryMatcher(BaseMatcher):
    # Matches based on the query of the request
    name = 'query'

    def to_dict(self, query):
        """Turn the query string into a dictionary."""
        return parse_qs(
            query or '',  # Protect against None
            keep_blank_values=True,
        )

    def match(self, request, recorded_request):
        request_query_dict = self.to_dict(urlparse(request.url).query)
        recorded_query = urlparse(recorded_request['uri']).query
        if recorded_query and isPY2:
            # NOTE(sigmavirus24): If we're on Python 2, the request.url will
            # be str/bytes and the recorded_request['uri'] will be unicode.
            # For the comparison to work for high unicode characters, we need
            # to encode the recorded query string before parsing it. See also
            # GitHub bug #43.
            recorded_query = recorded_query.encode('utf-8')
        recorded_query_dict = self.to_dict(recorded_query)
        return request_query_dict == recorded_query_dict
