# -*- coding: utf-8 -*-
from .base import BaseMatcher

from betamax import util


class BodyMatcher(BaseMatcher):
    # Matches based on the body of the request
    name = 'body'

    def match(self, request, recorded_request):
        recorded_request = util.deserialize_prepared_request(recorded_request)

        request_body = b''
        if request.body:
            request_body = util.coerce_content(request.body)

        recorded_body = b''
        if recorded_request.body:
            recorded_body = util.coerce_content(recorded_request.body)

        return recorded_body == request_body
