# -*- coding: utf-8 -*-
from .base import BaseMatcher


class MethodMatcher(BaseMatcher):
    # Matches based on the method of the request
    name = 'method'

    def match(self, request, recorded_request):
        return request.method == recorded_request['method']
