from __future__ import division

from openapi_core.templating.responses.exceptions import ResponseNotFound


class ResponseFinder(object):

    def __init__(self, responses):
        self.responses = responses

    def find(self, http_status='default'):
        if http_status in self.responses:
            return self.responses / http_status

        # try range
        http_status_range = '{0}XX'.format(http_status[0])
        if http_status_range in self.responses:
            return self.responses / http_status_range

        if 'default' not in self.responses:
            raise ResponseNotFound(http_status, self.responses)

        return self.responses / 'default'
