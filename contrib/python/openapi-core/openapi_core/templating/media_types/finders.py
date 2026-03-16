"""OpenAPI core templating media types finders module"""
from __future__ import division
import fnmatch

from openapi_core.templating.media_types.exceptions import MediaTypeNotFound


class MediaTypeFinder(object):

    def __init__(self, content):
        self.content = content

    def find(self, request):
        if request.mimetype in self.content:
            return self.content / request.mimetype, request.mimetype

        for key, value in self.content.items():
            if fnmatch.fnmatch(request.mimetype, key):
                return value, key

        raise MediaTypeNotFound(request.mimetype, list(self.content.keys()))
