# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os
import sys
import stat
import datetime

from tornado.web import StaticFileHandler, HTTPError
from jinja2 import BaseLoader, TemplateNotFound
from library.python import resource


class ResourceLoader(BaseLoader):
    def __init__(self, prefixes):
        self.prefixes = prefixes

    def get_source(self, environment, template):
        for prefix in self.prefixes:
            path = os.path.join(prefix, template)
            source = resource.find(path)
            if source:
                source = source.decode('utf8')
                return source, path, lambda: True
        source = resource.find(template)
        if source:
            source = source.decode('utf8')
            return source, template, lambda: True
        raise TemplateNotFound('Template {0!r}, paths: {1!r}'.format(template, self.prefixes))

    def list_templates(self):
        found = set()

        for prefix in self.prefixes:
            for key in resource.iterkeys(prefix, strip_prefix=True):
                found.add(key.lstrip('/'))

        return sorted(found)


class ResourceFileHandler(StaticFileHandler):
    @classmethod
    def get_absolute_path(cls, root, path):
        return os.path.join(root, path)

    @classmethod
    def get_content(cls, abspath, start=None, end=None):
        data = resource.find(abspath)
        return data[start:end]

    def validate_absolute_path(self, root, absolute_path):
        if resource.find(absolute_path) is None:
            raise HTTPError(404)
        return absolute_path

    def get_content_size(self):
        content_size = len(resource.find(self.absolute_path))
        return content_size

    def get_modified_time(self):
        stat_result = os.stat(sys.executable)
        return datetime.datetime.utcfromtimestamp(stat_result[stat.ST_MTIME])


class CacheControlStaticFilesHandler(ResourceFileHandler):
    """StaticFileHandler subclass that sets Cache-Control: no-cache without `?v=`

    rather than relying on default browser cache behavior.
    """

    def compute_etag(self):
        return None

    def set_extra_headers(self, path):
        if "v" not in self.request.arguments:
            self.add_header("Cache-Control", "no-cache")


class LogoHandler(ResourceFileHandler):
    """A singular handler for serving the logo."""

    def get(self):
        return super().get('')

    @classmethod
    def get_absolute_path(cls, root, path):
        """We only serve one file, ignore relative path"""
        return root
