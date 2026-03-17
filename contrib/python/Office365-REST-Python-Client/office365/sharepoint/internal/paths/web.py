from office365.runtime.compat import is_absolute_url, urlparse
from office365.runtime.paths.resource_path import ResourcePath


class WebPath(ResourcePath):
    @property
    def segment(self):
        return "Web"

    @property
    def web_path(self):
        if is_absolute_url(self._key):
            url_parts = urlparse(self._key)
            return url_parts.path
        else:
            return self._key

    @property
    def parent(self):
        return None
