from office365.runtime.compat import is_absolute_url, urlparse
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.v4.entity import EntityPath


class SitePath(EntityPath):
    """Resource path for addressing Site resource"""

    @property
    def segment(self):
        if is_absolute_url(self._key):
            url_result = urlparse(self._key)
            return ":".join([url_result.hostname, url_result.path])
        else:
            return super(SitePath, self).segment

    @property
    def collection(self):
        if self._collection is None:
            self._collection = ResourcePath("sites")
        return self._collection
