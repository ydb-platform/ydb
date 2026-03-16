from typing import TYPE_CHECKING

from typing_extensions import Self

from office365.onedrive.internal.paths.root import RootPath
from office365.runtime.paths.v4.entity import EntityPath

if TYPE_CHECKING:
    from office365.runtime.paths.resource_path import ResourcePath


class UrlPath(EntityPath):
    """Resource path for OneDrive entity path-based addressing"""

    def __init__(self, url, parent, collection=None):
        # type: (str, ResourcePath, ResourcePath) -> None
        """
        :param str url: File or Folder server relative url
        :type parent: office365.runtime.paths.resource_path.ResourcePath
        """
        if isinstance(parent, UrlPath):
            url = "/".join([parent._key, url])
            collection = parent.collection
            parent = parent.parent
        elif isinstance(parent, RootPath):
            collection = parent.collection
        elif isinstance(parent, EntityPath):
            collection = parent.collection
        super(UrlPath, self).__init__(url, parent, collection)

    def patch(self, key):
        # type: (str) -> Self
        return super(UrlPath, self).patch(key)

    @property
    def segment(self):
        return ":/{0}:/".format(self._key)

    @property
    def delimiter(self):
        return None
