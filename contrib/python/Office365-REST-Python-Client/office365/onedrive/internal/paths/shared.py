import base64

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.v4.entity import EntityPath


def _url_to_shared_token(url):
    # type: (str) -> str
    """Converts url into shared token"""
    value = base64.b64encode(url.encode("ascii")).decode("ascii")
    if value.endswith("="):
        value = value[:-1]
    return "u!" + value.replace("/", "_").replace("+", "-")


class SharedPath(EntityPath):
    """Shared token path"""

    def patch(self, key):
        self._key = "items"
        self._parent = ResourcePath(key, ResourcePath("drives"))
        self.__class__ = ResourcePath
        return self

    @property
    def segment(self):
        return _url_to_shared_token(self._key)
