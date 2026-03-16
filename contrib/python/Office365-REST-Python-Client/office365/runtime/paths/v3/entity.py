from office365.runtime.paths.resource_path import ResourcePath


class EntityPath(ResourcePath):
    """Path for addressing a single entity by key"""

    @property
    def segment(self):
        if self._key is None:
            return "(<key>)"
        elif isinstance(self._key, int):
            return "({0})".format(self._key)
        return "('{0}')".format(self._key)

    @property
    def delimiter(self):
        return None
