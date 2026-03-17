from office365.runtime.paths.resource_path import ResourcePath


class AppIdPath(ResourcePath):
    """Path for addressing a Service Principal or Application by appId where
    appId is referred to as Application (client) ID on the Azure portal"""

    @property
    def segment(self):
        return "(appId='{0}')".format(self._key)

    @property
    def delimiter(self):
        return None
