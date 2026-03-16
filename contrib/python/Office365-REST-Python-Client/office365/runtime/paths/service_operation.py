from office365.runtime.client_value import ClientValue
from office365.runtime.paths.builder import ODataPathBuilder
from office365.runtime.paths.resource_path import ResourcePath


class ServiceOperationPath(ResourcePath):
    """Path to address Service Operations which represents simple functions exposed by an OData service"""

    def __init__(self, name, parameters=None, parent=None):
        # type: (str, list|dict|ClientValue, ResourcePath) -> None
        super(ServiceOperationPath, self).__init__(name, parent)
        self._parameters = parameters

    @property
    def segment(self):
        return ODataPathBuilder.build_segment(self)

    @property
    def name(self):
        return self._key

    @property
    def parameters(self):
        return self._parameters
