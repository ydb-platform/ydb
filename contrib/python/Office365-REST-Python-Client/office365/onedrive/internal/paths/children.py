from office365.runtime.paths.v4.entity import EntityPath


class ChildrenPath(EntityPath):
    """Resource path for OneDrive children addressing"""

    def __init__(self, parent, collection=None):
        super(ChildrenPath, self).__init__("children", parent, collection)

    @property
    def collection(self):
        if self._collection is None:
            if isinstance(self.parent, EntityPath):
                self._collection = self.parent.collection
            else:
                self._collection = self.parent
        return self._collection
