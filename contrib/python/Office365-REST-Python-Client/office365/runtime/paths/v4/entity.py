from typing import Optional

from typing_extensions import Self

from office365.runtime.paths.resource_path import ResourcePath


class EntityPath(ResourcePath):
    def __init__(self, key=None, parent=None, collection=None):
        # type: (Optional[str], Optional[ResourcePath], Optional[ResourcePath]) -> None
        super(EntityPath, self).__init__(key, parent)
        self._collection = collection

    @property
    def collection(self):
        from office365.onedrive.internal.paths.children import ChildrenPath

        if self._collection is None:
            if isinstance(self.parent, ChildrenPath):
                self._collection = self.parent.collection
            else:
                self._collection = self.parent
        return self._collection

    @property
    def segment(self):
        return str(self._key or "<key>")

    def patch(self, key):
        # type: (str) -> Self
        """Patches the path"""
        self._key = key
        self._parent = self.collection
        self.__class__ = EntityPath
        return self
