from office365.runtime.paths.v4.entity import EntityPath


class RootPath(EntityPath):
    """Resource path for OneDrive path-based addressing"""

    def __init__(self, parent=None, collection=None):
        super(RootPath, self).__init__("root", parent, collection)
