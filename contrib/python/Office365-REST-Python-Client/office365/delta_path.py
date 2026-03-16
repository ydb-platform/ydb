from office365.runtime.paths.v4.entity import EntityPath


class DeltaPath(EntityPath):
    """Delta path"""

    def __init__(self, parent=None):
        super(DeltaPath, self).__init__("delta", parent, parent)
