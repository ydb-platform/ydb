from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.v4.entity import EntityPath


class MePath(EntityPath):
    """SignedIn user resource path"""

    def __init__(self):
        super(MePath, self).__init__("me", None, ResourcePath("users"))
