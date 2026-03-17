class FileSystemObjectType:
    """Specifies the file system object type."""

    def __init__(self):
        pass

    Invalid = -1
    """Enumeration whose values specify whether the object is invalid. The value = -1."""

    File = 0
    """Enumeration whose values specify whether the object is a file. The value = 0."""

    Folder = 1
    """Enumeration whose values specify whether the object is a folder. The value = 1."""

    Web = 2
    """Enumeration whose values specify whether the object is a site. The values = 2."""
