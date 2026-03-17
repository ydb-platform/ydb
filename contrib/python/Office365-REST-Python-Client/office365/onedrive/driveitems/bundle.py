from office365.runtime.client_value import ClientValue


class Bundle(ClientValue):
    """A bundle is a logical grouping of files used to share multiple files at once.
    It is represented by a driveItem entity containing a bundle facet and can be shared in the same way
    as any other driveItem."""

    def __init__(self, album=None, child_count=None):
        """
        :param Album album: If the bundle is an album, then the album property is included
        :param int child_count: Number of children contained immediately within this container.
        """
        self.album = album
        self.childCount = child_count
