from office365.runtime.client_value import ClientValue


class Album(ClientValue):
    """
    A photo album is a way to virtually group driveItems with photo facets together in a bundle.
    Bundles of this type will have the album property set on the bundle resource.
    """

    def __init__(self, cover_image_item_id=None):
        """
        :param str cover_image_item_id: Unique identifier of the driveItem that is the cover of the album.
        """
        self.coverImageItemId = cover_image_item_id
