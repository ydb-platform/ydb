from office365.runtime.client_value import ClientValue


class ListItemCreationInformation(ClientValue):
    def __init__(self, leaf_name=None, folder_url=None, underlying_object_type=None):
        """
        Specifies the properties of the new list item.

        :param int underlying_object_type: Specifies whether the new list item is a file or a folder.
        :param str leaf_name: Specifies the name of the new list item. It MUST be the name of the file if the parent
            list of the list item is a document library.
        :param str folder_url: Specifies the folder for the new list item. It MUST be NULL, empty, a server-relative
            URL, or an absolute URL. If the value is a server-relative URL or an absolute URL, it MUST be under the root
            folder of the list.
        """
        super(ListItemCreationInformation, self).__init__()
        self.FolderUrl = folder_url
        self.LeafName = leaf_name
        self.UnderlyingObjectType = underlying_object_type
