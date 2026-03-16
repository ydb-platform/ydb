from office365.runtime.client_value import ClientValue
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath


class ListItemCreationInformationUsingPath(ClientValue):
    def __init__(self, leaf_name, object_type, folder_path=None):
        """
        Specifies the properties of the new list item.

        :param str leaf_name: Specifies the name of the list item that will be created. In the case of a document
            library, the name is equal to the filename of the list item.
        :param int object_type: Specifies the file system object type for the item that will be created.
        :param str or SPResPath folder_path: Specifies the path of the folder of the new list item.
        """
        super(ListItemCreationInformationUsingPath, self).__init__()
        self.LeafName = leaf_name
        self.UnderlyingObjectType = object_type
        self.FolderPath = (
            folder_path
            if isinstance(folder_path, SPResPath)
            else SPResPath(folder_path)
        )
