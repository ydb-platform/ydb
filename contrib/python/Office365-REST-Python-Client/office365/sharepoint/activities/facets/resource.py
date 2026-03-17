from office365.runtime.client_value import ClientValue
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath


class ResourceFacet(ClientValue):
    def __init__(
        self,
        content_type_id=None,
        file_system_object_type=None,
        file_type=None,
        item_id=None,
        item_unique_id=None,
        list_id=None,
        org_id=None,
        server_relative_path=SPResPath(),
        site_id=None,
        title=None,
        web_id=None,
    ):
        """
        :param str content_type_id: The ID of the content type
        :param int file_system_object_type: List item’s object type in file system.
        :param int file_type: The list item’s file type
        :param str item_id: Identifies the changed item.
        :param str item_unique_id: The Document identifier of the item
        """
        self.contentTypeId = content_type_id
        self.fileSystemObjectType = file_system_object_type
        self.fileType = file_type
        self.itemId = item_id
        self.itemUniqueId = item_unique_id
        self.listId = list_id
        self.orgId = org_id
        self.serverRelativePath = server_relative_path
        self.siteId = site_id
        self.title = title
        self.webId = web_id

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.ResourceFacet"
