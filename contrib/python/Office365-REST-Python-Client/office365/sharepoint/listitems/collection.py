from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.listitems.listitem import ListItem


class ListItemCollection(EntityCollection[ListItem]):
    """List Item collection"""

    def __init__(self, context, resource_path=None):
        super(ListItemCollection, self).__init__(context, ListItem, resource_path)

    def get_by_id(self, item_id):
        """
        Returns the list item with the specified list item identifier.

        :param int item_id: The list item identifier.
        """
        return ListItem(
            self.context, ServiceOperationPath("GetById", [item_id], self.resource_path)
        )

    def get_by_string_id(self, s_id):
        """
        Returns the list item with either the specified list item identifier or the specified identifier
        for an instance of an external content type.

        :param str s_id: Specifies the list item identifier, or if the list is an external list, specifies the
        identifier for an instance of an external content type as specified in[MS-ECTPWPS] section 3.1.4.1.2.1.
        """
        return ListItem(
            self.context,
            ServiceOperationPath("GetByStringId", {"sId": s_id}, self.resource_path),
        )

    def get_by_url(self, url):
        """Returns the list item with the specified site or server relative url."""
        return self.single("FileRef eq '{0}'".format(url))
