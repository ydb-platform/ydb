from office365.entity_collection import EntityCollection
from office365.onedrive.listitems.list_item import ListItem
from office365.runtime.http.request_options import RequestOptions


class ListItemCollection(EntityCollection[ListItem]):
    """List Item's collection"""

    def __init__(self, context, resource_path):
        super(ListItemCollection, self).__init__(context, ListItem, resource_path)
        self._honor_nonindexed = True

    def honor_nonindexed(self, value):
        # type: (bool) -> "ListItemCollection"
        """Sets if true HonorNonIndexedQueriesWarningMayFailRandomly header
        to allow non-indexed properties to be queried."""
        self._honor_nonindexed = value
        return self

    def get(self):
        """Retrieve a list item"""

        def _construct_request(request):
            # type: (RequestOptions) -> None
            if self._honor_nonindexed:
                request.headers["Prefer"] = (
                    "HonorNonIndexedQueriesWarningMayFailRandomly"
                )

        return super(ListItemCollection, self).get().before_execute(_construct_request)

    def get_by_name(self, name):
        """Retrieve a list item by name"""
        return self.single("fields/FileLeafRef eq '{0}'".format(name))

    def get_by_path(self, path):
        """Retrieve a list item by path"""
        return self.single("fields/FileRef eq '{0}'".format(path))
