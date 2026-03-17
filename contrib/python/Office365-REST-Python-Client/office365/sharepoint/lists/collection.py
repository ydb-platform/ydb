from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.lists.creation_information import ListCreationInformation
from office365.sharepoint.lists.list import List


class ListCollection(EntityCollection[List]):
    """Specifies a collection of lists."""

    def __init__(self, context, resource_path=None):
        super(ListCollection, self).__init__(context, List, resource_path)

    def get_by_title(self, list_title):
        # type: (str) -> List
        """
        Returns the list with the specified display name.
        :param str list_title: Specifies the display name
        """
        return List(
            self.context,
            ServiceOperationPath("GetByTitle", [list_title], self.resource_path),
        )

    def get_by_id(self, list_id):
        # type: (str) -> List
        """
        Returns the list with the specified list identifier.
        :param str list_id: Specifies the list identifier
        """
        return List(
            self.context, ServiceOperationPath("GetById", [list_id], self.resource_path)
        )

    def ensure_client_rendered_site_pages_library(self):
        """
        Returns a list that is designated as a default location for site pages.
        """
        return_type = List(self.context)
        self.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "EnsureClientRenderedSitePagesLibrary", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ensure_events_list(self):
        """Returns a list that is designated as a default location for events."""
        return_type = List(self.context)
        self.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "EnsureEventsList", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ensure_site_assets_library(self):
        """Gets a list that is the default asset location for images or other files, which the users
        upload to their wiki pages."""
        return_type = List(self.context)
        self.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "ensureSiteAssetsLibrary", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ensure_site_pages_library(self):
        """Gets a list that is the default location for wiki pages."""
        return_type = List(self.context)
        self.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "ensureSitePagesLibrary", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def add(self, list_creation_information):
        # type: (ListCreationInformation) -> List
        """Creates a List resource"""
        return_type = List(self.context)
        self.add_child(return_type)
        qry = CreateEntityQuery(self, list_creation_information, return_type)
        self.context.add_query(qry)
        return return_type
