from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.listitems.versions.get_parameters import (
    GetListItemVersionsParameters,
)
from office365.sharepoint.listitems.versions.version import ListItemVersion


class ListItemVersionCollection(EntityCollection[ListItemVersion]):
    """Specifies a collection of versions of a list item."""

    def __init__(self, context, resource_path=None):
        super(ListItemVersionCollection, self).__init__(
            context, ListItemVersion, resource_path
        )

    def get_by_id(self, id_):
        """
        Gets an hosted app based on the Id.
        :param int id_:  A 32-bit integer that specifies the ID of the version to return.
        """
        return ListItemVersion(
            self.context, ServiceOperationPath("GetById", [id_], self.resource_path)
        )

    def get_versions(self, row_limit=None, sort_descending=None):
        """
        :param int row_limit: The number of return results
        :param bool sort_descending:
        """
        return_type = ListItemVersionCollection(self.context)
        payload = {
            "getVersionsParams": GetListItemVersionsParameters(
                row_limit, sort_descending
            )
        }
        qry = ServiceOperationQuery(
            self, "GetVersions", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type
