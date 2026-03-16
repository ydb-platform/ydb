from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity


class AppBdcCatalog(Entity):
    """
    Represents the Business Data Connectivity (BDC) MetadataCatalog for an application that contains external content
    types provisioned by the application.
    """

    def get_permissible_connections(self):
        """
        Gets the list of external connections that the application has permissions to use.
        """
        return_type = ClientResult(self.context, StringCollection())
        qry = ServiceOperationQuery(
            self, "GetPermissibleConnections", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.BusinessData.AppBdcCatalog"
