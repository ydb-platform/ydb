from office365.runtime.client_object import ClientObject
from office365.runtime.queries.service_operation import ServiceOperationQuery


class RequestContext(ClientObject):
    """
    Provides basic WSS context information: site, web, list, and list item.

    Use this class to return context information about such objects as the current Web application, site collection,
        site, list, or list item.
    """

    def get_remote_context(self):
        """
        Returns the SP.RequestContext for the mounted folder.
        Returns null if this is not an attempt to render or act upon a mounted folder.
        """
        return_type = RequestContext(self.context)
        qry = ServiceOperationQuery(
            self, "GetRemoteContext", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
