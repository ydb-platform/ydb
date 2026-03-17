from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class ClientWebPart(Entity):
    """Representation of a ClientWebPart. It provides with ClientWebPart metadata and methods to render it."""

    def render(self, properties=None):
        """
        Renders the ClientWebPart.  Returns HTML that can be inserted in a page.

        :param dict properties: Properties for the ClientWebPart, including edit mode.
        """
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(self, "Render", None, properties, None, return_type)
        self.context.add_query(qry)
        return return_type
