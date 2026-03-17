from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class NativeClient(Entity):
    """Gets endpoints for native client authentication relative to current request."""

    def __init__(self, context):
        super(NativeClient, self).__init__(
            context, ResourcePath("SP.OAuth.NativeClient")
        )

    def authenticate(self):
        """Authentication module to handle MicrosoftOnlineCredentials."""
        qry = ServiceOperationQuery(self, "Authenticate")
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "SP.OAuth.NativeClient"
