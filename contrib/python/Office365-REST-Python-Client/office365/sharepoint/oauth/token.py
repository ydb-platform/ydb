from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.oauth.token_response import TokenResponse


class Token(Entity):
    """"""

    def __init__(self, context):
        super(Token, self).__init__(context, ResourcePath("SP.OAuth.Token"))

    def acquire(self):
        """ """
        return_type = TokenResponse(self.context)
        qry = ServiceOperationQuery(self, "Acquire", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.OAuth.Token"
