from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.entity import Entity


class WopiHostUtility(Entity):
    """ """

    @staticmethod
    def generate_file_bundle(context):
        # type: (ClientContext) -> ClientResult[str]
        """ """
        return_type = ClientResult(context)
        qry = ServiceOperationQuery(
            WopiHostUtility(context),
            "GenerateFileBundle",
            None,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_wopi_origins(context):
        return_type = ClientResult(context, ClientValueCollection(str))
        qry = ServiceOperationQuery(
            WopiHostUtility(context),
            "GetWopiOrigins",
            None,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Utilities.WopiHostUtility"
