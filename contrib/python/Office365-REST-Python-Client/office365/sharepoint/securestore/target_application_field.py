from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class TargetApplicationField(Entity):
    """A name of a credential field and its associated credential type."""

    @staticmethod
    def create(context, name, masked, credential_type):
        """
        Creates a credential field

        :type context: office365.sharepoint.client_context.ClientContext
        :param str name:
        :param bool masked:
        :param int credential_type:
        """
        return_type = TargetApplicationField(context)
        payload = {"name": name, "masked": masked, "credentialType": credential_type}
        qry = ServiceOperationQuery(
            return_type, "", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.Office.SecureStoreService.Server.TargetApplicationField"
