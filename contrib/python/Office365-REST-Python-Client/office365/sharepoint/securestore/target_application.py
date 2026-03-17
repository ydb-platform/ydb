from typing import Optional

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.entity import Entity


class TargetApplication(Entity):
    """A logical entity that represents a software system for which credentials are maintained.
    It consists of metadata including the number and type of credentials that are required by the software system and
    a set of claims (2) that identify the administrators who can update, read, and delete the entity.
    """

    @staticmethod
    def create(context, application_id, friendly_name):
        # type: (ClientContext, str, str) -> "TargetApplication"
        """
        Creates a target application

        :type context: office365.sharepoint.client_context.ClientContext
        :param str application_id:
        :param str friendly_name:
        """
        return_type = TargetApplication(context)
        payload = {"applicationId": application_id, "friendlyName": friendly_name}
        qry = ServiceOperationQuery(
            return_type, "", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @property
    def application_id(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("ApplicationId", None)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.SecureStoreService.Server.TargetApplication"
