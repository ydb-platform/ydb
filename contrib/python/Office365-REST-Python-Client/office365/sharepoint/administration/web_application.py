from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.sites.site import Site


class WebApplication(Entity):
    @staticmethod
    def lookup(context, request_uri):
        # type:  (ClientContext, str) -> WebApplication
        """ """
        return_type = WebApplication(context)
        payload = {"requestUri": request_uri}
        qry = ServiceOperationQuery(
            return_type, "Lookup", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @property
    def outbound_mail_sender_address(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("OutboundMailSenderAddress", None)

    @property
    def sites(self):
        # type: () -> EntityCollection[Site]
        return self.properties.get(
            "Sites",
            EntityCollection(
                self.context, Site, ResourcePath("Sites", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Administration.SPWebApplication"
