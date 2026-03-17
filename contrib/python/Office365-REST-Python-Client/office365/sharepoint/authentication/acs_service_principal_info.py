from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class SPACSServicePrincipalInfo(ClientValue):
    def __init__(self, application_endpoint_authorities=None, display_name=None):
        self.ApplicationEndpointAuthorities = StringCollection(
            application_endpoint_authorities
        )
        self.DisplayName = display_name

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Authentication.SPACSServicePrincipalInfo"
