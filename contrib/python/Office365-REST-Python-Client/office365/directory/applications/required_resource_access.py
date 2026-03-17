from office365.directory.applications.resource_access import ResourceAccess
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class RequiredResourceAccess(ClientValue):
    """Specifies the set of OAuth 2.0 permission scopes and app roles under the specified resource that an
    application requires access to. The application may request the specified OAuth 2.0 permission scopes or app
    roles through the requiredResourceAccess property, which is a collection of requiredResourceAccess objects.
    """

    def __init__(self, resource_access=None, resource_app_id=None):
        """
        :param list[ResourceAccess] resource_access: The list of OAuth2.0 permission scopes and app roles that
             the application requires from the specified resource.
        :param str resource_app_id: The unique identifier for the resource that the application requires access to.
             This should be equal to the appId declared on the target resource application.
        """
        self.resourceAccess = ClientValueCollection(ResourceAccess, resource_access)
        self.resourceAppId = resource_app_id

    def __repr__(self):
        return self.resourceAppId or self.entity_type_name
