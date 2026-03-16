from office365.delta_collection import DeltaCollection
from office365.directory.applications.application import Application
from office365.directory.serviceprincipals.service_principal import ServicePrincipal
from office365.runtime.paths.appid import AppIdPath


class ServicePrincipalCollection(DeltaCollection[ServicePrincipal]):
    """Service Principal's collection"""

    def __init__(self, context, resource_path=None):
        super(ServicePrincipalCollection, self).__init__(
            context, ServicePrincipal, resource_path
        )

    def add(self, app_id):
        """
        Create a new servicePrincipal object.
        :param str app_id: The unique identifier for the associated application
        """
        return super(ServicePrincipalCollection, self).add(appId=app_id)

    def get_by_app_id(self, app_id):
        """Retrieves the service principal using appId.
        :param str app_id: appId is referred to as Application (Client) ID, respectively, in the Azure portal
        """
        return ServicePrincipal(self.context, AppIdPath(app_id, self.resource_path))

    def get_by_app(self, app):
        # type: (str|Application) -> ServicePrincipal
        if isinstance(app, Application):
            return_type = ServicePrincipal(self.context, parent_collection=self)

            def _app_resolved():
                return_type.set_property("appId", app.app_id)

            app.ensure_properties(["appId"], _app_resolved)
            return return_type
        else:
            return self.get_by_app_id(app)

    def get_by_name(self, name):
        # type: (str) -> ServicePrincipal
        """Retrieves the service principal using displayName."""
        return self.single("displayName eq '{0}'".format(name))
