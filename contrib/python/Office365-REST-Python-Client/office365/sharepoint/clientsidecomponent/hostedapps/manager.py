from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.clientsidecomponent.hostedapps.app import HostedApp
from office365.sharepoint.entity import Entity


class HostedAppsManager(Entity):
    def get_by_id(self, id_):
        """
        Gets an hosted app based on the Id.

        :param str id_: The Id of the hosted app to get.
        """
        return HostedApp(
            self.context, ServiceOperationPath("GetById", [id_], self.resource_path)
        )

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.ClientSideComponent.HostedAppsManager"
