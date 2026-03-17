from typing import Optional

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class CorporateCatalogAppMetadata(Entity):
    """App metadata for apps stored in the corporate catalog."""

    def __str__(self):
        return self.title or self.entity_type_name

    def deploy(self, skip_feature_deployment):
        """This method deploys an app on the app catalog.  It MUST be called in the context of the tenant app
        catalog web or it will fail.

        :param bool skip_feature_deployment: Specifies whether the app can be centrally deployed across the tenant.
        """
        payload = {"skipFeatureDeployment": skip_feature_deployment}
        qry = ServiceOperationQuery(self, "Deploy", None, payload)
        self.context.add_query(qry)
        return self

    def remove(self):
        """This is the inverse of the add step above. One removed from the app catalog,
        the solution can't be deployed."""
        qry = ServiceOperationQuery(self, "Remove")
        self.context.add_query(qry)
        return self

    def install(self):
        """This method allows an app which is already deployed to be installed on a web."""
        qry = ServiceOperationQuery(self, "Install")
        self.context.add_query(qry)
        return self

    def uninstall(self):
        """This method uninstalls an app from a web."""
        qry = ServiceOperationQuery(self, "Uninstall")
        self.context.add_query(qry)
        return self

    @property
    def aad_permissions(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("AadPermissions", None)

    @property
    def app_catalog_version(self):
        # type: () -> Optional[str]
        """The version of the app stored in the corporate catalog."""
        return self.properties.get("AppCatalogVersion", None)

    @property
    def can_upgrade(self):
        # type: () -> Optional[bool]
        """Whether an existing instance of an app can be upgraded."""
        return self.properties.get("CanUpgrade", None)

    @property
    def is_client_side_solution(self):
        # type: () -> Optional[bool]
        """Whether the app is a client-side solution."""
        return self.properties.get("IsClientSideSolution", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """The title of the app."""
        return self.properties.get("Title", None)

    @property
    def id(self):
        # type: () -> Optional[str]
        """The identifier of the app."""
        return self.properties.get("ID", None)

    @property
    def property_ref_name(self):
        # return "AadAppId"
        return "ID"

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Marketplace.CorporateCuratedGallery.CorporateCatalogAppMetadata"
