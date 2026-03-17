from typing import TYPE_CHECKING, Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class TenantAdminEndpoints(Entity):

    def __init__(self, context):
        # type: (ClientContext) -> None
        static_path = ResourcePath(
            "Microsoft.Online.SharePoint.TenantAdministration.TenantAdminEndpoints"
        )
        super(TenantAdminEndpoints, self).__init__(context, static_path)

    @property
    def aad_admin_center_endpoint(self):
        # type: () -> Optional[str]
        """The URL for the Azure Active Directory Admin Center."""
        return self.properties.get("AADAdminCenterEndpoint", None)

    @property
    def cdn_default_endpoint(self):
        # type: () -> Optional[str]
        """The base address of the default Content Delivery Network (CDN) used by SharePoint Online."""
        return self.properties.get("CdnDefaultEndpoint", None)

    @property
    def o365_admin_center_endpoint(self):
        # type: () -> Optional[str]
        """The endpoint (URL) to the Office 365 Admin Center."""
        return self.properties.get("O365AdminCenterEndpoint", None)

    @property
    def cfrms_graph_endpoint(self):
        # type: () -> Optional[str]
        """The URL of the Microsoft Graph API specifically for managing Conditional Access policies and Rights
        Management Services (RMS) settings within a tenant."""
        return self.properties.get("CFRMSGraphEndpoint", None)

    @property
    def migration_agent_url(self):
        # type: () -> Optional[str]
        """The endpoint where the migration agent software or services can be accessed"""
        return self.properties.get("MigrationAgentUrl", None)

    @property
    def mini_maven_endpoint(self):
        # type: () -> Optional[str]
        """URL used internally by SharePoint or Microsoft 365 services. It may be part of Microsoft's orchestration
        or lightweight service framework for tenant or feature management."""
        return self.properties.get("MiniMavenEndpoint", None)

    @property
    def sp_migration_tool_url(self):
        # type: () -> Optional[str]
        """URL to access the SharePoint Migration Tool, enabling users to download the software
        for performing migrations"""
        return self.properties.get("SPMigrationToolUrl", None)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.TenantAdminEndpoints"
