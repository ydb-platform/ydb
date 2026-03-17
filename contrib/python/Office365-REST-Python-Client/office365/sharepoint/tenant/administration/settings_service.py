from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.tenant.administration.default_time_zone_id import (
    TenantDefaultTimeZoneId,
)
from office365.sharepoint.tenant.administration.smtp_server import SmtpServer
from office365.sharepoint.tenant.administration.types import (
    AutoQuotaEnabled,
    DisableGroupify,
    DisableSelfServiceSiteCreation,
    EnableAutoNewsDigest,
)


class TenantAdminSettingsService(Entity):
    """Manage various tenant-level settings related to SharePoint administration"""

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.Online.SharePoint.TenantAdministration.TenantAdminSettingsService"
        )
        super(TenantAdminSettingsService, self).__init__(context, static_path)

    def get_tenant_sharing_status(self):
        """Retrieves the current state of sharing settings for a tenant"""
        return_type = ClientResult(self.context, int())
        qry = ServiceOperationQuery(
            self, "GetTenantSharingStatus", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def auto_quota_enabled(self):
        """Controls whether automatic quota management is enabled for SharePoint Online site collections.
        When this feature is enabled, SharePoint Online automatically adjusts the storage quota for site collections
        based on their usage, helping to ensure that site collections don't run out of space.
        """
        return self.properties.get("AutoQuotaEnabled", AutoQuotaEnabled())

    @property
    def available_managed_paths_for_site_creation(self):
        """
        Specifies the managed paths that are available for the creation of new SharePoint sites within a tenant.
        """
        return self.properties.get(
            "AvailableManagedPathsForSiteCreation", StringCollection()
        )

    @property
    def disable_groupify(self):
        """
        Controls whether SharePoint site owners in your tenant are allowed to convert classic SharePoint sites into
        Microsoft 365 Groups (a process commonly known as "groupifying" a site)
        """
        return self.properties.get("DisableGroupify", DisableGroupify())

    @property
    def disable_self_service_site_creation(self):
        """ " """
        return self.properties.get(
            "DisableSelfServiceSiteCreation", DisableSelfServiceSiteCreation()
        )

    @property
    def enable_auto_news_digest(self):
        """ " """
        return self.properties.get("EnableAutoNewsDigest", EnableAutoNewsDigest())

    @property
    def smtp_server(self):
        """Specifies the server address or endpoint of the SMTP server that SharePoint Online or tenant-related
        services use for sending emails"""
        return self.properties.get("SmtpServer", SmtpServer())

    @property
    def tenant_default_time_zone_id(self):
        """Default time zone configured for a SharePoint Online tenant"""
        return self.properties.get("TenantDefaultTimeZoneId", TenantDefaultTimeZoneId())

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.TenantAdminSettingsService"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "AvailableManagedPathsForSiteCreation": self.available_managed_paths_for_site_creation,
                "AutoQuotaEnabled": self.auto_quota_enabled,
                "DisableGroupify": self.disable_groupify,
                "DisableSelfServiceSiteCreation": self.disable_self_service_site_creation,
                "EnableAutoNewsDigest": self.enable_auto_news_digest,
                "SmtpServer": self.smtp_server,
                "TenantDefaultTimeZoneId": self.tenant_default_time_zone_id,
            }
            default_value = property_mapping.get(name, None)
        return super(TenantAdminSettingsService, self).get_property(name, default_value)
