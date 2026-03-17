from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.tenant.administration.internal.aad.permission_grant import (
    SPO3rdPartyAADPermissionGrantCollection,
)
from office365.sharepoint.tenant.administration.internal.appservice.permission_grant import (
    SPOWebAppServicePrincipalPermissionGrant,
)
from office365.sharepoint.tenant.administration.internal.appservice.permission_request import (
    SPOWebAppServicePrincipalPermissionRequest,
)


class SPOWebAppServicePrincipal(Entity):
    def __init__(self, context):
        stat_path = ResourcePath(
            "Microsoft.Online.SharePoint.TenantAdministration.Internal.SPOWebAppServicePrincipal"
        )
        super(SPOWebAppServicePrincipal, self).__init__(context, stat_path)

    def update_spfx_client_secret(self, secret_value):
        """
        :param str secret_value:
        """
        payload = {"secretValue": secret_value}
        qry = ServiceOperationQuery(self, "UpdateSpfxClientSecret", None, payload)
        self.context.add_query(qry)
        return self

    def update_spfx_third_party_app_id(self, app_id):
        payload = {"appId": app_id}
        qry = ServiceOperationQuery(self, "UpdateSpfxThirdPartyAppId", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def account_enabled(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("AccountEnabled", False)

    @property
    def app_id(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("AppId", False)

    @property
    def reply_urls(self):
        # type: () -> StringCollection
        """ """
        return self.properties.get("ReplyUrls", StringCollection())

    @property
    def grant_manager(self):
        return self.properties.get(
            "GrantManager",
            SPO3rdPartyAADPermissionGrantCollection(
                self.context,
                ResourcePath("GrantManager", self.resource_path),
            ),
        )

    @property
    def permission_grants(self):
        return self.properties.get(
            "PermissionGrants",
            EntityCollection(
                self.context,
                SPOWebAppServicePrincipalPermissionGrant,
                ResourcePath("PermissionGrants", self.resource_path),
            ),
        )

    @property
    def permission_requests(self):
        return self.properties.get(
            "PermissionRequests",
            EntityCollection(
                self.context,
                SPOWebAppServicePrincipalPermissionRequest,
                ResourcePath("PermissionRequests", self.resource_path),
            ),
        )

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.Internal.SPOWebAppServicePrincipal"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "GrantManager": self.grant_manager,
                "PermissionRequests": self.permission_requests,
                "PermissionGrants": self.permission_grants,
                "ReplyUrls": self.reply_urls,
            }
            default_value = property_mapping.get(name, None)
        return super(SPOWebAppServicePrincipal, self).get_property(name, default_value)
