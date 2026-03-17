from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.lists.version_policy_manager import VersionPolicyManager


class SiteVersionPolicyManager(Entity):
    """"""

    @property
    def major_version_limit(self):
        # type: () -> Optional[int]
        """ """
        return self.properties.get("MajorVersionLimit", None)

    @property
    def version_policies(self):
        return self.properties.get(
            "VersionPolicies",
            VersionPolicyManager(
                self.context, ResourcePath("VersionPolicies", self.resource_path)
            ),
        )

    def inherit_tenant_settings(self):
        """ """
        qry = ServiceOperationQuery(self, "InheritTenantSettings")
        self.context.add_query(qry)
        return self

    def set_auto_expiration(self):
        """"""
        qry = ServiceOperationQuery(self, "SetAutoExpiration")
        self.context.add_query(qry)
        return self

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "VersionPolicies": self.version_policies,
            }
            default_value = property_mapping.get(name, None)
        return super(SiteVersionPolicyManager, self).get_property(name, default_value)
