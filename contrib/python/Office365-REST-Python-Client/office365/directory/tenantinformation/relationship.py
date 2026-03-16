from office365.directory.tenantinformation.information import TenantInformation
from office365.directory.tenantinformation.multi_organization import (
    MultiTenantOrganization,
)
from office365.entity import Entity
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery


class TenantRelationship(Entity):
    """Represent the various type of tenant relationships."""

    def find_tenant_information_by_domain_name(self, domain_name):
        """Given a domain name, search for a tenant and read its tenantInformation. You can use this API to
        validate tenant information and use their tenantId to configure cross-tenant access settings between you
        and the tenant.

        :param str domain_name: Primary domain name of an Azure AD tenant.
        """
        return_type = ClientResult(self.context, TenantInformation())
        params = {"domainName": domain_name}
        qry = FunctionQuery(
            self, "findTenantInformationByDomainName", params, return_type
        )
        self.context.add_query(qry)
        return return_type

    def find_tenant_information_by_tenant_id(self, tenant_id):
        """Given a tenant ID, search for a tenant and read its tenantInformation. You can use this API to validate
        tenant information and use the tenantId to configure cross-tenant cross-tenant access settings between you
        and the tenant.

        :param str tenant_id: Unique tenant identifier of a Microsoft Entra tenant.
        """
        return_type = ClientResult(self.context, TenantInformation())
        params = {"tenantId": tenant_id}
        qry = FunctionQuery(
            self, "findTenantInformationByTenantId", params, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def multi_tenant_organization(self):
        """Represents a setting to control people-related admin settings in the tenant."""
        return self.properties.get(
            "multiTenantOrganization",
            MultiTenantOrganization(
                self.context,
                ResourcePath("multiTenantOrganization", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "multiTenantOrganization": self.multi_tenant_organization,
            }
            default_value = property_mapping.get(name, None)
        return super(TenantRelationship, self).get_property(name, default_value)
