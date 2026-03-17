from office365.directory.domains.dns_record import DomainDnsRecord
from office365.directory.domains.state import DomainState
from office365.directory.object_collection import DirectoryObjectCollection
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection


class Domain(Entity):
    """
    Represents a domain associated with the tenant.

    Use domain operations to associate domains to a tenant, verify domain ownership, and configure supported services.
    Domain operations enable registrars to automate domain association for services such as Microsoft 365.
    For example, as part of domain sign up, a registrar can enable a vanity domain for email, websites,
    authentication, etc.
    """

    def verify(self):
        """Validates the ownership of the domain."""
        return_type = Domain(self.context)
        self.parent_collection.add_child(return_type)
        qry = ServiceOperationQuery(self, "verify", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def supported_services(self):
        """
        The capabilities assigned to the domain. Can include 0, 1 or more of following values:
        Email, Sharepoint, EmailInternalRelayOnly, OfficeCommunicationsOnline, SharePointDefaultDomain,
        FullRedelegation, SharePointPublic, OrgIdAuthentication, Yammer, Intune.
        The values which you can add/remove using Graph API include: Email, OfficeCommunicationsOnline, Yammer.
        """
        return self.properties.get("supportedServices", StringCollection())

    @property
    def domain_name_references(self):
        """
        The objects such as users and groups that reference the domain ID. Read-only, Nullable.
        Supports $expand and $filter by the OData type of objects returned.
        For example /domains/{domainId}/domainNameReferences/microsoft.graph.user
        and /domains/{domainId}/domainNameReferences/microsoft.graph.group.
        """
        return self.properties.get(
            "domainNameReferences",
            DirectoryObjectCollection(
                self.context, ResourcePath("domainNameReferences", self.resource_path)
            ),
        )

    @property
    def service_configuration_records(self):
        """
        DNS records the customer adds to the DNS zone file of the domain before the domain can be used by
        Microsoft Online services. Read-only, Nullable. Supports $expand.
        """
        return self.properties.get(
            "serviceConfigurationRecords",
            EntityCollection(
                self.context,
                DomainDnsRecord,
                ResourcePath("serviceConfigurationRecords", self.resource_path),
            ),
        )

    @property
    def verification_dns_records(self):
        """
        DNS records that the customer adds to the DNS zone file of the domain before the customer can complete
        domain ownership verification with Azure AD. Read-only, Nullable. Supports $expand.
        """
        return self.properties.get(
            "verificationDnsRecords",
            EntityCollection(
                self.context,
                DomainDnsRecord,
                ResourcePath("verificationDnsRecords", self.resource_path),
            ),
        )

    @property
    def state(self):
        """Status of asynchronous operations scheduled for the domain."""
        return self.properties.get("state", DomainState())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "domainNameReferences": self.domain_name_references,
                "serviceConfigurationRecords": self.service_configuration_records,
                "verificationDnsRecords": self.verification_dns_records,
            }
            default_value = property_mapping.get(name, None)
        return super(Domain, self).get_property(name, default_value)
