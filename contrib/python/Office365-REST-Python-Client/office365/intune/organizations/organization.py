from office365.directory.certificates.auth_configuration import (
    CertificateBasedAuthConfiguration,
)
from office365.directory.domains.verified import VerifiedDomain
from office365.directory.extensions.extension import Extension
from office365.directory.licenses.assigned_plan import AssignedPlan
from office365.directory.object import DirectoryObject
from office365.entity_collection import EntityCollection
from office365.intune.organizations.branding import OrganizationalBranding
from office365.intune.provisioned_plan import ProvisionedPlan
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.types.collections import StringCollection


class Organization(DirectoryObject):
    """
    The organization resource represents an instance of global settings and resources
    which operate and are provisioned at the tenant-level.
    """

    @property
    def assigned_plans(self):
        """The plans that are assigned to the organization."""
        return self.properties.get("assignedPlans", ClientValueCollection(AssignedPlan))

    @property
    def branding(self):
        return self.properties.get(
            "branding",
            OrganizationalBranding(
                self.context, ResourcePath("branding", self.resource_path)
            ),
        )

    @property
    def business_phones(self):
        """
        Telephone number for the organization. Although this is a string collection,
        only one number can be set for this property.
        """
        return self.properties.get("businessPhones", StringCollection())

    @property
    def extensions(self):
        # type: () -> EntityCollection[Extension]
        """The collection of open extensions defined for the message. Nullable."""
        return self.properties.get(
            "extensions",
            EntityCollection(
                self.context, Extension, ResourcePath("extensions", self.resource_path)
            ),
        )

    @property
    def certificate_based_auth_configuration(self):
        """Navigation property to manage certificate-based authentication configuration.
        Only a single instance of certificateBasedAuthConfiguration can be created in the collection..
        """
        return self.properties.get(
            "certificateBasedAuthConfiguration",
            EntityCollection(
                self.context,
                CertificateBasedAuthConfiguration,
                ResourcePath("certificateBasedAuthConfiguration", self.resource_path),
            ),
        )

    @property
    def provisioned_plans(self):
        return self.properties.get(
            "provisionedPlans", ClientValueCollection(ProvisionedPlan)
        )

    @property
    def verified_domains(self):
        """The collection of domains associated with this tenant."""
        return self.properties.get(
            "verifiedDomains", ClientValueCollection(VerifiedDomain)
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "assignedPlans": self.assigned_plans,
                "certificateBasedAuthConfiguration": self.certificate_based_auth_configuration,
                "businessPhones": self.business_phones,
                "provisionedPlans": self.provisioned_plans,
                "verifiedDomains": self.verified_domains,
            }
            default_value = property_mapping.get(name, None)
        return super(Organization, self).get_property(name, default_value)
