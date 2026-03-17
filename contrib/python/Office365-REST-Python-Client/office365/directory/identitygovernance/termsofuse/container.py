from office365.directory.identitygovernance.termsofuse.agreement import Agreement
from office365.directory.identitygovernance.termsofuse.agreement_acceptance import (
    AgreementAcceptance,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class TermsOfUseContainer(Entity):
    """Container for the relationships that expose the terms of use API and its features.
    Currently exposes the agreements and agreementAcceptances relationships."""

    @property
    def agreements(self):
        """Represents a tenant's customizable terms of use agreement that's created and managed with
        Azure Active Directory (Azure AD)."""
        return self.properties.get(
            "agreements",
            EntityCollection(
                self.context, Agreement, ResourcePath("agreements", self.resource_path)
            ),
        )

    @property
    def agreement_acceptances(self):
        """
        Represents the current status of a user's response to a company's customizable terms of use agreement.
        """
        return self.properties.get(
            "agreementAcceptances",
            EntityCollection(
                self.context,
                AgreementAcceptance,
                ResourcePath("agreementAcceptances", self.resource_path),
            ),
        )
