from office365.directory.identitygovernance.accessreview.set import AccessReviewSet
from office365.directory.identitygovernance.appconsent.approval_route import (
    AppConsentApprovalRoute,
)
from office365.directory.identitygovernance.entitlementmanagement.entitlement_management import (
    EntitlementManagement,
)
from office365.directory.identitygovernance.privilegedaccess.root import (
    PrivilegedAccessRoot,
)
from office365.directory.identitygovernance.termsofuse.container import (
    TermsOfUseContainer,
)
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class IdentityGovernance(Entity):
    """
    The identity governance singleton is the container for the following Azure Active Directory identity governance
    features that are exposed through the following resources and APIs:

       - Access reviews
       - Entitlement management
       - App consent
       - Terms of use
    """

    @property
    def app_consent(self):
        """
        Container for base resources that expose the app consent request API and features.
        Currently, exposes only the appConsentRequests resource.
        """
        return self.properties.get(
            "appConsent",
            AppConsentApprovalRoute(
                self.context, ResourcePath("appConsent", self.resource_path)
            ),
        )

    @property
    def access_reviews(self):
        """Container for the base resources that expose the access reviews API and features."""
        return self.properties.get(
            "accessReviews",
            AccessReviewSet(
                self.context, ResourcePath("accessReviews", self.resource_path)
            ),
        )

    @property
    def privileged_access(self):
        """Container for the base resources that expose the access reviews API and features."""
        return self.properties.get(
            "privilegedAccess",
            PrivilegedAccessRoot(
                self.context, ResourcePath("privilegedAccess", self.resource_path)
            ),
        )

    @property
    def terms_of_use(self):
        """
        Container for the resources that expose the terms of use API and its features, including agreements
        and agreementAcceptances.
        """
        return self.properties.get(
            "termsOfUse",
            TermsOfUseContainer(
                self.context, ResourcePath("termsOfUse", self.resource_path)
            ),
        )

    @property
    def entitlement_management(self):
        """
        Container for entitlement management resources, including accessPackageCatalog, connectedOrganization,
        and entitlementManagementSettings.
        """
        return self.properties.get(
            "entitlementManagement",
            EntitlementManagement(
                self.context, ResourcePath("entitlementManagement", self.resource_path)
            ),
        )
