from office365.directory.identitygovernance.appconsent.request_collection import (
    AppConsentRequestCollection,
)
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class AppConsentApprovalRoute(Entity):
    """
    Container for base resources that expose the app consent request API and features.
    Currently exposes only the appConsentRequests relationship.
    """

    @property
    def app_consent_requests(self):
        """A collection of appConsentRequest objects representing apps for which admin consent has been requested by
        one or more users."""
        return self.properties.get(
            "appConsentRequests",
            AppConsentRequestCollection(
                self.context, ResourcePath("appConsentRequests", self.resource_path)
            ),
        )
