from office365.directory.protection.risk_detection import RiskDetection
from office365.directory.protection.riskyusers.collection import RiskyUserCollection
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class IdentityProtectionRoot(Entity):
    """Container for the navigation properties for Microsoft Graph identity protection resources."""

    @property
    def risk_detections(self):
        """Risk detection in Azure AD Identity Protection and the associated information about the detection."""
        return self.properties.get(
            "riskDetections",
            EntityCollection(
                self.context,
                RiskDetection,
                ResourcePath("riskDetections", self.resource_path),
            ),
        )

    @property
    def risky_users(self):
        """Get the teams in Microsoft Teams that the user is a direct member of."""
        return self.properties.get(
            "riskyUsers",
            RiskyUserCollection(
                self.context, ResourcePath("riskyUsers", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "riskDetections": self.risk_detections,
                "riskyUsers": self.risky_users,
            }
            default_value = property_mapping.get(name, None)
        return super(IdentityProtectionRoot, self).get_property(name, default_value)
