from typing import Optional

from office365.directory.protection.riskyusers.activity import RiskUserActivity
from office365.directory.protection.riskyusers.risky_user import RiskyUser


class RiskyUserHistoryItem(RiskyUser):
    """Represents the risk history of an Azure Active Directory (Azure AD) user as determined
    by Azure AD Identity Protection."""

    @property
    def activity(self):
        """The activity related to user risk level change."""
        return self.properties.get("activity", RiskUserActivity())

    @property
    def initiated_by(self):
        # type: () -> Optional[str]
        """The ID of actor that does the operation."""
        return self.properties.get("initiatedBy", None)
