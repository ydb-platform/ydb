from typing import Optional

from office365.directory.security.alerts.evidence import AlertEvidence
from office365.directory.security.alerts.history_state import AlertHistoryState
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection


class Alert(Entity):
    """This resource corresponds to the latest generation of alerts in the Microsoft Graph security API,
    representing potential security issues within a customer's tenant that Microsoft 365 Defender,
    or a security provider integrated with Microsoft 365 Defender, has identified."""

    @property
    def actor_display_name(self):
        # type: () -> Optional[str]
        """The adversary or activity group that is associated with this alert."""
        return self.properties.get("actorDisplayName", None)

    @property
    def alert_policy_id(self):
        # type: () -> Optional[str]
        return self.properties.get("alertPolicyId", None)

    @property
    def evidence(self):
        """Collection of evidence related to the alert."""
        return self.properties.get("evidence", ClientValueCollection(AlertEvidence))

    @property
    def history_states(self):
        """Collection of changes for the alert."""
        return self.properties.get(
            "historyStates", ClientValueCollection(AlertHistoryState)
        )
