from datetime import datetime
from typing import Optional

from office365.directory.security.alerts.alert import Alert
from office365.directory.security.alerts.comment import AlertComment
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath


class Incident(Entity):
    """
    An incident in Microsoft 365 Defender is a collection of correlated alert instances and associated metadata
    that reflects the story of an attack in a tenant.

    Microsoft 365 services and apps create alerts when they detect a suspicious or malicious event or activity.
    Individual alerts provide valuable clues about a completed or ongoing attack. However, attacks typically employ
    various techniques against different types of entities, such as devices, users, and mailboxes. The result is
    multiple alerts for multiple entities in your tenant. Because piecing the individual alerts together to gain
    insight into an attack can be challenging and time-consuming, Microsoft 365 Defender automatically aggregates the
    alerts and their associated information into an incident.
    """

    @property
    def assigned_to(self):
        # type: () -> Optional[str]
        """Owner of the incident, or null if no owner is assigned. Free editable text."""
        return self.properties.get("assignedTo", None)

    @property
    def classification(self):
        # type: () -> Optional[str]
        """The specification for the incident.
        Possible values are: unknown, falsePositive, truePositive, informationalExpectedActivity, unknownFutureValue.
        """
        return self.properties.get("classification", None)

    @property
    def comments(self):
        """Array of comments created by the Security Operations (SecOps) team when the incident is managed."""
        return self.properties.get("comments", ClientValueCollection(AlertComment))

    @property
    def created_datetime(self):
        """Time when the incident was first created."""
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def alerts(self):
        # type: () -> EntityCollection[Alert]
        """The list of related alerts. Supports $expand."""
        return self.properties.get(
            "alerts",
            EntityCollection(
                self.context, Alert, ResourcePath("alerts", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        # type: () -> str
        return "microsoft.graph.security.incident"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"createdDateTime": self.created_datetime}
            default_value = property_mapping.get(name, None)
        return super(Incident, self).get_property(name, default_value)
