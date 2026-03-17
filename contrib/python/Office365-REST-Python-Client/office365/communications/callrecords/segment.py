from office365.communications.callrecords.endpoint import Endpoint
from office365.entity import Entity


class Segment(Entity):
    """Represents a portion of a User-User communication or a User-Meeting communication in the case of a
    Conference call. A typical VOIP call will have one segment per session. In certain scenarios, such as PSTN calls,
    there will be multiple segments per session due to additional server-to-server communication required to connect
    the call."""

    @property
    def callee(self):
        """Endpoint that answered this segment."""
        return self.properties.get("callee", Endpoint())

    @property
    def caller(self):
        """Endpoint that initiated this segment."""
        return self.properties.get("caller", Endpoint())

    @property
    def entity_type_name(self):
        return "microsoft.graph.callRecords.segment"
