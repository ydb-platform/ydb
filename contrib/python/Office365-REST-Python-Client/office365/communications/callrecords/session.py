from office365.communications.callrecords.segment import Segment
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class Session(Entity):
    """Represents a user-user communication or a user-meeting communication in the case of a conference call."""

    @property
    def segments(self):
        """
        The list of segments involved in the session.
        """
        return self.properties.get(
            "segments",
            EntityCollection(
                self.context, Segment, ResourcePath("segments", self.resource_path)
            ),
        )
