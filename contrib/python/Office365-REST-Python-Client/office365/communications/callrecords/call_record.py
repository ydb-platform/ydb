from office365.communications.callrecords.session import Session
from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class CallRecord(Entity):
    """Represents a single peer-to-peer call or a group call between multiple participants,
    sometimes referred to as an online meeting."""

    @property
    def join_web_url(self):
        """Meeting URL associated to the call. May not be available for a peerToPeer call record type."""
        return self.properties.get("joinWebUrl", None)

    @property
    def organizer(self):
        """The organizing party's identity.."""
        return self.properties.get("organizer", IdentitySet())

    @property
    def sessions(self):
        """
        List of sessions involved in the call. Peer-to-peer calls typically only have one session, whereas group
        calls typically have at least one session per participant.
        """
        return self.properties.get(
            "sessions",
            EntityCollection(
                self.context, Session, ResourcePath("sessions", self.resource_path)
            ),
        )
