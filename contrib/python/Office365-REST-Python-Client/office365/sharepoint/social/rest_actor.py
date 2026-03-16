from office365.sharepoint.entity import Entity
from office365.sharepoint.social.actor import SocialActor


class SocialRestActor(Entity):
    """The SocialRestActor type contains information about an actor retrieved from server. An actor is a user, document,
    site, or tag. The SocialRestActor type is available when the protocol client sends an OData request to a protocol
    server using [MS-CSOMREST]. It is not available using [MS-CSOM]."""

    @property
    def me(self):
        """The Me property provides access to the current user.
        See section 3.1.5.3 for details on the SocialActor type"""
        return self.properties.get("Me", SocialActor())

    @property
    def entity_type_name(self):
        return "SP.Social.SocialRestActor"
