from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.social.rest_actor import SocialRestActor


class SocialRestFeedManager(Entity):
    """he SocialRestFeedManager class provides REST methods for creating posts, modifying threads,
    and consuming feeds on behalf of the current user. The SocialRestFeedManager class is available
    when the protocol client sends an OData request to a protocol server using [MS-CSOMREST].
    It is not available using [MS-CSOM]."""

    def __init__(self, content):
        super(SocialRestFeedManager, self).__init__(
            content, ResourcePath("SP.Social.SocialRestFeedManager")
        )

    def my(self):
        """The My method gets a SocialRestActor object that represents the current user. See section 3.1.5.35 for
        details on the SocialRestActor type."""
        return SocialRestActor(self.context, ResourcePath("My", self.resource_path))
