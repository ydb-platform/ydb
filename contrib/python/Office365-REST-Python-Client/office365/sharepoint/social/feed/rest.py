from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.social.feed.feed import SocialFeed


class SocialRestFeed(Entity):
    """
    The SocialRestFeed class specifies a feed, which is an array of thread, each of which specifies a root post
    and an array of response posts. The SocialRestFeed type is available when the protocol client sends an OData
    request to a protocol server using [MS-CSOMREST]. It is not available using [MS-CSOM].
    """

    def __init__(self, context):
        super(SocialRestFeed, self).__init__(
            context, ResourcePath("SP.Social.SocialRestFeed")
        )

    @property
    def social_feed(self):
        return self.properties.get("SocialFeed", SocialFeed())
