from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.social.attachment import SocialAttachment
from office365.sharepoint.social.data_overlay import SocialDataOverlay
from office365.sharepoint.social.link import SocialLink
from office365.sharepoint.social.posts.actor_info import SocialPostActorInfo


class SocialPost(ClientValue):
    """The SocialPost specifies a post read from the server."""

    def __init__(
        self,
        attachment=SocialAttachment(),
        overlays=None,
        source=SocialLink(),
        liker_info=SocialPostActorInfo(),
    ):
        """
        :param SocialAttachment attachment: The Attachment property specifies an image, document preview,
            or video preview attachment.
        :param list[SocialDataOverlay] overlays: The Overlays property is an array of objects in a post, where each
            object represents a user, document, site, tag, or link.
        :param SocialLink source: The Source property specifies the link to a web site (1) associated with the
            application that created the post.
        :param SocialPostActorInfo liker_info: The LikerInfo property specifies information about users who like the
            post.
        """
        self.Attachment = attachment
        self.Overlays = ClientValueCollection(SocialDataOverlay, overlays)
        self.Source = source
        self.LikerInfo = liker_info
