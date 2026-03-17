from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.social.actor import SocialActor
from office365.sharepoint.social.posts.post import SocialPost
from office365.sharepoint.social.posts.reference import SocialPostReference


class SocialThread(ClientValue):
    """The SocialThread property provides the object that contains the thread.
    For details on the SocialThread type, see section 3.1.5.42."""

    def __init__(
        self,
        thread_id=None,
        actors=None,
        replies=None,
        root_post=SocialPost(),
        post_reference=SocialPostReference(),
    ):
        """
        :param str thread_id: The Id property specifies the unique identification of the thread.
        :param list[SocialActor] actors: The Actors property is an array that specifies the users who have created
            a post in the returned thread and also contains any users, documents, sites, and tags that are referenced
            in any of the posts in the returned thread.
        :param list[SocialPost] replies: The Replies property returns an array of zero or more reply posts.
            The server can return a subset of the reply posts that are stored on the server.
        :param SocialPost root_post: The RootPost property returns the root post.
        :param SocialPostReference post_reference:
        """
        self.Id = thread_id
        self.Actors = ClientValueCollection(SocialActor, actors)
        self.RootPost = root_post
        self.Replies = ClientValueCollection(SocialPost, replies)
        self.PostReference = post_reference

    @property
    def entity_type_name(self):
        return "SP.Social.SocialThread"
