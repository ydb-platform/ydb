from office365.runtime.client_value import ClientValue
from office365.sharepoint.social.posts.post import SocialPost


class SocialPostReference(ClientValue):
    """The SocialPostReference class specifies a reference to a post in another thread.  The referenced post can be a
    post with a tag, a post that is liked, a post that mentions a user, or a post that is a reply. Threads that contain
    a SocialPostReference in the PostReference property (see section 3.1.5.42.1.1.6) are threads with root posts that
    are generated on the server and not created by a client."""

    def __init__(
        self, digest=None, post=SocialPost(), thread_id=None, thread_owner_index=None
    ):
        """
        :param SocialThread digest: The Digest property provides a digest of the thread containing the referenced post.
        :param SocialPost post: The Post property provides access to the post being referenced
        :param str thread_id: The ThreadId property specifies the unique identifier of the thread containing the
            referenced post.
        :param int thread_owner_index: The ThreadOwnerIndex property specifies the current owner of the thread as an
            index into the SocialThread Actors array
        """
        self.Digest = digest
        self.Post = post
        self.ThreadId = thread_id
        self.ThreadOwnerIndex = thread_owner_index

    @property
    def entity_type_name(self):
        return "SP.Social.SocialPostReference"
